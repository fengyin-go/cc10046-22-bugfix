"""结果存储领域服务：落库批次按配置编号与实际采纳版本可核对。

被调度退回的采集申请不会调用这里，因此不会留下已落库批次。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService


class ResultStoreService(BaseModuleService):
    MODULE = "result_store"
    ENTITY = "存储批次"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["批次标识", "来源配置", "采集时间", "原始行数", "有效行数", "存储路径", "压缩格式", "校验摘要"]
    REQUIRED_FIELDS = ["批次标识", "来源配置", "采集时间"]
    STATUS_ORDER = ["存储中", "已落库", "校验未过", "已归档"]
    ACTION_RULES = {"执行校验": "已落库", "发起归档": "已落库"}
    NEGATIVE_ACTIONS = []

    # 配置编号和实际采纳版本是核对批次真实归属的附加条件。
    EXTRA_FILTER_FIELDS = ["配置编号", "采用版本", "来源凭据", "调度任务"]
    FILTER_ALIASES = {
        "config_id": "配置编号",
        "configuration_id": "配置编号",
        "config_version": "采用版本",
        "adopted_version": "采用版本",
        "version": "采用版本",
        "credential_id": "来源凭据",
        "task_id": "调度任务",
    }

    def domain_store_result(
        self,
        batch_id: str,
        source_config: str,
        row_count: int,
        valid_count: int,
        *,
        config_id: int | None = None,
        config_version: str | None = None,
        credential_id: str | None = None,
        task_id: str | None = None,
    ) -> dict[str, Any]:
        """结构化存储采集结果，并记录来源配置编号与实际采纳版本。"""
        from app.store import store

        rows = store.rows(self.MODULE)
        store_path = f"/data/results/{batch_id}.json.gz"
        entry = {
            "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
            "批次标识": batch_id,
            "来源配置": source_config,
            "采集时间": time.strftime("%Y-%m-%d"),
            "原始行数": str(row_count),
            "有效行数": str(valid_count),
            "存储路径": store_path,
            "压缩格式": "gzip",
            "校验摘要": f"md5:{batch_id[:8]}",
            "status": "已落库",
            "pending": False,
            "abnormal": False,
            # 核对维度：配置编号 + 实际采纳的版本，而不是只看同名配置。
            "来源配置编号": config_id,
            "配置编号": config_id,
            "config_id": config_id,
            "配置版本": config_version,
            "采用版本": config_version,
            "config_version": config_version,
            "来源凭据": credential_id,
            "凭证标识": credential_id,
            "credential_id": credential_id,
            "调度任务": task_id,
            "task_id": task_id,
        }
        rows.append(entry)
        return entry

    def find_stored_batches(
        self,
        *,
        config_id: int | None = None,
        config_version: str | None = None,
        batch_id: str | None = None,
        task_id: str | None = None,
        credential_id: str | None = None,
        credentialed_only: bool = False,
    ) -> list[dict[str, Any]]:
        """按配置编号与实际采纳版本核对已落库批次。"""
        from app.store import store

        batches = [
            row
            for row in store.rows(self.MODULE)
            if row.get("status") == "已落库"
        ]
        if config_id is not None:
            batches = [
                row for row in batches
                if str(row.get("来源配置编号")) == str(config_id)
            ]
        if config_version is not None:
            batches = [
                row for row in batches
                if str(row.get("采用版本")) == str(config_version)
            ]
        if batch_id is not None:
            batches = [row for row in batches if row.get("批次标识") == batch_id]
        if task_id is not None:
            batches = [row for row in batches if row.get("调度任务") == task_id]
        if credential_id is not None:
            batches = [row for row in batches if row.get("来源凭据") == credential_id]
        if credentialed_only:
            batches = [row for row in batches if row.get("来源凭据")]
        return batches

    def domain_verify_stored(
        self,
        *,
        config_id: int | None = None,
        config_version: str | None = None,
        batch_id: str | None = None,
        task_id: str | None = None,
        credential_id: str | None = None,
    ) -> dict[str, Any]:
        """结果存储侧核对：返回命中批次，并指出是否存在冒名批次。

        ``fake_batch`` 为真表示：同一个批次标识确实存在于结果存储里，
        但它记录的配置编号或采纳版本与本次核对条件不符——也就是一笔
        属于别的来源却被当成这次采集产物的记录。目标批次完全不存在时
        ``fake_batch`` 为假，由 ``missing`` 表达。
        """
        from app.store import store

        matches = self.find_stored_batches(
            config_id=config_id,
            config_version=config_version,
            batch_id=batch_id,
            task_id=task_id,
            credential_id=credential_id,
        )
        existing_same_batch: list[dict[str, Any]] = []
        if batch_id is not None:
            existing_same_batch = [
                row for row in store.rows(self.MODULE)
                if row.get("批次标识") == batch_id and row.get("status") == "已落库"
            ]
        return {
            "config_id": config_id,
            "config_version": config_version,
            "batch_id": batch_id,
            "task_id": task_id,
            "credential_id": credential_id,
            "found": len(matches),
            "stored": bool(matches),
            "exists": bool(matches) or bool(existing_same_batch),
            "missing": batch_id is not None and not existing_same_batch,
            "fake_batch": bool(existing_same_batch) and not matches,
            "matches": matches,
            "batches": matches,
            "existing_same_batch": existing_same_batch,
        }

    # ------------------------------------------------------------------
    # 列表查询：把 config_id / config_version 之类的查询参数翻译成核对条件
    # ------------------------------------------------------------------
    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        from app.store import store

        rows = list(store.rows(self.MODULE))
        normalised: dict[str, str] = {}
        for key, value in (filters or {}).items():
            field = self.FILTER_ALIASES.get(key, key)
            if field in self.LIST_FIELDS or field in self.EXTRA_FILTER_FIELDS:
                normalised[field] = value
        for field, expected in normalised.items():
            if field in ("配置编号",):
                rows = [
                    row for row in rows
                    if str(row.get(field)) == str(expected)
                ]
            elif field in ("采用版本", "来源凭据", "调度任务"):
                rows = [
                    row for row in rows
                    if expected.casefold() in str(row.get(field, "")).casefold()
                ]
            else:
                rows = [
                    row for row in rows
                    if expected.casefold() in str(row.get(field, "")).casefold()
                ]
        if keyword and self.LIST_FIELDS:
            field = self.LIST_FIELDS[0]
            rows = [row for row in rows if keyword.casefold() in str(row.get(field, "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

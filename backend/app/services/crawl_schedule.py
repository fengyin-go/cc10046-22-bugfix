"""采集调度领域服务：凭据登记来源、确认前复核、任务号分配。

调度只在拿到真实校验结果签发的「采集来源校验凭据」、且确认时配置仍
处于可采集状态时，才登记来源并分配任务号。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService


class CrawlScheduleService(BaseModuleService):
    MODULE = "crawl_schedule"
    ENTITY = "调度任务"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["任务标识", "关联配置", "调度周期", "起始水位", "截止水位", "分片策略", "上次执行", "下次执行"]
    REQUIRED_FIELDS = ["任务标识", "关联配置", "调度周期"]
    STATUS_ORDER = ["待启动", "运行中", "已完成", "调度失败"]
    ACTION_RULES = {"启动调度": "待启动", "取消调度": "调度失败"}
    NEGATIVE_ACTIONS = []

    EXTRA_FILTER_FIELDS = ["配置编号", "来源配置编号", "采用版本", "配置版本", "来源凭据", "凭证标识", "受理状态"]
    FILTER_ALIASES = {
        "config_id": "来源配置编号",
        "config_version": "采用版本",
        "credential_id": "来源凭据",
        "task_id": "任务标识",
    }

    def list_entries(self, **kwargs: Any):
        """列表查询支持按配置编号和凭据核对来源，不再只看同名配置。"""
        from app.store import store

        filters = dict(kwargs.pop("filters", None) or {})
        keyword = kwargs.pop("keyword", None)
        status = kwargs.pop("status", None)
        page = kwargs.pop("page", 1)
        size = kwargs.pop("size", 20)

        rows = list(store.rows(self.MODULE))
        for key, expected in filters.items():
            field = self.FILTER_ALIASES.get(key, key)
            if field == "任务标识":
                rows = [
                    row for row in rows
                    if expected.casefold() in str(row.get(field, "")).casefold()
                ]
            elif field in ("来源配置编号", "配置编号"):
                rows = [row for row in rows if str(row.get(field)) == str(expected)]
            elif field in self.EXTRA_FILTER_FIELDS:
                rows = [
                    row for row in rows
                    if expected.casefold() in str(row.get(field, "")).casefold()
                ]
            elif field in self.LIST_FIELDS:
                rows = [
                    row for row in rows
                    if expected.casefold() in str(row.get(field, "")).casefold()
                ]
        if keyword:
            rows = [
                row for row in rows
                if keyword.casefold() in str(row.get(self.LIST_FIELDS[0], "")).casefold()
            ]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    @staticmethod
    def confirmation_reject_reason(
        credential: dict[str, Any] | None,
        current: dict[str, Any] | None,
    ) -> str:
        """确认前复核：凭据失效、配置停用或版本不一致时必须退回。"""
        if not credential:
            if current is not None and not current.get("found"):
                return f"未找到爬虫配置 {current.get('config_id')}"
            if current is not None and not current.get("valid"):
                return (
                    f"爬虫配置 {current.get('config_id')} 已停用，"
                    "不能发起新的采集申请"
                )
            return "缺少采集来源校验凭据，不能登记来源"
        if not credential.get("valid"):
            return "采集来源校验凭据已失效，采集申请被退回"
        config_id = credential.get("config_id")
        if not current:
            # 没提供当前校验结果时，仍以凭据本身的有效性为准。
            return ""
        if not current.get("found"):
            return f"确认前未找到爬虫配置 {config_id}"
        if not current.get("valid"):
            return f"确认前爬虫配置 {config_id} 已停用，采集申请被退回"
        if str(current.get("config_version")) != str(credential.get("config_version")):
            return (
                f"确认前爬虫配置 {config_id} 的版本已从 "
                f"{credential.get('config_version')} 变为 {current.get('config_version')}"
            )
        return ""

    def domain_create_schedule(
        self,
        config_name: str,
        period: str = "daily",
        credential: dict[str, Any] | None = None,
        current: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """用采集来源校验凭据登记来源；校验不通过时不分配任务号。"""
        from app.store import store

        requested_config_id = (
            credential.get("config_id") if credential else
            (current or {}).get("config_id")
        )
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        plan: dict[str, Any] = {
            "task_id": None,
            "任务标识": None,
            "config_name": config_name,
            "config_id": requested_config_id,
            "配置编号": requested_config_id,
            "config_version": None,
            "period": period,
            "调度周期": period,
            "shard_count": 4,
            "start_time": now,
            "watermark_start": "2026-01-01",
            "watermark_end": time.strftime("%Y-%m-%d"),
            "accepted": False,
            "decision": "已退回",
            "status": "已退回",
            "credential_id": credential.get("credential_id") if credential else None,
            "reason": "",
        }
        if credential:
            plan["config_version"] = credential.get("config_version")
            plan["采用版本"] = credential.get("config_version")

        reason = self.confirmation_reject_reason(credential, current)
        if reason:
            plan["reason"] = reason
            return plan

        rows = store.rows(self.MODULE)
        next_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        task_id = f"SCHED-{next_id}"
        entry = {
            "id": next_id,
            "任务标识": task_id,
            "关联配置": credential.get("config_name") or config_name,
            "调度周期": period,
            "起始水位": plan["watermark_start"],
            "截止水位": plan["watermark_end"],
            "分片策略": "hash",
            "上次执行": None,
            "下次执行": time.strftime(
                "%Y-%m-%d %H:%M:%S", time.localtime(time.time() + 86400)
            ),
            "status": "待启动",
            "pending": True,
            "abnormal": False,
            # 来源由采集来源校验凭据登记，同名的不同配置靠配置编号区分。
            "受理状态": "已受理",
            "来源凭据": credential.get("credential_id"),
            "凭证标识": credential.get("credential_id"),
            "credential_id": credential.get("credential_id"),
            "来源配置编号": credential.get("config_id"),
            "配置编号": credential.get("config_id"),
            "config_id": credential.get("config_id"),
            "配置版本": credential.get("config_version"),
            "采用版本": credential.get("config_version"),
            "config_version": credential.get("config_version"),
        }
        rows.append(entry)

        plan.update(
            {
                "task_id": task_id,
                "任务标识": task_id,
                "accepted": True,
                "decision": "已受理",
                "status": "已受理",
                "config_id": credential.get("config_id"),
                "config_name": entry["关联配置"],
                "config_version": credential.get("config_version"),
                "credential_id": credential.get("credential_id"),
                "凭证标识": credential.get("credential_id"),
                "来源配置编号": credential.get("config_id"),
                "registered": entry,
            }
        )
        return plan

    def domain_get_registered(self, task_id: str) -> dict[str, Any] | None:
        from app.store import store

        for row in store.rows(self.MODULE):
            if row.get("任务标识") == task_id:
                return row
        return None

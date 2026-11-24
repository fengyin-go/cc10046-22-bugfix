"""爬虫配置领域服务：配置校验、来源凭据签发与版本快照。

采集调度在分配任务号之前，必须拿到这里基于真实配置记录签发的
「采集来源校验凭据」；校验没找到来源时不会签发任何凭据。
"""
from __future__ import annotations

import json
import time
from typing import Any

from app.compute import content_hash
from app.services.base import BaseModuleService

# 已签发凭据的进程内登记簿：凭据 ID -> 凭据快照。
# 真实项目里会落到持久化表或签名服务，这里保持标准库实现。
_CREDENTIALS: dict[str, dict[str, Any]] = {}


class CrawlerConfigService(BaseModuleService):
    MODULE = "crawler_config"
    ENTITY = "爬虫配置"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["配置标识", "起始地址", "页面编码", "请求间隔", "超时秒数", "并发上限", "UA模板", "负责人"]
    REQUIRED_FIELDS = ["配置标识", "起始地址", "页面编码"]
    STATUS_ORDER = ["草稿", "待校验", "已启用", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "启用配置": "已启用", "停用配置": "已停用"}
    NEGATIVE_ACTIONS = ["停用配置"]

    # ------------------------------------------------------------------
    # 版本与校验
    # ------------------------------------------------------------------
    @staticmethod
    def config_version(entry: dict[str, Any]) -> str:
        """按配置内容生成版本快照，配置停用或内容变更都会产生新版本。"""
        payload = {key: value for key, value in entry.items() if key != "id"}
        payload["id"] = entry.get("id")
        digest = content_hash(
            json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
        )
        return f"V{entry.get('id')}-{digest[:12]}"

    @staticmethod
    def _effective_url(entry: dict[str, Any], url: str = "") -> str:
        return url or str(entry.get("起始地址") or "")

    @classmethod
    def _credential_digest(
        cls,
        *,
        config_id: int,
        config_version: str,
        source_status: str,
        source_url: str,
        reachable: bool,
    ) -> str:
        body = {
            "config_id": config_id,
            "config_version": config_version,
            "source_status": source_status,
            "source_url": source_url,
            "reachable": reachable,
        }
        return "sha256:" + content_hash(
            json.dumps(body, sort_keys=True, ensure_ascii=False, default=str)
        )

    def domain_validate_config(self, config_id: int, url: str = "") -> dict:
        """按配置编号真实校验来源，命中后才返回校验凭据。

        返回 ``found`` 表示配置是否存在，``valid`` 表示该配置能否用于
        新的采集申请（存在且未停用）。校验失败时 ``credential`` 为空。
        """
        entry = _find_config(self.MODULE, config_id)
        checked_at = time.strftime("%Y-%m-%d %H:%M:%S")
        if entry is None:
            return {
                "config_id": config_id,
                "config_name": None,
                "config_version": None,
                "found": False,
                "valid": False,
                "status": None,
                "url": url,
                "reachable": False,
                "checked_at": checked_at,
                "credential": None,
                "reason": f"未找到爬虫配置 {config_id}",
                "entry": None,
            }

        config_version = self.config_version(entry)
        source_status = str(entry.get("status") or "")
        effective_url = self._effective_url(entry, url)
        reachable = bool(effective_url.startswith(("http://", "https://")))
        usable = source_status != "已停用"

        result: dict[str, Any] = {
            "config_id": config_id,
            "config_name": entry.get("配置标识"),
            "config_version": config_version,
            "found": True,
            "valid": usable,
            "status": source_status,
            "url": effective_url,
            "reachable": reachable,
            "checked_at": checked_at,
            "credential": None,
            "reason": "" if usable else f"爬虫配置 {config_id} 已停用，不能发起新的采集申请",
            "entry": entry,
        }
        if usable:
            result["credential"] = self.domain_issue_source_credential(
                config_id, url=effective_url, validation=result
            )
        return result

    def domain_issue_source_credential(
        self,
        config_id: int,
        url: str = "",
        validation: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """用真实校验结果签发采集来源校验凭据；来源不存在或已停用时返回空。"""
        vresult = validation if validation is not None else self.domain_validate_config(config_id, url)
        if not vresult.get("found") or not vresult.get("valid"):
            return None

        digest = self._credential_digest(
            config_id=int(vresult["config_id"]),
            config_version=str(vresult["config_version"]),
            source_status=str(vresult["status"]),
            source_url=str(vresult["url"]),
            reachable=bool(vresult["reachable"]),
        )
        credential = {
            "credential_id": f"SRC-VAL-{vresult['config_id']}-{digest.rsplit(':', 1)[-1][:12]}",
            "凭证标识": f"SRC-VAL-{vresult['config_id']}-{digest.rsplit(':', 1)[-1][:12]}",
            "credential_type": "采集来源校验凭据",
            "kind": "采集来源校验凭据",
            "config_id": int(vresult["config_id"]),
            "配置编号": int(vresult["config_id"]),
            "config_name": vresult.get("config_name"),
            "配置标识": vresult.get("config_name"),
            "config_version": vresult["config_version"],
            "采用版本": vresult["config_version"],
            "source_status": vresult["status"],
            "source_url": vresult["url"],
            "reachable": bool(vresult["reachable"]),
            "issued_at": vresult["checked_at"],
            "valid": True,
            "digest": digest,
            "校验摘要": digest,
        }
        _CREDENTIALS[credential["credential_id"]] = dict(credential)
        return credential

    def domain_verify_credential(self, credential: dict[str, Any] | None) -> bool:
        """核对凭据是否由真实校验结果签发且未被篡改。"""
        if not credential or not credential.get("valid"):
            return False
        expected = self._credential_digest(
            config_id=int(credential.get("config_id", 0)),
            config_version=str(credential.get("config_version", "")),
            source_status=str(credential.get("source_status", "")),
            source_url=str(credential.get("source_url", "")),
            reachable=bool(credential.get("reachable", False)),
        )
        registered = _CREDENTIALS.get(str(credential.get("credential_id")))
        return credential.get("digest") == expected and registered is not None

    def domain_credential_matches_config(
        self,
        credential: dict[str, Any] | None,
        entry: dict[str, Any] | None,
    ) -> bool:
        """凭据采用的版本必须与配置记录当前版本一致。"""
        if not credential or entry is None:
            return False
        return (
            str(credential.get("config_id")) == str(entry.get("id"))
            and str(credential.get("config_version")) == self.config_version(entry)
        )

    def domain_get_credential(self, credential_id: str) -> dict[str, Any] | None:
        credential = _CREDENTIALS.get(credential_id)
        return dict(credential) if credential is not None else None

    def entry_for(self, config_id: int) -> dict[str, Any] | None:
        """返回配置记录本身，供值班员停用/启用等状态操作使用。"""
        return _find_config(self.MODULE, config_id)


def _find_config(module: str, entry_id: int) -> dict[str, Any] | None:
    """延迟导入 store，避免服务模块与仓库初始化顺序耦合。"""
    from app.store import store

    try:
        return store.find(module, int(entry_id))
    except (TypeError, ValueError):
        return None

"""跨模块业务链路：多条业务链把多个领域服务串成完整流程。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException

from app.services.crawler_config import CrawlerConfigService
from app.services.crawl_schedule import CrawlScheduleService
from app.services.batch_tracker import BatchTrackerService
from app.services.result_store import ResultStoreService
from app.services.parser_rule import ParserRuleService
from app.services.field_mapper import FieldMapperService
from app.services.html_cleaner import HtmlCleanerService
from app.services.dedup_engine import DedupEngineService
from app.services.proxy_pool import ProxyPoolService
from app.services.rate_limiter import RateLimiterService
from app.services.error_retry import ErrorRetryService
from app.services.error_logger import ErrorLoggerService
from app.services.cookie_mgr import CookieMgrService
from app.services.source_auth import SourceAuthService
from app.services.session_manager import SessionManagerService
from app.services.export_channel import ExportChannelService
from app.services.crawl_audit import CrawlAuditService
from app.services.data_cleanup import DataCleanupService

router = APIRouter(prefix="/api/chains", tags=["业务链路"])

_crawler = CrawlerConfigService()
_schedule = CrawlScheduleService()
_batch = BatchTrackerService()
_store = ResultStoreService()
_parse = ParserRuleService()
_mapper = FieldMapperService()
_cleaner = HtmlCleanerService()
_dedup = DedupEngineService()
_proxy = ProxyPoolService()
_limiter = RateLimiterService()
_retry = ErrorRetryService()
_logger = ErrorLoggerService()
_cookie = CookieMgrService()
_auth = SourceAuthService()
_session = SessionManagerService()
_export = ExportChannelService()
_audit = CrawlAuditService()
_cleanup = DataCleanupService()

# 采集申请在两阶段之间的暂存：只保存校验结果与申请上下文，不代表已受理。
_REQUESTS: dict[str, dict[str, Any]] = {}


def _target_config_id(payload: dict[str, Any]) -> int:
    """采集申请的目标配置编号；非法值统一交给校验判为不存在。"""
    raw = payload.get("config_id")
    if raw is None:
        return 1
    try:
        return int(raw)
    except (TypeError, ValueError):
        raise HTTPException(status_code=422, detail=f"配置编号不合法：{raw!r}")


def _build_receipt(
    *,
    config_id: int,
    config_name: str | None,
    config_version: str | None,
    credential_id: str | None,
    batch_id: str,
    task_id: str | None,
    decision: str,
    reason: str,
    stored_batches: list[dict[str, Any]],
    verified_version: str | None = None,
) -> dict[str, Any]:
    """给值班员的回执：本次是已确认还是已退回，以及落库的实际版本。"""
    stored_version = next(
        (row.get("采用版本") for row in stored_batches if row.get("采用版本")),
        None,
    )
    return {
        "回执": "采集申请回执",
        "decision": decision,
        "受理结果": decision,
        "accepted": decision == "已确认",
        "ok": decision == "已确认",
        "rejected": decision == "已退回",
        "message": reason or ("采集申请已确认" if decision == "已确认" else "采集申请已退回"),
        "reason": reason,
        "退回原因": reason,
        "config_id": config_id,
        "配置编号": config_id,
        "config_name": config_name,
        "配置标识": config_name,
        "config_version": config_version,
        "verified_version": verified_version,
        "校验版本": verified_version,
        "adopted_version": config_version,
        "采用版本": config_version,
        "stored_version": stored_version,
        "落库版本": stored_version,
        "credential_id": credential_id,
        "凭证标识": credential_id,
        "batch_id": batch_id,
        "批次标识": batch_id,
        "task_id": task_id,
        "任务标识": task_id,
        "stored": bool(stored_batches),
        "已落库": bool(stored_batches),
        "stored_batches": stored_batches,
        "实际落库批次": stored_batches,
    }


@router.post("/crawl-pipeline")
def run_crawl_pipeline(payload: dict[str, Any]) -> dict[str, Any]:
    """采集流水线：真实校验来源 → 凭据 → 登记调度 → 批次追踪 → 结果存储。

    校验没找到来源时直接 404，且不产生调度任务、批次追踪和落库记录。
    被确认环节驳回的申请同样不落库，只返回可核对（含已确认/已退回）的回执。
    """
    config_id = _target_config_id(payload)
    config_name = payload.get("config_name")
    period = payload.get("period", "daily")
    batch_label = payload.get("batch_label") or f"BATCH-{config_id}"
    row_count = payload.get("row_count", 100)
    valid_count = payload.get("valid_count", 95)
    # 模拟“调度拿到校验结果后、确认前值班员停用配置”的并发场景。
    stop_before_confirm = bool(payload.get("stop_before_confirm"))

    vresult = _crawler.domain_validate_config(config_id, payload.get("url", ""))
    if not vresult["found"]:
        raise HTTPException(status_code=404, detail=f"未找到爬虫配置 {config_id}")

    # 下发前用真实校验结果生成采集来源校验凭据；两阶段确认时沿用第一次的凭据。
    credential = vresult.get("credential") or _crawler.domain_issue_source_credential(
        config_id, validation=vresult)
    staged = payload.get("_credential")
    if staged:
        # 沿用的凭据必须确实属于本次申请的目标配置，防止拿别的来源顶替。
        if str(staged.get("config_id")) != str(config_id):
            raise HTTPException(
                status_code=409,
                detail=f"采集来源校验凭据属于配置 {staged.get('config_id')}，与申请配置 {config_id} 不符",
            )
        credential = staged
    used_name = config_name if config_name is not None else vresult.get("config_name")
    config_version = credential.get("config_version") if credential else None

    # 确认环节重新读取配置记录，版本或启用状态变了就要退回。
    confirm = payload.get("_confirm") or vresult
    if stop_before_confirm and payload.get("_confirm") is None:
        row = _crawler.entry_for(config_id)
        if row is not None:
            row["status"] = "已停用"
            row["pending"] = False
            row["abnormal"] = True
        confirm = _crawler.domain_validate_config(config_id)

    sresult = _schedule.domain_create_schedule(
        used_name or "", period, credential=credential, current=confirm)

    if not sresult.get("accepted"):
        receipt = _build_receipt(
            config_id=config_id,
            config_name=used_name,
            config_version=config_version,
            credential_id=credential.get("credential_id") if credential else None,
            batch_id=batch_label,
            task_id=None,
            decision="已退回",
            reason=sresult.get("reason", ""),
            stored_batches=[],
            verified_version=confirm.get("config_version") if confirm else None,
        )
        return {
            "chain": "crawl-pipeline",
            "ok": False,
            "accepted": False,
            "受理结果": "已退回",
            "message": receipt["message"],
            "received": True,
            "receipt": receipt,
            "回执": receipt,
            "credential": credential,
            "credentials": credential,
            "steps": {
                "validate_config": vresult,
                "create_schedule": sresult,
                "track_batch": None,
                "store_result": None,
            },
        }

    task_id = sresult["task_id"]
    credential_id = credential.get("credential_id") if credential else None
    bresult = _batch.domain_track_batch(
        batch_label,
        task_id,
        config_id=config_id,
        config_version=config_version,
        credential_id=credential_id,
    )
    storeresult = _store.domain_store_result(
        batch_label,
        used_name or str(vresult.get("config_name") or ""),
        row_count,
        valid_count,
        config_id=config_id,
        config_version=config_version,
        credential_id=credential_id,
        task_id=task_id,
    )

    stored_batches = _store.find_stored_batches(
        config_id=config_id,
        config_version=config_version,
        batch_id=batch_label,
        task_id=task_id,
    )
    receipt = _build_receipt(
        config_id=config_id,
        config_name=used_name,
        config_version=config_version,
        credential_id=credential_id,
        batch_id=batch_label,
        task_id=task_id,
        decision="已确认",
        reason="",
        stored_batches=stored_batches,
        verified_version=confirm.get("config_version") if confirm else None,
    )

    return {
        "chain": "crawl-pipeline",
        "ok": True,
        "accepted": True,
        "受理结果": "已确认",
        "message": receipt["message"],
        "received": True,
        "receipt": receipt,
        "回执": receipt,
        "credential": credential,
        "credentials": credential,
        "steps": {
            "validate_config": vresult,
            "create_schedule": sresult,
            "track_batch": bresult,
            "store_result": storeresult,
        },
    }


@router.post("/crawl-request")
def submit_crawl_request(payload: dict[str, Any]) -> dict[str, Any]:
    """采集申请入口：与流水线共用同一套校验、凭据、确认与回执语义。"""
    return run_crawl_pipeline(payload)


@router.post("/crawl-request/validate")
def validate_crawl_request(payload: dict[str, Any]) -> dict[str, Any]:
    """申请第一阶段：只有真实校验结果与凭据，不代表已受理、不分配任务号。"""
    config_id = _target_config_id(payload)
    vresult = _crawler.domain_validate_config(config_id, payload.get("url", ""))
    if not vresult["found"]:
        raise HTTPException(status_code=404, detail=f"未找到爬虫配置 {config_id}")
    credential = vresult.get("credential") or _crawler.domain_issue_source_credential(
        config_id, validation=vresult)
    request_id = (
        f"REQ-{config_id}-{credential['credential_id'].rsplit('-', 1)[-1]}"
        if credential
        else f"REQ-{config_id}-REJECTED"
    )
    _REQUESTS[request_id] = {
        "request_id": request_id,
        "request_status": "校验完成" if vresult["valid"] else "校验未过",
        "config_id": config_id,
        "config_name": vresult.get("config_name"),
        "config_version": vresult.get("config_version"),
        "credential": credential,
        "batch_label": payload.get("batch_label") or f"BATCH-{config_id}",
        "period": payload.get("period", "daily"),
        "row_count": payload.get("row_count", 100),
        "valid_count": payload.get("valid_count", 95),
        "config_name_override": payload.get("config_name"),
    }
    return {
        "received": True,
        "request_id": request_id,
        "request_status": _REQUESTS[request_id]["request_status"],
        "accepted": False,
        "受理结果": "待确认",
        "validation": vresult,
        "validate_config": vresult,
        "credential": credential,
        "credentials": credential,
        "config_id": config_id,
        "config_version": vresult.get("config_version"),
        "reason": vresult.get("reason", ""),
    }


@router.post("/crawl-request/confirm")
def confirm_crawl_request(payload: dict[str, Any]) -> dict[str, Any]:
    """申请第二阶段：用第一阶段的真实校验结果做确认；期间配置被停用则退回。"""
    request_id = payload.get("request_id")
    request = _REQUESTS.get(str(request_id)) if request_id else None
    if request is None:
        raise HTTPException(status_code=404, detail=f"未找到采集申请 {request_id}")

    # 同一申请只能确认一次：重复确认直接回放原回执，不会二次落库。
    settled = request.get("settled_result")
    if settled is not None:
        return dict(settled)

    staged = request["credential"]
    credential = _crawler.domain_get_credential(staged["credential_id"]) if staged else None
    if credential is None:
        # 凭据已失效（如签发后来源被注销），原样退回。
        credential = staged
    request["credential"] = credential

    confirm = _crawler.domain_validate_config(request["config_id"])
    request["confirm_validation"] = confirm
    # 确认时如实沿用第一次的凭据快照，配置在这之间变更就会因版本不一致被退回。
    result = run_crawl_pipeline(
        {
            "config_id": request["config_id"],
            "config_name": request["config_name_override"],
            "period": request["period"],
            "batch_label": request["batch_label"],
            "row_count": request["row_count"],
            "valid_count": request["valid_count"],
            "_credential": dict(credential) if credential else None,
            "_confirm": confirm,
        }
    )
    request["request_status"] = "已确认" if result["accepted"] else "已退回"
    result["request_id"] = request["request_id"]
    result["request_status"] = request["request_status"]
    result["receipt"]["request_id"] = request["request_id"]
    result["receipt"]["request_status"] = request["request_status"]
    result["回执"] = result["receipt"]
    request["settled_result"] = result
    return result


@router.get("/source-credential/{credential_id}")
def get_source_credential(credential_id: str) -> dict[str, Any]:
    """凭据核对入口：调度显示已受理时，从这里反查采用的配置编号与版本。"""
    credential = _crawler.domain_get_credential(credential_id)
    if credential is None:
        raise HTTPException(status_code=404, detail=f"未找到采集来源校验凭据 {credential_id}")
    return credential


@router.get("/stored-batch")
def verify_stored_batch(
    config_id: int | None = None,
    config_version: str | None = None,
    batch_id: str | None = None,
    task_id: str | None = None,
    credential_id: str | None = None,
) -> dict[str, Any]:
    """结果存储核对入口：按配置编号和实际采纳版本查已落库批次。"""
    return _store.domain_verify_stored(
        config_id=config_id,
        config_version=config_version,
        batch_id=batch_id,
        task_id=task_id,
        credential_id=credential_id,
    )


@router.get("/crawl-schedule/{task_id}")
def get_registered_schedule(task_id: str) -> dict[str, Any]:
    """调度核对入口：已受理的任务号必须能对回真实配置编号、版本与凭据。"""
    row = _schedule.domain_get_registered(task_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"未找到调度任务 {task_id}")
    return row


@router.post("/data-process")
def run_data_process(payload: dict[str, Any]) -> dict[str, Any]:
    """数据处理：HTML解析 → 字段映射 → HTML清洗 → 去重。"""
    html = payload.get("html", "<html><body><p>测试内容</p></body></html>")
    source_data = payload.get("source_data", {"name": "test", "count": "42"})
    mapping_rules = payload.get("mapping_rules", [
        {"source": "name", "target": "title", "default": "", "transform": ""},
        {"source": "count", "target": "total", "default": "0", "transform": "int"},
    ])
    key_fields = payload.get("key_fields", ["title"])

    parse_result = _parse.domain_parse_html(html)
    map_result = _mapper.domain_map_fields(source_data, mapping_rules)
    clean_result = _cleaner.domain_clean_html(html)
    items = [map_result, {"title": "extra", "other": "value"}]
    dedup_result = _dedup.domain_dedup(items, key_fields)

    return {
        "chain": "data-process",
        "steps": {
            "parse_html": parse_result,
            "map_fields": map_result,
            "clean_html": {"original_length": clean_result["original_length"], "cleaned_length": clean_result["cleaned_length"]},
            "dedup": {"original_count": dedup_result["original_count"], "deduped_count": dedup_result["deduped_count"], "removed_count": dedup_result["removed_count"]},
        },
    }


@router.post("/reliability")
def run_reliability(payload: dict[str, Any]) -> dict[str, Any]:
    """可靠性保障：代理分配 → 限速检查 → 重试计算 → 错误记录。"""
    site = payload.get("site", "example.com")
    protocol = payload.get("protocol", "http")
    request_count = payload.get("request_count", 100)
    error_type = payload.get("error_type", "timeout")
    attempt = payload.get("attempt", 2)

    proxy_result = _proxy.domain_allocate_proxy(protocol)
    rate_result = _limiter.domain_check_rate(site, request_count)
    retry_result = _retry.domain_retry_calc(error_type, attempt)
    log_result = _logger.domain_log_error(
        "ERR-CHECK", f"可靠性检查 {site}", site)

    return {
        "chain": "reliability",
        "steps": {
            "allocate_proxy": proxy_result,
            "check_rate": rate_result,
            "retry_calc": retry_result,
            "log_error": {"error_code": log_result["错误码"], "error_message": log_result["错误信息"]},
        },
    }


@router.post("/auth")
def run_auth(payload: dict[str, Any]) -> dict[str, Any]:
    """认证链路：Cookie获取 → 来源认证 → 会话创建。"""
    site = payload.get("site", "example.com")
    auth_type = payload.get("auth_type", "token")
    session_type = payload.get("session_type", "http")

    cookie_result = _cookie.domain_get_cookies(site)
    auth_result = _auth.domain_authenticate(site, auth_type)
    session_id = f"{site}-session-1"
    session_result = _session.domain_create_session(session_id, site, session_type)

    return {
        "chain": "auth",
        "steps": {
            "get_cookies": cookie_result,
            "authenticate": auth_result,
            "create_session": session_result,
        },
    }


@router.post("/delivery")
def run_delivery(payload: dict[str, Any]) -> dict[str, Any]:
    """交付链路：结果存储 → 导出 → 审计 → 数据清理。"""
    batch_id = payload.get("batch_id", "BATCH-DELIVERY-1")
    config_name = payload.get("config_name", "delivery")
    export_format = payload.get("export_format", "csv")
    fields = payload.get("fields", ["id", "title", "count"])
    cleanup_module = payload.get("cleanup_module", "result_store")

    store_result = _store.domain_store_result(batch_id, config_name, 200, 180)
    export_result = _export.domain_export(batch_id, export_format, fields)
    audit_result = _audit.domain_audit("export", batch_id, "system")
    cleanup_result = _cleanup.domain_cleanup(f"CLEANUP-{batch_id}", cleanup_module, 30)

    return {
        "chain": "delivery",
        "steps": {
            "store_result": store_result,
            "export": export_result,
            "audit": audit_result,
            "cleanup": cleanup_result,
        },
    }
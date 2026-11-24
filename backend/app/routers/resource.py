"""统一资源接口：查询、导出、登记和动作按模块 key 分派到领域服务。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services import service_for

router = APIRouter(prefix="/api", tags=["业务资源"])


def require_service(module: str):
    service = service_for(module)
    if service is None:
        raise HTTPException(status_code=404, detail=f"未知业务模块：{module}")
    return service


def _filter_fields(service) -> set[str]:
    """列表可筛选字段：模块字段之外，允许服务声明核对用的附加字段和别名。"""
    fields = set(service.LIST_FIELDS)
    fields.update(getattr(service, "EXTRA_FILTER_FIELDS", []) or [])
    fields.update(getattr(service, "FILTER_ALIASES", {}) or {})
    return fields


@router.get("/{module}", response_model=PageResult[dict])
def list_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    service = require_service(module)
    allowed = _filter_fields(service)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in allowed and value.strip()
    }
    items, total = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{module}/export")
def export_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    service = require_service(module)
    allowed = _filter_fields(service)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in allowed and value.strip()
    }
    items, _ = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=1, size=1_000_000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *service.LIST_FIELDS, "status"])
    for item in items:
        values = [item.get("id"), *(item.get(field, "") for field in service.LIST_FIELDS),
                  item.get("status", "")]
        writer.writerow([
            "'" + str(value) if str(value or "").startswith(("=", "+", "-", "@")) else value
            for value in values
        ])
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{module}.csv"'},
    )


@router.get("/{module}/verify-stored", response_model=dict)
def verify_stored_entries(module: str, request: Request) -> dict[str, Any]:
    """核对已落库批次：按配置编号、实际采纳版本、批次号或任务号反查。"""
    service = require_service(module)
    verify = getattr(service, "domain_verify_stored", None)
    if verify is None:
        raise HTTPException(status_code=404, detail=f"{service.ENTITY}不支持落库核对")

    params = request.query_params
    config_id: int | None = None
    if params.get("config_id"):
        try:
            config_id = int(params["config_id"])
        except ValueError:
            raise HTTPException(status_code=422, detail="配置编号必须是整数")
    try:
        return verify(
            config_id=config_id,
            config_version=params.get("config_version") or params.get("采用版本") or None,
            batch_id=params.get("batch_id") or params.get("批次标识") or None,
            task_id=params.get("task_id") or params.get("调度任务") or None,
            credential_id=params.get("credential_id") or params.get("来源凭据") or None,
        )
    except TypeError:
        raise HTTPException(status_code=404, detail=f"{service.ENTITY}不支持落库核对")


@router.get("/{module}/{entry_id}", response_model=dict)
def get_entry(module: str, entry_id: int) -> dict[str, Any]:
    service = require_service(module)
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"{service.ENTITY} {entry_id} 不存在或已归档")
    return entry


@router.post("/{module}", response_model=ActionResult)
def create_entry(module: str, payload: EntryPayload) -> ActionResult:
    service = require_service(module)
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message=f"{service.ENTITY}已登记", entry=entry)


@router.post("/{module}/{entry_id}/actions", response_model=ActionResult)
def run_action(module: str, entry_id: int, payload: ActionPayload) -> ActionResult:
    service = require_service(module)
    entry, message = service.run_action(entry_id, payload.action.strip())
    return ActionResult(ok=entry is not None, message=message, entry=entry)

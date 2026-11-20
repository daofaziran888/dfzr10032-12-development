"""采集调度批次接口：按调度时段生成批次清单、推进批次状态、确认依赖结果。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.schedule_batch import ScheduleBatchService

router = APIRouter(prefix="/api/schedule_batch", tags=["采集调度批次"])

service = ScheduleBatchService()

LIST_FIELDS = [
    "批次编号",
    "计划编号",
    "任务编号",
    "调度周期",
    "调度时段",
    "依赖任务",
    "依赖状态",
    "预计可用时间",
    "确认数据",
    "批次状态",
]
STATUSES = ["等待依赖", "待执行", "执行中", "已确认", "已挂起"]


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出采集调度批次清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "schedule_batch", "total": total, "items": items}


@router.get("", response_model=PageResult[dict])
def list_entries(
    request: Request,
    keyword: str | None = Query(default=None, description="按批次编号、任务编号或计划编号检索"),
    status: str | None = Query(default=None, description="等待依赖、待执行、执行中、已确认、已挂起"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按批次编号、任务编号与状态过滤采集调度批次；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    extra = {
        k: v
        for k, v in request.query_params.items()
        if k not in ("keyword", "status", "page", "size") and v.strip()
    }
    items, total = service.list_entries(
        keyword=keyword, status=status, page=page, size=size, extra=extra
    )
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条采集批次明细，附带该批次已生成的流转执行节点。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"采集批次 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def dispatch_batch(payload: EntryPayload) -> ActionResult:
    """按调度计划或调度时段派发采集批次。

    同一任务在同一调度时段只生成一条批次；周期切换时若前置任务尚未确认，
    新批次接续该前置批次并给出预计可用时间，而不是第二次派发执行任务。
    """
    ok, message, entry, data = service.dispatch(payload.values)
    if not ok:
        return ActionResult(ok=False, message=message)
    entry_out = dict(entry) if entry else None
    if entry_out is not None:
        entry_out["派发结果"] = {
            "created": [item.get("批次编号") for item in data.get("created") or []],
            "duplicates": [item.get("批次编号") for item in data.get("duplicates") or []],
            "failures": data.get("failures") or [],
        }
    return ActionResult(ok=True, message=message, entry=entry_out)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """执行开始执行、确认结果、挂起批次、恢复批次；依赖未确认的批次会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action, payload.values)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)

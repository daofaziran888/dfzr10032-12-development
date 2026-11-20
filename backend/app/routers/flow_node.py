"""数据流转执行节点接口：用批次清单里的依赖结果生成与前置批次关联的可执行节点。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.flow_node import FlowNodeService

router = APIRouter(prefix="/api/flow_node", tags=["数据流转执行节点"])

service = FlowNodeService()

LIST_FIELDS = [
    "节点编号",
    "管道编号",
    "批次编号",
    "任务编号",
    "调度时段",
    "前置批次",
    "依赖结果摘要",
    "节点状态",
]
STATUSES = ["待编排", "可执行", "执行中", "已完成", "已阻断"]


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出流转执行节点清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_nodes(page=1, size=10000)
    return {"module": "flow_node", "total": total, "items": items}


@router.get("", response_model=PageResult[dict])
def list_nodes(
    request: Request,
    keyword: str | None = Query(default=None, description="按节点编号、批次编号或任务编号检索"),
    status: str | None = Query(default=None, description="待编排、可执行、执行中、已完成、已阻断"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按节点编号、批次编号与状态过滤流转执行节点；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    extra = {
        k: v
        for k, v in request.query_params.items()
        if k not in ("keyword", "status", "page", "size") and v.strip()
    }
    items, total = service.list_nodes(
        keyword=keyword, status=status, page=page, size=size, extra=extra
    )
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{node_id}", response_model=dict)
def get_node(node_id: int) -> dict:
    """读取单个执行节点明细；不存在时给出可读的错误说明。"""
    node = service.get_node(node_id)
    if node is None:
        raise HTTPException(status_code=404, detail=f"执行节点 {node_id} 不存在或已归档")
    return node


@router.post("", response_model=ActionResult)
def generate_node(payload: EntryPayload) -> ActionResult:
    """用采集调度批次清单生成可执行节点：依赖结果未确认的批次会被阻断并说明原因。"""
    ok, message, node = service.generate(payload.values)
    if not ok:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=node)


@router.post("/{node_id}/actions", response_model=ActionResult)
def run_action(node_id: int, payload: EntryPayload) -> ActionResult:
    """执行开始流转、完成流转；已完成节点重复操作不会二次执行。"""
    action = str(payload.values.get("action") or "").strip()
    node, message = service.run_action(node_id, action)
    if node is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=node)

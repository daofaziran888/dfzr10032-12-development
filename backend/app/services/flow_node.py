"""数据流转编排中的可执行节点：由采集调度批次清单生成，并始终回指前置批次。

只有批次里的依赖结果都确认之后才允许生成节点；同一个采集批次在同一条管道下只生成
一个节点，重复编排返回已有节点，不会把同一份批次数据派发两次。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.schedule_batch import (
    MODULE as BATCH_MODULE,
    ScheduleBatchService,
)
from app.store import store

MODULE = "flow_node"
STATUS_ORDER = ["待编排", "可执行", "执行中", "已完成", "已阻断"]


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")



def _next_id(rows: list[dict[str, Any]]) -> int:
    return max((int(row.get("id", 0)) for row in rows), default=0) + 1


def _next_node_no(rows: list[dict[str, Any]]) -> str:
    used = {str(row.get("节点编号") or "") for row in rows}
    index = len(rows) + 1
    while f"NODE-{index:04d}" in used:
        index += 1
    return f"NODE-{index:04d}"


class FlowNodeService:
    batch_service = ScheduleBatchService()

    def list_nodes(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
        extra: dict[str, str] | None = None,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [
                row
                for row in rows
                if keyword in str(row.get("节点编号", ""))
                or keyword in str(row.get("批次编号", ""))
                or keyword in str(row.get("任务编号", ""))
            ]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        if extra:
            for field, val in extra.items():
                rows = [row for row in rows if val in str(row.get(field, ""))]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_node(self, node_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, node_id)

    def _pick_pipeline(self, pipeline_ref: Any) -> dict[str, Any] | None:
        text = str(pipeline_ref or "").strip()
        pipelines = store.rows("flow")
        if text:
            for pipe in pipelines:
                if str(pipe.get("管道编号") or "") == text:
                    return pipe
            if text.isdigit():
                return store.find("flow", int(text))
            return None
        for pipe in pipelines:
            if str(pipe.get("status") or "") == "已启用":
                return pipe
        return pipelines[0] if pipelines else None

    def _find_existing_node(self, batch_no: str, pipeline_no: str) -> dict[str, Any] | None:
        for node in store.rows(MODULE):
            if (
                str(node.get("批次编号") or "") == batch_no
                and str(node.get("管道编号") or "") == pipeline_no
            ):
                return node
        return None

    def generate(self, values: dict[str, Any]) -> tuple[bool, str, dict[str, Any] | None]:
        """按批次生成可执行节点。

        支持三种入口：批次编号、计划编号 + 调度时段、调度时段（覆盖该时段全部已就绪批次）。
        """
        pipeline = self._pick_pipeline(values.get("管道编号") or values.get("pipeline"))
        if pipeline is None:
            return False, f"流转管道 {values.get('管道编号')} 不存在或已归档", None
        pipeline_no = str(pipeline.get("管道编号") or "")

        batches, missing_reason = self._resolve_batches(values)
        if not batches:
            return False, missing_reason, None

        created: list[dict[str, Any]] = []
        reused: list[dict[str, Any]] = []
        blocked: list[str] = []
        for batch in batches:
            batch_no = str(batch.get("批次编号") or "")
            existing = self._find_existing_node(batch_no, pipeline_no)
            if existing is not None:
                reused.append(existing)
                continue
            waiting = [
                str(item.get("批次编号") or "")
                for item in batch.get("等待依赖") or []
            ]
            if waiting or self.batch_service._has_unconfirmed_dependency(batch):
                blocked.append(
                    f"{batch_no} 依赖未确认"
                    + (f"（{'、'.join(waiting)}）" if waiting else "")
                )
                continue
            created.append(self._build_node(batch, pipeline))

        if not created and not reused:
            return False, "；".join(blocked) or "没有可生成节点的采集批次", None

        parts = [f"生成 {len(created)} 个可执行节点"]
        if reused:
            parts.append(f"复用 {len(reused)} 个已有节点")
        if blocked:
            parts.append(f"{len(blocked)} 个批次被依赖阻断")
        message = f"管道 {pipeline_no}：" + "，".join(parts)
        primary = created[0] if created else reused[0]
        primary = dict(primary)
        primary["生成结果"] = {
            "created": [node.get("节点编号") for node in created],
            "reused": [node.get("节点编号") for node in reused],
            "blocked": blocked,
        }
        return True, message, primary

    def _resolve_batches(
        self, values: dict[str, Any]
    ) -> tuple[list[dict[str, Any]], str]:
        batch_ref = values.get("批次编号") or values.get("batch_no")
        if batch_ref not in (None, ""):
            batch = self.batch_service.find_batch(batch_ref)
            if batch is None:
                return [], f"采集批次 {batch_ref} 不存在或已归档"
            return [batch], ""

        window = values.get("调度时段") or values.get("调度窗口") or values.get("window")
        plan_ref = values.get("计划编号") or values.get("plan_no")
        rows = store.rows(BATCH_MODULE)
        if plan_ref not in (None, ""):
            rows = [row for row in rows if str(row.get("计划编号") or "") == str(plan_ref)]
        if window not in (None, ""):
            text = str(window).strip()
            rows = [
                row
                for row in rows
                if str(row.get("调度时段") or "") == text
                or str(row.get("调度时段") or "").startswith(text)
            ]
        if not rows:
            return [], "调度时段内没有采集批次，请先按调度时段派发批次"
        return rows, ""

    def _build_node(self, batch: dict[str, Any], pipeline: dict[str, Any]) -> dict[str, Any]:
        rows = store.rows(MODULE)
        dependencies = list(batch.get("已确认依赖") or [])
        dependency_results = [
            {
                "批次编号": str(item.get("批次编号") or ""),
                "任务编号": str(item.get("任务编号") or ""),
                "调度周期": str(item.get("调度周期") or ""),
                "调度时段": str(item.get("调度时段") or ""),
                "来源关系": str(item.get("来源关系") or ""),
                "结果标识": str(item.get("结果标识") or ""),
                "确认数据": str(item.get("确认数据") or ""),
                "确认时间": str(item.get("确认时间") or ""),
            }
            for item in dependencies
        ]
        node = {
            "id": _next_id(rows),
            "status": "可执行",
            "pending": True,
            "abnormal": False,
            "节点编号": _next_node_no(rows),
            "管道编号": str(pipeline.get("管道编号") or ""),
            "管道名称": str(pipeline.get("管道名称") or ""),
            "批次编号": str(batch.get("批次编号") or ""),
            "计划编号": str(batch.get("计划编号") or ""),
            "任务编号": str(batch.get("任务编号") or ""),
            "调度周期": str(batch.get("调度周期") or ""),
            "调度时段": str(batch.get("调度时段") or ""),
            "前置批次": "、".join(
                str(item.get("批次编号") or "") for item in dependencies
            ) or "无",
            "依赖结果": dependency_results,
            "依赖结果摘要": "；".join(
                filter(None, (item.get("确认数据") for item in dependency_results))
            ) or str(batch.get("确认数据") or ""),
            "输入来源": str(pipeline.get("起始节点") or ""),
            "输出目标": str(pipeline.get("终止节点") or ""),
            "流转条件": str(pipeline.get("流转条件") or ""),
            "生成时间": _now(),
        }
        rows.append(node)
        return node

    def run_action(self, node_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        node = store.find(MODULE, node_id)
        if node is None:
            return None, f"执行节点 {node_id} 不存在或已归档"
        action = str(action or "").strip()
        target = {"开始流转": "执行中", "完成流转": "已完成"}.get(action)
        if target is None:
            return None, f"动作「{action}」不属于流转执行节点可执行范围"
        if node.get("status") == "已完成":
            return node, f"节点 {node.get('节点编号')} 已完成，未重复执行"
        if target == "已完成" and node.get("status") != "执行中":
            return None, f"节点 {node.get('节点编号')} 尚未开始流转，不能直接完成"
        node["status"] = target
        node["pending"] = target != "已完成"
        return node, f"节点 {node.get('节点编号')} 已{action}"

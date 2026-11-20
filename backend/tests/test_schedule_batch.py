"""采集调度批次与流转执行节点的业务回归测试。

覆盖三条关键约束：
1. 同一执行时段不会因重复排期收到两份执行任务；
2. 后继批次只消费已确认的依赖结果，依赖未确认前不能执行、也不能生成流转节点；
3. 周期切换时，新批次接续已发出但未确认的前置批次，并给出后继数据的可用时间。
"""
from __future__ import annotations

import pytest

from app.seed import SEED_ROWS
from app.services.flow_node import FlowNodeService
from app.services.schedule_batch import ScheduleBatchService
from app.store import store


@pytest.fixture(autouse=True)
def reset_store() -> None:
    """每个用例都从同一份示例数据开始，避免批次编号互相影响。"""
    store._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}


def dispatch(**values):
    return ScheduleBatchService().dispatch(values)


def act(batch_id: int, action: str, **values):
    return ScheduleBatchService().run_action(batch_id, action, values)


def batch_of(result, task: str, window: str):
    _ok, _message, entry, data = result
    for item in (data.get("created") or []) + (data.get("duplicates") or []):
        if item.get("任务编号") == task and item.get("调度时段") == window:
            return item
    return entry


def test_same_window_is_not_dispatched_twice():
    first = dispatch(计划编号="SCHE-0002", 调度时段="2026-11-20 08:00")
    second = dispatch(计划编号="SCHE-0002", 调度时段="2026-11-20 08:00")

    ok, message, entry, data = second
    assert ok is True
    assert "未重复派发" in message
    assert data["created"] == []
    assert [item["批次编号"] for item in data["duplicates"]] == [entry["批次编号"]]
    assert entry["批次编号"] == first[2]["批次编号"]

    same_task = [
        row
        for row in store.rows("schedule_batch")
        if row.get("任务编号") == "库存汇总" and row.get("调度时段") == "2026-11-20 08:00"
    ]
    assert len(same_task) == 1


def test_dependency_must_be_confirmed_before_successor_runs():
    _ok, _message, inventory, _ = dispatch(
        计划编号="SCHE-0001", 调度时段="2026-11-20 06:00"
    )
    _ok, _message, summary, _ = dispatch(
        计划编号="SCHE-0002", 调度时段="2026-11-20 08:00"
    )

    assert summary["依赖状态"] == "接续等待"
    assert summary["接续批次"] == inventory["批次编号"]
    assert [item["批次编号"] for item in summary["等待依赖"]] == [inventory["批次编号"]]

    blocked, reason = act(summary["id"], "开始执行")
    assert blocked is None
    assert "等待依赖" in reason

    blocked, reason = act(summary["id"], "确认结果", 确认数据="提前确认")
    assert blocked is None
    assert "依赖结果尚未确认" in reason

    ok, message, node = FlowNodeService().generate({"批次编号": summary["批次编号"]})
    assert ok is False
    assert "依赖未确认" in message

    act(inventory["id"], "开始执行")
    confirmed, message = act(
        inventory["id"], "确认结果", 确认数据="库存快照 2026-11-20，12480 条"
    )
    assert confirmed is not None
    assert confirmed["status"] == "已确认"
    assert "已解除 1 个后继批次的依赖等待" in message

    released = ScheduleBatchService().get_entry(summary["id"])
    assert released["status"] == "待执行"
    assert released["依赖状态"] == "已确认"
    assert released["已确认依赖"][0]["确认数据"] == "库存快照 2026-11-20，12480 条"

    ok, _message, node = FlowNodeService().generate({"批次编号": summary["批次编号"]})
    assert ok is True
    assert node["批次编号"] == summary["批次编号"]
    assert node["前置批次"] == inventory["批次编号"]
    assert node["依赖结果"][0]["结果标识"] == confirmed["结果标识"]
    assert node["依赖结果"][0]["确认数据"] == "库存快照 2026-11-20，12480 条"


def test_generated_node_is_not_duplicated_for_same_batch():
    _ok, _message, inventory, _ = dispatch(
        计划编号="SCHE-0001", 调度时段="2026-11-20 06:00"
    )
    act(inventory["id"], "开始执行")
    act(inventory["id"], "确认结果", 确认数据="库存快照 2026-11-20")
    _ok, _message, summary, _ = dispatch(
        计划编号="SCHE-0002", 调度时段="2026-11-20 08:00"
    )

    first = FlowNodeService().generate({"批次编号": summary["批次编号"]})
    second = FlowNodeService().generate({"批次编号": summary["批次编号"]})

    assert first[0] is True and second[0] is True
    assert first[2]["节点编号"] == second[2]["节点编号"]
    nodes = [
        node
        for node in store.rows("flow_node")
        if node.get("批次编号") == summary["批次编号"]
    ]
    assert len(nodes) == 1


def test_period_switch_inherits_inflight_predecessor_and_delays_availability():
    _ok, _message, inventory, _ = dispatch(
        计划编号="SCHE-0001", 调度时段="2026-11-20 06:00"
    )
    act(inventory["id"], "开始执行")

    # 八点排期从日周期切成小时周期：不应新增一份库存采集执行任务。
    ok, message, summary, _ = dispatch(
        计划编号="SCHE-0002", 调度时段="2026-11-20 08:00"
    )
    assert ok is True
    assert "接续" in message

    inventory_batches = [
        row
        for row in store.rows("schedule_batch")
        if row.get("任务编号") == "库存采集"
        and row.get("调度时段") == "2026-11-20 06:00"
    ]
    assert len(inventory_batches) == 1
    assert inventory_batches[0]["派发次数"] == 1

    assert summary["依赖状态"] == "接续等待"
    assert summary["接续批次"] == inventory["批次编号"]
    assert summary["可用延迟分钟"] > 0
    assert summary["预计可用时间"] > "2026-11-20 08:00"

    # 同小时再次排期，仍然只有一条汇总批次。
    _ok, _message, again, _ = dispatch(
        计划编号="SCHE-0002", 调度时段="2026-11-20 08:00"
    )
    assert again["批次编号"] == summary["批次编号"]
    summary_batches = [
        row
        for row in store.rows("schedule_batch")
        if row.get("任务编号") == "库存汇总"
        and row.get("调度时段") == "2026-11-20 08:00"
    ]
    assert len(summary_batches) == 1

    # 前置确认后，后继立即进入待执行，可用时间不再延后。
    act(inventory["id"], "确认结果", 确认数据="库存快照 2026-11-20")
    released = ScheduleBatchService().get_entry(summary["id"])
    assert released["status"] == "待执行"
    assert released["可用延迟分钟"] == 0


def test_daily_plan_does_not_fire_every_hour():
    ok, message, _entry, _data = dispatch(调度时段="2026-11-20 08:00")
    assert ok is True
    # 只有对齐到点的库存汇总（小时周期）被派发，日周期的库存采集不参与。
    tasks = {
        row["任务编号"]
        for row in store.rows("schedule_batch")
        if row.get("调度时段") == "2026-11-20 08:00"
    }
    assert tasks == {"库存汇总"}
    assert "未到点" in message


def test_confirmed_predecessor_result_is_consumed_without_redispatch():
    # 20 点没有新的库存采集批次，但 11-19 的日批次已确认，后继直接消费这份结果，
    # 不再补发一份库存采集执行任务。
    ok, _message, summary, _ = dispatch(
        计划编号="SCHE-0002", 调度时段="2026-11-20 20:00"
    )
    assert ok is True
    assert summary["status"] == "待执行"
    assert summary["依赖状态"] == "已确认"
    assert [item["批次编号"] for item in summary["已确认依赖"]] == ["BATCH-0001"]
    inventory = [
        row
        for row in store.rows("schedule_batch")
        if row.get("任务编号") == "库存采集"
    ]
    assert [row["批次编号"] for row in inventory] == ["BATCH-0001"]


def test_adhoc_predecessor_is_created_once_when_no_batch_exists():
    store._tables["schedule_batch"] = []
    ok, _message, summary, _ = dispatch(
        计划编号="SCHE-0002", 调度时段="2026-11-20 20:00"
    )
    assert ok is True
    upstream = [
        row
        for row in store.rows("schedule_batch")
        if row.get("任务编号") == "库存采集"
    ]
    assert len(upstream) == 1
    assert upstream[0]["计划编号"] == "SCHE-0001"
    assert upstream[0]["调度时段"] == "2026-11-20 20:00"
    assert [item["批次编号"] for item in summary["等待依赖"]] == [upstream[0]["批次编号"]]

    # 再排下一小时的批次，仍然接续同一条前置任务，不会重复补建。
    _ok, _message, later, _ = dispatch(
        计划编号="SCHE-0002", 调度时段="2026-11-20 21:00"
    )
    upstream = [
        row
        for row in store.rows("schedule_batch")
        if row.get("任务编号") == "库存采集"
    ]
    assert len(upstream) == 1
    assert later["接续批次"] == upstream[0]["批次编号"]


def test_unconfirmed_state_cannot_be_started_twice():
    _ok, _message, inventory, _ = dispatch(
        计划编号="SCHE-0001", 调度时段="2026-11-20 06:00"
    )
    act(inventory["id"], "开始执行")
    again, message = act(inventory["id"], "开始执行")
    assert again is not None
    assert "未重复派发" in message

    act(inventory["id"], "确认结果", 确认数据="库存快照")
    twice, message = act(inventory["id"], "确认结果", 确认数据="重复确认")
    assert twice is not None
    assert "未重复记录确认数据" in message
    assert twice["确认数据"] == "库存快照"

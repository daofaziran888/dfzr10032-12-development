"""采集调度批次业务规则。

采集调度计划只描述“什么时候跑哪个任务、依赖谁”，真正驱动流转管道的是批次清单：
每个调度时段对应一条批次，批次记录等待中的依赖、已经确认的依赖结果，以及确认数据本身。
同一任务在同一调度时段只允许存在一条批次，重复排期返回已有批次，不会二次派发执行任务。

周期切换时（例如日周期切小时周期），上一批启动的前置任务可能还没有确认结果：
新批次接续这条已发出的前置批次，不重新派发执行任务，并把预计可用时间摊到下一批。
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from app.store import store

MODULE = "schedule_batch"

# 批次状态：依赖没到齐时停在“等待依赖”，依赖到齐后才允许开始执行。
STATUS_ORDER = ["等待依赖", "待执行", "执行中", "已确认", "已挂起"]

PERIOD_ALIASES = {
    "日": "日",
    "天": "日",
    "daily": "日",
    "day": "日",
    "小时": "小时",
    "时": "小时",
    "hour": "小时",
    "hourly": "小时",
    "周": "周",
    "weekly": "周",
    "week": "周",
}

ACTION_ALIASES = {
    "开始执行": "开始执行",
    "启动执行": "开始执行",
    "派发执行": "开始执行",
    "执行": "开始执行",
    "确认结果": "确认结果",
    "确认批次": "确认结果",
    "确认": "确认结果",
    "挂起批次": "挂起批次",
    "暂停批次": "挂起批次",
    "恢复批次": "恢复批次",
    "重试依赖": "重试依赖",
    "重新解析依赖": "重试依赖",
}

# 一个采集任务从开始执行到产出结果的默认耗时，用于估算后继批次的可用时间。
DEFAULT_TASK_MINUTES = 120
NO_DEPENDENCY = {"", "无", "无依赖", "-", "—", "none", "None", "null"}


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _normalize_period(value: Any) -> str:
    text = str(value or "").strip()
    return PERIOD_ALIASES.get(text, text or "日")


def _normalize_action(value: Any) -> str:
    text = str(value or "").strip()
    return ACTION_ALIASES.get(text, text)


def _is_no_dependency(value: Any) -> bool:
    return str(value or "").strip() in NO_DEPENDENCY


def _parse_window(value: Any) -> datetime | None:
    """把调度时段解析成时间点，兼容日期、日期+时分和 ISO 写法。"""
    text = str(value or "").strip()
    if not text:
        return None
    text = text.replace("/", "-").replace("T", " ")
    if text.endswith("Z"):
        text = text[:-1]
    candidates = [text]
    if len(text) == 13 and text[10] == " ":
        candidates.append(text + ":00")
    for candidate in candidates:
        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d %H",
            "%Y-%m-%d",
            "%Y-%m",
        ):
            try:
                return datetime.strptime(candidate, fmt)
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(candidate)
        except ValueError:
            continue
    return None


def _window_display(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%d %H:%M")


def _window_key(moment: datetime, period: str) -> str:
    """同一调度时段去重键：日批按天、小时批按小时、周批按 ISO 周。"""
    if period == "小时":
        return moment.strftime("%Y-%m-%dT%H")
    if period == "周":
        iso = moment.isocalendar()
        return f"{iso.year}-W{iso.week:02d}"
    return moment.strftime("%Y-%m-%d")


def _format_time(moment: datetime | None) -> str:
    return moment.strftime("%Y-%m-%d %H:%M") if moment else ""


def _snapshot(batch: dict[str, Any], relation: str = "") -> dict[str, Any]:
    """批次依赖快照：后继批次只引用快照，不直接改动前置批次。

    「来源关系」写明这条前置批次是同时段直连、跨周期接续、还是历史已确认结果，
    这样后继节点和前置批次之间始终有明确的对应关系。
    """
    return {
        "批次编号": str(batch.get("批次编号") or ""),
        "任务编号": str(batch.get("任务编号") or ""),
        "调度周期": str(batch.get("调度周期") or ""),
        "调度时段": str(batch.get("调度时段") or ""),
        "时段键": str(batch.get("时段键") or ""),
        "状态": str(batch.get("status") or ""),
        "来源关系": relation,
        "确认数据": batch.get("确认数据") or "",
        "结果标识": batch.get("结果标识") or "",
        "确认时间": batch.get("确认时间") or "",
        "预计完成时间": _format_time(_expected_finish(batch)),
    }


def _expected_finish(batch: dict[str, Any]) -> datetime | None:
    """估算前置批次的产出时间，用于推算后继批次的可用时间。"""
    explicit = _parse_window(batch.get("预计完成时间"))
    if explicit is not None:
        return explicit
    started = _parse_window(batch.get("开始时间"))
    if started is not None:
        return started + timedelta(minutes=DEFAULT_TASK_MINUTES)
    window = _parse_window(batch.get("调度时段"))
    if window is not None:
        return window + timedelta(minutes=DEFAULT_TASK_MINUTES)
    return None


def _window_matches_plan(plan: dict[str, Any], moment: datetime, period: str) -> bool:
    """判断调度时段是否落在计划的生命周期与周期节拍上。

    日周期只在自己设定的时刻触发，小时周期每个整点（对齐开始时间的分钟）触发，
    周周期只在与开始时间同一个星期几触发。按计划单独派发时不强制校验。
    """
    start = _parse_window(plan.get("开始时间"))
    if start is not None and moment < start:
        return False
    end = _parse_window(plan.get("结束时间"))
    if end is not None and moment >= end:
        return False
    if start is None:
        return period == "小时" or moment.hour == 0
    if period == "小时":
        return moment.minute == start.minute
    if period == "周":
        return moment.weekday() == start.weekday() and moment.time() == start.time()
    return moment.time() == start.time()


def _next_id(rows: list[dict[str, Any]]) -> int:
    return max((int(row.get("id", 0)) for row in rows), default=0) + 1


def _next_sequence(rows: list[dict[str, Any]], field: str, prefix: str) -> str:
    used = {str(row.get(field) or "") for row in rows}
    index = len(rows) + 1
    while f"{prefix}-{index:04d}" in used:
        index += 1
    return f"{prefix}-{index:04d}"


def _sync_flags(row: dict[str, Any]) -> None:
    status = row.get("status")
    row["pending"] = status not in ("已确认", "已挂起")
    row["abnormal"] = row.get("依赖状态") == "依赖缺失" or bool(row.get("依赖冲突"))


def _plan_task(plan: dict[str, Any]) -> str:
    return str(plan.get("关联任务") or plan.get("任务编号") or "").strip()


def _plan_dependency(plan: dict[str, Any]) -> str:
    value = plan.get("依赖任务")
    return "" if _is_no_dependency(value) else str(value).strip()


class ScheduleBatchService:
    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------
    def list_entries(
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
                if keyword in str(row.get("批次编号", ""))
                or keyword in str(row.get("任务编号", ""))
                or keyword in str(row.get("计划编号", ""))
            ]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        if extra:
            for field, val in extra.items():
                rows = [row for row in rows if val in str(row.get(field, ""))]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        detail = dict(entry)
        detail["关联执行节点"] = self.nodes_for_batch(str(entry.get("批次编号") or ""))
        detail["可生成执行节点"] = not self._has_unconfirmed_dependency(entry)
        return detail

    def get_by_batch_no(self, batch_no: Any) -> dict[str, Any] | None:
        text = str(batch_no or "")
        if not text:
            return None
        for row in store.rows(MODULE):
            if str(row.get("批次编号") or "") == text:
                return row
        return None

    def find_batch(self, batch_ref: Any) -> dict[str, Any] | None:
        text = str(batch_ref or "").strip()
        if not text:
            return None
        found = self.get_by_batch_no(text)
        if found is not None:
            return found
        if text.isdigit():
            return store.find(MODULE, int(text))
        return None

    def nodes_for_batch(self, batch_no: str) -> list[dict[str, Any]]:
        if not batch_no:
            return []
        return [
            dict(node)
            for node in store.rows("flow_node")
            if str(node.get("批次编号") or "") == batch_no
        ]

    def _has_unconfirmed_dependency(self, entry: dict[str, Any]) -> bool:
        if entry.get("依赖状态") in ("等待依赖", "同时段等待", "接续等待", "依赖缺失"):
            return True
        return any(
            str(item.get("状态") or "") != "已确认"
            for item in entry.get("等待依赖") or []
        )

    def _find_plan(self, plan_ref: Any) -> dict[str, Any] | None:
        text = str(plan_ref or "").strip()
        if not text:
            return None
        for plan in store.rows("schedule"):
            if str(plan.get("计划编号") or "") == text:
                return plan
        if text.isdigit():
            return store.find("schedule", int(text))
        return None

    def _find_plan_by_task(self, task: str, period: str) -> dict[str, Any] | None:
        candidates = [plan for plan in store.rows("schedule") if _plan_task(plan) == task]
        if not candidates:
            return None
        for plan in candidates:
            if _normalize_period(plan.get("调度周期")) == period:
                return plan
        return candidates[0]

    def _find_task_window(self, task: str, window_key: str) -> dict[str, Any] | None:
        for row in store.rows(MODULE):
            if (
                str(row.get("任务编号") or "") == task
                and str(row.get("时段键") or "") == window_key
            ):
                return row
        return None

    # ------------------------------------------------------------------
    # 排期 / 派发
    # ------------------------------------------------------------------
    def dispatch(
        self, values: dict[str, Any]
    ) -> tuple[bool, str, dict[str, Any] | None, dict[str, Any]]:
        """按计划或按调度时段派发批次。

        - 传 计划编号：只派发该计划；
        - 不传计划编号：派发该调度时段下周期匹配的全部计划；
        - 同一任务在同一时段已有批次时返回已有批次，不重复派发。
        """
        plan_ref = values.get("计划编号") or values.get("计划") or values.get("plan_no")
        raw_window = (
            values.get("调度时段")
            or values.get("调度窗口")
            or values.get("window")
            or values.get("开始时间")
        )
        stats: dict[str, list[Any]] = {"created": [], "duplicates": [], "failures": []}

        if plan_ref not in (None, ""):
            plan = self._find_plan(plan_ref)
            if plan is None:
                return False, f"调度计划 {plan_ref} 不存在或已归档", None, stats
            if str(plan.get("计划状态") or plan.get("status") or "") == "已暂停":
                return (
                    False,
                    f"调度计划 {plan.get('计划编号')} 已暂停，不能派发采集批次",
                    None,
                    stats,
                )
            if not raw_window:
                raw_window = plan.get("开始时间")
            period = _normalize_period(
                values.get("调度周期")
                or values.get("周期")
                or values.get("period")
                or plan.get("调度周期")
            )
            ok, message, entry, kind = self._dispatch_one(
                plan, raw_window, period, visited=frozenset(), depth=0
            )
            if entry is None:
                return False, message, None, stats
            stats[kind if kind in stats else "duplicates"].append(entry)
            return ok, message, entry, stats

        if not raw_window:
            return False, "请提供调度时段，例如 2026-11-20 08:00", None, stats
        moment = _parse_window(raw_window)
        if moment is None:
            return (
                False,
                f"调度时段「{raw_window}」格式无法识别，请使用 YYYY-MM-DD HH:MM",
                None,
                stats,
            )

        period_filter = values.get("调度周期") or values.get("周期") or values.get("period")
        period_filter = _normalize_period(period_filter) if period_filter else ""
        plans = [plan for plan in store.rows("schedule") if _plan_task(plan)]
        if period_filter:
            plans = [
                plan
                for plan in plans
                if _normalize_period(plan.get("调度周期")) == period_filter
            ]
        # 只派发在该时段真正到点的计划：日周期不会在每个小时都触发。
        due_plans = [
            plan
            for plan in plans
            if _window_matches_plan(
                plan, moment, _normalize_period(plan.get("调度周期"))
            )
        ]
        skipped = len(plans) - len(due_plans)
        if not due_plans:
            return (
                False,
                f"调度时段 {_window_display(moment)} 没有到点的调度计划",
                None,
                stats,
            )

        for plan in due_plans:
            if str(plan.get("计划状态") or plan.get("status") or "") == "已暂停":
                stats["failures"].append(f"{plan.get('计划编号')} 已暂停")
                continue
            ok, message, entry, kind = self._dispatch_one(
                plan,
                raw_window,
                _normalize_period(plan.get("调度周期")),
                visited=frozenset(),
                depth=0,
            )
            if entry is None:
                stats["failures"].append(message)
                continue
            stats[kind if kind in stats else "duplicates"].append(entry)
            if kind == "created" and not ok:
                stats["failures"].append(message)

        if not stats["created"] and not stats["duplicates"]:
            return (
                False,
                stats["failures"][0] if stats["failures"] else "没有生成采集批次",
                None,
                stats,
            )

        parts = [f"生成 {len(stats['created'])} 条采集批次"]
        if stats["duplicates"]:
            parts.append(f"复用 {len(stats['duplicates'])} 条同时段批次")
        if skipped:
            parts.append(f"{skipped} 条计划本时段未到点")
        if stats["failures"]:
            parts.append(f"{len(stats['failures'])} 条计划未派发")
        message = f"调度时段 {_window_display(moment)}：" + "，".join(parts)
        primary = stats["created"][0] if stats["created"] else stats["duplicates"][0]
        return True, message, primary, stats

    def _dispatch_one(
        self,
        plan: dict[str, Any],
        raw_window: Any,
        period: str,
        *,
        visited: frozenset[tuple[str, str]],
        depth: int,
    ) -> tuple[bool, str, dict[str, Any] | None, str]:
        """生成单个计划的批次。

        返回 (是否成功, 说明, 批次, 类型)，类型为 created / duplicate / failed。
        """
        task = _plan_task(plan)
        if not task:
            return False, f"调度计划 {plan.get('计划编号')} 未配置关联任务", None, "failed"
        moment = _parse_window(raw_window)
        if moment is None:
            return False, f"调度时段「{raw_window}」格式无法识别", None, "failed"
        period = _normalize_period(period or plan.get("调度周期"))
        window_key = _window_key(moment, period)
        display = _window_display(moment)

        existing = self._find_task_window(task, window_key)
        if existing is not None:
            return (
                True,
                f"任务「{task}」在 {display} 时段已有批次 {existing.get('批次编号')}，未重复派发",
                existing,
                "duplicate",
            )

        if (task, window_key) in visited or depth > 6:
            return (
                False,
                f"任务「{task}」在 {display} 的依赖链存在循环，批次未生成",
                None,
                "failed",
            )
        visited = visited | {(task, window_key)}

        dependency = _plan_dependency(plan)
        entry = self._blank_batch(
            plan_no=str(plan.get("计划编号") or ""),
            task=task,
            period=period,
            display=display,
            window_key=window_key,
            dependency=dependency,
        )

        if not dependency:
            entry["依赖状态"] = "无依赖"
            _sync_flags(entry)
            store.rows(MODULE).append(entry)
            return True, f"已生成采集批次 {entry['批次编号']}（{task} / {display}）", entry, "created"

        if dependency == task:
            entry["依赖状态"] = "依赖缺失"
            entry["status"] = "等待依赖"
            entry["依赖冲突"] = f"任务「{task}」不能依赖自身，请调整调度计划"
            _sync_flags(entry)
            store.rows(MODULE).append(entry)
            return (
                True,
                f"已生成采集批次 {entry['批次编号']}，依赖配置无效",
                entry,
                "created",
            )

        dependency_batch, relation, reason = self._resolve_dependency(
            dependency, moment, window_key, period, visited, depth
        )
        if dependency_batch is None:
            entry["依赖状态"] = "依赖缺失"
            entry["status"] = "等待依赖"
            entry["依赖冲突"] = reason or f"依赖任务「{dependency}」无法生成前置批次"
            _sync_flags(entry)
            store.rows(MODULE).append(entry)
            return (
                True,
                f"已生成采集批次 {entry['批次编号']}，依赖任务待补建",
                entry,
                "created",
            )

        # 只有前置批次发生在更早的调度时段，才算跨周期“接续”；
        # 同一时段里不同周期的任务只是普通等待，不标注接续。
        inherited = str(dependency_batch.get("调度时段") or "") != display
        snapshot = _snapshot(dependency_batch, "接续" if inherited else "同时段")

        if str(dependency_batch.get("status") or "") == "已确认":
            entry["已确认依赖"] = [snapshot]
            entry["依赖状态"] = "已确认"
            entry["status"] = "待执行"
            entry["预计可用时间"] = str(
                dependency_batch.get("确认时间")
                or dependency_batch.get("预计可用时间")
                or display
            )
            _sync_flags(entry)
            store.rows(MODULE).append(entry)
            return (
                True,
                f"已生成采集批次 {entry['批次编号']}，依赖结果 "
                f"{dependency_batch.get('批次编号')} 已确认",
                entry,
                "created",
            )

        entry["等待依赖"] = [snapshot]
        entry["status"] = "等待依赖"
        entry["依赖状态"] = "接续等待" if inherited else "同时段等待"
        relation_text = ""
        if inherited:
            entry["接续批次"] = str(dependency_batch.get("批次编号") or "")
            entry["依赖冲突"] = (
                f"前置任务「{dependency}」在 {dependency_batch.get('调度时段')} 的"
                f"{dependency_batch.get('调度周期')}批次仍为{dependency_batch.get('status')}，"
                f"本批次接续该前置任务，未重复派发"
            )
            relation_text = "接续未确认的前置批次，"
        remaining = self._estimate_availability(entry, dependency_batch, moment)
        _sync_flags(entry)
        store.rows(MODULE).append(entry)
        suffix = f"预计 {remaining} 可用" if remaining else "可用时间待前置批次确认"
        return (
            True,
            f"已生成采集批次 {entry['批次编号']}，{relation_text}等待依赖 "
            f"{dependency_batch.get('批次编号')} 确认，{suffix}",
            entry,
            "created",
        )

    def _estimate_availability(
        self,
        entry: dict[str, Any],
        dependency_batch: dict[str, Any],
        moment: datetime,
    ) -> str:
        expected = _expected_finish(dependency_batch)
        if expected is None:
            return ""
        if expected < moment:
            expected = moment
        entry["预计可用时间"] = _format_time(expected)
        entry["可用延迟分钟"] = max(
            0, int((expected - moment).total_seconds() // 60)
        )
        return entry["预计可用时间"]

    def _resolve_dependency(
        self,
        dependency: str,
        moment: datetime,
        window_key: str,
        period: str,
        visited: frozenset[tuple[str, str]],
        depth: int,
    ) -> tuple[dict[str, Any] | None, str, str]:
        """为依赖任务找到可消费的前置批次：同批次 → 接续 → 已确认 → 自动编排。"""
        same_window = self._find_task_window(dependency, window_key)
        if same_window is not None:
            return same_window, "同批次", ""

        rows = [
            row
            for row in store.rows(MODULE)
            if str(row.get("任务编号") or "") == dependency
        ]

        # 仍未确认的前置批次：周期切换时接续它，而不是重新发一份执行任务。
        inflight = [row for row in rows if str(row.get("status") or "") != "已确认"]
        if inflight:
            inflight.sort(key=lambda row: (str(row.get("时段键") or ""), int(row.get("id", 0))))
            return inflight[-1], "接续", ""

        # 已有确认结果：后继直接消费最近一次确认的数据。
        confirmed = [row for row in rows if str(row.get("status") or "") == "已确认"]
        if confirmed:
            confirmed.sort(key=lambda row: (str(row.get("时段键") or ""), int(row.get("id", 0))))
            return confirmed[-1], "已确认", ""

        # 没有任何前置批次：按同时段自动编排依赖计划，保证批次清单能真正驱动管道。
        if depth > 6:
            return None, "缺失", f"依赖任务「{dependency}」的依赖链层级过深"
        dependency_plan = self._find_plan_by_task(dependency, period)
        if dependency_plan is not None:
            ok, message, entry, _kind = self._dispatch_one(
                dependency_plan,
                moment,
                _normalize_period(dependency_plan.get("调度周期") or period),
                visited=visited,
                depth=depth + 1,
            )
            if entry is not None:
                return entry, "自动派发", ""
            if not ok:
                return None, "缺失", message

        return self._create_adhoc_batch(dependency, moment, window_key, period), "自动派发", ""

    def _blank_batch(
        self,
        *,
        plan_no: str,
        task: str,
        period: str,
        display: str,
        window_key: str,
        dependency: str,
    ) -> dict[str, Any]:
        rows = store.rows(MODULE)
        return {
            "id": _next_id(rows),
            "status": "待执行",
            "pending": True,
            "abnormal": False,
            "批次编号": _next_sequence(rows, "批次编号", "BATCH"),
            "计划编号": plan_no,
            "任务编号": task,
            "调度周期": period,
            "调度时段": display,
            "时段键": window_key,
            "依赖任务": dependency,
            "依赖状态": "无依赖",
            "等待依赖": [],
            "已确认依赖": [],
            "接续批次": "",
            "依赖冲突": "",
            "预计可用时间": display,
            "可用延迟分钟": 0,
            "确认数据": "",
            "结果标识": "",
            "派发次数": 1,
            "创建时间": _now(),
            "开始时间": "",
            "预计完成时间": "",
            "确认时间": "",
        }

    def _create_adhoc_batch(
        self,
        task: str,
        moment: datetime,
        window_key: str,
        period: str,
        *,
        dependency: str = "",
    ) -> dict[str, Any]:
        entry = self._blank_batch(
            plan_no="自动编排",
            task=task,
            period=period,
            display=_window_display(moment),
            window_key=window_key,
            dependency=dependency,
        )
        entry["依赖状态"] = "等待依赖" if dependency else "无依赖"
        _sync_flags(entry)
        store.rows(MODULE).append(entry)
        return entry

    # ------------------------------------------------------------------
    # 批次动作
    # ------------------------------------------------------------------
    def run_action(
        self, entry_id: int, action: str, values: dict[str, Any] | None = None
    ) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"采集批次 {entry_id} 不存在或已归档"
        values = values or {}
        action = _normalize_action(action)
        if action == "开始执行":
            return self._start(entry)
        if action == "确认结果":
            return self._confirm(entry, values)
        if action == "挂起批次":
            if entry.get("status") == "已确认":
                return None, f"批次 {entry.get('批次编号')} 已确认，不能挂起"
            entry["status"] = "已挂起"
            _sync_flags(entry)
            return entry, f"批次 {entry.get('批次编号')} 已挂起"
        if action == "恢复批次":
            if entry.get("status") != "已挂起":
                return None, f"批次 {entry.get('批次编号')} 当前不是挂起状态"
            entry["status"] = "等待依赖" if self._has_unconfirmed_dependency(entry) else "待执行"
            _sync_flags(entry)
            return entry, f"批次 {entry.get('批次编号')} 已恢复"
        if action == "重试依赖":
            return self._retry_dependency(entry)
        return None, f"动作「{action}」不属于采集调度批次可执行范围"

    def _start(self, entry: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        if entry.get("status") == "已确认":
            return None, f"批次 {entry.get('批次编号')} 已确认，不能重复执行"
        if entry.get("status") == "已挂起":
            return None, f"批次 {entry.get('批次编号')} 已挂起，请先恢复批次"
        if self._has_unconfirmed_dependency(entry):
            waiting = "、".join(
                str(item.get("批次编号") or "") for item in entry.get("等待依赖") or []
            )
            return None, (
                f"批次 {entry.get('批次编号')} 仍在等待依赖结果"
                + (f"（{waiting}）" if waiting else "")
                + "，依赖确认前不能开始执行"
            )
        if entry.get("status") == "执行中":
            return entry, f"批次 {entry.get('批次编号')} 已在执行中，未重复派发"
        entry["status"] = "执行中"
        entry["开始时间"] = _now()
        entry["预计完成时间"] = _format_time(
            datetime.now() + timedelta(minutes=DEFAULT_TASK_MINUTES)
        )
        _sync_flags(entry)
        return entry, f"批次 {entry.get('批次编号')} 已开始执行"

    def _confirm(
        self, entry: dict[str, Any], values: dict[str, Any]
    ) -> tuple[dict[str, Any] | None, str]:
        if entry.get("status") == "已确认":
            return entry, f"批次 {entry.get('批次编号')} 已确认，未重复记录确认数据"
        if self._has_unconfirmed_dependency(entry):
            waiting = "、".join(
                str(item.get("批次编号") or "") for item in entry.get("等待依赖") or []
            )
            return None, (
                f"批次 {entry.get('批次编号')} 的依赖结果尚未确认"
                + (f"（{waiting}）" if waiting else "")
                + "，不能提前确认本批次"
            )

        result = (
            values.get("确认数据")
            or values.get("结果")
            or values.get("result")
            or values.get("data")
        )
        result_id = (
            values.get("结果标识")
            or values.get("result_id")
            or values.get("确认编号")
        )
        now = _now()
        if not result:
            result = (
                f"{entry.get('任务编号')} {entry.get('调度时段')} 执行完成，"
                f"批次 {entry.get('批次编号')}"
            )
        if not result_id:
            result_id = f"RESULT-{entry.get('批次编号')}"

        entry["status"] = "已确认"
        entry["确认数据"] = str(result)
        entry["结果标识"] = str(result_id)
        entry["确认时间"] = now
        entry["预计可用时间"] = now
        entry["可用延迟分钟"] = 0
        entry["依赖冲突"] = ""
        if entry.get("依赖状态") not in ("无依赖",):
            entry["依赖状态"] = "已确认"
        _sync_flags(entry)

        released = self._cascade_confirmation(entry)
        suffix = f"，已解除 {released} 个后继批次的依赖等待" if released else ""
        return entry, f"批次 {entry.get('批次编号')} 已确认{suffix}"

    def _cascade_confirmation(self, confirmed: dict[str, Any]) -> int:
        """前置批次确认后，把等待它的后继批次转入已确认依赖并放行。"""
        batch_no = str(confirmed.get("批次编号") or "")
        released = 0
        for other in store.rows(MODULE):
            if other is confirmed:
                continue
            waiting = list(other.get("等待依赖") or [])
            if not waiting:
                continue
            remaining: list[dict[str, Any]] = []
            matched: list[dict[str, Any]] = []
            for item in waiting:
                if str(item.get("批次编号") or "") == batch_no:
                    matched.append(item)
                else:
                    remaining.append(item)
            if not matched:
                continue

            snapshots = list(other.get("已确认依赖") or [])
            for item in matched:
                snapshots.append(_snapshot(confirmed, str(item.get("来源关系") or "")))
            other["已确认依赖"] = snapshots
            other["等待依赖"] = remaining
            if remaining:
                other["依赖状态"] = "接续等待" if other.get("接续批次") else "同时段等待"
                _sync_flags(other)
                continue

            other["依赖状态"] = "已确认"
            other["接续批次"] = ""
            other["预计可用时间"] = confirmed.get("确认时间") or _now()
            other["可用延迟分钟"] = 0
            other["依赖冲突"] = (
                f"前置批次 {batch_no} 已确认，依赖等待已解除"
            )
            if str(other.get("status") or "") == "等待依赖":
                other["status"] = "待执行"
            _sync_flags(other)
            released += 1
        return released

    def _retry_dependency(self, entry: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        """依赖缺失或前置任务已补建时，重新解析依赖关系。"""
        if entry.get("status") == "已确认":
            return None, f"批次 {entry.get('批次编号')} 已确认，无需重试依赖"
        dependency = str(entry.get("依赖任务") or "").strip()
        if not dependency or _is_no_dependency(dependency):
            return None, f"批次 {entry.get('批次编号')} 没有可重试的依赖任务"
        moment = _parse_window(entry.get("调度时段"))
        if moment is None:
            return None, f"批次 {entry.get('批次编号')} 的调度时段无法解析"
        window_key = str(entry.get("时段键") or "") or _window_key(
            moment, str(entry.get("调度周期") or "日")
        )
        period = _normalize_period(entry.get("调度周期"))
        dependency_batch, relation, reason = self._resolve_dependency(
            dependency, moment, window_key, period, frozenset(), 0
        )
        if dependency_batch is None:
            return None, reason or f"依赖任务「{dependency}」仍无法生成前置批次"

        snapshot = _snapshot(dependency_batch, relation)
        if str(dependency_batch.get("status") or "") == "已确认":
            entry["已确认依赖"] = [snapshot]
            entry["等待依赖"] = []
            entry["依赖状态"] = "已确认"
            entry["接续批次"] = ""
            entry["依赖冲突"] = ""
            entry["预计可用时间"] = str(
                dependency_batch.get("确认时间") or entry.get("预计可用时间") or ""
            )
            if str(entry.get("status") or "") == "等待依赖":
                entry["status"] = "待执行"
            _sync_flags(entry)
            return entry, (
                f"批次 {entry.get('批次编号')} 已接上已确认依赖 "
                f"{dependency_batch.get('批次编号')}"
            )

        entry["已确认依赖"] = []
        entry["等待依赖"] = [snapshot]
        entry["依赖状态"] = "接续等待" if relation == "接续" else "同时段等待"
        entry["接续批次"] = (
            str(dependency_batch.get("批次编号") or "") if relation == "接续" else ""
        )
        entry["依赖冲突"] = (
            f"前置批次 {dependency_batch.get('批次编号')} 仍为"
            f"{dependency_batch.get('status')}，继续等待"
        )
        if str(entry.get("status") or "") in ("待执行", "已挂起"):
            entry["status"] = "等待依赖"
        self._estimate_availability(entry, dependency_batch, moment)
        _sync_flags(entry)
        return entry, (
            f"批次 {entry.get('批次编号')} 重新接续前置批次 "
            f"{dependency_batch.get('批次编号')}"
        )

"""抽样偏差与补采缺口领域服务。

职责：
- 基于抽样快照（绑定当时的样本、校验条件、校验字段）识别偏差并生成抽样偏差报告；
- 维护补采缺口台账：缺口身份为「端点 + 来源记录 + 校验字段」，同一补采证据不可重复执行；
- 两次抽样的重叠缺口只记一次成功补采，第二次报告新发现的缺口独立保留；
- 向重放队列同步按字段及来源取数的待办，并在补采成功后交回补采证据；
- 提供报告汇总 / 重放清单 / 实际待补范围三者的对账能力。

台账表（内部表，不进前端模块配置，通过 /api/chain 专用接口查询）：
- deviation_gap   补采缺口台账
- deviation_report 抽样偏差报告
"""
from __future__ import annotations

from typing import Any

from app.store import store


def parse_ratio(value: Any) -> float:
    """把 '10%' / '0.1' / 10 统一解析为小数比例（0.1）。无法解析时回退 0.1。"""
    try:
        text = str(value).strip().replace("%", "")
        ratio = float(text)
    except (TypeError, ValueError):
        return 0.1
    return ratio / 100.0 if ratio > 1 else ratio


def parse_threshold(value: Any) -> float:
    """把偏差阈值 '10%' / '0.1' / 10 统一解析为小数。无法解析时回退 0.1。"""
    return parse_ratio(value)


def gap_key(endpoint: str, source_log_id: Any, field: str) -> str:
    """缺口唯一身份：同一端点同一来源记录同一字段只允许一条补采证据。"""
    return f"{endpoint}#LOG-{source_log_id}#{field}"


def _next_id(rows: list[dict[str, Any]]) -> int:
    return max((int(r.get("id", 0)) for r in rows), default=0) + 1


class DeviationService:
    """抽样偏差报告与补采缺口台账服务（无状态，数据全部落在 store 台账表）。"""

    GAP_TABLE = "deviation_gap"
    REPORT_TABLE = "deviation_report"

    # ---------- 台账查询 ----------

    def list_gaps(self, endpoint: str | None = None) -> list[dict[str, Any]]:
        rows = store.rows(self.GAP_TABLE)
        if endpoint:
            rows = [g for g in rows if g.get("目标端点") == endpoint]
        return rows

    def list_reports(self, endpoint: str | None = None) -> list[dict[str, Any]]:
        rows = store.rows(self.REPORT_TABLE)
        if endpoint:
            rows = [r for r in rows if r.get("目标端点") == endpoint]
        return rows

    def _find_gap(self, key: str) -> dict[str, Any] | None:
        for gap in store.rows(self.GAP_TABLE):
            if gap.get("缺口键") == key:
                return gap
        return None

    # ---------- 缺口登记（去重核心） ----------

    def _register_gap(
        self,
        *,
        endpoint: str,
        row: dict[str, Any],
        field: str,
        threshold: float,
        report_no: str,
    ) -> tuple[dict[str, Any], str]:
        """登记一个偏差缺口。已存在则复用，返回 (缺口, 关系类型)。

        关系类型：
        - new              本次新发现缺口
        - overlap_open     历史报告已发现、尚未补采（重叠，复用既有重放任务，不重复建任务）
        - already_backfilled 历史报告已完成补采（重叠，不再生成可执行补采）
        """
        source_log_id = row["来源日志ID"]
        key = gap_key(endpoint, source_log_id, field)
        existing = self._find_gap(key)
        if existing is not None:
            if report_no not in existing.setdefault("发现于报告", []):
                existing["发现于报告"].append(report_no)
            relation = "already_backfilled" if existing.get("补采状态") == "补采成功" else "overlap_open"
            return existing, relation

        gap_id = _next_id(store.rows(self.GAP_TABLE))
        gap = {
            "id": gap_id,
            "缺口键": key,
            "缺口编号": f"GAP-{str(gap_id).zfill(4)}",
            "目标端点": endpoint,
            "来源记录": row.get("来源记录编号", ""),
            "来源日志ID": source_log_id,
            "校验字段": field,
            "观测值": row.get("字段值"),
            "基线值": row.get("基线值"),
            "偏差率": row.get("偏差率"),
            "命中阈值": threshold,
            "发现于报告": [report_no],
            "关联重放任务": "",
            "补采状态": "待补采",
            "补采证据": "",
            "补采时间": "",
            "status": "待补采",
            "pending": True,
            "abnormal": True,
        }
        store.rows(self.GAP_TABLE).append(gap)
        return gap, "new"

    # ---------- 报告生成 ----------

    def build_report(
        self,
        check: dict[str, Any],
        snapshot: dict[str, Any],
        *,
        round_no: int,
        recheck: bool,
    ) -> dict[str, Any]:
        """依据复验口径（样本快照 + 校验条件 + 字段）生成偏差报告并同步重放待办。"""
        endpoint = str(check.get("目标端点", ""))
        field = str(check.get("校验字段", ""))
        threshold = float(snapshot.get("偏差阈值", parse_threshold(check.get("偏差阈值"))))
        plan_no = str(check.get("计划编号", f"SAMP-{check.get('id', 0)}"))
        report_rows = store.rows(self.REPORT_TABLE)
        report_no = f"GOVE-DEV-{str(check.get('id', 0)).zfill(4)}-R{round_no}"

        sampled_rows = snapshot.get("样本行", [])
        breach_rows = [r for r in sampled_rows if float(r.get("偏差率", 0.0)) > threshold]

        new_gaps: list[dict[str, Any]] = []
        overlap_open_gaps: list[dict[str, Any]] = []
        backfilled_gaps: list[dict[str, Any]] = []
        for row in breach_rows:
            gap, relation = self._register_gap(
                endpoint=endpoint, row=row, field=field,
                threshold=threshold, report_no=report_no,
            )
            if relation == "new":
                new_gaps.append(gap)
            elif relation == "overlap_open":
                overlap_open_gaps.append(gap)
            else:
                backfilled_gaps.append(gap)

        replay_task = self._create_replay_task(
            check=check, snapshot=snapshot, report_no=report_no, round_no=round_no,
            recheck=recheck, threshold=threshold, new_gaps=new_gaps,
        )
        if replay_task is not None:
            for gap in new_gaps:
                gap["关联重放任务"] = replay_task["任务编号"]

        report = {
            "id": _next_id(report_rows),
            "报告编号": report_no,
            "计划编号": plan_no,
            "抽样计划ID": check.get("id"),
            "复验轮次": round_no,
            "报告类型": "阈值变更复验" if recheck else "首次偏差告警",
            "目标端点": endpoint,
            "校验字段": field,
            "抽样比例": snapshot.get("抽样比例", check.get("抽样比例")),
            "偏差阈值": threshold,
            "样本快照ID": snapshot.get("快照编号"),
            "样本来源记录": snapshot.get("来源记录ID", []),
            "校验条件": (
                f"字段[{field}]观测值相对基线[{snapshot.get('基线值')}]偏差率 > {threshold:.2%}"
            ),
            "新发现缺口": [g["缺口键"] for g in new_gaps],
            "重叠待补缺口": [g["缺口键"] for g in overlap_open_gaps],
            "已补采重叠缺口": [g["缺口键"] for g in backfilled_gaps],
            "全部命中缺口": [
                g["缺口键"] for g in (new_gaps + overlap_open_gaps + backfilled_gaps)
            ],
            "新发现数量": len(new_gaps),
            "重叠数量": len(overlap_open_gaps) + len(backfilled_gaps),
            "独立遗漏数量": len(new_gaps),
            "关联重放任务": replay_task["任务编号"] if replay_task else "",
            "status": "已生成",
            "pending": bool(new_gaps or overlap_open_gaps),
            "abnormal": True,
        }
        report_rows.append(report)
        return {"report": report, "replay_task": replay_task}

    def _create_replay_task(
        self,
        *,
        check: dict[str, Any],
        snapshot: dict[str, Any],
        report_no: str,
        round_no: int,
        recheck: bool,
        threshold: float,
        new_gaps: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        """只为未补采的新缺口创建重放任务；重叠缺口复用既有任务，已补采缺口不再建任务。"""
        if not new_gaps:
            return None
        endpoint = str(check.get("目标端点", ""))
        field = str(check.get("校验字段", ""))
        retry_rows = store.rows("retry_queue")
        task_no = f"RETR-DEV-{str(check.get('id', 0)).zfill(4)}-R{round_no}"
        task = {
            "id": _next_id(retry_rows),
            "任务编号": task_no,
            "失败端点": endpoint,
            "失败原因": (
                f"抽样偏差复验:字段={field},阈值={threshold:.2%},"
                f"新缺口{len(new_gaps)}个,报告={report_no}"
            ),
            "重试次数": 0,
            "最大重试": 3,
            "退避策略": "linear:5s",
            "最后重试": "",
            "队列状态": "sample_deviation",
            # 按字段及来源取数的待办范围与复验口径绑定，中断后重试仍可核对范围。
            "关联缺口": [g["缺口键"] for g in new_gaps],
            "取数字段": field,
            "取数来源": [g["来源记录"] for g in new_gaps],
            "复验绑定": {
                "报告编号": report_no,
                "样本快照ID": snapshot.get("快照编号"),
                "校验条件": f"偏差率 > {threshold:.2%}",
                "校验字段": field,
                "复验轮次": round_no,
                "历史阈值兼容": bool(recheck),
            },
            "补采证据": [],
            "status": "待重试",
            "pending": True,
            "abnormal": True,
        }
        retry_rows.append(task)
        return task

    # ---------- 补采执行（幂等，重叠缺口只成功一次） ----------

    def mark_backfilled(self, task: dict[str, Any]) -> dict[str, Any]:
        """按重放任务关联的缺口执行补采并交回证据。

        已是「补采成功」的缺口直接跳过（同一补采证据不可再次执行），
        因此重放中断后重试、或两份样本重叠的缺口，都只会成功补采一次。
        """
        evidence = list(task.get("补采证据", []))
        processed: list[str] = []
        skipped: list[str] = []
        for key in task.get("关联缺口", []):
            gap = self._find_gap(key)
            if gap is None:
                skipped.append(key)
                continue
            if gap.get("补采状态") == "补采成功":
                skipped.append(key)
                continue
            evidence_no = f"EVI-{task.get('任务编号')}-{gap.get('缺口编号')}"
            gap["补采状态"] = "补采成功"
            gap["补采证据"] = evidence_no
            gap["补采时间"] = "2026-11-22"
            gap["status"] = "补采成功"
            gap["pending"] = False
            gap["abnormal"] = False
            if not gap.get("关联重放任务"):
                gap["关联重放任务"] = task.get("任务编号", "")
            evidence.append({
                "证据编号": evidence_no,
                "缺口键": key,
                "目标端点": gap.get("目标端点"),
                "来源记录": gap.get("来源记录"),
                "校验字段": gap.get("校验字段"),
                "取数任务": task.get("任务编号"),
            })
            processed.append(key)
        task["补采证据"] = evidence
        return {"processed": processed, "skipped": skipped, "evidence": evidence}

    # ---------- 对账：报告汇总 / 重放清单 / 实际待补范围 ----------

    def reconcile(self, endpoint: str | None = None) -> dict[str, Any]:
        """核对抽样报告汇总和重放清单是否与实际待补范围一致。"""
        gaps = self.list_gaps(endpoint)
        reports = self.list_reports(endpoint)
        retries = [
            r for r in store.rows("retry_queue")
            if r.get("队列状态") == "sample_deviation"
            and (endpoint is None or r.get("失败端点") == endpoint)
        ]
        gap_by_key = {g["缺口键"]: g for g in gaps}

        problems: list[str] = []
        reported_keys: set[str] = set()
        for report in reports:
            for key in report.get("全部命中缺口", []):
                reported_keys.add(key)
                if key not in gap_by_key:
                    problems.append(f"报告{report['报告编号']}引用了台账不存在的缺口{key}")

        open_gaps = [g for g in gaps if g.get("补采状态") != "补采成功"]
        backfilled = [g for g in gaps if g.get("补采状态") == "补采成功"]

        # 待补缺口必须能在重放清单里找到对应待办；已补采缺口必须持有证据且不在待办中。
        for gap in open_gaps:
            if not gap.get("关联重放任务"):
                problems.append(f"待补缺口{gap['缺口键']}缺少重放待办")
            elif not any(
                r.get("任务编号") == gap["关联重放任务"] and r.get("status") != "已成功"
                for r in retries
            ):
                problems.append(f"待补缺口{gap['缺口键']}的重放待办已不在待重试清单")
        for gap in backfilled:
            if not gap.get("补采证据"):
                problems.append(f"已补采缺口{gap['缺口键']}缺少补采证据")

        retry_open_keys = {
            key for r in retries if r.get("status") != "已成功"
            for key in r.get("关联缺口", [])
        }
        ledger_open_keys = {g["缺口键"] for g in open_gaps}
        if retry_open_keys != ledger_open_keys:
            problems.append(
                f"重放待办范围与台账待补范围不一致:重放={sorted(retry_open_keys)} "
                f"台账={sorted(ledger_open_keys)}"
            )

        return {
            "endpoint": endpoint or "ALL",
            "report_count": len(reports),
            "gap_total": len(gaps),
            "gap_open": len(open_gaps),
            "gap_backfilled": len(backfilled),
            "replay_tasks": len(retries),
            "replay_open_tasks": sum(1 for r in retries if r.get("status") != "已成功"),
            "reported_gap_keys": sorted(reported_keys),
            "ledger_gap_keys": sorted(gap_by_key),
            "consistent": not problems,
            "problems": problems,
        }

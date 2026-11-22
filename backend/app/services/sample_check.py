"""抽样校验领域服务：管理数据抽样校验，与 call_log、govern_report、data_version、cost_account 联动。

增强点（抽样偏差复验与补采范围核对）：
- 「启动采样」按比例对端点调用日志做确定性取样并保存样本快照，逐字段计算偏差率；
- 「标记告警」用样本偏差数据校验待补采范围，生成绑定口径的抽样偏差报告和重放待办；
- 「阈值复验」支持值班员变更阈值后用新口径重算：样本快照独立保存，兼容历史阈值，
  重叠缺口只补采一次、新发现缺口独立保留；
- 复验口径（样本、校验条件、字段、阈值）随报告和重放任务持久化，中断重试仍可核对范围。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.deviation import DeviationService, parse_ratio, parse_threshold
from app.store import store


class SampleCheckService(BaseModuleService):
    MODULE = "sample_check"
    ENTITY = "抽样计划"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["计划编号", "目标端点", "抽样比例", "校验字段", "参考基线", "偏差阈值", "样本数量", "最近采样"]
    REQUIRED_FIELDS = ["计划编号", "目标端点", "抽样比例"]
    STATUS_ORDER = ["待采样", "采样中", "已通过", "偏差告警"]
    # 阈值复验不改变计划状态（仍为偏差告警），在 run_action 中单独处理。
    ACTION_RULES = {"启动采样": "采样中", "确认通过": "已通过", "标记告警": "偏差告警"}
    NEGATIVE_ACTIONS = []

    def __init__(self) -> None:
        self.deviation = DeviationService()

    def run_action(self, entry_id: int, action: str):
        """采样中取样建版本；确认通过更新用量；告警联动偏差报告/重放；阈值复验独立处理。"""
        if action == "阈值复验":
            return self._recheck(entry_id)
        entry, message = super().run_action(entry_id, action)
        if entry is None:
            return entry, message
        if action == "启动采样":
            snapshot = self._sample_from_call_logs(entry)
            self._create_data_version(entry)
            message += "，已从调用日志取样并创建数据版本"
            message += f"，样本快照{snapshot['快照编号']}已绑定校验口径"
        elif action == "确认通过":
            self._update_cost_quota(entry)
            message += "，已更新用量配额"
        elif action == "标记告警":
            result = self._trigger_deviation_report(entry, recheck=False)
            self._trigger_legacy_govern(entry)
            message += (
                f"，已生成抽样偏差报告{result['report']['报告编号']}"
                f"（新缺口{result['report']['新发现数量']}个、"
                f"重叠缺口{result['report']['重叠数量']}个）"
            )
            if result["replay_task"]:
                message += f"并同步重放待办{result['replay_task']['任务编号']}"
        return entry, message

    # ---------- 取样与样本快照 ----------

    def _endpoint_logs(self, endpoint: str) -> list[dict[str, Any]]:
        return [l for l in store.rows("call_log") if l.get("接口端点") == endpoint]

    def _pick_sample_indices(self, total: int, ratio: float) -> list[int]:
        """确定性等距取样：同一端点不同比例的样本天然存在交集，且结果可复现。"""
        count = max(1, int(total * ratio))
        if total == 0:
            return []
        step = max(1, total // count)
        return sorted({(i * step) % total for i in range(count)})

    def _baseline_of(self, check: dict) -> float:
        try:
            return float(str(check.get("参考基线", "100")).replace("%", ""))
        except (TypeError, ValueError):
            return 100.0

    def _field_value(self, log: dict, field: str) -> float | None:
        """按字段从来源记录取数：优先同名字段，其次从请求参数中解析，最后回退返回行数。"""
        if field in log:
            try:
                return float(log[field])
            except (TypeError, ValueError):
                pass
        params = str(log.get("请求参数", ""))
        token = f"{field}="
        if token in params:
            raw = params.split(token, 1)[1].split("&", 1)[0].split(",", 1)[0]
            try:
                return float(raw)
            except ValueError:
                pass
        try:
            return float(log.get("返回行数", 0))
        except (TypeError, ValueError):
            return None

    def _sample_from_call_logs(self, check: dict) -> dict[str, Any]:
        """从调用日志中确定性抽取样本，逐字段计算偏差率并保存样本快照。"""
        endpoint = check.get("目标端点", "")
        field = str(check.get("校验字段", "返回行数"))
        ratio = parse_ratio(check.get("抽样比例", 0.1))
        threshold = parse_threshold(check.get("偏差阈值", 0.1))
        baseline = self._baseline_of(check)
        logs = self._endpoint_logs(endpoint)
        indices = self._pick_sample_indices(len(logs), ratio)

        sampled_rows: list[dict[str, Any]] = []
        source_ids: list[int] = []
        for idx in indices:
            log = logs[idx]
            value = self._field_value(log, field)
            if value is None:
                continue
            deviation_rate = abs(value - baseline) / baseline if baseline else 0.0
            sampled_rows.append({
                "来源日志ID": log.get("id"),
                "来源记录编号": log.get("记录编号"),
                "字段": field,
                "字段值": value,
                "基线值": baseline,
                "偏差率": round(deviation_rate, 6),
                "超阈": deviation_rate > threshold,
            })
            source_ids.append(log.get("id"))

        check["样本数量"] = len(sampled_rows)
        check["最近采样"] = "2026-11-22"

        snapshots = check.setdefault("抽样快照", [])
        round_no = len(snapshots) + 1
        snapshot = {
            "快照编号": f"SNAP-{str(check['id']).zfill(4)}-R{round_no}",
            "复验轮次": round_no,
            "抽样比例": ratio,
            "偏差阈值": threshold,
            "校验字段": field,
            "基线值": baseline,
            "样本数量": len(sampled_rows),
            "来源记录ID": source_ids,
            "样本行": sampled_rows,
        }
        snapshots.append(snapshot)
        check["当前快照编号"] = snapshot["快照编号"]
        return snapshot

    def _create_data_version(self, check: dict):
        ver_rows = store.rows("data_version")
        sid = str(check["id"]).zfill(4)
        ver_rows.append({
            "id": max((int(r.get("id", 0)) for r in ver_rows), default=0) + 1,
            "版本编号": f"DATA-SAMP-{sid}",
            "数据来源": check.get("目标端点", ""),
            "行数统计": check.get("样本数量", 0),
            "版本标签": "sample_check_baseline",
            "Schema摘要": f"抽样{check.get('抽样比例', '10')}%",
            "创建原因": f"抽样计划{sid}采样完成",
            "依赖版本": check.get("参考基线", ""),
            "创建时间": "2026-11-22",
            "status": "可用", "pending": True, "abnormal": False,
        })

    def _update_cost_quota(self, check: dict):
        """通过后更新用量账单中关联端点的消费预估。"""
        endpoint = check.get("目标端点", "")
        for cost in store.rows("cost_account"):
            if cost.get("提供方") and endpoint in str(cost.get("提供方", "")):
                cost["调用次数"] = int(cost.get("调用次数", 0)) + check.get("样本数量", 0)
                cost["费用预估"] = float(cost.get("费用预估", 0.0)) + check.get("样本数量", 0) * 0.01

    # ---------- 偏差报告与阈值复验 ----------

    def _trigger_deviation_report(self, check: dict[str, Any], *, recheck: bool) -> dict[str, Any]:
        snapshots = check.setdefault("抽样快照", [])
        snapshot = snapshots[-1]
        round_no = int(snapshot["复验轮次"])
        return self.deviation.build_report(check, snapshot, round_no=round_no, recheck=recheck)

    def _trigger_legacy_govern(self, check: dict):
        """保留原治理报告联动，治理报告条目仍可被覆盖率统计与既有页面消费。"""
        sid = str(check["id"]).zfill(4)
        endpoint = check.get("目标端点", "")
        gov_rows = store.rows("govern_report")
        gov_rows.append({
            "id": max((int(r.get("id", 0)) for r in gov_rows), default=0) + 1,
            "报告编号": f"GOVE-SAMP-{sid}",
            "评估范围": f"抽样偏差告警:{endpoint}",
            "合规指标": "数据质量一致性",
            "当前得分": 55.0,
            "改进事项": f"端点{endpoint}抽样偏差超出阈值{check.get('偏差阈值', '')}",
            "评估周期": "2026-Q4",
            "负责人": "数据质量组",
            "生成时间": "2026-11-22",
            "status": "待生成", "pending": True, "abnormal": True,
        })

    def _recheck(self, entry_id: int, *, new_ratio: Any = None, new_threshold: Any = None) -> tuple[dict[str, Any] | None, str]:
        """值班员变更阈值后复验：新口径独立取样存快照，历史阈值与旧报告保持不变。"""
        check = store.find(self.MODULE, entry_id)
        if check is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        # 入参为小数口径（0.05 表示 5%），统一转成百分数字符串存储，避免 '0.05%' 被解析成 0.0005。
        if new_ratio is not None:
            check["抽样比例"] = f"{float(new_ratio) * 100:g}%"
        if new_threshold is not None:
            check["偏差阈值"] = f"{float(new_threshold) * 100:g}%"
        # 复验始终留在「偏差告警」状态，避免覆盖首轮结论。
        check["status"] = "偏差告警"
        check["pending"] = True
        check["abnormal"] = True
        snapshot = self._sample_from_call_logs(check)
        result = self._trigger_deviation_report(check, recheck=True)
        report = result["report"]
        message = (
            f"阈值复验完成，新快照{snapshot['快照编号']}按"
            f"{snapshot['抽样比例']:.0%}抽样、阈值{snapshot['偏差阈值']:.2%}校验；"
            f"报告{report['报告编号']}新发现独立遗漏{report['新发现数量']}个、"
            f"重叠缺口{report['重叠数量']}个（不重复补采）"
        )
        if result["replay_task"]:
            message += f"，新增重放待办{result['replay_task']['任务编号']}"
        else:
            message += "，无新增可执行补采（重叠缺口沿用既有待办或已补采）"
        return check, message

    def calc_sampling_coverage(self) -> dict:
        """跨模块抽样覆盖率统计。"""
        checks = store.rows("sample_check")
        versions = [v for v in store.rows("data_version") if "SAMP" in str(v.get("版本编号", ""))]
        govern = [g for g in store.rows("govern_report") if "SAMP" in str(g.get("报告编号", ""))]
        dev_reports = self.deviation.list_reports()
        gaps = self.deviation.list_gaps()
        return {
            "total_plans": len(checks),
            "active_sampling": sum(1 for c in checks if c.get("status") == "采样中"),
            "deviations": sum(1 for c in checks if c.get("status") == "偏差告警"),
            "version_snapshots": len(versions),
            "govern_items": len(govern),
            "deviation_reports": len(dev_reports),
            "gap_total": len(gaps),
            "gap_open": sum(1 for g in gaps if g.get("补采状态") != "补采成功"),
            "gap_backfilled": sum(1 for g in gaps if g.get("补采状态") == "补采成功"),
        }

"""端到端验证：抽样偏差复验 → 补采缺口去重 → 重放幂等 → 三方对账。

场景（同一端点 20 条来源记录，基线 100，校验字段=返回行数）：
- 先按 10% 抽样、10% 偏差阈值告警；
- 值班员改阈值后按 5% 抽样、5% 阈值复验；
- 两个样本同时涵盖的遗漏只补采一次，第二个样本新发现的遗漏仍可补齐；
- 模拟重放中断后重试，复验口径仍可核对范围，补采证据不重复；
- 抽样报告汇总、重放清单与实际待补范围三方一致。

直接用服务层驱动内存 store，运行：python -I tests/e2e_deviation_recheck.py
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.deviation import DeviationService, gap_key
from app.services.retry_queue import RetryQueueService
from app.services.sample_check import SampleCheckService
from app.store import store

ENDPOINT = "ENDP-CSV-0001"
FIELD = "返回行数"
PASS = "✅"
FAIL = "❌"
failures: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    mark = PASS if condition else FAIL
    print(f"{mark} {name}" + (f" —— {detail}" if detail else ""))
    if not condition:
        failures.append(name)


def setup_logs() -> None:
    """20 条来源记录：index0 偏差 12%（两阈值都超），index10 偏差 6%（仅 5% 超），其余合规。"""
    rows = store.rows("call_log")
    for i in range(20):
        if i == 0:
            value = 112  # 偏差率 12%
        elif i == 10:
            value = 106  # 偏差率 6%
        else:
            value = 100
        rows.append({
            "id": i + 1,
            "记录编号": f"CALL-E2E-{i + 1:04d}",
            "接口端点": ENDPOINT,
            "请求参数": f"seq={i}",
            "响应状态": "200",
            "响应耗时(ms)": 80,
            "返回行数": value,
            "错误信息": "",
            "调用时间": "2026-11-22",
            "status": "成功", "pending": False, "abnormal": False,
        })


def find_task(no: str) -> dict:
    for task in store.rows("retry_queue"):
        if task.get("任务编号") == no:
            return task
    raise AssertionError(f"重放任务 {no} 不存在")


def main() -> int:
    setup_logs()
    sample_svc = SampleCheckService()
    retry_svc = RetryQueueService()
    deviation_svc = DeviationService()

    # 1. 登记抽样计划：10% 抽样、10% 阈值
    plan, missing = sample_svc.create_entry({
        "计划编号": "SAMP-E2E-01",
        "目标端点": ENDPOINT,
        "抽样比例": "10%",
        "校验字段": FIELD,
        "参考基线": "100",
        "偏差阈值": "10%",
    })
    check("抽样计划登记成功", plan is not None and not missing, str(missing))
    plan_id = plan["id"]

    # 2. 启动采样：确定性等距抽样，样本应为 index {0, 10}
    sample_svc.run_action(plan_id, "启动采样")
    snap1 = plan["抽样快照"][-1]
    check(
        "首轮按10%抽取2条样本且含来源0/10",
        snap1["样本数量"] == 2 and set(snap1["来源记录ID"]) == {1, 11},
        str(snap1["来源记录ID"]),
    )
    check("首轮快照绑定10%阈值与字段", snap1["偏差阈值"] == 0.10 and snap1["校验字段"] == FIELD)

    # 3. 标记告警：用样本偏差校验待补范围并生成报告 + 重放待办
    sample_svc.run_action(plan_id, "标记告警")
    reports = deviation_svc.list_reports(ENDPOINT)
    check("首轮生成1份偏差报告", len(reports) == 1)
    r1 = reports[0]
    g0 = gap_key(ENDPOINT, 1, FIELD)
    g10 = gap_key(ENDPOINT, 11, FIELD)
    check(
        "首轮报告仅圈中12%超阈样本(6%未超10%阈值)",
        set(r1["全部命中缺口"]) == {g0},
        str(r1["全部命中缺口"]),
    )
    # 10% 阈值下只有 12% 超阈，6% 不超
    check("10%阈值仅12%样本命中", r1["新发现数量"] == 1 and r1["独立遗漏数量"] == 1)
    task1 = find_task(r1["关联重放任务"])
    check("首轮重放任务按字段及来源取数", task1["取数字段"] == FIELD and task1["取数来源"] == ["CALL-E2E-0001"])
    check("重放任务绑定复验口径(快照/条件/字段)", task1["复验绑定"]["样本快照ID"] == snap1["快照编号"])

    # 4. 值班员变更阈值：按 5% 抽样、5% 复验（样本与首轮有交集 index0）
    entry, msg = sample_svc._recheck(plan_id, new_ratio=0.05, new_threshold=0.05)
    snap2 = entry["抽样快照"][-1]
    check("复验保留首轮快照、新增独立快照", len(entry["抽样快照"]) == 2 and snap2["偏差阈值"] == 0.05)
    check("5%抽样样本与首轮有交集(index0)", set(snap2["来源记录ID"]) == {1})
    check("计划仍为偏差告警且历史阈值可追溯", entry["status"] == "偏差告警" and snap1["偏差阈值"] == 0.10, msg)

    r2 = deviation_svc.list_reports(ENDPOINT)[-1]
    check("第二份报告类型为阈值变更复验", r2["报告类型"] == "阈值变更复验")
    check(
        "第二份报告圈中首份样本(index0)为重叠缺口",
        r2["重叠待补缺口"] == [g0] and r2["新发现缺口"] == [],
        f"overlap={r2['重叠待补缺口']} new={r2['新发现缺口']}",
    )
    # index10(6%) 不在 5% 样本内，所以复验本身不新发现；为验证“独立遗漏仍可补齐”，
    # 再用覆盖 index10 的样本口径复验一次（比例 10%、阈值 5%）。
    entry, _ = sample_svc._recheck(plan_id, new_ratio=0.10, new_threshold=0.05)
    r3 = deviation_svc.list_reports(ENDPOINT)[-1]
    check(
        "第三份报告发现独立遗漏index10且重叠index0不重复",
        set(r3["新发现缺口"]) == {g10} and r3["重叠待补缺口"] == [g0],
        f"new={r3['新发现缺口']} overlap={r3['重叠待补缺口']}",
    )
    check("重叠缺口未生成第二份可执行重放任务", r2["关联重放任务"] == "")
    task2 = find_task(r3["关联重放任务"])
    check("独立遗漏生成独立重放待办", task2["关联缺口"] == [g10])

    # 5. 缺口台账：同一身份只有一条，发现于多份报告
    gaps = {g["缺口键"]: g for g in deviation_svc.list_gaps(ENDPOINT)}
    check("台账仅2条缺口(重叠未重复登记)", set(gaps) == {g0, g10} and len(gaps) == 2)
    check(
        "重叠缺口来源可追溯到全部相关报告(首报+两次复验)",
        set(gaps[g0]["发现于报告"]) == {r1["报告编号"], r2["报告编号"], r3["报告编号"]},
        str(gaps[g0]["发现于报告"]),
    )
    check("独立遗漏仅由第三份报告发现", gaps[g10]["发现于报告"] == [r3["报告编号"]])
    check("两条缺口均处于待补采", all(g["补采状态"] == "待补采" for g in gaps.values()))

    # 6. 模拟重放中断：任务1先“加入队列”（重试中），再手动重试成功
    retry_svc.run_action(task1["id"], "加入队列")
    retry_svc.run_action(task1["id"], "手动重试")
    gaps = {g["缺口键"]: g for g in deviation_svc.list_gaps(ENDPOINT)}
    check("重叠缺口index0只成功补采一次并持有证据", gaps[g0]["补采状态"] == "补采成功" and bool(gaps[g0]["补采证据"]))
    evidence_after_first = list(task1["补采证据"])
    check("任务1交回1份补采证据", len(evidence_after_first) == 1)

    # 7. 中断后重试：对已成功任务再次手动重试，必须幂等、不产生重复证据
    retry_svc.run_action(task1["id"], "手动重试")
    check("重试不重复补采(证据仍为1份)", len(task1["补采证据"]) == 1)
    check("重试后缺口证据编号不变", gaps[g0]["补采证据"] == evidence_after_first[0]["证据编号"])

    # 8. 独立遗漏任务补采成功
    retry_svc.run_action(task2["id"], "加入队列")
    retry_svc.run_action(task2["id"], "手动重试")
    gaps = {g["缺口键"]: g for g in deviation_svc.list_gaps(ENDPOINT)}
    check("第二样本新发现遗漏补齐", gaps[g10]["补采状态"] == "补采成功" and bool(gaps[g10]["补采证据"]))
    check("两个缺口证据各自独立", gaps[g0]["补采证据"] != gaps[g10]["补采证据"])

    # 9. 对账：报告汇总 / 重放清单 / 实际待补范围一致
    recon = deviation_svc.reconcile(ENDPOINT)
    check("三方对账一致(无问题)", recon["consistent"], str(recon["problems"]))
    check("待补缺口清零、两缺口均补采", recon["gap_open"] == 0 and recon["gap_backfilled"] == 2)
    check("重放清单任务数与报告待办匹配", recon["replay_tasks"] == 2 and recon["replay_open_tasks"] == 0)
    check("报告缺口并集与台账缺口完全一致", set(recon["reported_gap_keys"]) == set(recon["ledger_gap_keys"]))

    # 10. 覆盖率统计纳入偏差报告与缺口
    coverage = sample_svc.calc_sampling_coverage()
    check(
        "覆盖率统计含3份报告/2缺口/2已补采",
        coverage["deviation_reports"] == 3 and coverage["gap_total"] == 2 and coverage["gap_backfilled"] == 2,
        str(coverage),
    )

    print("\n复验口径留痕：")
    for report in deviation_svc.list_reports(ENDPOINT):
        print(
            f"  - {report['报告编号']} [{report['报告类型']}] 快照={report['样本快照ID']} "
            f"条件={report['校验条件']} 新={report['新发现数量']} 重叠={report['重叠数量']}"
        )

    if failures:
        print(f"\n{FAIL} 失败 {len(failures)} 项：{failures}")
        return 1
    print(f"\n{PASS} 全部断言通过：重叠缺口只补采一次，独立遗漏补齐，复验口径可追溯，三方对账一致。")
    return 0


if __name__ == "__main__":
    sys.exit(main())

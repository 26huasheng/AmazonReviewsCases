# Repository Cleanup TODO

目标不是重做 benchmark，而是让仓库能够清楚、可复现地表达已经发布的 `Electronics_v1_cases`。

## P0 — must fix without changing benchmark semantics

- [ ] 找回/确认本地生成 `users/histories/events.jsonl` 中 `review_title / review_text` 的实际代码。
- [ ] 把 review enrichment 接回仓库，使其只对已确定的 pre-t0 history events 补源字段，不改变 event/user/product/t0/GT1。
- [ ] 梳理 `market_build/pipeline.py` 与 `market_build/behavior_graph/*` 的版本拼接，修复当前 import/API 不一致。
- [ ] 确认当前 focal selection 实际消费的 behavior-component schema，并只保留“behavior graph → focal diversity”的 production path。
- [ ] 对 `scripts/package_market_cases.py` 加 release-contract validation：无空 time_box、history event < t0、choice in local shelf、Case shelf 为 surviving-focal union。

## P1 — classify before touching

- [ ] 对所有 Python 文件/函数做 `CURRENT / LEGACY / EXPERIMENTAL / UNCERTAIN` inventory。
- [ ] `benchmark_export/` 标记 legacy exporter；不要作为 v1 release path。
- [ ] `benchmark_split/` 标记为旧 Case-key 设计，等待 multi-focal redesign。
- [ ] `evaluation/` 标记为 GT2-era / experimental evaluator。
- [ ] `case_build/population/`、`population_scan/` 标记 GT2 engineering，不作为 GT1 dependency。
- [ ] behavior graph 的 focal-centered competitor-selection branch 标记 legacy，并追踪是否仍有本地入口引用。
- [ ] `external_signals/` 标记 optional/experimental，不作为 v1 hard dependency。

## P2 — tests after lineage is known

- [ ] 固定 Case identity test：同一 Market×time_box 最多一个 Case。
- [ ] focal selection deterministic test。
- [ ] competitor eligibility/ranking test。
- [ ] GT1 reducer deterministic tie-break test。
- [ ] GT1 history>=3 / recency<=365 test。
- [ ] Quality focal-first removal + Case shelf rebuild test。
- [ ] history review-text enrichment must not change row identity/count test。
- [ ] package tree regression test against `SCHEMA.md`。

## Do not do automatically

- 不重跑/覆盖 `Electronics_v1_cases` Release。
- 不因代码清理改变 schema。
- 不把旧 `case_users / market_population / GT2` 塞回 GT1。
- 不用 behavior graph 重新选 competitor。
- 不增加 future-success quality gate。
- 不删除 `UNCERTAIN` 代码；先报告调用链与证据。

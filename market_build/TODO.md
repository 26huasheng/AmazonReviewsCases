# market_build TODO

- [ ] inventory `pipeline.py`、`behavior_graph/*` 当前 import/producer/consumer。
- [ ] 找到本地与 Electronics v1 实际一致的 behavior-component producer。
- [ ] 统一 production component schema，使 `case_build/focal_selection.py` 能明确消费。
- [ ] 保证 behavior graph production role 只有 focal diversity。
- [ ] 保留 `market_population` 但标记 GT2 experimental；不得成为 GT1 dependency。
- [ ] 补回/确认 full-review → pre-t0 history text enrichment path。
- [ ] enrichment 前后 event identity、行数、t0 cutoff 必须有 regression check。

在以上审计完成前，不删除任何无法确认 lineage 的实现。

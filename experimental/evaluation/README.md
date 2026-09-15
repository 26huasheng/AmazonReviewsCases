# evaluation — Experimental / GT2-era Evaluator

现有 evaluator 同时要求 `choice_truth / population_truth / market_truth`，并计算 GT2 outcome、market entry、demand/ranking 等指标。当前 `Electronics_v1_cases` 正式 Release 只冻结 GT1 package，没有把 GT2 population/none truth 打包进去。

因此本目录不是当前 Release 的可直接运行正式 evaluator。

未来 evaluation 应至少区分：

```text
GT1 conditional individual choice
product-level aggregates derived from the chosen benchmark population/definition
GT2 ex-ante population/none task (if reintroduced)
```

在 GT2 重新冻结前，不要让这里的旧 assumptions 反向改变 GT1 schema 或 Quality。

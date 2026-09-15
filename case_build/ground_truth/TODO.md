# ground_truth TODO

- [ ] 保持 GT1 不依赖 `case_users`。
- [ ] 固定 `first_observed_event` tie-break：event timestamp 后用 product_id 确定性排序。
- [ ] 回归检查 choice ∈ focal local shelf 且 timestamp 在 evaluation window。
- [ ] 回归检查 history>=3、recency<=365。
- [ ] 将 GT2 branch 在 CLI/docs 中明确标记 experimental，不影响 GT1 run。
- [ ] 不把 future `review_text` 设成 choice_truth 必需字段。

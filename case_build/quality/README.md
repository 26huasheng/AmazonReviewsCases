# case_build/quality

Quality 是两层：

1. **focal quality**：按 `case_id + focal_id` 检查结构与 GT1 完整性，决定每个 focal 是否保留。
2. **case quality**：同一 Case 删掉不合格 focal 后，只要还剩 `>=1` 个 accepted focal 就接受该 Case，并重建最终 shelf。

GT1 由 `ground_truth_gt1` 按冻结口径生成；本层只消费，不重新定义、不截断人数。

```text
case_focals
+ focal_competitors (selected)
+ case_shelf
+ GT1 choice_truth
        ↓
quality_focal_metrics.parquet
quality_focal_decisions.parquet
        ↓ 过滤 focal，重建 Case
accepted_focals / rejected_focals
accepted_cases / rejected_cases
accepted_focal_competitors
accepted_case_shelf
```

## 正式门槛（默认冻结）

```text
evaluation_window_complete = true
selected_competitor_count in [6, 16]
GT1 合格用户数 >= 20
history_product_count >= 3
days_since_last_event <= 365
```

GT1 **没有**最大人数。全部 GT1 用户保留。

未来 choice / demand / post90 / review activity **只统计，不作为 acceptance gate**。
即使窗口内没人选 focal 自己，只要 competitor∈[6,16]、GT1 users≥20、结构完整，也可以通过。

默认不启用：

```text
min_market_positive_users
max_none_rate
min_focal_demand_count
min_post90_rating_count
min_market_pre_t0_review_count
require_review_activity_truth
max_gt1_users
```

## Focal 结构检查

任一失败则该 focal reject：

- focal 必须在对应 Case shelf 上
- competitor ≠ focal，且同一 focal 下不重复
- selected competitor 数与关系表一致，且 ∈[6,16]
- 每个 selected competitor 在 Case shelf 上
- 每个 selected competitor 满足 `first_rating_date < t0` 且 `last_rating_date >= t0`
- local shelf = focal ∪ 该 focal 的 selected competitors

## GT1 完整性检查

- GT1 user 数 = `choice_truth` 中该 focal 去重 `user_id`
- `(case_id, focal_id, user_id)` 唯一
- choice `product_id` 非空且属于该 focal 的 local shelf
- `event_time ∈ [t0, evaluation_end_exclusive)`
- `history_product_count >= 3` 且 `days_since_last_event <= 365`
- first_observed_event reducer 结果唯一
- 若有 raw window events，choice 必须能追溯到窗口事件
- GT1 合格用户数 ≥ 20

## Case 层

不把多个 focal 的 GT1 人数相加做门槛。

```text
accepted_focal_count >= 1  → Case accepted
accepted_focal_count = 0   → Case rejected
```

多 focal Case 中某个 focal reject 只删除该 focal。最终 `accepted_case_shelf` 是剩余 accepted focals 及其 selected competitors 的 `product_id` union，不为被删 focal 残留商品。

主键：focal = `case_id + focal_id`；case = `case_id`。不再使用 `focal_rows=1` 或 `case_candidate_id` 作为 Quality 主键。

## 运行

```bash
python -m case_build.quality.cli \
  --cases /path/to/cases.parquet \
  --case-focals /path/to/case_focals.parquet \
  --case-shelf /path/to/case_shelf.parquet \
  --focal-competitors /path/to/focal_competitors.parquet \
  --choice-truth /path/to/ground_truth_gt1/choice_truth.parquet \
  --gt1-users /path/to/gt1_users.parquet \
  --gt1-raw-outcomes /path/to/_work/gt1_raw_outcomes.parquet \
  --gt1-shelf-events /path/to/_work/gt1_shelf_events.parquet \
  --timeline /path/to/market_product_timeline.parquet \
  --output-dir outputs/quality
```

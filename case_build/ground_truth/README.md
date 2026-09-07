# case_build/ground_truth

GT1 不再从预先抽的 `case_users` 里筛。对每个 focal：

```text
局部货架 = focal + selected competitors
窗口 = [t0, t0+90)
窗口内对货架任意商品有真实评分的用户
再过滤 t0 前 history>=3 且 recency<=365
first_observed_event → choice_truth
```

GT2 仍可用可选的 `case_users` union（Market + background）看 product / none。

```text
cases
+ case_users
+ case_shelf
+ canonical_user_events
        ↓
future_market_events.parquet       # 保留全部真实命中事件
        ↓
positive_user_outcomes.parquet     # 一用户压成一个商品结果
        ↓
├── population_truth.parquet       # GT2：全部用户 -> product / none
├── choice_truth.parquet           # GT1：正例用户 -> product
└── market_truth.parquet           # GT2 聚合后的需求量 / 份额 / 排名
```

如果提供 `rating_daily_summary.parquet`，还会额外生成：

```text
review_activity_truth.parquet
```

它对完整 shelf 的 future 评论量直接做排名，迁自 v5 `market_ranking_truth` 的聚合思想，作为辅助商品级真值 / 质量信号，不替代 GT2。

## 1. future_market_events

只保留：

```text
用户属于 case_users
商品属于 case_shelf
evaluation_start <= event < evaluation_end_exclusive
```

一个用户在未来窗口碰多个 shelf 商品时，这张表会保留全部事件，不在扫描阶段丢信息。

## 2. 一用户一个 outcome

当前实现的显式 policy：

```text
first_observed_event
```

即按 `event_timestamp` 排序取用户在当前 Case shelf 的第一条观测事件，同一时间再按 `product_id` 确定性打破并列。

policy 会写进中间表和 GT，之后如果研究口径改成别的规则，可以从 `future_market_events` 重算，不必重新扫原始数据。

## 3. GT2：完整人口选择

`population_truth.parquet` 覆盖 `case_users` 全部用户：

```text
user_id -> outcome_product_id / NULL
```

`NULL` 就是 `none`：evaluation window 内没有观测到该用户对当前 shelf 商品产生目标交互。

## 4. GT1：条件商品选择

GT1 直接取 GT2 正例：

```text
outcome_product_id IS NOT NULL
```

得到：

```text
user_id -> target_product_id
```

因此 GT1 / GT2 共享同一套未来事实，不会出现两套口径。

## 5. 商品级 truth

`market_truth.parquet` 对 GT2 正例聚合：

```text
product_id
demand_count
demand_share
rank
population_count
market_positive_count
none_count
market_entry_rate
```

最终 benchmark export 中保留 schema 要求的核心四列；额外统计留在构建长表里做质量审计。

## 6. 运行

```bash
python -m case_build.ground_truth.cli \
  --cases /path/to/cases.parquet \
  --case-users outputs/case_population/case_users.parquet \
  --case-shelf outputs/case_build/shelf/case_shelf.parquet \
  --canonical-user-events outputs/market_build/canonical_user_events.parquet \
  --rating-daily-summary /path/to/rating_daily_summary.parquet \
  --output-dir outputs/ground_truth
```

未冻结事项见 [`TODO.md`](TODO.md)。

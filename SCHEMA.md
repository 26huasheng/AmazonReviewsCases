# SEMS Benchmark Data Schema

本文件定义 `AmazonReviewsCases` 最终 benchmark 产物。主结构固定为 **Market → Cases**。Case 是 **Final Market × time_box**：一个 Case 可含 1 个或少数几个 focal，共享 users 与 union shelf；GT 保留到 focal 粒度。

---

# 1. 最终目录

```text
benchmark_data/
├── benchmark_manifest.json
├── validation_report.json
├── markets/
│   └── <market_id>/
│       ├── market_manifest.json
│       ├── products.parquet
│       ├── population/
│       │   ├── users.parquet
│       │   └── interactions.parquet
│       └── cases/
│           └── <case_id>/
│               ├── case_manifest.json
│               ├── shelf.parquet
│               ├── users.parquet
│               └── ground_truth/
│                   ├── choice_truth.parquet
│                   ├── population_truth.parquet
│                   └── market_truth.parquet
└── splits/
    ├── learning.json
    ├── validation.json
    └── evaluation.json
```

---

# 2. Market 层

## 2.1 `market_manifest.json`

```json
{
  "market_id": "market_001",
  "market_name": "smart_watch",
  "source_partition": "Electronics",
  "source_market_ids": ["local_xxx", "local_yyy"],
  "source_category_paths": [
    ["Electronics", "Wearable Technology", "Smartwatches"]
  ],
  "n_products": 37,
  "n_population_users": 58214,
  "case_ids": ["case_001", "case_002"]
}
```

| 字段 | 含义 |
|---|---|
| `market_id` | Final Market ID |
| `market_name` | 规范化后的 Market 名称 |
| `source_partition` | Amazon Reviews 大类 |
| `source_market_ids` | cross-path 合并前来源 local market IDs |
| `source_category_paths` | 来源 category paths |
| `n_products` | 长期商品 universe 大小 |
| `n_population_users` | Market shared population 大小 |
| `case_ids` | 当前 Market 下 accepted Case IDs |

Market manifest 不保存某个具体 `t0` 的 focal / competitor 角色。

## 2.2 `products.parquet`

一行一个长期商品：

| 字段 | 含义 |
|---|---|
| `product_id` | 商品键 |
| `title` | 商品标题 |
| `source_partition` | Amazon 大类 |
| `category_path` | category path |
| `first_review_date` | 首评时间 |
| `first_available_date` | metadata 可获得时保存 |
| `store` | metadata store / 展示品牌字段，可获得时保存 |
| `metadata_available` | 是否存在 metadata |
| `metadata_snapshot_price` | Amazon metadata 快照价，可选；不等于历史 t0 价格 |

---

# 3. Market Population

## 3.1 `population/users.parquet`

最终最小字段：

```text
user_id
```

Market 下多个 Case 共用这份 shared population。Case 再从这里选自己的用户子集。

## 3.2 `population/interactions.parquet`

Market population 可用的共享用户轨迹：

| 字段 | 含义 |
|---|---|
| `user_id` | 用户键 |
| `product_id` | 商品键 |
| `timestamp` | 观测事件时间 |
| `rating` | 星级，可为空 |
| `source_partition` | 事件来源大类 |
| `verified_purchase` | 源数据可获得时保存 |

同一用户历史不在 Case 下重复存储。运行某个 Case 时按它自己的 `t0` 使用历史部分。

---

# 4. Case 层

## 4.1 `case_manifest.json`

```json
{
  "case_id": "case_001",
  "market_id": "market_001",
  "time_box_id": "2022-H2",
  "n_focals": 2,
  "population_cutoff": "2022-07-03",
  "n_shelf_products": 18,
  "n_selected_users": 2000,
  "quality_status": "accepted"
}
```

时间窗口统一使用半开区间：

```text
[evaluation.start, evaluation.end_exclusive)
```

| 字段 | 含义 |
|---|---|
| `case_id` | Case ID = hash(source_partition, market_id, time_box_id) |
| `market_id` | 所属 Final Market |
| `time_box_id` | 时间段 |
| `n_focals` | 本 Case 选中的 focal 数 |
| `population_cutoff` | 共享用户历史截断，当前 `min(focal.t0)` |
| `n_shelf_products` | union shelf 商品数 |
| `n_selected_users` | 本 Case 固定用户数 |
| `quality_status` | 最终质量状态 |

每个 focal 自己的 `t0` / evaluation window 在 `case_focals` / GT 行上，不挤进单一 Case manifest 字段。

## 4.2 `shelf.parquet`

| 字段 | 含义 |
|---|---|
| `product_id` | 商品键 |
| `is_focal` | 是否为本 Case 的某个 focal |
| `is_competitor` | 是否为某个 focal 的 selected competitor；可与 `is_focal` 同时为真 |
| `pre_t0_review_count` | t0 前累计评论 / 评分事件量 |
| `pre_t0_rating_mean` | t0 前平均评分，可为空 |
| `price_at_t0` | 历史 t0 价格，可获得时保存 |
| `pre_t0_recent_review_count` | 最近活动窗口内评论量，可作为扩展字段 |
| `metadata_snapshot_price` | metadata 快照价，可作为扩展字段 |

一个 competitor 的当前基础时间资格：

```text
同一 Market
product_id != focal
first_review_date < t0
last_review_date >= t0
```

## 4.3 `users.parquet`

最终只需要：

```text
user_id
```

用户 eligibility / sampling 特征属于构建审计表，不复制进最终 Case。

---

# 5. Ground Truth

## 5.1 GT1 — `ground_truth/choice_truth.parquet`

**Conditional Individual Choice**：已知用户在 evaluation window 内发生了当前 shelf 的目标交互，实际目标商品是什么。

| 字段 | 含义 |
|---|---|
| `case_id` / `focal_id` | Case × focal |
| `user_id` | 用户键 |
| `product_id` | 确定目标商品 |
| `event_timestamp` | 被 outcome policy 选中的目标事件时间 |

GT1 不包含 `none`。

## 5.2 GT2 — `ground_truth/population_truth.parquet`

覆盖 Case `users.parquet` 中全部用户：

| 字段 | 含义 |
|---|---|
| `case_id` / `focal_id` | Case × focal |
| `user_id` | 用户键 |
| `outcome_product_id` | 真实商品结果；无目标交互时为空 |
| `event_timestamp` | 有商品 outcome 时对应事件时间，否则为空 |

语义：

```text
outcome_product_id = product_id  -> 命中 shelf 商品
outcome_product_id = NULL        -> none
```

`none` 表示公开 Amazon Reviews 观测数据中没有看到该用户在 evaluation window 对当前 shelf 产生目标交互，不代表完整订单世界里一定没有购买。

同一用户 future window 有多条 shelf 事件时，由显式 `outcome_policy` 压成一个商品。构建层会保留全部 future events 供重新计算；正式 benchmark 需要冻结 policy 版本。

## 5.3 `ground_truth/market_truth.parquet`

由 GT2 聚合：

| 字段 | 含义 |
|---|---|
| `case_id` / `focal_id` | Case × focal |
| `product_id` | 该 focal 局部货架商品 |
| `demand_count` | GT2 中命中该商品的用户数 |
| `demand_share` | 在 market-positive 用户中的份额 |
| `rank` | 按 `demand_count` 排名，确定性 tie-break 用 `product_id` |

`none` 不作为商品参加排名。

GT1 必须与 GT2 正例完全一致。

---

# 6. 辅助 / 派生 Truth

构建过程中可以存在但不要求进入最终 canonical GT 目录：

```text
future_market_events.parquet
positive_user_outcomes.parquet
review_activity_truth.parquet
weekly / cumulative truth（以后可选）
```

其中 `review_activity_truth` 对完整 shelf 的 future 评论量直接做排名，属于商品侧辅助真值 / 质量信号；它不替代 GT2 聚合的 `market_truth`。

---

# 7. Benchmark Split

Split 文件只保存引用：

```json
{
  "split_name": "evaluation",
  "markets": [
    {
      "market_id": "market_001",
      "case_ids": ["case_003"],
      "evaluation_regime": "seen_market_temporal"
    },
    {
      "market_id": "market_009",
      "case_ids": ["case_080", "case_081"],
      "evaluation_regime": "unseen_market"
    }
  ]
}
```

支持：

```text
seen-market temporal evaluation
unseen-market evaluation
```

Split 不复制 Case 数据，也不根据 GT 数值决定归属。

---

# 8. 构建层长表

为了全量处理效率，上游阶段主要保留长表，最后由 `benchmark_export/` 才按 Market / Case 物化小文件。

主要构建表：

```text
population_scan/users.parquet

market_discovery/final_market.parquet

market_build/
  market_products.parquet
  market_population.parquet
  canonical_user_events.parquet
  user_history_cumulative.parquet
  user_category_history_cumulative.parquet
  user_market_history_cumulative.parquet

case_build/
  case_candidates.parquet
  case_shelf.parquet

case_build/population/
  case_user_features.parquet
  case_user_eligibility.parquet
  case_users.parquet

case_build/ground_truth/
  choice_truth.parquet
  population_truth.parquet
  market_truth.parquet

case_build/quality/
  quality_focal_metrics.parquet
  quality_focal_decisions.parquet
  quality_case_metrics.parquet
  quality_case_decisions.parquet
  accepted_focals.parquet
  rejected_focals.parquet
  accepted_cases.parquet
  rejected_cases.parquet
  accepted_focal_competitors.parquet
  accepted_case_shelf.parquet

benchmark_split/
  split_assignments.parquet
```

这些表可以比最终 schema 多保留审计字段；`benchmark_export/` 负责裁成最终字段并执行跨表一致性检查。

---

# 9. 核心不变量

最终 benchmark 必须满足：

```text
Market product universe 可复用
Case shelf 属于所属 Market
Case = Market × time_box，含 1 个或少数 focal
Case users 属于 Market shared population
Case population 在 future GT 查询前固定（cutoff = min focal t0）
每个 focal 的 GT2 覆盖全部 Case users
每个 focal 的 GT2 product outcome 只能是该 focal 局部货架 / none
GT1 == GT2 positives（同一 Case×focal）
market_truth == 该 focal 的 GT2 聚合
split 对 accepted cases 一一覆盖
```

这套 schema 的目标是把共享资产、时间事件、用户选择真值和最终评测隔离清楚，同时避免在每个 Case 下重复保存 Market 级历史。

# End-to-End Pipeline Contract

这份文件只描述整条产线的 **阶段顺序、输入、输出和依赖关系**。概念说明看 `README.md`，最终文件字段看 `SCHEMA.md`，未冻结研究口径看 `TODO.md`。

## 0. Upstream 基础数据

`data_prep/` 下载 Amazon Reviews 2023 一个大类的全量源文件（rating-only、metadata、full reviews），并做成 Market Discovery 之前的基础表。本地文件通过校验后不重下。

```text
product_core.parquet
product_time_summary.parquet        # 可选；没有时可从 rating_daily 重建
rating_daily_summary.parquet
storage_metadata.json
rating_event_store/ 或等价用户事件表
```

Market Discovery 还会读取自己的 `product_core` 输入并生成 `final_market.parquet`。

---

## 1. Population Scan

```text
用户事件
    ↓
population_scan
    ↓
users.parquet
summary.json
```

用途：扫描大类用户池与历史厚度分布。

---

## 2. Market Discovery

```text
product_core.parquet
    ↓
market_discovery
    ↓
first_market.parquet
    ↓
normalized exact-name cross-path merge
    ↓
final_market.parquet
```

Cross-path 不调用 LLM。

---

## 3. Market Build

```text
final_market.parquet
product_core.parquet
product_time_summary.parquet（可选）
population_scan/users.parquet
用户事件
    ↓
market_build
```

核心输出：

```text
market_products.parquet
market_population.parquet
canonical_user_events.parquet
user_history_cumulative.parquet
user_category_history_cumulative.parquet
user_market_history_cumulative.parquet
```

---

## 3.5 Behavior Graph 累计索引

目录：

```text
market_build/behavior_graph/
```

正式 pair 只在：

```text
同一个 Final Market
+同一个 leaf category
```

内部生成。

用户对 A-B 的共同用户贡献从：

```text
max(first_A_date, first_B_date)
```

开始生效，长期累计成：

```text
product_user_cumulative.parquet
pair_cumulative.parquet
```

历史 Case 查询统一使用：

```text
event_date < t0
```

完整时期的：

```text
full_graph_edges.parquet
full_graph_components.parquet
```

只用于 audit / 预实验复现，不参与正式 Case 的 Market 分裂或竞品分组。

---

## 4. Case Discovery

```text
final_market.parquet
product_core.parquet
product_time_summary / rating_daily
storage_metadata.json
    ↓
case_build discover
```

输出：

```text
market_product_timeline.parquet
case_candidates.parquet
case_candidates_evaluable.parquet
```

这里只筛结构完整的新品进入事件（focal candidate pool），不产生正式 Case，不执行最终质量阈值。

然后 `case_build select` 按 Market × time_box 选 focal，写出 `cases.parquet` / `case_focals.parquet`。

---

## 5. Case Shelf：先生成完整时间资格池

```text
cases + case_focals
market_product_timeline.parquet
rating_daily_summary.parquet
    ↓
case_build shelf
    ↓
focal_competitors.parquet
case_shelf.parquet
```

基础 competitor 资格：

```text
同 Final Market
product_id != focal
first_rating_date < t0
last_rating_date >= t0
```

并计算：

```text
pre_t0_review_count
pre_t0_rating_mean
pre_t0_recent_review_count
```

这里的 `case_shelf.parquet` 是完整的时间资格竞品池。

---

## 5.5 Behavior Graph 竞品截断：固定 K = 16

输入：

```text
case_shelf.parquet
market_products.parquet
product_user_cumulative.parquet
pair_cumulative.parquet
```

输出：

```text
case_shelf_selected.parquet
```

固定规则：

```text
competitor 数 <= 16
→ 全部保留

competitor 数 > 16
→ 只比较 focal 与每个 competitor 的 pre-t0 共评关系
```

强共评 competitor：

```text
same leaf
focal_users_pre_t0 >= 100
competitor_users_pre_t0 >= 100
shared_users_pre_t0 >= 5
```

排序 / 补位：

```text
1. 强共评优先
2. 强共评内部 shared_users_pre_t0 降序
3. 不足16时按 pre_t0_recent_review_count 补
4. 再以 pre_t0_review_count / product_id tie-break
```

因此最终一个 Case 最多：

```text
1 focal + 16 competitors
```

这一阶段 **不做 graph component，不分裂 Final Market**。

---

## 6. Case Population

下游从这里开始统一使用：

```text
case_shelf_selected.parquet
```

然后：

```text
cases（含 population_cutoff）
market_population.parquet
三套 user history cumulative
    ↓
case_build.population
```

输出：

```text
case_user_features.parquet
population_threshold_scan.parquet
case_user_eligibility.parquet
case_users.parquet
```

这里完全不读 future GT。

---

## 7. Ground Truth

```text
cases + case_focals
case_users.parquet
case_shelf_selected.parquet
focal_competitors.parquet
canonical_user_events.parquet
rating_daily_summary.parquet（可选）
    ↓
case_build.ground_truth
```

输出：

```text
choice_truth.parquet
population_truth.parquet
market_truth.parquet
review_activity_truth.parquet（可选）
```

---

## 8. External Signals（可选）

```text
cases
case_shelf_selected.parquet
provider-agnostic price / BSR history
    ↓
external_signals
```

输出：

```text
case_product_external_signals.parquet
case_external_signals.parquet
case_shelf_with_external.parquet
```

如果要让历史 t0 价格进入最终 benchmark，后续 Quality / Export 使用 `case_shelf_with_external.parquet` 作为 shelf 输入。

---

## 9. Quality Gate

```text
cases + case_focals
case_shelf + focal_competitors
GT1 choice_truth（ground_truth_gt1）
quality_rules.json（默认 min_competitors=6, max=16, min_gt1_users=20）
    ↓
case_build.quality
```

先做 focal quality，再重建 Case：

```text
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
```

Case 接受 ⇔ 至少 1 个 accepted focal。GT1 不截断人数。未来 choice 不作为 Quality 门槛。

---

## 10. Benchmark Split

```text
accepted_cases.parquet
    ↓
benchmark_split
```

输出：

```text
split_assignments.parquet
learning.json
validation.json
evaluation.json
```

Split 不使用 GT 数值决定归属。

---

## 11. Benchmark Export

```text
final_market
market_products
market_population
canonical_user_events
accepted_cases
最终 case_shelf
case_users
GT1 / GT2 / market truth
split_assignments
    ↓
benchmark_export
```

先做跨表一致性检查，再生成 `SCHEMA.md` 的最终 `benchmark_data/`。

---

## 12. Evaluation

模拟器 / agent 至少输出：

```text
case_candidate_id
user_id
predicted_outcome_product_id
```

然后：

```text
predictions
+ GT1 / GT2 / market truth
    ↓
evaluation
    ↓
individual_metrics.parquet
market_metrics.parquet
case_metrics.parquet
evaluation_summary.json
```

---

# 核心依赖图

```text
population_scan ─────────────┐
                             ▼
market_discovery ───────► market_build ─────► behavior graph cumulative
       │                                               │
       ▼                                               │
case discovery                                         │
       ▼                                               │
完整 case shelf ───────────────────────────────────────┘
       ▼
Top-16 focal-centered selection
       ▼
case_shelf_selected
       ▼
case population
       ▼
ground truth
       │
external ─┤
       ▼
quality
       ▼
split
       ▼
export
       ▼
evaluation
```

# 版本化原则

正式 benchmark 发布时至少要记录：

```text
market discovery version
population policy + seed
behavior graph rule version（100 / 5 / K=16）
Case eligibility thresholds
GT outcome policy
quality rules version
split strategy + seed
external signal source/version（如果使用）
schema version
```

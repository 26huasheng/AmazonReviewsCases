# case_build

这一目录负责从 Final Market 生成完整的 Case 构建链。正式 Case 是 **Final Market × time_box**，一个 Case 内可以有 1 个或少数几个 focal。

```text
Final Market
    ↓
1. Case Discovery          # 全部合格 focal candidates
    ↓
2. Focal Selection         # 组成正式 Cases
    ↓
3. t0 Shelf                # 每 focal 最多 16 competitor，再 union
    ↓
4. Case Population         # 共享 users，cutoff = min(focal.t0)
    ↓
5. Ground Truth            # Case × focal
    ↓
6. Quality Gate
```

最终 accepted cases 再交给根目录的 `benchmark_split/` 和 `benchmark_export/`。

---

## 1. Case Discovery

主要代码：

```text
product_timeline.py
market_timeline.py
case_discovery.py
time_windows.py
case_features.py
pipeline.py
cli.py
```

主要迁自 `AmazonReviewrepo@v5` 的商品时间、temporal segmentation 和 focal feature 计算。

保留：

- `t0 = first_rating_date`；
- Market 商品时间轴；
- active competitor 区间累计；
- `market_pre_t0_review_count` 的 many-to-one + ASOF 计算；
- 既有 `product_time_summary.parquet` 接口。

删除 / 后移：

- 一个时间段只取 top-1 focal；
- `post90>=50` 等旧 hard gate；
- competitor 数旧 hard gate。

一个 Market 中每个结构完整的新品进入事件都先进入 **focal candidate pool**。`case_candidates_evaluable` **不是正式 Case**。

输出：

```text
market_product_map.parquet
market_product_timeline.parquet
case_candidates.parquet
case_candidates_evaluable.parquet   # 全部合格 focal candidates
```

`case_candidates_evaluable` 这里只要求：

```text
valid_t0
evaluation_window_complete
```

`case_candidate_id` 保留；同值 alias 为 `focal_candidate_id`。

---

## 1.5 Focal Selection

代码：`focal_selection.py`。CLI：`python -m case_build.cli select`。

Case = Market × time_box。有效行为团 `component_size >= 6`：

- 0 或 1 个有效团：该 slot 全部候选里稳定随机取 1 个 focal
- ≥2 个有效团：每个在该 time_box 有候选的有效团各取 1 个

输出：`cases.parquet`、`case_focals.parquet`。`population_cutoff = min(focal.t0)`，policy=`earliest_focal_t0`。

---

## 2. t0 Shelf

主要代码：

```text
shelf.py
CaseShelfBuilder
```

一个 competitor 进入基础 Case shelf 需要：

```text
同一 Market
product_id != focal
first_rating_date < focal.t0
last_rating_date >= focal.t0
```

候选 ≤16 全留；>16 按 `pre_t0_recent_review_count DESC, pre_t0_review_count DESC, product_id` 取 16。关系表 `focal_competitors.parquet`。Case shelf 是 focals ∪ competitors 的 product 去重，`is_focal` / `is_competitor` 可同时为真。

保留 v5 已验证的商品累计表 + ASOF 查询，计算：

```text
pre_t0_review_count
pre_t0_rating_mean
pre_t0_recent_review_count
```

基础 shelf 先保留所有通过时间资格的竞品，然后交给：

```text
market_build/behavior_graph
```

做最终规模控制。

当前第一版固定：

```text
K = 16 competitors
```

即：

```text
competitor 数 <= 16
→ 全部保留

competitor 数 > 16
→ 只比较 focal 与每个 competitor 的严格 pre-t0 共评关系
→ 强共评竞品优先
→ 强共评不足 16 时按 pre_t0_recent_review_count 补齐
```

强共评条件沿用 Electronics 预实验：

```text
same leaf_category
focal_users_pre_t0 >= 100
competitor_users_pre_t0 >= 100
shared_users_pre_t0 >= 5
```

最终一个 Case 最多：

```text
1 focal + 16 competitors
```

不再使用 `Top-150`、`8 CORE + 8 RESERVE`、component-based Market 分裂。

Amazon metadata snapshot price 只保留成 `metadata_snapshot_price`，不会冒充历史 `price_at_t0`。

---

## 3. Case Population

目录：[`population/`](population/README.md)

```text
Market shared population
+ Case t0
+ 用户累计历史
        ↓
case_user_features
        ↓
threshold scan
        ↓
eligibility
        ↓
case_users
```

所有用户资格字段只来自 `population_cutoff` 以前；未来正例不能参与用户筛选。ASOF 使用 `event_date < population_cutoff`。

核心输出：

```text
case_user_features.parquet
population_threshold_scan.parquet
case_user_eligibility.parquet
case_users.parquet
```

---

## 4. Ground Truth

目录：[`ground_truth/`](ground_truth/README.md)

Case 用户锁定之后才查询 future：

```text
future_market_events
        ↓
GT2: all case users -> product / none
        ↓
GT1: GT2 positives -> product
        ↓
market demand / share / rank
```

核心输出：

```text
choice_truth.parquet
population_truth.parquet
market_truth.parquet
```

可选：

```text
review_activity_truth.parquet
```

它直接对最终 shelf 的未来评论量排名，作为辅助商品侧真值 / 质量信号。

---

## 5. Quality Gate

目录：[`quality/`](quality/README.md)

两层：先按 focal（`case_id + focal_id`）筛，再按 Case 重建。

正式门槛：

```text
competitor ∈ [6, 16]
GT1 users ≥ 20
history ≥ 3
recency ≤ 365
evaluation window 完整
focal / competitor / local shelf / GT1 结构完整
```

GT1 不设最大人数。GT1 的未来 choice outcome 只用于评测，不根据 focal 成功/失败淘汰 Case。

Case 接受条件：过滤后仍有 `>=1` 个 accepted focal。输出包括重建后的 `accepted_case_shelf`（accepted focals ∪ 其 selected competitors）。

---

## 6. 时间段

时间段仍使用：

- 2021 年以前按两年；
- 2021-2023 按半年。

它现在只是 Case 的 `time_box_id` 属性，不再产生 `Market Segment` 层级。

behavior graph 的选择统计按每个 Case 自己精确的 `< t0` 计算，与 time box 的结束时间无关。

所有时间窗口使用半开区间 `[start, end)`。

---

## 7. 当前完整接口

```text
market_discovery/final_market.parquet
        ↓
case_build discover
        ↓
case_candidates_evaluable.parquet
        ↓
case_build select
        ↓
cases.parquet + case_focals.parquet
        ↓
case_build shelf
        ↓
case_shelf.parquet                 # 完整时间资格 shelf
        ↓
market_build.behavior_graph case
        ↓
case_shelf_selected.parquet        # 最多16个竞品
focal_competitors.parquet + case_shelf.parquet
        ↓
case_build.population
        ↓
case_users.parquet
        ↓
case_build.ground_truth
        ↓
GT1 / GT2 / market truth  （Case × focal）
        ↓
case_build.quality
        ↓
accepted_cases.parquet
```

商品侧仍未冻结的规则见 [`TODO.md`](TODO.md)；用户、GT、Quality 各自的研究 TODO 放在对应子目录里。

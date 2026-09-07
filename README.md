# AmazonReviewsCases

Amazon Reviews 2023 上的 **SEMS（Self-Evolving Market Simulation）benchmark 构造仓库**。

当前主线已经从“一个新品 = 一个 Case”改成：

```text
Final Market × time_box = Case
Case 内包含 1 个或少数几个 focal
每个 focal 有自己的 t0、local shelf、90 天 evaluation window 和 GT1
```

仓库负责 Market 构造、Case 构造、focal / competitor 选择、GT1、Quality、最终 JSON/JSONL 打包等数据侧工作；模拟器本体不在这里。

## Current Release

当前公开数据版本：[`Electronics_v1_cases`](https://github.com/26huasheng/AmazonReviewsCases/releases/tag/Electronics_v1_cases)

| 内容 | 数量 |
|---|---:|
| Final Markets with accepted Cases | **381** |
| Accepted Cases | **2424** |
| Accepted focals | **2493** |
| GT1 choice rows | **301,037** |

Release 文件：`Electronics_v1_cases.tar.gz`。

该 release 只包含 Quality 后的正式 JSON/JSONL Case package，不包含生产过程中的大规模 Parquet 中间表。

---

# 1. 数据层级

```text
MARKET
├── products/                 # Market 长期商品 universe
├── users/                    # 当前 release 中实际 GT1 用户及 pre-t0 histories
└── cases/
    └── time_box/
        └── case_id/
            ├── Case metadata
            ├── 1..N focals
            ├── Case union shelf
            ├── focal → competitor relations
            └── GT1
```

Market 是长期竞争边界；Case 是这个 Market 在一个时间格子里的新品进入评测槽；真正的评测对象是 Case 里的 focal。

一个 Case 可以有多个 focal，但每个 focal 始终单独拥有：

```text
t0 = first_rating_date
evaluation window = [t0, t0 + 90 days)
local shelf = focal + its selected competitors
GT1 users / choices
```

`case_id` 由 `source_partition + market_id + time_box_id` 稳定生成，因此同一个 Market、同一个 time box 最多只有一个正式 Case。

---

# 2. Case 是怎么构造的

## 2.1 Focal candidate discovery

对 Final Market 内每个商品建立时间轴：

```text
t0 = first_rating_date
```

只要 `t0` 有效且 90 天 evaluation window 完整，就先进入完整 focal candidate pool。

Discovery 阶段不根据未来表现筛新品，不使用 `post90` 成功门槛，也不在这里决定最终 Case。

## 2.2 Market × time_box 组成正式 Case

默认时间格：

- `1996` 单独一年；
- `1997–2020` 主要按两年一格；
- `2021–2023` 按半年一格；
- 所有窗口均使用半开区间 `[start, end)`。

Behavior graph 用于 focal diversity：

```text
有效行为团：component_size >= 6

0 / 1 个有效行为团
→ 该 Market × time_box 的全部 evaluable candidates 中稳定随机取 1 个 focal

>= 2 个有效行为团
→ 每个在当前 time_box 有 candidate 的有效行为团稳定随机取 1 个 focal
```

Behavior graph 不重新定义 Final Market，也不用于正式 competitor 排序。

---

# 3. Competitor / Shelf

每个 focal 单独找自己的 competitor pool。

基础资格：

```text
同一个 Final Market
product_id != focal_product_id
first_rating_date < focal.t0
last_rating_date >= focal.t0
```

也就是：新品进入 `t0` 时，该商品已经出现，并且仍处于 Amazon Reviews 的观测区间。

每个 focal 最多保留 16 个 competitors：

```text
candidate competitors <= 16
→ 全部保留

candidate competitors > 16
→ pre_t0_recent_review_count DESC
→ pre_t0_review_count DESC
→ product_id
→ Top 16
```

其中 recent window 默认 120 天，只使用 `t0` 之前的数据。

一个 Case 的 `shelf` 是所有 surviving focals 及其 competitors 的 `product_id` union，因此多 focal Case 的 union shelf 可以超过 17 个商品。

---

# 4. GT1

当前 Electronics v1 release 的正式 Ground Truth 是 **GT1：已知用户在局部货架上发生了真实可观测选择后，预测其选择哪个商品**。

对每个 focal：

```text
local shelf = focal + selected competitors
window = [t0, t0 + 90 days)

完整 Amazon user events
→ 找窗口内对 local shelf 任意商品有真实 rating/review event 的用户
→ 每个用户取 first_observed_event
→ 再检查该用户在 t0 前：
   history_product_count >= 3
   days_since_last_event <= 365
→ GT1 users + choice truth
```

GT1 **不经过预抽的 `case_users`**，也不设置最大人数截断；所有满足口径的真实用户都会保留。

需要注意：Amazon Reviews 2023 提供的是 rating/review observation。这里的 GT1 是基于真实评分/评论行为构造的 **observed choice proxy**，不是完整购买日志。

当前 release 不把实验性的 GT2 population/none 任务打包进正式 Case package。

---

# 5. Quality Gate

Quality 先在 focal 层判断，再重建 Case。

正式默认门槛：

```text
evaluation_window_complete = true
selected_competitor_count ∈ [6, 16]
GT1 users >= 20
history_product_count >= 3
days_since_last_event <= 365
```

并检查：

- focal 必须存在于 Case shelf；
- competitor 不能指向 focal 自己，也不能重复；
- competitor 数量与关系表一致；
- competitor 必须满足 `first_rating_date < t0 <= last_rating_date`；
- focal 的 local shelf 必须与 focal + selected competitors 一致；
- `(case_id, focal_id, user_id)` 的 GT1 choice 唯一；
- choice 必须属于该 focal 的 local shelf；
- choice event 必须落在 `[t0, evaluation_end)`；
- GT1 用户必须满足冻结的 history / recency 规则；
- choice 必须可追溯到真实窗口事件。

多 focal Case 中，一个 focal 不合格只删除这个 focal。只要 Case 还剩至少 1 个 accepted focal，Case 就继续保留，并重新构建最终 `accepted_case_shelf`。

未来新品是否成功不作为 Quality 门槛：focal 被选多少次、choice share、rank、post90 评论量等可以统计，但不会因为新品表现差而事后删除 Case。

---

# 6. Electronics v1 Release 目录

Release 解压后按 **Market 名**组织：

```text
Electronics_v1_cases/
└── {market_name}/
    ├── market.json
    │
    ├── users/
    │   ├── users.jsonl
    │   └── histories/
    │       ├── summary.jsonl
    │       └── events.jsonl
    │
    ├── products/
    │   └── products.jsonl
    │
    └── cases/
        └── {time_box_id}/          # 只创建实际有 accepted Case 的时间窗
            └── {case_id}/
                ├── case.json
                ├── focals.jsonl
                ├── shelf.jsonl
                ├── focal_competitors.jsonl
                └── ground_truth/
                    ├── gt1_users.jsonl
                    └── choice_truth.jsonl
```

### Market-level files

`market.json`
: Market 标识、名称、商品数、Case 数、focal 数、实际存在的 time boxes。

`products/products.jsonl`
: 该 Final Market 的长期商品 universe；来自现有 Market product asset，不重新抓取 metadata。

`users/users.jsonl`
: 该 Market 所有 accepted focals 的正式 GT1 用户去重集合。

`users/histories/summary.jsonl`
: `case_id + focal_id + user_id` 粒度的 pre-t0 历史摘要。

`users/histories/events.jsonl`
: 对应 focal `t0` 之前的真实用户事件，用于 agent history 初始化；不会包含 evaluation-window future events。

### Case-level files

`case.json`
: `case_id`、Market、time box、population cutoff、focal 列表等 Case metadata。

`focals.jsonl`
: 一行一个 surviving focal，包含自己的 `t0`、90 天窗口、competitor 数、GT1 用户数等。

`shelf.jsonl`
: Quality 后的最终 Case union shelf。

`focal_competitors.jsonl`
: 每个 focal 与其 selected competitors 的显式关系，可恢复 focal-specific local shelf。

`ground_truth/gt1_users.jsonl`
: 每个 focal 的正式 GT1 用户。

`ground_truth/choice_truth.jsonl`
: 每个 `focal × user` 的 `first_observed_event` 真实 choice。

正式 package 使用 JSON / JSONL；Parquet 只保留在生产层用于大规模计算。

---

# 7. Pipeline

当前主要数据流：

```text
Amazon Reviews 2023 / AmazonReviewrepo@v5 assets
        ↓
market_discovery
        ↓
Final Market
        ↓
market_build
  ├── Market products
  ├── canonical user events
  ├── user history cumulative indexes
  └── behavior components
        ↓
case_build discover
        ↓
focal candidate pool
        ↓
case_build select
        ↓
cases + case_focals
        ↓
case_build shelf
        ↓
focal_competitors + case_shelf
        ↓
GT1 construction
        ↓
Quality Gate
        ↓
accepted focals / cases / rebuilt shelf
        ↓
clean Market subset
        ↓
scripts/package_market_cases.py
        ↓
Market → time_box → Case JSON/JSONL package
```

Production Parquet 不会被 packager 修改；`scripts/package_market_cases.py` 只读取已经完成的 Quality / GT1 / Market assets，重新组织为发布目录。

---

# 8. Repository Layout

```text
AmazonReviewsCases/
├── README.md
├── PIPELINE.md
├── SCHEMA.md
├── TODO.md
│
├── data_prep/
├── population_scan/
├── market_discovery/
├── market_build/
├── case_build/
│   ├── population/
│   ├── ground_truth/
│   └── quality/
├── external_signals/
├── benchmark_split/
├── benchmark_export/
├── evaluation/
└── scripts/
    └── package_market_cases.py
```

核心实现可直接查看：

- [`case_build/focal_selection.py`](case_build/focal_selection.py)
- [`case_build/shelf.py`](case_build/shelf.py)
- [`case_build/quality/README.md`](case_build/quality/README.md)
- [`scripts/package_market_cases.py`](scripts/package_market_cases.py)

---

# 9. Release

- **Electronics v1**: [`Electronics_v1_cases`](https://github.com/26huasheng/AmazonReviewsCases/releases/tag/Electronics_v1_cases)
- Archive: `Electronics_v1_cases.tar.gz`
- Contents: **381 Markets / 2424 Cases / 2493 focals**
- Quality freeze: **6–16 competitors / GT1 users ≥ 20 / history ≥ 3 / recency ≤ 365 days**

后续版本如修改 Market、Case、GT 或 Quality 口径，应通过新的 release/tag 发布，避免覆盖已有 benchmark 数据。
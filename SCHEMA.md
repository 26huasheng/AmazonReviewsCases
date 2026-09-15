# SEMS Electronics v1 Release Schema

本文件定义当前 `Electronics_v1_cases` 的 **冻结输出 contract**。这里描述的是正式发布包，不是内部 Parquet 表，也不是未来 GT2 设计。

> 结构冻结原则：不得因为整理代码而重命名、移动、合并或拆分这些正式文件；不得把旧版 `case_users / market_population / GT2` 重新塞回当前 GT1 Release。内部实现和中间表可以重构，但必须保持发布语义与输出兼容。

## 1. Directory contract

```text
Electronics_v1_cases/
└── {market_name}/
    ├── market.json
    ├── users/
    │   ├── users.jsonl
    │   └── histories/
    │       ├── summary.jsonl
    │       └── events.jsonl
    ├── products/
    │   └── products.jsonl
    └── cases/
        └── {time_box_id}/          # only when an accepted Case exists
            └── {case_id}/
                ├── case.json
                ├── focals.jsonl
                ├── shelf.jsonl
                ├── focal_competitors.jsonl
                └── ground_truth/
                    ├── gt1_users.jsonl
                    └── choice_truth.jsonl
```

每个 Final Market 以规范化 `market_name` 作为顶层目录名。若某个 time box 没有 accepted Case，不创建空目录。同一 `Market × time_box` 最多一个 Case，但仍保留显式 `{case_id}/` 层。

## 2. Market level

### `market.json`

Market-level metadata。核心语义至少包括：

```text
market_id
market_name
source_partition
n_products
n_cases
n_focals
available_time_boxes
```

这里只描述长期 Market 与本 Release 中实际存在的 Cases，不承担某个 focal 的 t0/local-shelf/GT1 信息。

### `products/products.jsonl`

一行一个 Final Market 长期商品。当前生产资产来自 `market_build/market_products.parquet`。核心字段包括：

```text
market_id
market_label / market_name context
source_partition
product_id
title
category_path
first_review_date
first_available_date           # source available 时
store                          # source available 时
metadata_available
metadata_snapshot_price        # snapshot only; not historical t0 price
```

这是长期商品 universe，不是某个 Case 的 shelf。

## 3. Market users and pre-t0 histories

### `users/users.jsonl`

当前 v1 中它是该 Market 所有 **accepted focals 的正式 GT1 users 的去重 registry**，最小字段：

```text
user_id
```

它不是旧版随机抽样 `market_population`，也不是 GT2 background population。

### `users/histories/summary.jsonl`

粒度：

```text
case_id + focal_id + user_id
```

因为历史摘要依赖 focal 自己的 t0，所以同一用户在不同 focal 下可以出现不同摘要。Release/生产中可包含：

```text
case_id
focal_id
user_id
t0
history_event_count
history_product_count
last_event_date
days_since_last_event
category_history_event_count
category_history_product_count
market_history_event_count
market_history_product_count
```

只写实际已有、可追溯的字段；不要为了“补全 schema”凭空计算不存在的字段。

### `users/histories/events.jsonl`

粒度是 focal-specific pre-t0 user history event。核心键/字段：

```text
case_id
focal_id
user_id
event_date
event_timestamp
product_id
rating
verified_purchase
source_partition
review_title                  # 当前 Release 已确认可包含
review_text                   # 当前 Release 已确认可包含
```

硬约束：

```text
event_timestamp < focal.t0
```

当前公开 Release 明确说明这里的 history events 带 `review_title / review_text`；3,894,971 条 history events 中 3,417,022 条有 `review_text`。正文只在源数据实际存在时保留，不伪造，不要求 `users.jsonl` 或 `products.jsonl` 保存正文。

## 4. Case level

### Case identity

```text
Case = Final Market × time_box
case_id = stable hash(source_partition, market_id, time_box_id)
```

Case 内包含 1..N surviving focals。不同 focal 拥有各自 t0、evaluation window、competitor relation 与 GT1。

### `case.json`

核心字段：

```text
case_id
market_id
market_name
time_box_id
time_box_start
time_box_end
n_focals
focal_ids
```

现有生产表/Release 中可能还保留：

```text
population_cutoff
population_cutoff_policy
```

这两个字段属于旧 GT2/population lineage 的 provenance，不是当前 GT1 语义的必要输入，也不得据此重新把 GT1 定义成 preselected Case population。

### `focals.jsonl`

一行一个 surviving focal。核心语义：

```text
case_id
focal_id
focal_product_id
t0
evaluation_start
evaluation_end_exclusive
selected_competitor_count
gt1_user_count
```

允许保留上游已有的审计字段。不要把多个 focal 强行压成一个 Case-level t0。

### `shelf.jsonl`

这是 **Quality 后重建的 Case union shelf**，必须来自 accepted/surviving focals：

```text
case_shelf = union(
  each surviving focal product,
  each surviving focal selected competitors
)
```

多 focal Case 中，一个商品可以既是某个 focal，又是另一个 focal 的 competitor。Case union shelf 不等于任意一个 focal 的 local shelf。

### `focal_competitors.jsonl`

显式记录 focal → selected competitor 关系。对任一 focal：

```text
local_shelf = {focal_product_id} ∪ {selected competitor_product_id}
```

正式 competitor 时间资格：

```text
same Final Market
product_id != focal_product_id
first_rating_date < t0
last_rating_date >= t0
```

最多 16 个。候选 >16 时按 t0 前 recent activity、累计 activity、product_id 确定性排序。Behavior graph 不参与这里的正式排序。

## 5. GT1

### `ground_truth/gt1_users.jsonl`

一行一个 `case_id + focal_id + user_id` 的正式 GT1 membership。GT1 user 必须：

1. 在 focal local shelf 的 `[t0,t0+90d)` 中至少出现一个真实 Amazon rating/review observation；
2. 经过 `first_observed_event` reducer 后拥有唯一 choice；
3. t0 前 `history_product_count >= 3`；
4. `days_since_last_event <= 365`。

GT1 不从旧 `case_users` 预抽，不设上限 cap。

### `ground_truth/choice_truth.jsonl`

粒度：

```text
case_id + focal_id + user_id
```

核心字段/语义：

```text
product_id              # first observed choice on local shelf
outcome_is_focal
event_timestamp
rating
verified_purchase
outcome_policy           # first_observed_event
history_product_count
days_since_last_event
```

它是 observed choice proxy，不是完整 purchase log，也不包含 `none`。

**正文边界：** 当前 Release 明确保证 review 正文的是 `users/histories/events.jsonl` 的 pre-t0 history。`choice_truth` 对应 future event 的 `review_title / review_text` 是否打包没有被当前 Release contract 保证，因此不能把 future review text 设为必需字段，也不能从 history 有正文推断它一定存在。

## 6. Time boxes

当前正式默认：

```text
1996: single-year box
1997–2020: mainly 2-year boxes
2021–2023: half-year boxes
```

所有窗口使用半开区间 `[start,end)`。无 accepted Case 的 time box 不出现在目录中。

## 7. Quality invariants

每个 surviving focal 必须满足：

```text
valid t0
evaluation window complete
selected competitors in [6,16]
GT1 users >= 20
history_product_count >= 3
days_since_last_event <= 365
```

同时必须满足：

- focal 在 final Case shelf 中；
- competitor 不自指、不重复；
- relation count 与 stored count 一致；
- 每个 selected competitor 都在 final Case shelf；
- competitor 满足 `first_rating_date < t0 <= last_rating_date`；
- focal local shelf 恰好等于 focal + selected competitors；
- `(case_id,focal_id,user_id)` choice 唯一；
- choice product 属于 focal local shelf；
- choice timestamp 落在 `[t0,evaluation_end_exclusive)`；
- GT1 history/recency 满足规则；
- 有中间事件表时，choice 可追溯到真实 window event。

Quality 先在 focal 层执行；失败 focal 移除后重建 Case union shelf。Case 只在没有任何 surviving focal 时整体拒绝。

## 8. What is NOT part of current Release contract

以下不属于 `Electronics_v1_cases` 的正式输出：

```text
pre-sampled case_users
market_population as GT1 users
GT2 population_truth
GT2 none task
GT2 market_truth
benchmark split files
legacy parquet exporter layout
behavior-graph-based competitor selection
future-success filtering
```

## 9. Backward-compatible data-detail changes

在不改目录/主键/语义的前提下，未来可以修复或补齐：

- 由真实源数据补 `review_title / review_text`；
- 修正 source plumbing、provenance、nullable source fields；
- 增加不会改变 GT1 membership/choice 的可追溯审计字段；
- 重构内部 Parquet/SQL/索引以提升性能或可复现性。

禁止把这些“数据细节改进”变成新的筛选门槛，禁止重算已经冻结的 `Electronics_v1_cases` 语义来覆盖现有 Release。

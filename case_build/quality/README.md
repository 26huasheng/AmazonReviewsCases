# case_build.quality

Quality 是当前 Electronics v1 production path，采用 **focal-first** 判定，再重建 Case。

## Default hard gates

```text
valid_t0 = true
evaluation_window_complete = true
selected_competitor_count >= 6
selected_competitor_count <= 16
GT1 user count >= 20
history_product_count >= 3
days_since_last_event <= 365
```

`max_gt1_users` 不设上限。

## Structural checks

- focal 在 Case shelf；
- competitor 不自指、不重复；
- competitor relation count 与 stored count 一致；
- selected competitor 在 Case shelf；
- competitor 满足 `first_rating_date < t0 <= last_rating_date`；
- focal local shelf = focal + selected competitors；
- `(case_id,focal_id,user_id)` choice 唯一；
- choice product 在 focal local shelf；
- choice event 在 `[t0,evaluation_end_exclusive)`；
- GT1 user 满足 history/recency；
- 有原始中间表时 choice 可追溯到真实 event。

## Multi-focal handling

一个 focal 失败只删除该 focal。Case 仍有至少一个 surviving focal 时保留，并用 surviving focals + their competitors 重建 `accepted_case_shelf`。没有 surviving focal 才 reject 整个 Case。

## Not acceptance gates

以下只可统计，不得因为表现差而事后删新品：

```text
focal choice count/share/rank
future review activity
post90 rating count
GT2 none rate
GT2 market-positive count
future demand/success rank
```

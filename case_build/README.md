# case_build

`case_build/` 是当前 Electronics v1 的核心生产层。

```text
Final Market × time_box = Case
Case contains 1..N focals
```

## Discovery

每个商品以 `first_rating_date` 作为候选 t0。只要 t0 有效且 90 天 evaluation window 完整，就进入 evaluable focal pool。这里不按 future success、post90 review count 或 competitor count 筛选。

## Focal selection

Behavior components 只用于同一 Market×time_box 内的 focal diversity。稳定随机选择保证可复现。

## Shelf

每个 focal 独立确定 competitors：

```text
same Final Market
product_id != focal
first_rating_date < t0
last_rating_date >= t0
```

候选 `<=16` 全留；`>16` 按：

```text
pre_t0_recent_review_count DESC
pre_t0_review_count DESC
product_id ASC
```

Case shelf 是所有 focal local shelves 的 union。

本目录不应重新引入旧 Top150、CORE/RESERVE、recent>=10 hard gate、graph competitor selection、price gate 或 future event requirement。

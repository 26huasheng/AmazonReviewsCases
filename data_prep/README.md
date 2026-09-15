# data_prep

`data_prep/` 是 Amazon Reviews 2023 的输入准备层，不定义 Market/Case/GT。

当前需要区分三类源数据：

```text
rating-only events   -> 大规模时间/用户/商品事件索引
metadata             -> 商品标题、类目、snapshot fields
full reviews         -> review_title / review_text enrichment source
```

现有代码已经知道三类路径并可下载 `rating / metadata / reviews`。但当前主 materialization 主要用 rating-only + metadata；full review 正文在正式 Release 中用于 **pre-t0 history event enrichment**，其完整 plumbing 需要在代码梳理阶段补回/确认。

禁止让 full-review enrichment 改变 rating-only canonical event 的主键语义、时间或 GT1 membership。

核心稳定输入字段：

```text
user_id
parent_asin/product_id
rating
timestamp
```

商品 metadata 仅保留源中实际存在的字段。`metadata_snapshot_price` 不是历史 t0 价格。

# market_build

`market_build/` 把 Final Market 转成可被多个 Cases 复用的 Market-level assets。

当前 GT1/Case 主线需要：

```text
market_products.parquet
canonical_user_events.parquet
user_history_cumulative.parquet
user_category_history_cumulative.parquet
user_market_history_cumulative.parquet
behavior-component asset for focal selection
```

`market_population.parquet` 是 GT2 engineering lineage，不是当前 GT1 users。

## Important mixed-version warning

当前 main 的 `market_build/pipeline.py` 与 `market_build/behavior_graph/*` 存在版本接口不一致；本目录必须先做调用链审计再改。不要简单选择“看起来更新”的文件覆盖另一套实现。

当前正式 behavior graph 语义只有：

```text
full-period co-review graph -> valid components -> focal diversity
```

不允许用于正式 competitor ranking。

Full review 正文可以作为 pre-t0 history enrichment 的 source，但应在已经固定 event identity 后只读回表补字段。

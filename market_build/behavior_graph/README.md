# market_build.behavior_graph

## Current v1 role

Behavior graph 在 `Electronics_v1_cases` 中 **只用于 focal diversity**。

强边：

```text
same Final Market
same leaf category
n_users(endpoint A) >= 100
n_users(endpoint B) >= 100
shared_users >= 5
```

每个 leaf 内做 connected components。当前 focal-selection 语义只把 `component_size >= 6` 视为 valid behavior group。

它不重新定义 Final Market，不压缩 Case shelf，不选 competitor，不产生 GT。

## Legacy code warning

本目录仍混有一套旧 branch：对 focal 做 pre-t0 co-review feature，然后在 competitor pool >16 时优先挑 strong co-review competitors。该逻辑 **不是 Electronics v1 正式 competitor policy**。

正确 competitor policy 位于 Case shelf build：资格由 `first_rating_date < t0 <= last_rating_date` 决定，超过 16 后按 pre-t0 recent activity / cumulative activity / product_id 排序。

代码梳理时先标记旧 branch 和所有 callers；在证明 production 不依赖后再隔离，不能直接删除不确定实现。

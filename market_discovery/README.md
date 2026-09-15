# market_discovery

该模块定义长期 **Final Market** 边界。Market 是可被多个 time_box/Case 复用的竞争 universe，不等于某个 focal 的 local shelf。

当前 v1 的下游假设：

```text
Final Market
  × time_box
  = Case
```

Discovery 不使用 future success、GT1 size 或 post90 activity 来挑 Market/Case。Cross-path 合并与 market naming 属于 Market definition；后续 behavior graph 不能重新定义 Final Market。

`Electronics_v1_cases` 已基于已确认的 Market 结果发布。仓库清理期间不得重新跑 Market discovery 并覆盖 Release；本模块只在明确构建新版本/新大类时使用。

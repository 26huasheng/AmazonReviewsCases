# market_build TODO

当前代码把商品资产、统一用户事件、Market shared population 和三套历史累计索引都接好了。以下研究口径仍需以后冻结。

## 需要冻结的 benchmark 配置

1. `population_source` 最终采用 `category`、`global`，还是分别做两套 benchmark 设置。
2. 每个 Market shared population 的目标规模 `population_size`。
3. 是否对极大 / 极小 Market 使用不同 population size，还是全 benchmark 固定规模。
4. 当前实现已丢弃 `product_count < 9` 的 Market；若以后改门槛，改 `MIN_FINAL_MARKET_PRODUCT_COUNT`。
5. behavior graph 阈值（100 / 5 / 6）沿用 Electronics 预实验，尚未作为论文最终口径冻结。

## 数据接口待补

- 如果后续拿到更丰富的订单级或人口属性数据，在 canonical user event schema 上扩展，不修改 Case GT 基础接口。
- 发布版是否对 `user_id` 做稳定匿名化；内部构建阶段当前保留源 user key。
- `store / brand / price` 等 metadata 字段的最终上游字段名需要和正式 product_core 对齐。

## 工程优化

- 全局 population 模式会产生很大的 Market×user 候选空间；正式跑全量时可以增加按 Market 分批 / 预采样执行器。
- `user_event_store/` 当前用于按用户分桶；如果后续查询性能需要，可以再增加桶内显式排序重写。

这些 TODO 不改变核心原则：Market population 先于 Case future GT 固定，Case 只引用 Market 用户，不重复保存完整用户历史。

# Case population TODO

## 已冻结

```text
min_history_products = 3
max_days_since_last_event = 365
min_category_products = None
min_market_products = None
target_users_per_case = 1000
```

## 分层抽样

当前代码已经计算 `relation_stratum`，正式抽样先实现稳定均匀哈希。后续如果确定需要控制：

```text
market_history / category_only / outside_category
```

的比例，只需替换 `sampling.py` 的策略，不需要重算历史特征或 future GT。

## 特殊 Case

- 某 Case 合格用户数少于目标人数时，当前保留全部合格用户；最终质量门槛由 quality gate 决定。
- global population 模式下可能出现大量 `outside_category` 用户，需要结合真实漏斗决定是否限额。

## 不允许改的原则

- 禁止根据 evaluation window 是否有正例来筛用户；
- 禁止为了提高 GT1 数量而删除 `none` 用户；
- 所有 eligibility 特征必须只使用 population_cutoff 前数据。

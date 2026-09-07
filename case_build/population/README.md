# case_build/population

这一层把用户拆成两套 cohort，**Market 用户不再经过大类随机 2 万截断**：

```text
case_market_users.parquet       # 完整高质量 Market-history 用户，GT1 核心
case_background_users.parquet   # 大类无 Market 历史用户，质量筛选后抽样，GT2 扩展
case_population_users.parquet   # union，带 user_group，只给 GT2 用
```

主链：

```text
Market shared population
+ Case population_cutoff
+ 用户历史累计索引
        ↓
case_user_features.parquet
        ↓
population_threshold_scan.parquet
        ↓
case_user_eligibility.parquet
        ↓
确定性抽样
        ↓
case_users.parquet
```

## 1. t0 前用户特征

每个 `Case × Market user` 计算：

```text
history_event_count
history_product_count
last_event_date
days_since_last_event
category_history_event_count
category_history_product_count
market_history_event_count
market_history_product_count
relation_stratum
```

`relation_stratum` 当前分为：

```text
market_history     t0 前碰过当前 Market 商品
category_only      t0 前碰过当前大类，但没碰过当前 Market
outside_category   t0 前连当前大类都没有历史
```

所有字段都通过累计表做 `ASOF < t0` 查询，不查询 future。

## 2. 阈值扫描

代码会同时生成：

```text
population_threshold_scan.parquet
```

默认扫描：

```text
历史不同商品数 >= 1 / 3 / 5 / 10
最近活动 <= 90 / 180 / 365 / 730 天
```

这张表用于看真实漏斗后确定正式 benchmark 阈值，不属于最终 Case 数据。

## 3. 正式资格规则

当前支持可配置：

```text
min_history_products
max_days_since_last_event
min_category_products（可选）
min_market_products（可选）
```

其中前两项是主要的 history richness / recency 条件；category / market relation 保留为可选门槛和抽样分层信息。

代码没有把具体数字写死，等真实统计后冻结参数即可。

## 4. Case 用户抽样

`case_users.parquet` 从 `eligible_pre_t0=true` 的用户里按：

```text
seed + case_candidate_id + user_id
```

做稳定哈希排序，再取目标人数。这样同一配置重跑会得到相同用户。

最终 benchmark 的单 Case `users.parquet` 只需要 `user_id`；这里的特征和 eligibility 表保留为构建审计产物，不复制进最终 Case。

## 5. 运行

```bash
python -m case_build.population.cli \
  --cases outputs/case_build/discovery/case_candidates_evaluable.parquet \
  --market-population outputs/market_build/market_population.parquet \
  --user-history outputs/market_build/user_history_cumulative.parquet \
  --user-category-history outputs/market_build/user_category_history_cumulative.parquet \
  --user-market-history outputs/market_build/user_market_history_cumulative.parquet \
  --min-history-products 3 \
  --max-days-since-last-event 365 \
  --target-users-per-case 1000 \
  --output-dir outputs/case_population
```

当前冻结的正式资格 / 抽样：

```text
min_history_products = 3
max_days_since_last_event = 365
min_category_products = None
min_market_products = None
target_users_per_case = 1000
```

## 6. 关键约束

- 用户集合在查询 future GT 之前锁定；
- `none` 用户也必须保留在 `case_users` 中；
- 不允许用“未来在 shelf 买过东西”作为 population eligibility；
- 一个 Market 的用户历史资产只存一次，Case 只保存用户 ID。

未冻结事项见 [`TODO.md`](TODO.md)。

# AmazonReviewsCases

Amazon Reviews 2023 上的 SEMS benchmark 构造与评测仓库。

核心层级固定为 **Market → Cases**。本仓负责从 Amazon Reviews / `AmazonReviewrepo@v5` 已有基础表继续构造 Market、Case、用户、GT、质量筛选、split、最终 benchmark 文件以及数值评测入口；模拟器本体不放在这里。

最终数据格式见 [`SCHEMA.md`](SCHEMA.md)，所有尚未冻结的研究口径总表见 [`TODO.md`](TODO.md)。

---

# 1. 核心结构

```text
MARKET
├── 长期商品 universe
├── shared population
├── shared user history
│
└── CASES
    ├── focal product
    ├── t0
    ├── evaluation window
    ├── t0 shelf
    ├── selected user ids
    └── Ground Truth
        ├── GT1: 已知发生市场内选择 -> product
        └── GT2: 全部 Case 用户 -> product / none
                                ↓
                         demand / share / rank
```

一个 Market 可以包含多个不同时间的新品进入 Case。Case 共享 Market 级商品和人口资产，不重复保存完整用户历史。

---

# 2. 完整流程

```text
Amazon Reviews 2023
        │
        ▼
     data_prep
  全量下载 + 初筛表
        │
        ├──────────────────────────────┐
        ▼                              ▼
population_scan                  market_discovery
大类用户基础扫描                   path-local Market 发现
        │                              │
        │                              ▼
        │                    规范化后同名 cross-path merge
        │                         （不调用 LLM）
        └──────────────┬───────────────┘
                       ▼
                  Final Market
                       │
                       ▼
                  market_build
          ┌────────────┼────────────┐
          ▼            ▼            ▼
   Market products  shared users  user history indexes
                       │
                       ▼
                  case_build
          ┌────────────┼────────────┐
          ▼            ▼            ▼
 Case Discovery      t0 Shelf    Case Population
          └────────────┬────────────┘
                       ▼
                  Ground Truth
                   GT1 + GT2
                       │
             ┌─────────┴─────────┐
             ▼                   ▼
      external_signals      review activity truth
       价格 / BSR 可选          可选辅助信号
             └─────────┬─────────┘
                       ▼
                  Quality Gate
                       │
                       ▼
                  Accepted Cases
                       │
                       ▼
                benchmark_split
                       │
                       ▼
                benchmark_export
                       │
                       ▼
                  benchmark_data/
                       │
                       ▼
                    evaluation
```

---

# 3. 模块状态

| 模块 | 主要职责 | 状态 |
|---|---|---|
| `data_prep/` | 全量下载 + Market Discovery 前基础表 | **已有代码** |
| `population_scan/` | 大类级用户基础盘点 | **已有代码** |
| `market_discovery/` | local Market Discovery + 安全 cross-path 同名合并 | **已有代码** |
| `market_build/` | Market 商品、shared population、用户事件与累计历史 | **已有代码** |
| `case_build/` | Case discovery、shelf、用户选择、GT、Quality Gate | **已有代码** |
| `external_signals/` | 历史价格 / BSR 对齐 Case 的稳定接口 | **已有代码；provider 获取待接** |
| `benchmark_split/` | learning / validation / evaluation 划分 | **已有代码** |
| `benchmark_export/` | 跨表校验 + 最终 Market→Cases 物化 | **已有代码** |
| `evaluation/` | GT1 / GT2 / 商品需求与排名评测 | **已有代码** |

这里的“已有代码”指主要数据接口与计算逻辑已经落仓。研究阈值、Keepa provider 请求和部分扩展指标仍按各目录 TODO 冻结。

---

# 4. Population

## `population_scan/`

对一个大类或 v5 `rating_event_store` 扫描：

```text
source_partition
user_id
n_events
n_products
n_verified_purchases
first_event_date
last_event_date
```

它只做 case-agnostic 基础盘点，不按未来结果选用户。

## `market_build/`

把 Final Market 变成可被多个 Case 复用的资产：

```text
market_products.parquet
market_population.parquet
canonical_user_events.parquet
user_event_store/
user_history_cumulative.parquet
user_category_history_cumulative.parquet
user_market_history_cumulative.parquet
```

Market population 支持 `category / global` 两种来源与确定性哈希抽样；最终策略和规模仍在 TODO 中冻结。

详细见：

- [`population_scan/README.md`](population_scan/README.md)
- [`market_build/README.md`](market_build/README.md)

---

# 5. Market Discovery

`market_discovery/` 的 path-local Discovery 主体迁自 `AmazonReviewrepo@v5`。

Cross-path 已改成固定窄规则：

```text
同一 source_partition
+ market_label 安全规范化后完全相等
→ 直接合并
```

例如：

```text
Phone_Case
phone-case
phone case
```

统一为 `phone_case`。

Cross-path 不调用大模型，不做开放同义词发现、embedding merge 或 complete-link clustering。

输出：

```text
final_market.parquet
final_market.csv
```

详细见 [`market_discovery/README.md`](market_discovery/README.md)。

---

# 6. Case Build

## 6.1 Case Discovery

继续复用 v5 已验证的商品时间、区间累计与 ASOF 逻辑：

```text
Final Market
→ Market 商品时间轴
→ 每个商品形成 candidate focal
→ t0 = first_rating_date
→ evaluation window
```

结构完整的新品事件都先进入 focal candidate pool。正式 Case 由 Market × time_box 组装，一个 Case 可有多个 focal。旧的“每时间段只选 top-1 focal”、`post90>=50`、固定 competitor 数不再在这里提前筛。

## 6.2 t0 Shelf

competitor 基础时间资格：

```text
同 Market
product_id != focal
first_rating_date < t0
last_rating_date >= t0
```

ASOF 计算：

```text
pre_t0_review_count
pre_t0_rating_mean
pre_t0_recent_review_count
```

Top-150、8 CORE + 8 RESERVE 等旧 selection policy 没有继续写死。

## 6.3 Case Population

[`case_build/population/`](case_build/population/README.md) 只使用 `t0` 前历史计算：

```text
history_product_count
days_since_last_event
category_history_product_count
market_history_product_count
relation_stratum
```

同时输出 threshold scan，正式阈值冻结以后执行 eligibility 和确定性用户抽样。Case 用户集合在查询 future GT 前锁定。

## 6.4 Ground Truth

[`case_build/ground_truth/`](case_build/ground_truth/README.md)：

```text
GT2: all Case users -> product / none
GT1: GT2 positives -> target product
GT2 aggregate -> demand / share / rank
```

全部 future shelf 命中事件先保存在构建层，中间的 one-user-one-outcome policy 可以版本化重算。

可选生成：

```text
review_activity_truth.parquet
```

用于完整 shelf 的未来评论量排名辅助对照。

## 6.5 Quality Gate

[`case_build/quality/`](case_build/quality/README.md) 汇总：

```text
商品侧结构与活动量
selected users
GT1 样本
GT2 positive / none
focal demand
review activity
外部价格 / BSR signals（可选）
```

输出：

```text
quality_metrics.parquet
quality_decisions.parquet
accepted_cases.parquet
rejected_cases.parquet
```

结构性校验固定；研究阈值由版本化 JSON 配置。

---

# 7. External Signals

[`external_signals/`](external_signals/README.md) 定义 provider-agnostic 历史表接口：

```text
source_partition
product_id
event_timestamp / event_date
price
bsr / sales_rank
```

对齐 Case 后输出：

```text
case_product_external_signals.parquet
case_external_signals.parquet
case_shelf_with_external.parquet
```

因此 Keepa API 获取逻辑以后只需要把原始响应整理成统一历史表，不侵入 Case / GT 主链。

当前实际 Keepa token 调度、响应解析等 provider-specific 客户端仍在 [`external_signals/TODO.md`](external_signals/TODO.md)。

---

# 8. Benchmark Split

[`benchmark_split/`](benchmark_split/README.md) 只读取：

```text
market_id
case_candidate_id
t0
```

不读取 GT 数值决定归属。

支持：

```text
market_holdout
    整个 Market held out

temporal_within_market
    同 Market 早期 learning、后期 evaluation

hybrid
    unseen-market evaluation
    + seen-market temporal evaluation
```

---

# 9. Final Export

[`benchmark_export/`](benchmark_export/README.md) 在最终物化前检查：

- Case ID 唯一；
- focal 在 shelf 恰好一行；
- GT2 覆盖全部 Case users；
- GT2 target 都属于 shelf；
- GT1 与 GT2 positives 一致；
- `market_truth` 与 GT2 聚合一致；
- split 对 accepted cases 一一覆盖。

然后按 [`SCHEMA.md`](SCHEMA.md) 生成：

```text
benchmark_data/
├── markets/<market_id>/
│   ├── market_manifest.json
│   ├── products.parquet
│   ├── population/
│   └── cases/<case_id>/...
└── splits/
```

构建阶段尽量使用长表，只有 exporter 才按 Market / Case 正式物化文件。

---

# 10. Evaluation

[`evaluation/`](evaluation/README.md) 是模拟器输出的数值评测入口。

最小用户预测格式：

```text
case_candidate_id
user_id
predicted_outcome_product_id   # 商品 / NULL
```

当前指标包括：

```text
GT1 choice accuracy
GT2 full outcome accuracy
market entry accuracy
market-positive count error
Kendall tau
NDCG
demand total error
```

如果没有单独商品预测，商品需求直接从个体用户预测聚合；也支持额外输入 `predicted_demand_count / score / rank`。

概率校准、更多 ranking 指标和 review text Turing test 在 [`evaluation/TODO.md`](evaluation/TODO.md)。

---

# 11. 从 v5 继续复用的逻辑

新仓库主要继续使用 / 改造：

```text
market_discovery/*
product_time_summary
rating daily aggregation
Market-product timeline
active competitor interval sweep
Market pre-t0 cumulative features
competitor time qualification
product cumulative review counts
ASOF pre-t0 features
future-event join
market review activity ranking
```

主要移除的是旧 benchmark 强绑定的 selection / packaging policy：

```text
每时间段 top-1 focal
固定 post90 hard gate
固定 competitor hard gate
Top-150
8 CORE + 8 RESERVE
旧 case packaging 层级
```

---

# 12. 目录

```text
AmazonReviewsCases/
├── README.md
├── SCHEMA.md
├── TODO.md
├── requirements.txt
├── paths.py
├── utils.py
│
├── data_prep/
├── population_scan/
├── market_discovery/
├── market_build/
├── case_build/
│   ├── population/
│   ├── ground_truth/
│   └── quality/
├── external_signals/
├── benchmark_split/
├── benchmark_export/
└── evaluation/
```

每个主要目录都有自己的 `README.md` 和 `TODO.md`（Market Discovery 使用 `TODO_CROSS_PATH.md`）。已经确定的层级、时间语义和 GT 两层结构保持稳定；尚未确定的阈值与研究选择通过 TODO 和配置接口显式保留。

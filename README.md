# AmazonReviewsCases

Amazon Reviews 2023 上的 **SEMS（Self-Evolving Market Simulation）benchmark 数据构造仓库**。

当前公开基准以 `Electronics_v1_cases` Release 为事实标准。仓库正在做代码清理：**文档与 Release 语义已经冻结，但 main 中仍混有旧版/实验代码，不能仅凭目录名判断某段代码是否属于当前生产链。**

## Current Release

- Release: `Electronics_v1_cases`
- Archive: `Electronics_v1_cases.tar.gz`
- 381 个带 accepted Case 的 Final Markets
- 2424 个 accepted Cases
- 2493 个 accepted focals
- Release 中 `users/histories/events.jsonl` 共 3,894,971 条 pre-t0 history events，其中 3,417,022 条（87.7%）包含 `review_text`

Release 采用 JSON/JSONL 的 `Market → time_box → Case` 目录。生产 Parquet 中间表不打包进入正式 Release。

## Frozen benchmark semantics

```text
Final Market × time_box = Case
Case contains 1..N focals
Each focal owns:
  - t0 = first_rating_date
  - evaluation window [t0, t0+90d)
  - local shelf = focal + selected competitors
  - GT1 users / choice truth
```

同一个 Market、同一个 time box 最多只有一个正式 Case。多 focal Case 中，不同 focal 可以有不同 t0、不同 local shelf 和不同 GT1。

### Competitors

对每个 focal，candidate competitor 必须：

```text
same Final Market
product_id != focal_product_id
first_rating_date < focal.t0
last_rating_date >= focal.t0
```

如果候选数不超过 16，全部保留；超过 16 时按：

```text
pre_t0_recent_review_count DESC
pre_t0_review_count DESC
product_id ASC
```

取前 16。recent window 默认为 t0 前 120 天。Behavior graph **不参与正式 competitor 排序**。

### Behavior graph

当前 v1 中 behavior graph 只用于 focal diversity。正式强边口径是同一 Final Market、同一 leaf category，两个端点各至少 100 个用户，且共享用户至少 5。连通分量大小至少 6 才作为有效 behavior group 用于 focal selection。

仓库内仍存在“用 focal-centered co-review 选 competitor”的旧代码；该逻辑不属于 `Electronics_v1_cases` 正式语义。

### GT1

GT1 是 conditional observed choice：对每个 focal 的 local shelf，在 `[t0,t0+90d)` 中找到真实 Amazon rating/review observations；每个用户取确定性的 `first_observed_event`，再要求其 t0 前：

```text
history_product_count >= 3
days_since_last_event <= 365
```

GT1 不经过预抽 `case_users`，不设最大人数 cap，也不包含 `none`。这里的 truth 是 Amazon Reviews 中可观测评分/评论行为形成的 **observed choice proxy**，不是完整订单日志。

当前 Release 明确保证有正文的是 **用户 t0 前 history events**。GT1 future `choice_truth` 对应的 future review 正文不是 v1 的必需字段，也不能因为 history 有正文就推断 future truth 一定打包了正文。

### Quality

正式 hard gates：

```text
evaluation_window_complete = true
selected_competitor_count in [6,16]
GT1 users >= 20
history_product_count >= 3
days_since_last_event <= 365
```

同时执行 focal/local-shelf/competitor/GT1 唯一性、时间范围和可追溯性检查。多 focal Case 先按 focal 判定；失败 focal 被移除，只要 Case 仍有至少一个 surviving focal 就保留，并重新 union 构建最终 Case shelf。

未来新品表现（focal choice share、rank、post90 活跃度等）不是 acceptance gate。

## Release layout

详见 [`SCHEMA.md`](SCHEMA.md)。核心目录：

```text
Electronics_v1_cases/
└── {market_name}/
    ├── market.json
    ├── users/
    │   ├── users.jsonl
    │   └── histories/
    │       ├── summary.jsonl
    │       └── events.jsonl
    ├── products/
    │   └── products.jsonl
    └── cases/
        └── {time_box_id}/
            └── {case_id}/
                ├── case.json
                ├── focals.jsonl
                ├── shelf.jsonl
                ├── focal_competitors.jsonl
                └── ground_truth/
                    ├── gt1_users.jsonl
                    └── choice_truth.jsonl
```

只为实际存在 accepted Case 的 time box 建目录。

## Production path vs mixed legacy code

当前应当作为 v1 生产语义依据的模块：

```text
market_discovery/
market_build/                 # assets；其中 behavior_graph 实现仍需代码梳理
case_build/ discover/select/shelf
case_build/ground_truth/      # GT1 为 production；GT2 branch 为 experimental
case_build/quality/
scripts/package_market_cases.py
```

当前 Release **没有**把 GT2 population/none 任务打包为正式 benchmark。以下目录包含旧版或实验语义，不能直接视为 `Electronics_v1_cases` 正式链路：

```text
population_scan/
case_build/population/
benchmark_split/
benchmark_export/
evaluation/
external_signals/
market_build/behavior_graph/ 中的 competitor-selection branch
```

这些代码在完成本地版本审计前不应删除，也不应反向覆盖已确认正确的生产实现。

## Documentation

- [`SCHEMA.md`](SCHEMA.md): **冻结的 Release 输出 contract**
- [`PIPELINE.md`](PIPELINE.md): 当前生产数据流、版本边界与代码状态
- [`TODO.md`](TODO.md): 代码梳理任务和不可破坏约束

当代码、旧文档与已经发布的 `Electronics_v1_cases` 发生冲突时，优先级是：

```text
已确认的 Release artifact / benchmark semantics
> 当前根文档
> 经过审计确认的生产代码
> 未审计的旧代码、旧 README、旧 TODO
```

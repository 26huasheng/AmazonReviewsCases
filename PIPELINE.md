# SEMS Current Pipeline and Version Boundaries

本文件描述 `Electronics_v1_cases` 对应的 **当前生产语义**，同时标记仓库中仍混杂的旧版/实验代码。代码清理时必须以这里和 `SCHEMA.md` 为边界，不能为了统一风格破坏已经正确的实现。

## 1. Canonical production flow

```text
Amazon Reviews 2023
  ├── rating-only events
  ├── metadata
  └── full reviews (used for history text enrichment)
        ↓
market_discovery
        ↓
Final Markets
        ↓
market_build
  ├── market_products
  ├── canonical user/rating events
  ├── cumulative user histories
  └── full-period behavior components
        ↓
case_build discover
  └── evaluable focal candidates
        ↓
case_build select
  └── Final Market × time_box Cases + focals
        ↓
case_build shelf
  ├── focal_competitors
  └── Case union shelf
        ↓
GT1
  ├── focal-local future shelf events
  ├── first_observed_event
  ├── history / recency filter
  └── choice_truth
        ↓
Quality
  ├── focal-first accept/reject
  └── rebuild accepted Case shelf
        ↓
clean-market subset
        ↓
history review-text enrichment
        ↓
scripts/package_market_cases.py
        ↓
Market → time_box → Case JSON/JSONL Release
```

## 2. Market discovery

`market_discovery/` 负责确定长期 Final Market 边界。它不决定 GT1、不决定 future success、不把 focal candidate 当成正式 Case。

当前 Case 构造只消费已经冻结/审计后的 Final Market 资产。代码清理不得重新定义 Electronics v1 的 Market membership。

## 3. Market build

当前需要的 Market-level assets：

```text
market_products.parquet
canonical_user_events.parquet
user_history_cumulative.parquet
user_category_history_cumulative.parquet
user_market_history_cumulative.parquet
behavior components used by focal selection
```

`market_population.parquet` 属于 GT2 engineering lineage，不是 GT1 user source。

### Review text

rating-only canonical event path 可以继续作为大规模构建主索引。Full review JSONL 用于对 **已经确定的 pre-t0 history events** 做正文 enrichment。Review enrichment 不应改变用户集合、event timestamp、product identity、GT1 membership 或 choice。

当前公开 Release 已经包含 history `review_title / review_text`，但 main 中对应 enrichment plumbing 尚未完整反映出来，这是待修复的可复现性缺口。

## 4. Behavior graph

当前正式用途只有 **focal diversity**。

强边口径：

```text
same Final Market
same leaf category
endpoint n_users >= 100
shared_users >= 5
```

在 leaf 内做 connected components；`component_size >= 6` 才作为 valid behavior group。

Focal selection：

```text
0 or 1 valid group in Market × time_box
→ stable random select 1 focal from all evaluable candidates

>=2 valid groups
→ for each valid group that has candidates in this time box,
   stable random select 1 focal
```

仓库中仍有 focal-centered pre-t0 co-review competitor-selection branch。它属于旧版，不进入当前正式链。代码审计时先证明调用关系，再隔离/标记；不要直接删除可能仍被某个本地版本引用的函数。

## 5. Case discovery and selection

Candidate 的 `t0 = first_rating_date`。只要求：

```text
valid_t0
evaluation_window_complete for 90 days
```

Discovery 阶段不使用：

```text
post90 success threshold
future review count
future focal rank
competitor-count quality gate
GT1 size
```

正式 Case：

```text
Final Market × time_box
```

而不是“一 focal 一 Case”。

## 6. Competitor and shelf build

每个 focal 独立选 competitor：

```text
same Final Market
product_id != focal
first_rating_date < t0
last_rating_date >= t0
```

`<=16` 全留；`>16`：

```text
pre_t0_recent_review_count DESC
pre_t0_review_count DESC
product_id ASC
```

Case shelf 是所有 surviving focal local shelves 的 union。

禁止重新引入：Top150、CORE/RESERVE、recent>=10 hard gate、price gate、future-window event requirement、graph-based competitor ranking。

## 7. GT1

GT1 不依赖 `case_users`。

```text
for each focal:
  local shelf = focal + selected competitors
  scan [t0,t0+90d) complete canonical events
  keep users with >=1 local-shelf event
  deterministic first_observed_event per focal×user
  ASOF pre-t0 history
  require history_product_count >=3
  require days_since_last_event <=365
```

正式 truth key：

```text
case_id + focal_id + user_id
```

Choice 必须是 focal-local shelf 内真实 event。Review body 不是 GT1 choice 的必要字段。

同目录中 GT2/population branch 是实验代码；当前 Release 不打包 GT2。

## 8. Quality

Quality focal-first。默认：

```text
min_competitors = 6
max_competitors = 16
min_gt1_users = 20
max_gt1_users = none
```

结构与 GT1 一致性检查见 `case_build/quality/README.md`。未来 success/activity 只可作为统计，不得作为 selective acceptance gate。

## 9. Packaging

正式 v1 packager 是：

```text
scripts/package_market_cases.py
```

职责是只读现有 Quality/GT1/Market assets 并组织 Release，不重新跑 discovery/select/shelf/GT1/quality。

目标目录 contract 见 `SCHEMA.md`。

注意：当前 main 的 packager/上游 canonical events 尚未完整体现 Release 中已经存在的 history review-text enrichment。修复时应增加一个只读 enrichment path，而不是改 Case/GT1/Quality。

## 10. Mixed-version modules

### Production/current semantics

```text
market_discovery/
case_build/focal_selection.py
case_build/shelf.py
case_build/ground_truth/ GT1 path
case_build/quality/
scripts/package_market_cases.py
```

### Current concept but implementation requires reconciliation

```text
market_build/
market_build/behavior_graph/
review-text enrichment path
clean-market packaging inputs
```

### Experimental / legacy for Electronics v1

```text
population_scan/
case_build/population/
benchmark_split/
benchmark_export/
evaluation/
external_signals/
behavior_graph competitor-selection branch
GT2 branch in case_build/ground_truth/
```

## 11. Safe code-cleanup rule

整理代码时按以下顺序：

```text
1. inventory only — no edits
2. trace actual producers/consumers and local output paths
3. classify files/functions as current / legacy / uncertain
4. add tests around confirmed current behavior
5. only then isolate legacy code
6. never delete or rewrite uncertain code merely because another version looks newer
```

任何修改如果会改变 Final Market membership、Case IDs、focal selection、competitor set、GT1 membership/choice、Quality acceptance 或 frozen Release directory，必须停止并报告，而不是自动修改。

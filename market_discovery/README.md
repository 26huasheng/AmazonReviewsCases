# market_discovery

Market Discovery 从 `AmazonReviewrepo` 的 `v5` 迁入本仓，并作为 `Market → Cases` benchmark 构造链的第一段正式代码。

源逻辑主要来自：

```text
AmazonReviewrepo@v5
└── sems_market_pipeline/
    ├── market_discovery/
    └── market_merge/        # 这里只保留安全的同名合并思想
```

## 当前职责

```text
product_core.parquet
        ↓
按 Amazon category path 抽样商品标题
        ↓
path-local Market Discovery
        ↓
local_market_definitions.parquet
market_assignment.parquet
first_market.parquet
        ↓
跨 path 名称规范化
        ↓
仅合并规范化后名称完全相等的 market
        ↓
final_market.parquet
```

## 1. Path-local discovery

这一段保留 v5 的核心机制：

1. 对每个 Amazon category path 做确定性标题抽样；
2. LLM 判断该 path 应 `KEEP / SPLIT / REVIEW`；
3. LLM 给出 `market_label / center_term / equivalent_terms / support_terms`；
4. 固定规则对该 path 全部商品标题做 assignment；
5. `SPLIT` 时允许第二轮最终语义复核；
6. path 内 `AMBIGUOUS` 商品可以在既有候选 market 之间做轻量仲裁；
7. 单个 path 的 API/解析失败会记录为 `ERROR/REVIEW`，不会让其他 path 停止。

这里的 LLM 调用用于 **path 内 market 定义和商品归属**。

## 2. Cross-path merge

Cross-path merge 已经并入 Discovery，规则固定为：

> 同一 `source_partition` 内，`market_label` 经过安全格式规范化以后完全相等，直接合并。

例如以下名称视为同一个 label：

```text
Phone_Case
phone-case
phone case
PHONE---CASE
```

都会规范化为：

```text
phone_case
```

合并时：

- `source_market_ids` 做去重 union；
- `source_path_ids` 做去重 union；
- `source_category_paths` 做去重 union；
- `product_ids` 做去重 union；
- `product_count` 重新计算；
- final `market_id` 使用该组最小的已有 local market id，保持确定性；
- final `market_label` 使用规范化后的名称。

### 明确不做

Cross-path merge **不调用 LLM**，也不做：

- synonym discovery；
- embedding / semantic similarity merge；
- complete-link 聚类；
- 根据商品重叠自动合并；
- `phone_case` / `phone_cover` 这类语义近似名称的自动合并。

这里采用宁可漏并、不要错并的口径。

## 3. 输出

主要输出：

```text
<output_root>/<discovery_version>/
├── path_summary.parquet
├── local_market_definitions.parquet
├── market_assignment.parquet
├── first_market.parquet
├── first_market.csv
│
├── cross_path_market_overlap.csv
├── cross_path_market_overlap_summary.json
├── cross_path_exact_merge_audit.json
├── cross_path_exact_merge_summary.json
│
├── final_market.parquet
└── final_market.csv
```

其中：

- `first_market.*`：每个 path 的 local markets；
- `final_market.*`：完成安全同名/格式归一合并后的最终 Market，供后续 Case 构造使用；
- 合并后 `product_count < 9`（即商品数 ≤8）的 Market 不写入 `final_market`，只记在 `dropped_small_markets.json`；
- `cross_path_exact_merge_audit.json`：实际发生了哪些合并。

## 4. 运行

从仓库根目录运行完整 Discovery：

```bash
python -m market_discovery.cli \
  --product-core /path/to/product_core.parquet \
  --discovery-version market_v1 \
  --source-partition Electronics
```

需要设置兼容 OpenAI Chat Completions 的：

```text
LLM_API_KEY
LLM_MODEL
LLM_BASE_URL
```

也可以通过 fixture 做离线测试。

### 已经有 `first_market.csv` 时只跑合并

已有 v5 / 旧 Discovery 结果时，不需要为了新的 cross-path 规则重新调用 LLM。直接运行：

```bash
python -m market_discovery.merge_cli \
  --discovery-dir /path/to/existing/discovery/version_dir
```

该命令只读取：

```text
first_market.csv
```

并生成新的：

```text
final_market.csv
final_market.parquet
cross_path_exact_merge_audit.json
cross_path_exact_merge_summary.json
```

整个 merge-only 流程不会调用 LLM。

依赖见仓库根目录 `requirements.txt`。

## 5. 与 v5 的主要修改

- 补回了之前漏迁的 `discovery_pipeline.py`、`market_llm.py`、`prompts.py`；
- 改成独立仓库可运行的 import / path；
- 清掉 prompt 里已经不存在的 focal oversampling 描述；
- Cross-path merge 直接成为 Discovery 的最后一步；
- Cross-path 不再使用原 v5 的 semantic LLM merge；
- 合并范围固定为名称相同或纯格式差异；
- 增加 `final_market.parquet/csv` 和合并审计；
- 增加 merge-only CLI，允许复用已有 Discovery 输出；
- 增加对应单元测试。

剩余边角项见 [`TODO_CROSS_PATH.md`](TODO_CROSS_PATH.md)。

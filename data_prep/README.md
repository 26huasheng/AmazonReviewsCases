# data_prep

下载 Amazon Reviews 2023 一个大类的全量源文件，并做成 Market Discovery 之前需要的基础表。

不选择运行模式：始终要求 rating-only、metadata、full reviews 三类文件都在盘上。本地文件通过字段校验后不重下。

## 源文件

固定从 `https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023` 拉取：

```text
data/reviews2023/benchmark/0core/rating_only/<Category>.csv
data/reviews2023/raw/meta_categories/meta_<Category>.jsonl
data/reviews2023/raw/review_categories/<Category>.jsonl
```

默认 `data_root` 会解析到仓库旁已有的 `data/reviews2023`。

## 初筛产物

```text
outputs/data_prep/<Category>/
├── product_core.parquet
├── product_core_cleaning.json
├── rating_daily_summary.parquet
├── product_time_summary.parquet
├── rating_event_store/
├── storage_metadata.json
└── population_scan/
    ├── users.parquet
    ├── review_events.parquet   # 瘦表，不含正文
    └── summary.json
```

`product_core` 丢掉无 ID / 无标题 / 无 category path 的商品，并且只保留至少有一条 rating 的商品。`post90_rating_count` 是首评日起 90 天左闭右开窗口。

本阶段不调用 LLM，不停在 Market Discovery。

## 运行

唯一需要确认的是大类名称：

```bash
python -m data_prep.cli
```

非交互：

```bash
python -m data_prep.cli --category Electronics
```

已有完整 cache 时默认跳过重建；`--force-rebuild` 会重做 parquet，仍不会重下已通过校验的源文件。

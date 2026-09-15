# benchmark_export — Legacy Exporter

该目录是旧版 Parquet `Market → Cases` exporter，依赖 `market_population / case_users / population_truth / market_truth / split_assignments` 等旧 GT2-era contract。

它 **不是** `Electronics_v1_cases` 的正式 exporter，也不应被用来重打当前 Release。

当前正式 packager：

```text
scripts/package_market_cases.py
```

当前 frozen JSON/JSONL layout 见根目录 `SCHEMA.md`。

保留本目录是为了旧实验可追溯；代码梳理前不要直接删除。

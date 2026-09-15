# configs

配置文件只描述可显式参数化的构建规则；它们不能反向改变已经冻结的 `Electronics_v1_cases` 语义。

当前 Quality 默认：

```text
min_competitors = 6
max_competitors = 16
min_gt1_users = 20
min_history_product_count = 3
max_days_since_last_event = 365
max_gt1_users = null
```

旧 GT2/future-success 字段可以继续保留为兼容/实验配置，但在 Electronics v1 中必须是 inactive，不得因为配置存在就成为 acceptance gate。

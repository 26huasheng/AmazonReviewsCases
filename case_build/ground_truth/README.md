# case_build.ground_truth

本目录同时包含当前 production GT1 和实验/旧 GT2 branch，必须明确区分。

## GT1 — production

对每个 focal：

```text
local shelf = focal + selected competitors
window = [t0,t0+90d)
scan complete canonical events
users with >=1 local-shelf event
→ deterministic first_observed_event
→ history_product_count >= 3
→ days_since_last_event <= 365
→ GT1 users + choice_truth
```

GT1 不预先限制 `case_users`。Truth 粒度为：

```text
case_id + focal_id + user_id
```

Choice 是 Amazon rating/review observation 的 observed choice proxy；不是完整订单世界购买记录。

`choice_truth` 的 future review 正文不是 v1 contract 的必需字段。当前 Release 确认有正文的是 pre-t0 user history events。

## GT2 — experimental/inactive for Electronics v1

当 `case_users` 被提供时，现有 pipeline 仍能生成 population/none 相关 truth。这是 GT2 engineering branch，不进入当前 `Electronics_v1_cases` Release，也不能被拿来重新定义 GT1。

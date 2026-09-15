# case_build.population — Experimental GT2 Engineering

该目录不是当前 Electronics v1 GT1 production path。

它来自 ex-ante population / none 任务设计：在 future window 之前固定一批用户，再预测其未来是否进入 shelf 以及选择哪个商品。这里的 `case_users`、eligibility、sampling、population cutoff 等概念属于 GT2 lineage。

当前 Release 的 `users/users.jsonl` 是 accepted GT1 users 的 Market-level 去重 registry，**不是**这里生成的 case population。

代码清理时保留本目录作为实验实现，除非明确启动 GT2 新版本；不得让它重新成为 GT1 dependency。

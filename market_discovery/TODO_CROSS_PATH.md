# Cross-path TODO / Status

Cross-path 的职责是合并语义上属于同一长期竞争市场的 local markets，输出 Final Market；不是用行为图替代市场定义。

当前代码梳理要求：

- 保留已经用于 Electronics v1 的 Final Market 结果作为 frozen input。
- 审核 cross-path merge 的输入、LLM failure handling、resume/checkpoint，避免单次失败阻塞全 pipeline。
- 不因整理代码重新生成并覆盖 Electronics v1 Market IDs。
- 若未来修复 discovery，只为新版本/新 category 产出新 lineage，并明确 version/tag。

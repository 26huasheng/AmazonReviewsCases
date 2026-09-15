# behavior_graph TODO

- [ ] 找出当前实际生成 Electronics v1 focal-selection components 的本地代码。
- [ ] 核对 component 输出必须携带 focal selection 所需的 Market identity 与 component size/status。
- [ ] 修复 main 中 behavior graph producer 与 `market_build/pipeline.py` 的 API 混版。
- [ ] 将 legacy focal-centered competitor-selection path 标记为 inactive/legacy。
- [ ] 搜索所有 CLI/import/caller，确认没有 production path 仍调用 legacy selection 后再考虑移动或删除。
- [ ] 增加测试：graph 只影响 focal diversity，不影响 competitor set。

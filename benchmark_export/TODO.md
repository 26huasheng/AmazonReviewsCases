# benchmark_export TODO

- [ ] 标记 legacy，不作为 Electronics v1 CLI/documentation default。
- [ ] 搜索外部/本地 callers，确认是否仍有旧实验依赖。
- [ ] 不尝试把本 exporter 强改成当前 Release schema；正式 v1 继续使用 `scripts/package_market_cases.py`。
- [ ] 若确认无人依赖，可后续移动到 legacy namespace，但不要在首次梳理中删除。

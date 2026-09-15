# benchmark_split — Legacy / Needs Redesign

当前实现基于旧的“一条 accepted case 有一个 `case_candidate_id + t0`”假设。Electronics v1 已经是：

```text
Case = Final Market × time_box
Case may contain multiple focals with different t0
```

因此该目录不能直接作为当前 Release 的正式 split 逻辑。

保留代码仅用于旧实验与未来 redesign 参考。新的 split 若需要，应明确以 `case_id` 为单位，并决定 multi-focal/time-box 的 temporal semantics，再单独版本化。

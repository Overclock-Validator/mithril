An ordinary completion-journal write could stall block delivery while holding the spool mutex. Update in-memory completeness immediately and submit the disposable hint to one bounded writer without blocking. Queue overflow may lose a persistent hint but preserves live completeness.

Deletion and corrupt-tail truncation still wait for an ordered tombstone before mutating the slot file, preventing an older queued hint from resurrecting a replacement file. Write failures stop appends and invalidate the journal; failed invalidation refuses the file mutation. Clean close drains the writer and retries dropped hints. Other slot I/O, invalidations and shutdown may still block.

This is a repairable shred cache, not vote history or a durable account checkpoint. Lost hints cause reassembly/repair and cannot authorize votes or replace block validation.

On Zen 5, three 300-iteration runs of the same `BenchmarkShredSpoolMarkComplete` fixture measured median run p99 **6.201 → 0.581 µs**. The baseline is the preceding deployed combined source, not #278. Packet append and close/drain are outside timing; disk work is moved, not eliminated. Historical live trials did not establish overall large-block p99 improvement.

[Design, method and limitations](https://github.com/Overclock-Validator/mithril/blob/d587643472ba768b4f149ccd2dc2b97e88b8937b/docs/shred-spool-completion.md).
Fresh local Turbine/blockstream race suites passed against the combined FEC/streaming parent, including blocked-writer, queue-overflow, tombstone and failed-invalidation cases. Combined integration validation also covers #278 and the other performance reviews. Historical native benchmarks retain their original baselines; this preparation pass ran locally, without touching the live validator.

[Historical benchmark evidence](https://github.com/Overclock-Validator/mithril/tree/f0b72ab239efbbd4811498d73b36ba73eb6192e1/docs/results) is preserved outside the proposed merge; reusable benchmarks and maintained contracts remain in source.

Stacked on `7layer/review-streaming-preparation`, which includes #278 at `e1204b32`. Review this diff against that parent.

[Rebased validation and exact source heads](https://github.com/Overclock-Validator/mithril/blob/7layer/review-integration-20260915/docs/results/review-preparation/2026-09-16/README.md).

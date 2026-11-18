# PR #351 review evidence and operator notes

The review follow-ups deduplicate stake-index appends, expose the sparse-store
compatibility requirement, and make leader completion/reward preparation
observable. They preserve ordered Harmonic execution and the mandatory join of
scratch execution before finalization. The reserve remains configurable at a
15 ms default.

## Defaults and compatibility

| Area | Change relative to `alpenglow-dev` |
| --- | --- |
| Completion reserve | 75 ms to 15 ms; override with `validator.block_completion_reserve_ms` or `--leader-completion-reserve-ms`. This is a scheduling target, not an execution timeout. |
| Votor receiver | 512 to 4,000 total connections: `MaximumVATValidators` (2,000 voters) × `DefaultReceiverMaxConnsPerPeer` (2). Temporal/Harmonic recommends two connections per peer. There is no additional headroom hidden in this calculation. |
| Prepared TPU transactions | At most 2,048 eager preparations; later entries prepare lazily. The transaction queue capacity and execution checks are separate. |
| Harmonic health | Requires `SubscribeBundles2` as well as authenticated packet/block streams. Harmonic establishes upstream Jito authentication through the bundle subscription; its messages are drained, never separately executed. Ordered execution remains on `SubscribeBlocks2`. |
| Sparse AccountsDB | Kind-3 manifests identify sparse record files. Older binaries reject this kind during compaction. Disabling compaction does not undo conversion; rollback requires a compatible binary or a store copy made before conversion. |
| Memory tuning | `tuning.memory_limit_mb` is a Go soft limit, not an RSS cap. The trial used 12,288 MiB and two 256 MiB serialization arenas; these are deployment settings, not new universal defaults. |
| RPC/snapshot configuration | RPC bind address and known snapshot-source filters are explicit configuration. Snapshot ranking and eligibility remain distinct from execution verification. |

The sparse compatibility notice is emitted once per opened AccountsDB, after
the first durable conversion or when compaction first encounters an existing
kind-3 marker. It does not add another on-disk marker or alter publication /
hole-punch ordering. See [the storage format notes](../../../storage-sparse-reclamation.md).

## Leader observations

The host is an AMD Ryzen 9 9950X (16 cores / 32 hardware threads), with 128 GB
installed DDR5 configured at 3,600 MT/s. It is not a 16 GB host. The later run
uses Go 1.27.3 linux/amd64, Harmonic FBA with eight workers and a 15 ms reserve.

The fixed later sample is the first 440 completed leader blocks from run
`20261111-234132Z_f85d0da_7ca89416`, slots 468312444–468498423. All 440 used
Harmonic. The binary was built as `f85d0da0-dirty`; its fork/epoch work was
subsequently committed as `7f05abfe`. It predates these review follow-ups.

| Logged metric | Median | p95 | p99 | Maximum |
| --- | ---: | ---: | ---: | ---: |
| Completion duration | 5 ms | 8 ms | 14 ms | 27 ms |
| Freeze phase | 0.014 ms | 0.042 ms | 1.245 ms | 5.063 ms |
| Drain-deadline lag | 0 ms | 0 ms | 0.081 ms | 0.652 ms |

Four of the 440 completion margins were negative; the worst was −12 ms.
The median margin was 8 ms. Reward preparation was ready in all 440 blocks,
with zero missing preparations. Ready can legitimately mean empty certificates.
These figures support exposing tail latency; they do not establish that a
15 ms reserve is adequate on every host, or that every negative margin causes
an on-chain skip.

The earlier `cbd95db` reward-pipeline sample contained 348 blocks with a 40 ms
reserve: completion median 12 ms, p95 19 ms, p99 27 ms and maximum 34 ms; two
negative margins. This is historical context, not an isolated A/B comparison:
blocks, reserve, workload and other code changed between the runs.

Timing caveats: the old completion/margin log fields truncate to integer
milliseconds. `drain_deadline_lag_ms` is captured before `Freeze()` and excludes
the scratch join. `freeze_ms` includes the join plus locking/entry flushing.
The new `execution_quiescence_ms` measures only `executing.Wait()`.

## Replay, memory and tip observations

The November 10 memory trial's warm process RSS median changed from 16.01 GiB
to 11.57 GiB (23 post-warmup memory samples). The baseline window was about
123 seconds; the later warm window was 11 minutes. Code and memory settings
changed together, and the later long-running process can use more memory.
These observations are not proof that a complete validator fits a 16 GB host.

| Replay sample | Blocks | Mean consumed CU/block | SlotReplay median / p95 | Total CU / total SlotReplay ns |
| --- | ---: | ---: | ---: | ---: |
| Before memory trial | 8,379 | 26,240,026 | 5.798 / 12.023 ms | 3.8833 |
| After memory trial | 4,327 | 19,131,003 | 5.130 / 10.670 ms | 3.1655 |

Each row uses CU and time from the same blocks, but the two rows use different
blocks. The lower latency after the change also accompanied lower CU per block;
this table does not establish a replay speedup or regression. Streaming execution
overlaps block reception, so CU/foreground SlotReplay ns is not an isolated VM
instruction-throughput measurement. A controlled parent-account-lock experiment
is reported separately, with its workload and timing exclusions.

### Matched parent-account-lock experiment

The [runner](../../../../scripts/bench_parent_account_lock.py) builds isolated
test binaries from `7f05abfe`, changing only the parent-account accessor for the
unlocked baseline. The production checkout remains locked. Each repetition
replays the same independently funded system transfers from the same immutable
parent; transaction digests, bank hashes and consumed CU must agree across both
variants. The test checks successful transfer balances as well as consumed CU.

The larger follow-up has four alternating pairs of 500 measured blocks per
workload/variant (2,000 per variant/size), after 20 warmup blocks per trial.
It uses `GOMAXPROCS=2`, four transaction workers and the same host described above.

| Transfers/block | CU/block | ProcessBlock mean, unlocked → locked | CU/ns, unlocked → locked |
| --- | ---: | ---: | ---: |
| 64 | 9,600 | 0.383 → 0.399 ms | 0.02505 → 0.02405 |
| 256 | 38,400 | 1.422 → 1.442 ms | 0.02701 → 0.02664 |
| 1,024 | 153,600 | 5.789 → 5.903 ms | 0.02653 → 0.02602 |

The means differ by approximately +4.2%, +1.4% and +2.0%, but paired changes vary
in sign and magnitude (about −10% to +29% across workloads). The locked runs also
contain individual ~66 ms outliers; the raw samples retain them. Normal validator
load and some validation tooling were active on the host. The earlier 100-block
trials were noisier still. Neither set establishes a stable causal overhead or a
worst-case bound. Keep the safety lock; representative matched mainnet replay on
an isolated host is still required to quantify its end-to-end effect.

This measures whole `ProcessBlock` execution, including memory-backed account
loading, execution and bank hashing, but **not** the outer production `SlotReplay`
interval. Signature verification is pre-marked complete; disk/network/consensus
and durable folds are excluded. This low-CU/high-account-churn synthetic workload
must not be compared directly with the streaming mainnet CU/ns table above.

[Follow-up results and distributions](parent-account-lock-followup/results.json)
and [initial exploratory results](parent-account-lock/results.json) retain per-block
samples, exact CU totals, source/fixture/binary hashes, hardware and paired results.
Reproduce into a new directory, without starting a validator:

```sh
python3 scripts/bench_parent_account_lock.py \
  --revision 7f05abfe --blocks 500 --trials 4 \
  --out /tmp/mithril-parent-account-lock-results
```

The initial subscription observation changed from 34/48 blocks with positive
Jito tip-account credits to 8/8. Those counts exclude failed transactions and
crank withdrawals. Different opportunities and times prevent attributing a
precise tip/revenue increase to one code change. The authentication requirement
is the reason to keep the subscription, independently of that small sample.

All selected observations and source artifact hashes are in
[observations.json](observations.json). No change in these follow-ups has been
deployed as part of collecting this evidence.

## Metrics and alerts

Metric names are exported in seconds where applicable:

- `block_production_completion_duration_seconds`: finalization start through
  network completion, excluding the local replay handoff.
- `block_production_deadline_margin_seconds`: protocol deadline minus network
  completion; negative values mean late. This histogram's sum can decrease:
  use bucket/count rates for quantiles, not `rate(..._sum)`.
- `block_production_deadline_misses_total`: negative margins, including
  sub-millisecond misses; accompanied by a warning with the slot and margin.
- `block_production_execution_quiescence_duration_seconds`: time spent joining
  scratch execution in Freeze; recorded even if the attempt later aborts.
- `block_production_reward_preparation_total{status}`: `ready`, `missing`,
  `disabled`, or `synchronous`. Missing preparation emits a warning; this metric
  counts attempts that reach preparation, including later-aborted attempts.

For example, alert on `increase(block_production_deadline_misses_total[10m]) > 0`
or `increase(block_production_reward_preparation_total{status="missing"}[10m]) > 0`.
Completion observations count successfully published blocks, not aborted leader
attempts; keep the existing terminal-outcome counters alongside them.

No timeout was placed around `executing.Wait()`. A scratch transaction may still
read mutable bank state even when its eventual result is discarded. The new
regression test holds such a read across both drain and protocol deadlines and
requires finalization to wait. Cooperative cancellation or admission rules need
their own design and measurement; bypassing the join is not a safe optimization.
Likewise, missing reward preparation does not trigger a new synchronous fallback
that would reintroduce completion latency.

## Stake fix and review boundaries

The original stake-index/candidate-epoch-cache fix can be reviewed independently
on `smcio/stake-index-durable-fold` (local commit `1df45ec2`), based on
`alpenglow-dev` at `a8beea31`, together with the
per-flush deduplication tests. It excludes the diagnostic-artifact expansion,
storage format change, leader timing work and newer fork/rewards implementation.

The incident surfaced at slot 467856000 (epoch 1082→1083): accepted stake writes
were absent from the derived enumeration index after speculative hints were
discarded. A subsequent mismatch at 467857531 exposed reuse of a stale candidate
epoch-stake cache. Index keys now come from committed fold deltas, and candidate
stakes are checked/rebuilt from the verified boundary-parent scan. Deduplication
reduces raw index writes; scan-time deduplication already prevented double stake
accounting. Future-slot hints remain pending until their own fold or unwind.

The newer fork/epoch changes in `7f05abfe` are separate from these small review
follow-ups and deserve a separate review. This evidence does not claim a live
fork unwind or epoch-transition validation for that work.

## Validation

The full race suites passed for AccountsDB, global state, Alpenglow, Harmonic,
statsd, block production, replay and node startup/configuration. `go vet`,
`git diff --check` and a complete node build also passed. The standalone stake
branch passed its full global/replay race suites independently.

[Commands, source hashes and scope](validation.json) record these review follow-ups.
The earlier [fork/epoch validation](../../fork-epoch-implementation/2026-11-11/validation.json)
and [epoch scan-reuse measurements](../../epoch-scan-reuse/2026-11-11/results.json)
are retained separately. These checks did not restart or install a binary into
the running validator. Local PR drafts and scratch artifacts are excluded from Git.

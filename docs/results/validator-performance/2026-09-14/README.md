# Consolidated validator validation and checkpoint measurements

The combined runtime code was archived and tested at commit
`4a5a9761847a4fd8e7d60c72810082cf91fb73d5` against base
`33dde4050d9250557583395810799aaac2f54017`. Subsequent additions are review
documentation, attributes and evidence through `b24de8e6`. The later queue
retention fix and automated regression CI have a separate
[validation report](../../queue-retention/2026-09-14/README.md).
The live validator was not redeployed for these validation runs.
This folder adds the checkpoint/voting evidence to the existing standalone
streaming and leader-packing benchmark reports.

## Combined branch validation

Go 1.26.4, Apple M4 Pro locally and Ryzen 7 9700X on Linux. Local race testing
used GOMAXPROCS=4 and package parallelism two; native used GOMAXPROCS=2,
package parallelism one and nice 15 while the live validator remained active.

Race tests passed on both machines for accounts, alpenglow, block,
blockprod/scheduler, config, consensus, costmodel, merkletree, replay, sbpf,
sigverify, statsd, the synthetic transaction fixture and node. Metrics has no
tests. The full Turbine race suite passed on both machines. Native vet across
all selected packages and the complete `./cmd/mithril` build passed.

The selected multi-package race command nevertheless exits **1**, because the
existing `pkg/sealevel` BPF-loader suite fails: 19 tests report failures before
`TestExecute_Tx_BpfLoader_Close_ProgramData_Success` panics inside
`UpgradeableLoaderClose`. The unchanged base on both machines reproduces the same failed-test
names and panic. This is not a claim that the full repository test suite passes.
The failure list and raw baseline/candidate logs are included for review.

The native command and exits are recorded in [validation.json](validation.json):

```sh
GOMAXPROCS=2 go test -race -p 1 -count=1 \
  ./pkg/accounts ./pkg/alpenglow ./pkg/block ./pkg/blockprod/... \
  ./pkg/config ./pkg/consensus ./pkg/costmodel ./pkg/merkletree \
  ./pkg/metrics ./pkg/replay ./pkg/sbpf ./pkg/sealevel \
  ./pkg/sigverify ./pkg/statsd ./pkg/tpu/txfixture ./pkg/turbine \
  ./cmd/mithril/node
```

`go vet -p 1` used the same package list; `go build -p 1 ./cmd/mithril` passed.
The review binary SHA256 is
`67fd57f8fadd73d9a433c16a5d3d9bff814654e744a641822732659821d3fb66`.
It is an isolated review build, not the running validator binary.

On an unchanged checkout of the base, reproduce the known failure with:

```sh
go test -race -p 1 -count=1 -run '^TestExecute_Tx_BpfLoader_' ./pkg/sealevel
```

Raw logs: [local](local-race.log), [native](native-race.log),
[unchanged local base](baseline-sealevel-race.log),
[unchanged native base](baseline-native-sealevel-race.log), and
[failed-test comparison](baseline-failure-comparison.json).
The original native runner is retained as [validate-native.py](validate-native.py);
its absolute paths identify the isolated historical test directories.

`git diff --check` passes. Raw Go benchmark output is preserved verbatim, with
attributes allowing its padded CPU-identification lines and collapsing generated
evidence in GitHub review. A credential-pattern scan found no GitHub tokens or
private PEM keys in the changed files; runtime keys and state were not copied.

## Checkpoint benchmark

On Zen 5, GOMAXPROCS=1, 300 roots with 5,000 unique keys each: approximately
30 MB encoded output, three repetitions using `-benchtime=200ms -count=3`.
The differential fixture implements the original synchronous selection and
encoding path. That algorithm is unchanged in the PR's base; this benchmark
was run when the checkpoint patch was integrated into the preserved live source.

| Stage | Three sample results |
| --- | --- |
| Original synchronous snapshot | 206.55 / 207.35 / 202.44 ms/op |
| Capture immutable view on replay | 5.936 / 6.057 / 5.922 µs/op |
| Encode captured view on worker | 202.52 / 201.84 / 200.83 ms/op |

Capture allocates 35,168 bytes in 11 allocations. Encoding still allocates
approximately 99 MB and consumes its CPU time in the background. The old
additional full-payload copy is excluded from the benchmark baseline. This is
a change in which work blocks replay, not a 30,000-fold speedup of checkpoint
creation or the whole validator.

```sh
GOMAXPROCS=1 go test -run '^$' \
  -bench '^BenchmarkTransactionStatusCheckpointCapture$' \
  -benchtime=200ms -count=3 ./pkg/replay
```

See [raw benchmark output](checkpoint-native-benchmark.log).

## Live checkpoints and vote progress

Before the patch, a five-minute trace found nine synchronous status snapshot
stalls of 170–269 ms, exactly 128 replayed banks between consecutive events
(median interval 31 seconds). The first ten post-deployment checkpoint commits
reported capture median **30.653 µs**, range **28.694–34.464 µs**, for encoded
payloads of 24.5–43.4 MB. Worker encoding median was **230.855 ms**, range
**179.073–366.582 ms**; total background fold median was 324.5 ms.
Records and measurement boundaries are in [checkpoint-live.json](checkpoint-live.json).

A clean restart verified the final version-2 history against its reservation
marker before resuming the same identity and ledger. The bounded probe check
observed 130 replay/admission/notarize events, no admission rejections and no
exhausted reservations; 666 reservation checks retained at least 16 slots of
headroom. [Probe validation](checkpoint-probe-sanity.json).

Separate earlier live comparisons motivate the other voting changes: an
84.296 ms history rename occurred inside an 84.960 ms history-to-injection
interval despite removing per-vote fsync. Moving writes to the ordered worker
kept filesystem work off the voter. For the atomic replay watermark, sequential
40-second samples measured replay-callback-to-broadcast p95 12.885 → 2.031 ms;
median was 1.684 → 1.622 ms. These compare deployed intermediate versions, not
the current development base against the full combined PR.

Three subsequent completed five-minute captures with large-block generation
resumed included our vote in 1,746 of 1,796 unique FAST certificates (97.216%).
The matched replay-to-enqueue median was 1.554 ms and p95 2.008 ms. The public
dashboard window and observer differ. Some FAST improvement began before the
checkpoint patch while load was paused, and restart changed transient queue
state; no isolated FAST-score gain is claimed. See
[post-deployment observation summary](post-deploy-voting-summary.json).

These live observations include the separate FEC/producer work from #259. They
do not replace standalone testing of this PR and do not establish mainnet
power-loss or complete consensus safety. The opt-in mode's assumptions and
operator requirements are in [reserved-vote-history.md](../../../reserved-vote-history.md).

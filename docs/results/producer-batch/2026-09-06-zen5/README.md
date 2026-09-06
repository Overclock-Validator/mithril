# Producer batching and shred-generation benchmarks

Measured September 6, 2026 on an AMD Ryzen 7 9700X (Zen 5), Linux amd64,
Go 1.26.4, `GOAMD64=v1`, `CGO_ENABLED=0`. Each process used `GOMAXPROCS=1`
and was pinned to logical CPU 6. SMT and frequency boost were enabled on the
shared host. These are serial producer workloads on one logical CPU.

## Comparison with current alpenglow-dev

A subsequent comparison uses dev head `7e4e8af1`, which already has incremental
size accounting and one-FEC batching. The PR is **2.60x faster** for maximum-size
transactions and **2.10x faster** for small transfers in the serial producer
workload. The maximum-size comparison uses three slots on both sides to fit
dev's slot limits. See [the branch-head comparison](alpenglow-dev-head/README.md)
for raw samples, matched-batch controls, and full scope.

The tables below retain the earlier comparison against PR commit `84776893`;
they do not use the current dev head as their baseline.

## What changed

The reviewed branch already increased the batch target to 61,632 bytes.
Its EntryBuilder reserialized all pending transactions on every append to
estimate the next batch size, making batch construction quadratic in the
number of transactions per batch. The follow-up changes:

1. Measure each appended transaction once and accumulate its encoded size.
2. Return that tracked size on flush, eliminating serialization whose output
   was immediately discarded.
3. Carry generated packets and every FEC root directly into BroadcastSession,
   eliminating owning shred parsing, payload copies, and root reconstruction.

## Complete-workload results

Medians of five samples per version, one full 50,000-transaction workload per
sample. Versions ran sequentially with order reversed on alternating rounds.

| Workload | Original | Incremental accounting | Final | Overall speedup |
| --- | ---: | ---: | ---: | ---: |
| 1,232-byte transactions, two slots of 25k | 2,114.572 ms | 384.560 ms | 285.456 ms | 7.41x |
| 215-byte transfers, one slot of 50k | 2,810.088 ms | 97.931 ms | 75.332 ms | 37.30x |

The maximum-size workload is split to fit the default data-shred budget:
32,768 data shreds per generated slot. The benchmark includes entry building,
serialization, erasure coding, Merkle proofs, shred signing, and session/block
bookkeeping, including final flushes. The packet sink only counts packets.
Fixture construction and validation, execution, admission signature checks,
routing, UDP, and receiver processing are outside timing. These are CPU-stage
measurements, not executed-block latency or whole-validator throughput.

Detailed methods, allocations, packet counts, and raw samples:

- [Maximum-size transactions](max-size/README.md)
- [Small transfer transactions](small-transfer/README.md)
- [Steady-state per-transaction measurements](steady-state.md), which compare
  the intermediate and final implementations only

## Baselines and reproduction

**Original** uses the producer implementation at reviewed branch commit
`847768930cde10b6c885e4a82648d192b34098a9`. **Intermediate** adds incremental
transaction-size accounting. **Final** also removes the flush serialization
pass and uses generated packets/FEC roots directly in BroadcastSession.
All three use the same 61,632-byte target and the same workload code.
The table measures follow-up fixes against the already-expanded batching
implementation; it is **not a comparison with the PR base branch**, whose
batch target was only 1,926 bytes.

The timed production files in the original snapshot match that reviewed
commit byte-for-byte. The intermediate snapshot changes only EntryBuilder
in that path. The final snapshot's timed production files match this PR.
The benchmark files checked in here are the measured sources with gofmt
formatting; no workload or timing logic was changed.

To run the final benchmarks on Linux:

```sh
CGO_ENABLED=0 GOAMD64=v1 go test -c -o /tmp/mithril-producer.test ./pkg/blockprod
GOMAXPROCS=1 taskset -c 6 /tmp/mithril-producer.test \
  -test.run '^TestMaxSizeProducerFixture$'
GOMAXPROCS=1 taskset -c 6 /tmp/mithril-producer.test \
  -test.run '^$' -test.bench '^(BenchmarkProducerBlock50k|BenchmarkProducer50kMaxSize)$' \
  -test.benchmem -test.benchtime=1x -test.count=5
```

For an A/B reproduction, create separate clean checkouts at the reviewed
commit and at this PR's final commit. Copy these identical files from the
final checkout into each baseline:

- `pkg/blockprod/entry_bench_test.go` (shared packet-counting sink)
- `pkg/blockprod/producer_block_bench_test.go`
- `pkg/blockprod/producer_maxsize_bench_test.go`

For the intermediate checkout, additionally apply
[incremental-accounting.patch](incremental-accounting.patch) from its repo root
with `git apply --unidiff-zero <path-to-patch>`.
Build each test binary with the same toolchain and environment. Run each
workload separately with `-test.count=1`, alternating original/intermediate/final
and final/intermediate/original for five rounds, as in the recorded run.
Report the median ns/op, B/op, and allocs/op across each version's five samples.
Select an available CPU on your host in place of CPU 6 if necessary.

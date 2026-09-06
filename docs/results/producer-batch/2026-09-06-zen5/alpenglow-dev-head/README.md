# Producer comparison with alpenglow-dev head

Measured September 6, 2026 on an AMD Ryzen 7 9700X (Zen 5), Linux amd64,
Go 1.26.4, `CGO_ENABLED=0`, `GOAMD64=v1`, `GOMAXPROCS=1`, pinned to logical
CPU 6. The host was shared, with SMT and boost enabled; no running service
or CPU-affinity setting was changed. Medians of five alternating one-iteration
samples per arm. Both test binaries were built on this host with the same
compiler and identical benchmark source.

## Exact baselines

- `alpenglow-dev`: `7e4e8af115a27aa15f3401d59f3be1f0d79a4c1b`, the fetched head
  at measurement time. It already has incremental size accounting, a
  30,816-byte (one FEC) batch target, slot entry-byte limits, and an asynchronous
  shred worker.
- PR #259 producer: `a598cd0508cab1788c688b8ce667c6c810e1f2d5`, using its own
  61,632-byte (two FEC) default.
- Control: the same PR code, setting `Limits.MaxBatchBytes = 30,816` only in
  the benchmark, to match the dev head's target.

Production source was unchanged in both snapshots. The standalone benchmark
was copied into each snapshot's `pkg/blockprod` directory. This compares the
two heads' serial producer CPU stages; it does not measure a rebased or ported
version of this PR on top of dev. The PR's target branch remains unchanged.

## Results: 50,000 transactions

| Workload | alpenglow-dev default | PR default | Speedup | Time reduction |
| --- | ---: | ---: | ---: | ---: |
| 1,232-byte transactions, three slots | 734.052 ms | 282.296 ms | 2.60x | 61.54% |
| 215-byte transfers, one slot | 150.339 ms | 71.442 ms | 2.10x | 52.48% |

With both implementations using the **same 30,816-byte target**:

| Workload | alpenglow-dev | PR at one FEC | Speedup |
| --- | ---: | ---: | ---: |
| 1,232-byte transactions, three slots | 734.052 ms | 288.378 ms | 2.55x |
| 215-byte transfers, one slot | 150.339 ms | 72.424 ms | 2.08x |

The measured benefit largely survives with a one-FEC target. Moving the PR
from one to two FECs reduces elapsed time by about 2.1% for maximum-size
transactions and 1.4% for small transfers in these samples. This does not
establish the best batch target for live latency or pipelined throughput.

The earlier 7.41x/37.30x figures compare follow-up fixes against the older PR
implementation, before incremental size accounting. They are historical
within-branch results, not improvements over the current dev head.

## Workload and limits

The same pool of 512 signed, parsed transactions is reused. Small transfers
are exactly 215 bytes. Maximum-size transactions are legacy system transfers
plus a 981-byte UTF-8 memo, exactly 1,232 bytes. All fixtures pass structural
sanitization, signature verification, and canonical byte round trips before
timing.

Small transfers use one 50,000-transaction generation pass. Maximum-size
transactions use **16,667 + 16,667 + 16,666 transactions across three slots**
on every arm. This fits both dev's entry-byte bound (20 MiB minus the reserved
48-byte ending tick) and the default 32,768 data-shred limit. The harness
asserts both limits, transaction/batch/packet counts, and a nonzero block ID.
It does not run the bank's admission or execution paths.

The original two-slot maximum-size workload cannot be reused unchanged for
the dev baseline: 25,000 such transactions exceed its entry-byte limit, and
its one-FEC batching would also exceed the data-shred limit. This report uses
three slots on both sides; its timings should be compared within this table.

Included: EntryBuilder append/flush, entry serialization, erasure coding,
Merkle proofs, shred signing, headers/footers/ending ticks, chained roots, and
block-ID bookkeeping. The sink only counts packets. Excluded: fixture setup,
transaction execution, admission signature verification, scheduler/reservation
policy, asynchronous shred-worker queueing/overlap, routing, UDP, and receiver
work. **This is not whole-validator throughput or executed-block latency.**

## Work and allocation counts

| Workload / implementation | Entry batches | Packets, data + coding | Max data shreds / slot | Allocated bytes / workload | Allocations / workload |
| --- | ---: | ---: | ---: | ---: | ---: |
| Maximum / dev | 2,085 | 134,016 | 22,336 | 2,129,429,048 | 3,844,267 |
| Maximum / PR default | 1,023 | 131,328 | 21,888 | 708,002,592 | 974,514 |
| Maximum / PR one FEC | 2,085 | 134,016 | 22,336 | 712,676,760 | 997,857 |
| Small / dev | 350 | 22,592 | 11,296 | 374,665,624 | 1,215,632 |
| Small / PR default | 175 | 22,592 | 11,296 | 141,461,632 | 731,204 |
| Small / PR one FEC | 350 | 22,592 | 11,296 | 141,777,944 | 735,400 |

## Reproduction and raw data

Copy [producer_branch_bench_test.go](../../../../../pkg/blockprod/producer_branch_bench_test.go)
into `pkg/blockprod` in clean checkouts of the exact dev and PR commits above.
This file contains its own fixture and sink helpers and can be used without
the PR's other benchmark files.

Build each binary on Linux:

```sh
CGO_ENABLED=0 GOAMD64=v1 go test -c -o /tmp/producer-dev.test ./pkg/blockprod
# Repeat in the PR checkout, changing the output to /tmp/producer-pr.test.
GOMAXPROCS=1 taskset -c 6 /tmp/producer-dev.test \
  -test.run '^TestProducerBranchComparisonFixtures$' -test.v
GOMAXPROCS=1 taskset -c 6 /tmp/producer-pr.test \
  -test.run '^TestProducerBranchComparisonFixtures$' -test.v
```

Run each workload/arm in its own process with one iteration. For example:

```sh
GOMAXPROCS=1 taskset -c 6 /tmp/producer-dev.test \
  -test.run '^$' \
  -test.bench '^BenchmarkProducerBranchHeads$/^maximum-1232B$/^default$' \
  -test.benchmem -test.benchtime=1x -test.count=1
```

Repeat with the PR binary using `default` and `one-fec`, and with `small-215B`
in place of `maximum-1232B`. Alternate dev/default, PR/default, PR/one-fec
and the reverse order over five rounds, as recorded in
[sample-order.json](sample-order.json). Each combined file below contains five
sample outputs in round order, with trailing whitespace trimmed.

| Workload | dev/default | PR/default | PR/one-fec |
| --- | --- | --- | --- |
| Maximum | [raw](dev-default-maximum-1232B.txt) | [raw](pr-default-maximum-1232B.txt) | [raw](pr-one-fec-maximum-1232B.txt) |
| Small | [raw](dev-default-small-215B.txt) | [raw](pr-default-small-215B.txt) | [raw](pr-one-fec-small-215B.txt) |

[Method and source digest](method.json), [medians and ranges](summary.json),
[dev validation](dev-validation.txt), [PR validation](pr-validation.txt).

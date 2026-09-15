# Bounded scheduler references and regression CI

Follow-up to PR 279 at `b24de8e616b28eccd1f86c3384a1af96bf9624c0`, with the
queue fix in `667d002f`. This bounds scheduler references and adds regression
CI. The existing slot-local skip scanning/rebuffering policy is unchanged;
`scheduler.go` is identical to the comparison commit.

## Queue lifetime

Each entry now records its position in both priority heaps. Consumption,
eviction and expiration remove its counterpart directly, clear the removed
backing-array pointer and release the deduplication-map reference. Returned
transactions retain their owned wire, decoded and prepared data for the caller;
rebuffering inserts exactly one reference in each heap again. Neither priority
ordering, equal-reward FIFO, capacity, fee scoring nor cross-slot retry rules
change.

The regression keeps one low-reward entry while inserting and consuming 100,000
higher-reward entries. With capacity 256, the old algorithm retained 100,001
min-heap references. The candidate finishes with **one active entry and one
reference in each heap**, without requiring a later cleanup sweep. Additional
tests cover symmetric eviction, expiration, repeated rebuffering, new
higher-priority arrivals, retry in a later bank, concurrent operations, exact
reference-model ordering and zeroed slice storage beyond the visible length.

Both heaps' lengths and map membership equal the number of buffered entries.
Backing-array allocation can retain its high-water capacity, but unused positions
are nil and cannot retain transaction payloads.

## Validation

Apple M4 Pro, Go 1.26.4. The scheduler race suite passed with GOMAXPROCS=4.
Scheduler vet and a complete `./cmd/mithril` build also passed.
The exact new CI command also passed locally with GOMAXPROCS=2:

```sh
GOMAXPROCS=2 go test -race -p 2 -count=1 \
  ./pkg/alpenglow ./pkg/consensus ./pkg/replay \
  ./pkg/turbine ./pkg/sigverify ./pkg/blockprod/... ./cmd/mithril/node
```

The new `regression-tests` GitHub job runs these complete package suites,
including reservation/crash recovery, background history ordering, checkpoint
capture and streaming cancellation, alongside the existing build job. This is
not the full repository suite; pre-existing sealevel failures remain documented
in the [combined validation report](../../validator-performance/2026-09-14/README.md).

Logs: [scheduler race](queue-fixed-race.log), [CI command locally](ci-race-local.log),
[retention result](retention.log).

## Removal-cost tradeoff

Three alternating paired rounds of the existing `BenchmarkBufferDrain`,
GOMAXPROCS=1, each selecting all 120,000 transactions from a prefilled queue.
Queue construction and transaction preparation are excluded. Baseline uses the
PR's `b24de8e6` buffer; candidate uses indexed counterpart removal. Both run the
same benchmark. Timing excludes GC costs outside the timed region and does not
measure a sustained validator workload.

| Priority distribution | Baseline median | Candidate median |
| --- | ---: | ---: |
| Equal rewards / FIFO | 143.0 ns/pop | 188.0 ns/pop |
| Mixed rewards | 254.0 ns/pop | 274.2 ns/pop |

Both report zero timed allocations per pop. Equal-reward candidate samples were
188.0 / 302.4 / 183.5 ns, so the raw variability should not be hidden behind the
median. These measurements show the direct removal cost, not a net validator
speedup. Memory retention is bounded; the effect on block fullness, garbage
collection and FAST omissions remains unmeasured. No live deployment or
validator restart was performed for this follow-up.

```sh
GOMAXPROCS=1 go test ./pkg/blockprod/scheduler -run '^$' \
  -bench '^BenchmarkBufferDrain$' -benchtime=120000x -count=3
```

[All sample values](drain-summary.json), [execution order](drain-runs.json),
raw baseline/candidate logs alongside this report. These results are local M4
measurements and must not be substituted for the earlier Zen 5 bank benchmarks.

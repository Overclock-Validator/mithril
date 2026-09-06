# Producer batch serialization and broadcast

Measured on September 6, 2026 on an AMD Ryzen 7 9700X, Linux amd64,
Go 1.26.4, `GOAMD64=v1`, `CGO_ENABLED=0`, `GOMAXPROCS=1`, pinned to CPU 6.
Each result is the median of five one-second samples. Before/after binaries
ran sequentially, with their order reversed on alternating pairs. The host
was shared, with SMT and frequency boost enabled.

The baseline is branch `7layer/erasure-repair-performance` at `84776893`
plus the earlier review fixes, including incremental transaction-size
accounting. These measurements isolate two subsequent changes:

1. Return the already tracked encoded batch size on flush, eliminating the
   serialization pass whose bytes were discarded.
2. Retain each FEC root during generation and send generated packets directly
   through `BroadcastSession`, eliminating packet parsing, payload copies,
   and root reconstruction in the producer path.

Both versions use the same 61,632-byte batch target and benchmark source.

| Benchmark | Before ns/tx | After ns/tx | Time reduction | Before B/tx | After B/tx | Before allocs/tx | After allocs/tx |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `append` | 777.7 | 466.1 | 40.1% | 2,142 | 990 | 14 | 7 |
| `append-and-shred` | 1,887 | 1,545 | 18.1% | 4,620 | 3,468 | 22 | 15 |
| `append-and-broadcast` | 1,872 | 1,407 | 24.8% | 4,620 | 2,817 | 22 | 14 |

`append-and-broadcast` exercises entry construction, serialization, erasure
coding, Merkle proofs, signing, and broadcast-session root/index bookkeeping.
The packet sink only counts emitted packets. Transactions are signed and
parsed before timing. Transaction execution, admission signature verification,
peer routing, UDP, and receiver processing are excluded. This is a producer
CPU-stage improvement, not a measurement of whole-validator throughput or
transaction latency.

`append-and-shred` retains the public API that returns owning parsed shred
objects; its improvement mainly reflects the removed flush serialization.
The direct packet path is used by the live broadcast session.

Run the current benchmark on Linux with:

```sh
GOMAXPROCS=1 CGO_ENABLED=0 GOAMD64=v1 taskset -c 6 go test ./pkg/blockprod \
  -run '^$' -bench '^BenchmarkEntryBuilder$' -benchmem -benchtime=1s -count=5
```

Raw samples: [before](before.txt), [after](after.txt).

Validation covers existing golden packet digests; FEC roots matched against
every packet's Merkle proof across unsigned and signed boundary sizes;
packet, index, chained-root, and block-ID equality with the parsed-shred path;
and exact encoded sizes for legacy/v0 transaction batches. A generation
error must leave broadcast packets and commitments unchanged. The affected
repair, turbine, costmodel, and blockprod packages passed race tests, and vet
passed for blockprod, turbine and its subpackages, and the repair simulator CLI.
The packet, root, block-ID, and entry-size checks also passed on the Zen 5.

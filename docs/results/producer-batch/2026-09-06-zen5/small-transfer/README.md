# Small-transfer transaction workload

Same hardware and sampling method as the [maximum-size workload](../max-size/README.md):
one pinned logical CPU, `GOMAXPROCS=1`, five alternating one-iteration samples
per version. Each iteration processes 50,000 pre-signed, pre-parsed 215-byte
transfer fixtures through entry batching and the production broadcast session.
The pool contains 512 fixtures and is reused.

| Producer version | Median ms / 50k transactions | Allocated bytes / workload | Allocations / workload |
| --- | ---: | ---: | ---: |
| Original reviewed implementation | 2,810.088 | 10,374,805,784 | 52,011,173 |
| Incremental size accounting | 97.931 | 232,001,264 | 1,131,417 |
| Final implementation | 75.332 | 141,461,424 | 731,202 |

The combined speedup is **37.30x**, or **97.32% less elapsed time**. All versions
produce 175 entry batches and 22,592 packets. A full batch holds 286 small
transactions, so the original repeated serialization does substantially more
redundant work than in the maximum-size workload's 49-transaction batches.
The 37x result is specific to this fixture size and baseline.

The timed path includes final flush, serialization, erasure coding, Merkle
proofs, shred signing, header/footer/ending tick, and block-ID construction.
The packet sink only counts packets. Execution, admission signature
verification, routing, UDP, and receiver processing are excluded. This is
one synthetic slot-generation pass, not a valid executed 50k-transaction block.

Source: [BenchmarkProducerBlock50k](../../../../../pkg/blockprod/producer_block_bench_test.go).
Raw samples: [original](original.txt), [intermediate](intermediate.txt),
[final](current.txt). Each file contains the five sample outputs (trailing whitespace trimmed) in
sample order. See [baseline definitions and reproduction](../README.md).

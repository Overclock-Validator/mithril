# Maximum-size transaction workload

Five samples per version on an AMD Ryzen 7 9700X, Go 1.26.4, Linux amd64,
`GOAMD64=v1`, `CGO_ENABLED=0`, `GOMAXPROCS=1`, pinned to logical CPU 6.
Each sample times one complete 50,000-transaction workload. Version order
alternates original/intermediate/current and current/intermediate/original.
The host was shared, with SMT and frequency boost enabled.

Each of the 512 precomputed fixtures is a signed legacy system transfer plus
a 981-byte UTF-8 memo, exactly 1,232 bytes in canonical wire form. All fixtures
pass structural sanitization, signature verification, and byte-for-byte
reserialization checks before timing. The fixture pool is reused.

Each iteration completes two slots of 25,000 transactions, including final
flushes, headers, footers, ending ticks, block IDs, and chained roots. Each
slot produces 32,768 data shreds, fitting the default data-shred budget.
Compute and account budgets are not enforced; transactions are not executed.
A single 50,000-transaction slot at this wire size exceeds the default
32,768 data-shred cap.

| Producer version | Median ms / 50k transactions | Allocated bytes / workload | Allocations / workload |
| --- | ---: | ---: | ---: |
| Original reviewed implementation | 2,114.572 | 10,132,867,648 | 12,525,832 |
| Incremental size accounting | 384.560 | 1,243,725,728 | 1,663,372 |
| Final implementation | 285.456 | 707,691,696 | 974,131 |

The combined speedup is **7.41x**, or **86.50% less elapsed time**. The last
two optimizations reduce time by a further 25.77% from the intermediate version.
Dividing the final median by two gives 142.728 ms per 25,000-transaction
slot-generation pass; this is an aggregate average, not measured slot latency.

All versions produce 1,022 entry batches and 131,072 packets (data plus coding).
A full entry batch holds 49 maximum-size transactions at 60,424 serialized
bytes including its 56-byte header. Total transaction bytes are 61,600,000.

The timed path includes entry construction, serialization, erasure coding,
Merkle proofs, shred signing, and broadcast-session/block bookkeeping. The
packet sink only counts packets. Fixture signing/parsing/validation happens
before timing. Execution, admission signature verification, routing, UDP, and
receiver processing are excluded. This measures the producer CPU stage,
not whole-validator throughput or the latency of an executed block.

Source: [BenchmarkProducer50kMaxSize](../../../../../pkg/blockprod/producer_maxsize_bench_test.go).
Raw samples: [original](original.txt), [intermediate](intermediate.txt),
[final](current.txt). Each file contains the five sample outputs (trailing whitespace trimmed) in
sample order. See [baseline definitions and reproduction](../README.md).

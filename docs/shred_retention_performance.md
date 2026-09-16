# Shred retention sweep scheduling

`SlotAssembler` previously scanned its incomplete/completed slots, block-ID hints,
rejected IDs, partial observations, and priority repair state on every incoming
shred, including duplicates and completed-slot packets. The new schedule skips
age sweeps when neither the observed edge nor the retention floor nor relevant
retained state has changed. The original sweep algorithm and age limits remain.

Mutations invalidate the cached sweep after adding old identity hints or partial
observations, resetting a generation, or terminating completion. Completion
success, error, cancellation and abort all release protected parent-ID state.
Repair-floor advancement, reduction and clearing are checked on the next packet.
The hard incomplete-slot capacity check remains unconditional on every packet,
including catch-up insertion at an unchanged edge. Eviction preference and
protection for completing generations and the repair head are unchanged.

Regression coverage includes changing the repair floor without advancing the
edge, every terminal completion outcome, newly added old identity hints, and
capacity overflow at a fixed edge. Existing completion, cancellation, generation,
FEC and repair tests also run as part of the full Turbine suite.

## Isolated benchmark

`BenchmarkRetentionRepeatedCompletedShred` drives the public `AddShred` path
with a completed-slot packet and 513 retained entries in each of four metadata
maps. It measures the repeated scan/rejection case, not full packet decoding,
authentication, FEC, replay, or a whole-validator speedup. Five 500 ms runs:

| Host | Baseline median | Candidate median | Allocation |
| --- | ---: | ---: | ---: |
| Apple M4 Pro | 11,617 ns/packet | 7.718 ns/packet | 0 on both |
| Ryzen 7 9700X, GOMAXPROCS=2 | 11,443 ns/packet | 12.99 ns/packet | 0 on both |

Baseline and candidate use the same fixture; Go's source overlay selects the old
assembler for baseline runs without changing other source. This fixture shows the
avoided work, not a prediction for a validator's actual map population.

Validation passed locally and natively: race suites for Turbine, replay,
consensus and node; Turbine vet; complete validator build. The live integration
applies this patch over the exact deployed FEC/peer-isolation/status-expiry source.

Historical live trials and their limitations are in the
[archived evidence](streaming-preparation-evidence.md). Component results do not establish
a sustained FAST-inclusion improvement.

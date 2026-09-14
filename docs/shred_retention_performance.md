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

## Initial live trial

Deployed on the Ryzen 7 9700X at 20:47:35 UTC on September 14. A bounded
30-second post-deployment CPU profile recorded no retention-sweep samples,
compared with 7.66% of sampled CPU in the preceding profile. These are different
live workloads; absence of samples does not mean zero cost.

After excluding startup and extra profiling, local FAST inclusion was
1,381/1,408 (98.08%) across about 7.4 minutes, compared with 3,646/3,792 (96.15%)
in the prior 20-minute sample. For >=10,000-transaction targets it was 168/190
(88.42%) versus 375/509 (73.67%). These are selected observed FAST certificates,
not all blocks or reward opportunities. Different leaders/workloads, network
variation and restart connection refresh prevent assigning the entire change to
this optimization. All 27 remaining omissions had locally observed notarize
votes; local serialization does not establish remote delivery.

Voting remained current, with all 87 desired peers connected and no transport
queue drops/send errors/timeouts or safety/history/fold faults in the reviewed
run. Continuous load produced all 28 blocks in seven completed own-leader
windows. The validator and automatic load/monitor services were left running.

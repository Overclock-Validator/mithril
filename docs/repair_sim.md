# Deterministic repair simulation

`cmd/repair-sim` is a single-process harness for measuring the local path from
an incomplete slot to a block that is available to replay. It exists because
near-tip repair and deep catch-up optimize different outcomes:

- near the tip, latency of the first replay-blocking slot matters;
- during catch-up, sustained useful data and completed slots per second matter.

The first implementation deliberately stops before UDP and transaction
execution. It establishes a deterministic, correctness-checked baseline before
network realism or alternative scheduling policies are introduced.

## What is real and what is simulated

| Stage | Implementation |
| --- | --- |
| block-component serialization | production `turbine.MarshalBlockComponent` |
| 32+32 FEC generation and Merkle signing | production `turbine.Shredder` |
| missing-shred selection | production `SlotAssembler.RepairRequests` |
| packet parsing and Merkle/signature validation | production `ParseShred` and `VerifySignature` |
| verified-shred insertion | production `ShredSpool` |
| threshold detection and Reed-Solomon recovery | production `SlotAssembler.AddShredFrom` |
| component decode and transaction-signature gate | production slot completion path |
| remote peer, latency, jitter, loss, duplication, bandwidth | deterministic in-process simulator |
| replay notification | block emission is recorded as “offered to replay” |
| transaction execution | not run in this version |

Synthetic entries are valid Alpenglow entry-batch components but contain no
transactions. This isolates shred/FEC/repair/storage costs; it is not a replay
execution benchmark.

## Scenarios

### Near tip

`near-loss` alternates two useful patterns across FEC sets:

- 30 data + 1 coding shred: one repair response crosses the threshold and
  reconstructs the other missing data shred;
- 31 data shreds: the one missing data shred must be fetched directly.

Selected omitted shreds can also arrive through the simulated live path while
a repair response is outstanding. This measures cancellation/late-response
behavior without changing production scheduling.

### Deep catch-up

`mixed` begins every FEC set with 16 data + 15 coding shreds. One fetched data
shred crosses the threshold and reconstructs the remaining 15.

`sparse` begins every FEC set with two data shreds and no coding layout. The
ordinary repair interface serves data shreds only, so nearly all missing data
must arrive over the simulated network. Comparing `mixed` with `sparse`
quantifies the network work avoided by already-held coding shreds.

## Commands

```sh
go test ./pkg/turbine/repairsim

go run ./cmd/repair-sim \
  -scenario=near-tip \
  -slots=200 \
  -fec-sets=4 \
  -seed=1 \
  -output=/tmp/repair-near.json

go run ./cmd/repair-sim \
  -scenario=deep-catchup \
  -availability=mixed \
  -slots=1000 \
  -fec-sets=4 \
  -seed=1 \
  -repair-latency=20ms \
  -repair-bandwidth=104857600 \
  -output=/tmp/repair-deep-mixed.json

go run ./cmd/repair-sim \
  -scenario=deep-catchup \
  -availability=sparse \
  -slots=1000 \
  -fec-sets=4 \
  -seed=1 \
  -repair-latency=20ms \
  -repair-bandwidth=104857600 \
  -output=/tmp/repair-deep-sparse.json

go test ./pkg/turbine/repairsim \
  -run '^$' -bench '^BenchmarkScenarios$' -benchmem -count=5
```

Logical trace timestamps are deterministic. `wall_elapsed_ns`, allocations,
and `stage_cpu_ns` are actual local measurements and therefore are not expected
to be byte-identical across runs.

## Correctness gates

The current tests require:

- exact requested FEC-set counts from authentic generated shreds;
- Merkle/signature validity for every canonical packet;
- no completed slot when loss is present and repair is disabled;
- complete, canonical entry streams after threshold recovery;
- a complete `ShredSpool` journal record before replay admission;
- rejection and retry of a corrupted repair response;
- deterministic logical traces for identical seeds;
- late repair responses to leave a completed block unchanged.

The Turbine package also retains focused byte-for-byte Reed-Solomon recovery
tests. The simulator verifies the stronger end-to-end consequence: recovered
shreds must decode to the exact canonical entry sequence and pass the normal
completion gates.

## Generator compatibility finding

Building this harness exposed a multi-FEC component bug: the local generator
set `DATA_COMPLETE_SHRED` at every FEC boundary. The decoder correctly treats
that flag as the end of one serialized component, so a component spanning more
than one FEC set was truncated and failed to decode. The generator now follows
Agave's ordering: construct every FEC set, then mark only the final data shred
of the component complete (or last-in-slot). A regression test round-trips one
1,300-entry component across multiple FEC sets.

## Future mode selection

No production mode switch is added here. A later policy experiment should use
both replay distance and observed Turbine usefulness, with hysteresis:

- stay in near-tip mode while the replay gap is small and Turbine supplies a
  high fraction of useful shreds before repair deadlines;
- enter catch-up mode only when the replay-blocking gap is sustained and live
  Turbine delivery is insufficient to approach FEC thresholds;
- return to near-tip mode only after both the gap and repair backlog fall below
  lower thresholds.

That signal is preferable to slot distance alone: a node may be numerically
close to the tip while receiving too few live shreds, or far behind while its
local spool already holds most FEC thresholds.

## Next steps

1. Add shallow catch-up and whole-block-pressure configurations.
2. Add a loopback-UDP transport without replacing the deterministic mode.
3. Expose internal FEC start/finish timing through opt-in instrumentation.
4. Run replay execution against a reusable synthetic bank fixture.
5. Compare ordinary requests with explicit test-only threshold-acquisition and
   earliest-blocked-slot policies.


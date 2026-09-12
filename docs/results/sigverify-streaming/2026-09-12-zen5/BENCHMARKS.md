# Signature batching and overlap benchmark harness

The repository adds `pkg/turbine/transaction_verifier_flow_benchmark_test.go`.
It uses the production transaction verifier with two/four workers and signature
batch targets four/eight. It verifies every signature and rejects internal native
fallbacks. Generated inputs have distinct, valid signatures and exact serialized
sizes of 228 or 1,232 bytes. Optional captured public block transactions replace
generated inputs.

These are controlled verifier-pool measurements, **not complete validator replay
or a recorded shred-arrival trace**. Transaction decoding and fixture generation
are outside the timer. Each component is submitted as soon as its scheduled bytes
are available; partial vector groups never wait for another component.

## Workloads

- `catchup`: all transactions are available immediately in one request.
- `tip_200ms_after_complete`: components become available across 200 ms, then the
  full block is submitted. This isolates the cost of waiting for block completion
  with the same improved batching pool.
- `tip_200ms_overlap`: complete components become available across 200 ms and are
  submitted immediately. Components contain at most 60 KiB of transaction bytes.
  This is a synthetic workload parameter; serialized entry overhead, shred
  recovery, and actual cluster component boundaries are omitted.
- `sparse_{4,7,8}tx_200ms_overlap`: at most 256 transactions arrive in tiny complete
  components across 200 ms, exposing partial-batch latency without a high-rate
  sub-millisecond network timer simulation.

For each measured run, `cpu-ms/block` is process user+system CPU time summed across
cores, including pool/message preparation, scheduling, allocations and benchmark
bookkeeping. `signatures/s` in 200 ms workloads includes the deliberate network
availability waits; it is delivered workload throughput, not peak crypto speed.

`ready_p95-ms` measures scheduled component availability to that component's
verification future completing. It is a component latency, not individual
transaction execution latency. `submit_p95-ms` measures time inside bounded pool
submission. `feed_lag_p95-ms` captures lateness between scheduled availability and
calling submission, including timer granularity, CPU contention and backlog from
an earlier blocked submission. `residual_p95-ms` measures final scheduled
availability to the final future completing. Completion timestamps come from the
pool, so a delayed observer goroutine does not inflate completion latency.

Capture/profile workload delays alongside residual: delayed synthetic arrivals
must not be described as successfully hidden signature work.

## Build and run on Zen 5

Build fresh package test executables from the candidate checkout with the native
Go toolchain; run `-test.run='^$'` when selecting `r51` explicitly so unrelated tests
cannot initialize a different backend first:

```sh
go test -c ./pkg/turbine -o /tmp/turbine-streaming.test
go test -c ./pkg/replay -o /tmp/replay-streaming.test

GOMAXPROCS=8 \
MITHRIL_SIGVERIFY_FLOW_BACKEND=r51 \
MITHRIL_SIGVERIFY_FLOW_FIXTURES=/srv/mithril-planner-pr257-trial-20260912/fixtures \
/tmp/turbine-streaming.test \
  -test.run='^$' \
  -test.bench='^BenchmarkTransactionVerificationFlow$/^captured_2622581$' \
  -test.benchtime=3x -test.count=3
```

Omit `MITHRIL_SIGVERIFY_FLOW_FIXTURES` for generated 228/1,232-byte workloads.
`MITHRIL_SIGVERIFY_FLOW_COUNT` changes generated transaction count or limits
captured fixtures to a prefix. Leave it unset for full captured blocks. A fresh
test executable is required when changing native backend.

Fixture helper checks are `TestTransactionVerificationFlowFixtureShape` and
`TestTransactionVerificationFlowComponentBoundaries`. The former verifies actual
marshaled byte lengths, signature validity, and distinct signer identities.

## Actual execution contention probe

`run_execution_contention.py` runs the existing production
`LoadAndExecuteTransaction` benchmark for a signed system transfer
(`BenchmarkLoadAndExecuteTransferResultMode/lean`) before and during signature
verification. It runs two **separate processes**, both restricted to the same eight
physical cores with `GOMAXPROCS=8` each. The runner verifies that the selected CPU
IDs represent eight distinct physical cores; inspect the server's topology before
selecting IDs.

This execution probe includes the transaction load/execute function. It excludes
account commit, dependency planning, full-block replay, and interaction through a
single shared Go scheduler/heap. It provides evidence of hardware/OS contention;
live whole-validator timings are still needed to choose the production default.

```sh
python3 run_execution_contention.py \
  --turbine-bin /tmp/turbine-streaming.test \
  --replay-bin /tmp/replay-streaming.test \
  --fixtures /srv/mithril-planner-pr257-trial-20260912/fixtures \
  --slot 2622581 --cpus 0-7 \
  --out /tmp/mithril-execution-contention --samples 3
```

The output directory must be new. Conditions (workers two/four, target four/eight,
catch-up/200 ms overlap) are deterministically shuffled within each repeated
sample. Every condition gets its own immediately preceding execution baseline.
The runner waits for the verifier's warmup/calibration to finish, releases its
explicit rendezvous, then measures concurrent execution. It refuses to use a
sample if verification load ends before the execution probe finishes. Increase
`--catchup-blocks` or `--tip-blocks` if that happens. It stops only its own benchmark
child on failure; it never interacts with the validator service.

Raw output and commands are preserved in each sample directory. `comparison.json`
records CPU topology, run arguments, execution slowdown ratios, all signature
metrics and medians across repeats. Signature metrics describe the **whole load
window**, while the execution probe occupies a portion of that window; do not
interpret the load-window signature numbers as continuously contended results.
Background server activity remains a possible confounder and must be noted in
any conclusions.

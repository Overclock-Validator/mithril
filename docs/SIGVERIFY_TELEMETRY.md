# Sigverify workload telemetry

Sigverify workload telemetry is disabled by default. Capacity alone enables a
**passive** trace:

```text
MITHRIL_SIGVERIFY_TELEMETRY_CAPACITY=65536
MITHRIL_SIGVERIFY_TELEMETRY_OUTPUT=/var/tmp/mithril-sigverify.jsonl
```

Passive mode does not drain replay jobs or TPU packets, change which worker
claims queued work, wait for a batch, or add a timer. Each worker processes the
same one item it received under the ordinary scheduling policy. It records
queue depth at that claim, reserves a one-job dispatch ID, and correlates the
exact verification attempts and outcomes produced by that job.

An intrusive nonblocking scheduling experiment is separately opt-in:

```text
MITHRIL_SIGVERIFY_TELEMETRY_CAPACITY=65536
MITHRIL_SIGVERIFY_TELEMETRY_SCHEDULING_SIMULATION=true
MITHRIL_SIGVERIFY_TELEMETRY_OUTPUT=/var/tmp/mithril-sigverify-simulation.jsonl
```

Scheduling simulation lets a worker claim work already visible in its input
channel. Replay stops at 64 jobs or after reaching 64 signature lanes; TPU
stops at 64 packets. Neither path waits for a fuller group or adds deliberate
latency. This changes worker ownership and therefore is not a passive sample of
the normal scheduler. The snapshot, every dispatch record, JSONL summary, and
Prometheus dispatch metrics label the mode as `scheduling_simulation`.

## What is measured

The capacity independently bounds recent exact verification attempts,
public-key recurrence state, and worker dispatch records. Enabled collection
intentionally allocates an exact byte key for
`public key || signature || signed message`; it is a profiling facility, not
an always-on cache.

The Prometheus endpoint reports, separately for turbine, replay, and TPU:

- signatures per transaction and signed-message sizes;
- individual verification attempts, including valid/invalid outcomes in the
  retained trace;
- exact duplicate-verification hits;
- public-key reuse hits and reuse distance;
- queue occupancy at worker claims;
- scheduling-simulation signature-lane widths.

Queue and simulated-width histograms include a `collection_mode` label. Passive
dispatches are deliberately excluded from the simulated-width aggregate:
without claiming the queued items, their exact per-job signature counts cannot
be known. Passive dispatch records still retain the exact signature count of
the single job actually processed and the contemporaneous queue depth.

Exact duplicate means byte-for-byte equality within the bounded history; no
probabilistic digest is used. A history miss does not prove global uniqueness,
so capacity should exceed the recurrence window being evaluated. Public-key
reuse is independent of exact duplicate detection and must not be inferred
from the accounts cache.

The duplicate and key-recurrence histories are process-wide, so they can detect
a transaction verified first by turbine and later by replay, or first by TPU
and later elsewhere. The `source` on a hit identifies the current verifier,
not the source of the earlier matching attempt. Unknown source values fold into
the bounded `unknown` label.

## Exact ordering and dispatch correlation

`sigverifytelemetry.Current().VerificationTrace` is the retained chronological
attempt suffix. Each entry contains:

- exact public key, signature, and original signed message bytes;
- process-wide attempt `Sequence`;
- `BeginEventSequence` and `CompletionEventSequence`;
- cryptographic outcome;
- `DispatchID`, zero-based `JobIndex`, and zero-based signature `LaneIndex`.

`DispatchID == 0` means the verifier was not entered through an instrumented
replay/TPU worker claim (for example, a direct verifier call or turbine). A
nonzero tuple maps the attempt and its outcome to one stable dispatch record,
even when multiple workers complete out of order.

A dispatch is reserved at worker claim time, before TPU parsing. Its
`ClaimEventSequence` therefore precedes parser and verification work. After
parsing discovers the exact signature counts for every claimed packet/job, the
record receives `JobSignatures`, `SignatureLanes`, and `ReadyEventSequence`.
An exported record with ready sequence zero was still in preparation (or was
evicted before a late ready update) when the snapshot was taken. Malformed TPU
packets that cannot reach cryptographic verification have a zero-lane job.

Attempt begin, attempt completion, dispatch claim, and dispatch ready events
share one observer-lock event clock. This is an exact process event order, not
a wall-clock timestamp, and concurrent runs need not produce the same
interleave. A valid-only cache replay performs lookup at begin and admission at
completion; it must not infer completion order from attempt order.

Production hooks begin an attempt immediately before Ed25519 and finalize it
immediately afterward. Invalid signatures therefore do not create trace entries
for later signatures that were never attempted. The lower-level
`RecordVerification` API intentionally leaves the outcome unknown.

## JSONL export and bounded-history caveats

When `MITHRIL_SIGVERIFY_TELEMETRY_OUTPUT` is nonempty, `mithril run` atomically
replaces the file with mode `0600` during orderly shutdown. The path alone does
not enable collection. Schema `mithril-sigverify-v3` writes:

1. one summary with `collection_mode`;
2. retained verification records in attempt order, including dispatch
   correlation;
3. retained dispatch records in dispatch-ID order, each with
   `dispatch_mode`, claim/ready events, queue samples, and exact job boundaries.

Fixed-size cryptographic inputs use lowercase hex and the arbitrary binary
message uses base64. Consumers merge claim, ready, begin, and completion fields
by event sequence when a single timeline is needed. A hard kill or `os.Exit`
cannot run shutdown defers, so stop the profiling node normally.

Taking a large snapshot duplicates retained messages and job-boundary slices
and is intended for infrequent checkpoints or shutdown export. Once a ring
entry is evicted, a late outcome/ready update is ignored rather than modifying
the newer occupant. No bounded trace can reconstruct recurrence state beyond
its retained prefix.

The trace contains complete signed transaction messages. Although normal
ledger traffic is public, treat it as measurement data, copy it only to the
intended analysis host, and remove it after the study.

## Offline cache and SIMD policy replay

`cmd/sigverifytrace` strictly parses schema v3 and replays bounded cache
policies without contacting a node:

```text
go run ./cmd/sigverifytrace \
  -input /var/tmp/mithril-sigverify-simulation.jsonl \
  -key-entries 4096 \
  -key-bytes 10485760 \
  -table-bytes 2560 \
  -admit-after 8 \
  -duplicate-entries 65536 \
  -simd 4,8
```

When `-output` is used, the analyzer creates a new mode-0600 evidence file. It
refuses to overwrite an existing file and refuses any path that resolves to the
input trace, including a hard link, so a policy run cannot destroy the source
measurement. The report records the input SHA-256 and every cache/SIMD policy
parameter so results remain bound to one exact trace and reproducible policy.

`table-bytes` is an explicit policy input. For example, a compact per-key
radix-32 four-coordinate table is a different cache object from an x8 SoA
batch table, so the analyzer does not guess one from an arithmetic backend.
It reports lookups, hits, misses, estimated miss preparations, valid-miss
completions, retained-build attempts, admissions, evictions, rejected
admissions, and current/peak table bytes.

Key lookup occurs at the attempt's begin event. Only a valid miss completion
earns admission credit; completion events are merged with begin events using
their exact event sequence, so concurrent out-of-order completion is not
silently reordered. An overlapping miss cannot double-admit a table that a
different completion has already installed.

The exact-duplicate LRU answers the recurrence-frequency question and inserts
an exact tuple at attempt begin, matching telemetry's observation semantics.
It does not claim that a hit on an earlier still-in-flight attempt can reuse a
cryptographic result. Likewise, “estimated miss preparations” is conservative:
schema v3 does not identify the precise precheck stage at which an invalid
signature failed.

Every analysis starts with cold policy state at the first retained record. A
`truncated_prefix` result means the bounded export omitted earlier attempts,
so its initial hit rate is cold-start biased. Choose telemetry capacity large
enough for the intended recurrence window and compare multiple capacities and
admission thresholds.

Exact x4/x8 reconstruction is available only for
`scheduling_simulation`. Passive traces explicitly return
`unavailable_for_passive_trace`, even when queue depth happens to be large.
Simulation groups never cross dispatch boundaries. A lane with no retained
verification is reported as `missing`, not invalid: scalar verification may
have stopped after an earlier invalid signature, or the independently bounded
verification ring may have evicted it. Group reports include tails,
valid/invalid/unknown masks, key/duplicate-hit masks, job boundaries, distinct
public-key counts, and repeated-same-key groups.

## Cost checks

Every disabled recording API loads the observer pointer, returns, and allocates
nothing. A full verifier may contain more than one hook (for example, a worker
mode check and a transaction observation check), so it is inaccurate to call
the entire disabled verifier path “one atomic load.” The focused allocation and
throughput checks are:

```text
go test -run '^$' -bench '^BenchmarkVerifyTxSigTelemetry$' -benchmem ./pkg/tpu
go test -run '^$' -bench 'BenchmarkRecord(Verification|SignatureDispatch)$' -benchmem ./pkg/sigverifytelemetry
```

`Enable` starts a fresh passive snapshot. Tests and tools may call
`EnableSchedulingSimulation` for the explicit simulation mode. Prometheus
counters and histograms remain monotonic for the process lifetime, as expected;
the capacity gauge returns to zero when collection is disabled.

## Frozen-baseline disabled-path A/B

The focused benchmarks above compare enabled and disabled modes inside the
current executable. The release A/B additionally compares the current source
tree with exact commit `9f43c94203a5f705c8bff09033fa5995448132cc`, before
the telemetry hooks existed:

```text
./scripts/sigverify-unaffected-gate.sh \
  2 \
  ../mithril-sigverify-unaffected-run-001
```

The result directory must be fresh and outside the Mithril worktree. The driver
runs only on Linux x86-64 and requires the Ryzen 7 PRO 8700GE release CPU. It
builds the current and baseline benchmark binaries once, pins both to the same
core with `GOMAXPROCS=1`, forcibly removes all sigverify-telemetry environment
variables before either process starts, and alternates current/baseline order
for ten three-second samples.

`scripts/sigverifyunaffectedbench` is an external test package that imports only
public APIs present in both revisions. The exact same harness is copied into a
`git archive` of the baseline commit. Its deterministic valid legacy
transaction and wire encoding exercise:

- `pkg/tpu.VerifyTxSig` and `pkg/tpu.VerifyPacket`;
- `pkg/tpu/sigverify.VerifyTransaction` and `VerifyPacket`;
- `pkg/txverify.VerifyTransaction`.

Every expected row must appear exactly ten times. A row passes only when its
current median is no more than 1% slower than the baseline and neither its
maximum `B/op` nor maximum `allocs/op` increases. The result bundle contains
the raw samples, alternating run order, binary hashes/build information,
baseline archive hash, current dirty-tree status and diff hashes, and a manifest
of every tracked and untracked nonignored source file. Start/end manifests must
match, and a successful fresh bundle is marked complete and protected by
`SHA256SUMS`.

This is intentionally not called a replay wall-time comparison. Replay's worker
loop and transaction snapshot verifier do not expose an equivalent public API
at the frozen baseline, and a synthetic replacement would understate queueing,
worker ownership, and block-level effects. The ordinary replay regression and
the final 5% sigverify-heavy replay gain therefore remain end-to-end gates after
the reviewed Narya backend is integrated.

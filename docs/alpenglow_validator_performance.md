# Alpenglow validator performance and voting recovery

This change consolidates streaming signature verification, leader block packing,
certificate compatibility, voting persistence and checkpoint latency work on
`alpenglow-dev` at `33dde4050d9250557583395810799aaac2f54017`.
The aim is to spend less of each slot preparing completed blocks or waiting to
enqueue a vote, while keeping the signing and durable-state boundaries explicit.

## Review order

The commits preserve the implementation sequence and can be reviewed by subsystem:

1. **Streaming verification:** update Narya; verify completed entry components
   during shred arrival; dispatch small ready batches immediately and bundle
   larger requests; compare cached component bytes directly against shreds;
   reuse authenticated FEC-root inputs and avoid repeated ordering work.
   [Design and configuration](transaction_sigverify_streaming.md).
2. **Leader packing:** decode and statically prepare owned transactions before
   leadership, revalidate bank-dependent inputs, reduce scratch allocation,
   compute signature roots without unused proof nodes, and improve scheduler
   ordering. Remove consumed, evicted and expired entries from both indexed
   priority heaps so retained references stay bounded by queue capacity. Expose
   queue capacity and completion-reserve settings while keeping
   their defaults. Correct slot-duration-dependent resource budgets.
   [Design and synthetic block tests](leader_block_packing.md).
3. **Observer and certificate compatibility:** enable VM pooling
   defaults and give retained vote-state deques their own backing storage, reduce observer statistics overhead, and use the Votor-compatible
   certificate layout accepted by Agave/Firedancer. The regression fixture is
   an X.509 certificate, with no private signing key.
4. **Voting persistence and recovery:** optional durable signing reservations,
   operator `--wait-to-vote-slot`, one ordered background history writer, clean
   shutdown sealing, and conservative recovery after a crash. Default history
   persistence remains synchronous. [Storage contract](reserved-vote-history.md).
5. **Vote admission and progress:** retain valid live voting opportunities within
   the existing pool/history window, order durable pruning behind replay events,
   and update the trusted monotonic replay watermark without taking the
   certificate-verification mutex. Verified finality still gates crash recovery.
6. **Status checkpoint capture:** select and pin the exact immutable transaction
   status lineage on replay; serialize it on the existing fold worker. Sidecar
   installation, checkpoint format, manifest selection and commit ordering are
   preserved. Capture/encoding failures cannot publish a new durable checkpoint.

This PR does not absorb FEC acceleration PR #259, genesis bootstrap PR #276,
or the skipped-parent replay fix in PR #278. The small DATA_COMPLETE boundary
correction shared with #259 is included because the streaming implementation
must identify actual serialized-component boundaries. Tests exercise the public
generator without depending on accelerated FEC APIs. The earlier standalone
streaming and leader branches are represented here rather than opened as
duplicate PRs.

## Performance evidence

These measurements have different scopes and must not be combined into a
whole-validator speedup multiplier.

| Measurement | Reference | Result |
| --- | --- | --- |
| Build a bank of 48,622 single-signature, 198-byte transactions from wire bytes | Current base `33dde405` | 209.64 → 150.30 ms |
| Same bank from decoded transactions | Current base `33dde405` | 189.73 → 132.40 ms |
| Capture a synthetic 300-root / 1.5M-key status checkpoint on replay | Original synchronous snapshot algorithm, unchanged in current base | 202.44–207.35 ms → 5.92–6.06 µs; roughly 201 ms encoding moves to the worker |
| 33,760 maximum-wire-size transactions arriving over 200 ms: final-shred-to-ready | Same extracted implementation, overlap disabled/enabled | 99.89 → 6.504 ms |
| Replay callback to broadcast enqueue, p95 | Sequential live samples before/after atomic watermark | 12.885 → 2.031 ms |

The bank comparison uses a serial caller on a Ryzen 7 9700X, Go 1.26.4,
GOMAXPROCS=8, with three alternating paired rounds. It includes admission,
execution, account publication, entry construction and signature-root hashing;
it excludes signature verification, real AccountsDB, network broadcast and
consensus. The statically prepared bank phase measured 101.92 ms, but its earlier
preparation work is excluded and is not eliminated from total CPU use.
[Bank benchmark methods and raw evidence](results/leader-block-packing/2026-09-13/README.md).

The checkpoint benchmark uses GOMAXPROCS=1 on the same Zen 5 model and about 30 MB
of encoded status data. It measures removal of the direct replay wait, not
elimination of encoding CPU or allocations. The original synchronous encoder
is unchanged; the additional old full-payload copy is not in the baseline.
The live deployment's first ten captures took 28.694–34.464 µs for 24.5–43.4 MB
snapshots, while worker encoding took 179–367 ms.
[Checkpoint and voting measurements](results/validator-performance/2026-09-14/README.md).

The streaming example uses exactly 1,232-byte valid transactions, two workers
and vector target eight. It includes decoding, assembly, identity checks,
signature verification and publication; it excludes transaction execution,
packet/shred authentication, loss recovery, disk and commit. Both sides use
the new batching/Narya/completion code, so it is an overlap comparison rather
than old `alpenglow-dev` versus this PR. Its catch-up case offers every shred
immediately and showed modest overhead from overlap in this short run.
[Standalone verification and allocation measurements](results/streaming-pr-review/2026-09-13/README.md).
Older component-by-component experiments retain their own intermediate baselines.

## Live scope and remaining work

The live testnet binary also contains #259's FEC/producer changes and retains a
two-FEC batching configuration. This independently based PR uses the base's
one-FEC configuration and includes the final pointer-view allocation improvement
from the standalone streaming branch. It is therefore not byte-identical to the
live binary. Vote-history, consensus admission/progress, checkpoint, leader
runtime and TLS implementation files match the preserved live source, apart
from explicitly identified FEC/shared-baseline code and configuration comments.

The first recorded post-checkpoint leader window contained 25,200 / 48,622 /
41,136 / 39,687 transactions. The offline fixture reaches 48,622 under the
tested 50M cost budget and rejects the next transaction; 50,000 is not achieved.
Changing live leader windows and shared-host load are not controlled benchmarks.
Later FAST scores improved, but some improvement preceded the checkpoint change
while load was paused, and a restart also reset transient queue state. No causal
FAST-score improvement is claimed for this PR.

Reserved voting remains experimental and opt-in. Its signed high-water bound,
history-version migration, clean marker consumption, directory locking, leader
gates and failure handling require consensus/storage review. Software crash
tests and a clean live restart do not qualify host power-loss behavior. A halted
cluster can leave conservative crash recovery waiting indefinitely. Preserve
the reservation and latest history across application or AccountsDB rollback.

The scheduler now removes entries from both heaps, including repeated cross-slot
rebuffering. Its existing scan/retry policy is preserved: newly arrived
higher-priority transactions remain eligible for normal selection. The queue
retention fix does not establish a measured improvement in block fullness or
FAST participation. Remaining FAST/Titan reward omissions are not assumed to
be solved. Server
loader scheduling, faucet automation, monitoring scripts and validator keys are
not part of the product changes in this PR.

## Validation

The combined source is tested separately from the live deployment. Exact commands,
results and the baseline BPF-loader failures are recorded in
[the validation report](results/validator-performance/2026-09-14/README.md).
Archived benchmark output is retained verbatim and marked as generated for
review; the Markdown explanations and executable test harnesses remain visible.

CI additionally runs complete race suites for Alpenglow, consensus, replay,
Turbine, signature verification, block production/scheduling and node startup.
These include signing-reservation recovery, history-writer ordering, checkpoint
capture, streaming cancellation and queue-retention regressions. The separately
documented base failures in the sealevel suite remain outside this selected CI
command. [Queue follow-up validation](results/queue-retention/2026-09-14/README.md).

## Review follow-up

Retained vote-state deques now own their backing storage before TowerSync returns
its scratch deque to the pool. Reset prefetch generations retain their capacity
until both queued work and verification readers retire. Local transport setup
precedes consumption of the clean voting marker. Starter configs emit the
current `[sigverify]` keys; the two-worker default and default-mode voting
changes are documented in the linked configuration and recovery guides.

The slot-time cost, account-data, shred and entry-byte table was checked against
[Agave v4.3.0-rc.1 slot_params.rs](https://github.com/anza-xyz/agave/blob/v4.3.0-rc.1/runtime/src/slot_params.rs),
including next-epoch activation, shortest-duration precedence and the 100/60
account/block cost multiplier. No table correction was needed.

Avoiding static preparation for duplicate or below-floor ingress transactions is
left for the separate block-production work; this follow-up changes no scheduler
admission policy. Existing raw benchmark evidence stays linked to this PR.

Local validation of this follow-up passed: full race suites for Turbine, node,
config generation, signature verification, consensus, Alpenglow, replay and cost
model; vote-program tests selected by `Test.*Vote`; vet for the changed Go
packages; and a complete validator build. Both ownership and reset-capacity
regressions fail against the prior PR implementation and pass with these fixes.
The reset regression also passed ten race runs. CI now includes config generation
and the targeted vote-deque regression. The previously documented unrelated
full-sealevel failures remain outside these passing targeted suites. This
follow-up has not been deployed or benchmarked on the live validator.

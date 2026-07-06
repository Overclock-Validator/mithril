# Alpenglow Branch Engine

This build targets Alpenglow clusters exclusively. It replaces the old
TowerBFT confirmed-block fork-choice heuristic (which buffered candidate
blocks and executed only after vote tallies confirmed a path) with an
**execute-on-receipt** engine where certificates gate *promotion to durable
state* rather than execution.

> The historical TowerBFT heuristic — vote parsing, confirmed-leaf resolution,
> buffered execution — lives on the `dev` branch. It has been removed here
> along with `pkg/forkchoice`.

## Model in one paragraph

Blocks execute the moment they are assembled. Alpenglow certificates never
hold up execution; they drive fork/skip decisions and gate how far durable
on-disk state is allowed to advance. Because certificates attest block *data*
(the `block_id` = slice merkle root), not execution *results*, a trailing
execution verifier is the only execution-correctness oracle at the tip — so
durable state is promoted only up to the minimum of certificate finality and
verified execution. State is one canonical timeline plus a short in-RAM mutable
suffix; a certificate that contradicts an already-executed slot triggers an
unwind and re-execution of the certified alternative.

## The pipeline

### 1. Execute-on-receipt

The replay loop (`pkg/replay/block.go`) runs each block as it is assembled from
the block source. There is no confirmation gate before execution and no
buffered-path resolver. Emission still applies three cheap consensus behaviors
at the block source (`applyAlpenglowDecisionLocked` in
`pkg/blockstream/block_source.go`): mark certificate-skipped slots, discard a
not-yet-emitted candidate whose block id a decisive certificate contradicts,
and halt on an equivocation conflict.

### 2. Certificates as the decision oracle

`ChainTracker` (`pkg/alpenglow/chain.go`) ingests certificates (from block
footers and from the local certificate pool, `pkg/alpenglow/certpool.go`, which
assembles certificates early from raw Votor votes). It answers the questions the
engine needs:

- `CertifiedBlockAt(slot)` — the slot's decisively certified block (a
  unique-strength notarize / fast-finalize / genesis certificate, at most one
  per slot by protocol, or a block finalized directly or by ancestry). Fallback
  certificates are ambiguous and never decisive.
- `SkipCertifiedAt(slot)` — whether the slot is certified skipped.
- `WantedBlocks(afterSlot, max)` — certified-but-unobserved blocks, for repair.

A second decisive block in one slot, or a finalized block contradicted by a
skip, is Byzantine evidence: the tracker records a conflict and the node halts
(write-once, survives pruning).

### 3. State: canonical timeline + WorkingSet suffix

Durable AccountsDB holds one rooted timeline. Executed-but-not-yet-durable
slots live in an in-RAM `WorkingSet` (`pkg/accounts/working_set.go`): a flat map
for O(1) reads plus a per-slot undo journal. Siblings are never materialized as
state — they are parked as block bytes and re-served on demand. This bounds RAM
and makes the common (no-fork) path cheap while the rare fork case pays.

- `PromotePrefix(through)` folds the oldest suffix slots into durable state.
- `EvictFrom(slot)` unwinds a suffix by replaying its undo journal
  newest-layer-first, restoring the exact pre-suffix values.

### 4. Dual-watermark promotion (the safety keystone)

Because certificates do not attest execution, promotion to disk is clamped to
`min(certificate finality, trailing-verification watermark)`
(`pkg/replay/promotion.go`). The trailing verifier
(`pkg/replay/trailing_verifier.go`) re-derives a compact per-transaction digest
(`pkg/replay/txdigest.go` — fee, success/failure, balances; compute units
deliberately excluded) from finalized RPC block metadata some slots behind the
tip and compares it against what replay produced. A mismatch is an execution
divergence: record evidence and halt. With the verifier required and its RPC
feed cut, folds stall and the in-RAM tail eventually hits its cap and halts —
fail-closed by design.

### 5. Fork switch: sweep + unwind

Since a slot can execute before its certificate arrives, a later certificate can
contradict an executed slot (a sibling we lost the shred race on, or a skip over
a block we ran). The switch sweep (`pkg/replay/alpenglow_switch.go`, gated on new
certificate arrivals) walks the executed-but-unfolded window and reports the
first contradiction as a typed `CertifiedSwitch`. When the parent context is
retained and the span is safe (same epoch, not mid-rewards-distribution), the
engine unwinds in-RAM (`tryInLoopUnwind` → `WorkingSet.EvictFrom`) and
re-executes the certified alternative; otherwise it falls back to re-replay from
the durable rooted checkpoint. The block source's emission frontier is rewound
in lockstep (`RewindForAlpenglowSwitch`).

### 6. Cert-driven repair

`WantedBlocks` feeds a near-tip repair loop in the block source
(`alpenglowRepairLoop`): pin the turbine assembler to certified block ids, pull
repair for certified-but-unobserved slots, discard buffered candidates carrying
the wrong id, and cancel shred state for certificate-skipped slots. This also
keeps re-hinting the certified sibling after a switch until its data arrives.

## Durable storage (wear-first)

Rooted slots fold to disk in batches (`pkg/accountsdb/fold.go`,
`CommitBatch`): K slots union-deduped into one sequential segment file + one
manifest + one atomic index flip, instead of per-slot in-place writes. The
manifest is simultaneously the commit record, the index redo log (which allows
running the account index without a WAL), the undo-pointer log, and the carrier
of batch bankhashes plus the resume context at the batch boundary. Recovery
(`pkg/accountsdb/recovery.go`) reconstructs the durable frontier from the store
itself, so a hard `kill -9` no longer forces a re-bootstrap.

Two capabilities fall out of the undo-pointer log:

- **Rewind** (`pkg/accountsdb/rewind.go`, `RewindToBatchBoundary`): restore
  durable state to an earlier retained fold boundary by applying undo pointers
  in reverse. This is why durable state deliberately lags — a divergence whose
  root cause was already folded can be undone without a snapshot restart. Wired
  as `--rewind-to-slot` and as an automatic recovery arm.
- **Compaction** (`pkg/accountsdb/compact.go`, `CompactOnce`): reclaim dead
  bytes from out-of-horizon segments and bootstrap appendvecs, pinned so it
  never touches a file any in-horizon undo pointer still needs.

## Voting readiness (one fork choice, no modes)

The fork-choice layer is built so a voting engine can be added ON TOP without
changing it — the same single behavior serves observer and voting nodes, and a
mixed (heterogeneous-client) or Mithril-only cluster identically:

- **Certificate semantics are Agave-parity.** Thresholds, vote-to-cert unions,
  per-validator vote budgets, base/fallback bitmap disjointness, fallback-only
  assembly (base3 with an empty base group), and slow finalization were
  cross-checked against `anza-xyz/alpenglow` (votor) and SIMD-0326; wire
  encoding is validated by `agave-votor-messages` fixtures in
  `pkg/alpenglow/testdata/`.
- **Trigger freshness is guaranteed by the pool, always on.** Votor's fallback
  triggers (transcribed from Agave `votor/src/common.rs`):
  SafeToNotar(b) = `notar(b) >= 40%` OR `notar(b) >= 20% AND notar(b)+skip >= 60%`;
  SafeToSkip = `skip + (notarTotal - topNotar) >= 40%` — over plain
  notarize/skip votes only. The cert pool folds (batch-verifies) the involved
  tallies the moment any trigger predicate passes on candidate stake, so
  verified stake is never stale when a predicate could have crossed. A voting
  engine evaluates the predicates on `CertPool.VerifiedVotorStakes(slot)` and
  observes crossings at the same time an eager-verification client would.
  Sub-trigger tallies still cost zero verification.
- **The voting engine's plug-in points**: `VerifiedVotorStakes` (trigger
  stake), the cert emit callback (round-2 finalize votes react to notarization
  certs), and the ChainTracker decision queries (parent-ready sequencing).
  What voting mode adds lives entirely above this layer: the vote loop and
  timeouts, vote signing/transmission, durable vote-history persistence, and
  standstill participation.

## What this proves — and does not

The certificate layer proves which block *data* the cluster settled on. The
trailing verifier proves execution *correctness* against the cluster's
finalized results. Neither replaces Mithril's own transaction execution,
account loading, bankhash calculation, reward/rent handling, or bankhash
verification — those still run in full. The engine's job is to pick the right
block to execute, promote durable state only when it is safe, and halt (never
silently diverge) when it cannot.

## Relevant code

- `pkg/replay/block.go` — execute-on-receipt loop, promotion + switch wiring
- `pkg/replay/promotion.go` — dual-watermark fold gating, unrooted tail
- `pkg/replay/trailing_verifier.go`, `pkg/replay/txdigest.go` — execution oracle
- `pkg/replay/alpenglow_switch.go` — switch sweep + in-loop unwind
- `pkg/alpenglow/chain.go` — ChainTracker decisions / skips / wanted blocks
- `pkg/alpenglow/certpool.go` — early cert assembly from raw votes
- `pkg/accounts/working_set.go` — canonical-suffix state with undo journal
- `pkg/accountsdb/fold.go`, `recovery.go`, `rewind.go`, `compact.go` — durable store
- `pkg/blockstream/block_source.go` — emission, decision application, cert-driven repair

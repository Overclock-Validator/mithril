# Voting persistence and crash recovery

## Intended guarantee

A crash must not lead to conflicting externally published votes or reserved-mode
leader actions because the validator forgot its earlier local decisions. The design permits
loss of recent detailed history and sacrifices voting availability when its
completeness is uncertain. It does not promise immediate restart voting, that
every signed vote reaches disk, or recovery from rolled-back safety files.
Normal anti-equivocation, execution, parent and validator-binding checks remain
necessary; this is a persistence contract, not a proof of the entire protocol.

The failure model includes process termination and host/power failure, provided
successful file and directory syncs survive, the current safety files are
preserved, and only one fenced owner uses the signing identity. Software tests
exercise the recovery decisions; they do not qualify actual storage against
power loss. Valid signatures on saved files establish integrity, not freshness.

## Two different publication guarantees

| Mode | Required before a vote can escape to the pool/network | What a restart may trust |
| --- | --- | --- |
| Default synchronous history | Exact validated history is written, file-synced, renamed and directory-synced before local pool admission or network enqueue. | Retained exact decisions and their rooted boundary, subject to normal restoration checks. |
| Opt-in reserved history | The vote's slot is covered by an acknowledged durable reservation before signing. Its exact history snapshot is prepared and queued before publication, without waiting for per-vote I/O. | The startup reservation bound, unless a separately validated clean-history seal proves exact retained history. |

In synchronous mode, BLS bytes may be computed privately in RAM before history
is synced. The guarantee is **persist before publication**, including local
pool admission because it can publish a certificate. In reserved mode, a
successful queue submission, background rename, or `written` counter is **not**
a durable acknowledgement of that vote. Replacing a whole file atomically is
not the same as making its bytes/directory entry survive power loss.

Both modes retain complete decisions in memory while running and prune them
only through the normal verified-root rules. The synchronous history guarantee
covers votes; it is not a complete journal of produced leader blocks. The
additional leader reservation barrier applies only in reserved mode.

## Reserved-mode restart rule

Let **H** be the reservation's `Through` value loaded at startup, **F** the
verified finality/checkpoint floor, and **S** a proposed signing slot. H bounds
what the previous process *might* have signed; it is not its last actual vote.

Without a validated clean seal, every vote type and historical local-vote
restoration must obey all of these conditions:

- **S > H**: never re-sign in the uncertain range during this run, even when
  an older history file contains that particular vote.
- **F >= H**: do not sign above the range until verified finality/checkpoint
  state has reached its end. F equal to H is sufficient; S equal to H is not.
- **S <= the current acknowledged reservation**, plus all ordinary protocol,
  live-joining and configured minimum-slot checks.

The startup recovery bound stays fixed during the run. Background renewal may
raise the current signing allowance; it does not move the recovery target.
Newly received blocks, elapsed wall time, an RPC tip, replay progress alone, and
`--wait-to-vote-slot` cannot substitute for verified finality/checkpoint state.
The recovery wait can be indefinite if the cluster halts below H.

For example, suppose votes through 1,015 escaped, detailed history survives only
through 1,012, and the durable reservation is 1,032. After an unclean restart,
slots <= 1,032 remain forbidden. Slot 1,033 is also forbidden while F < 1,032.
Once F >= 1,032, it can pass the recovery gate only after an acknowledged grant
covers 1,033 and all normal voting checks pass. Seeing a new block after restart
does not by itself meet these conditions.

## Clean shutdown is a durable protocol

1. Stop/join the voter and leader producer. Halt/join the reservation worker;
   drain/join the history writer so an older rename cannot overwrite the seal.
2. Require no latched safety fault or history-write failure, no unresolved
   reservation-write uncertainty, and verified recovery through the startup H.
3. Sync exact retained history, including the verified rooted boundary.
4. Sync a reservation record containing the digest of that exact history.

A successful process exit, a signal handler running, or an attempted final save
is not sufficient. Startup must load and validate the history and reservation,
match the clean digest, and **durably consume the clean marker with a dirty
successor before new vote/leader signing or new history decisions**. A later crash uses
the reservation again, even if the old history still looks valid.

| Restart state | Vote recovery | Leader recovery |
| --- | --- | --- |
| Valid history and reservation, no matching clean seal | Enforce S > H and F >= H, including restoration. | Enforce S > H and F >= H. |
| Valid matching clean seal, successfully consumed | Resume using exact retained voting decisions and ordinary checks; no additional vote quarantine through H. | Still enforce S > H and F >= H: vote history does not enumerate every block that may have been signed. |
| Missing, corrupt, unreadable or incompatible enrolled state | Refuse automatic reset/startup; do not infer safety from an RPC tip. | Same refusal. |

Errors during sealing do not authorize treating the session as clean; startup
must validate whichever durable record survived. A failed reservation write
may have reached storage despite its error. Running signers retain only the
previous acknowledged allowance; a later monotonic successful write can resolve
that uncertainty. An unresolved error prevents deliberately sealing clean.

## Lifetime, storage and enrollment

The reservation grants up to 32 slots beyond the requested slot and renews when
16 or fewer remain. Only successful file-and-directory sync acknowledgement
publishes new permission. Exhaustion pauses signing while replay/verification
continue. Repeated restarts without new grants do not advance the bound.

Detailed history uses one ordered writer with one in-flight and at most one
newer pending complete snapshot. New complete snapshots may supersede unwritten
ones. Validation/encoding/signing of the snapshot remain on the voter goroutine;
only file I/O is asynchronous. Writer errors latch a safety fault and stop
voting. An already in-flight vote is still covered by its durable reservation.

Preserve both `vote_history-<identity>.mithril.json` and
`vote_reservation-<identity>.mithril.json` independently of AccountsDB snapshots.
The checkpoint encoding cache is only an encoding optimization; it is not the
vote journal or a replacement for the reservation. Rolling back AccountsDB or
an application binary must not roll back either signing-safety file.

The directory lock only excludes concurrent owners of the same history path.
It cannot fence copies of the identity on other hosts/paths. Signed records and
generation numbers cannot detect an operator restoring an old valid pair of
safety files. Media loss, stale safety-file restoration, dishonest sync behavior
and compromised/copied signing keys are outside this automatic recovery contract.

Use `--reserved-vote-history` to opt in; default persistence remains synchronous.
First enrollment additionally requires `--initialize-vote-reservation`, exclusive
identity ownership and complete synchronous history from the stopped previous
writer. An empty directory is appropriate only for a previously unused identity.
The software cannot distinguish that case from deleting both files for an old
identity; initialization is not a safe disaster-recovery reset. Remove the
initialization flag afterward. These are CLI flags, not TOML settings.

Enrolled history uses version 2, rejected by older binaries that do not enforce
the reservation. Missing/corrupt enrolled state or a changed identity, vote
account, authorized voter, genesis or shred version must not be automatically
re-enrolled. Disabling the flag or deleting files is not a supported downgrade.
Transport binding/validation runs before the clean marker is consumed.

## Live admission is separate from restart recovery

The live vote admission floor uses retained consensus-pool state and the history
root, not the newest finalization certificate. Replay can still contribute a
notarization within the bounded 16-slot retained tail before later certificates
are collected; finalized retained slots can also receive skips. Durable-root
pruning is ordered behind completed replay events on the voter. These admission
changes also apply to synchronous mode. They do not weaken reserved-mode F >= H.
`--wait-to-vote-slot N` adds an inclusive minimum; it cannot bypass recovery,
execution, retained-root or parent checks.

## Evidence and limits

Existing tests map the recovery contract to these cases:

| Contract | Tests |
| --- | --- |
| Lost valid history suffix; fixed bound across repeated crashes | `TestReservedVotingLostHistorySuffixAndRepeatedCrash` |
| All five vote types, restoration and leader gates at H/H+1 | `TestReservedVotingEverySignatureTypeAtBound` |
| Clean-marker consumption and stricter leader restart | `TestReservedVotingCleanMarkerConsumedBeforeSigning` |
| Digest mismatch, missing/corrupt/domain-mismatched state | `TestReservedCleanDigestMismatchUsesCrashRecovery`, `TestReservedVotingRejectsMissingCorruptOrWrongDomain` |
| Pending/uncertain sync cannot authorize signing | `TestSigningReservationUnacknowledgedSyncCannotAuthorize`, `TestSigningReservationUncertainWriteSurvivesRestart` |
| Writer drain, failure and lost pending snapshots | `TestAsyncHistoryBlockedWriteDoesNotDelayVotesAndCleanCloseDrains`, `TestAsyncHistoryFailureStopsVoterAndPreventsCleanMarker`, `TestAsyncHistoryProcessCrashLosesPendingSnapshots` |

The subprocess-kill test exercises lost in-flight/pending application snapshots;
it does not power-cycle a host or prove filesystem durability. Fault-injection
and older-history fixtures check the algorithm's decisions under the stated
storage contract. This is not a formal consensus proof or mainnet storage
qualification. Persistence benchmarks likewise do not establish replay or FAST
performance.

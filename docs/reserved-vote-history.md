# Reserved vote history (experimental, opt-in)

Use `--reserved-vote-history` to keep detailed history with atomic
write-and-rename on an ordered background writer, without explicitly syncing each vote. An independent signed
reservation is synced before its slot range can be used. The default remains
synchronous history. `--wait-to-vote-slot N` is an additional inclusive minimum;
it cannot override recovery, retained-root, execution or ParentReady checks.

Live vote admission uses the retained consensus-pool root and the persisted
vote-history root. Receiving a finalization certificate does not itself retire
that slot for voting: successful replay may still supply a valid notarization
before a later fast or slot+8 reward certificate is collected. The pool keeps
its existing bounded 16-slot finality tail; expired catch-up work is still
discarded. Exact execution, ParentReady, parent-vote and anti-equivocation rules
are unchanged. Durable checkpoint pruning is queued behind completed replay
events on the voter, so it cannot remove a block's voting state before its
earlier replay event is processed. Observer-only pruning remains synchronous.
Crash recovery uses a separate verified-finality/checkpoint floor, not this
live admission window.

For the first enrollment, stop the previous validator normally and start with
`--reserved-vote-history --initialize-vote-reservation`. Enrollment requires the
complete history from the synchronous writer and exclusive ownership of the
identity. Remove the initialization flag on subsequent starts. It cannot reset
missing reservations for history already marked as enrolled. Initialization on
an empty directory is only appropriate for a previously unused identity.

The first durable baseline is established before any unsynchronized writes.
History format version 2 deliberately prevents older binaries from opening it.
Disabling the flag, deleting safety files, or rolling back to a binary that does
not understand reservations is not a supported downgrade procedure. Preserve
both `vote_history-<identity>.mithril.json` and
`vote_reservation-<identity>.mithril.json` in the ledger directory, independently
of AccountsDB snapshot restoration. Do not copy older safety files back during
an application rollback. Keys, vote account, genesis and shred-version changes
fail closed and require an explicit domain migration, not automatic re-enrollment.

Each history update validates and signs an immutable, complete snapshot on the
voter goroutine, then submits it without waiting for filesystem I/O. One worker
per voter owns all history replacements; it retains at most one in-flight and
one newer pending snapshot. While a write is blocked, the newest complete
snapshot supersedes an older pending snapshot. There is no intentional batching
delay. Complete in-memory voting decisions remain authoritative while running,
and the normal verified-root pruning rules still apply. Snapshot preparation
keeps serialization/signing CPU work on the voter but performs no filesystem
operations. Worker errors latch the engine safety fault and stop voting; an
already in-flight vote remains protected by the durable reservation.

The reservation worker grants up to 32 slots beyond the requested slot and
renews when 16 or fewer remain. At the nominal 200 ms cadence those correspond
to about 6.4 seconds of reserve and 3.2 seconds of renewal slack. They are slot
counts, not time guarantees. Sync runs on the worker; only its successful
acknowledgement publishes new permission. Exhaustion pauses signing while
replay/verification continue. Failed writes retain the old permission and retry;
uncertain writes prevent a clean shutdown marker until a later successful
monotonic write resolves the uncertainty. Vote events waiting on renewal are
retained in a bounded queue and re-evaluated against current consensus state.

On an unclean restart, the bound loaded at startup is fixed: all vote types and
historical local-vote restoration are disabled in that uncertain range. Voting
above it requires verified finality/checkpoint state to have reached it, plus
normal live joining and protocol checks. RPC wall-clock estimates cannot release
this recovery gate. Repeated crashes without new permission do not advance it.
The conservative finality wait can be indefinite on a halted cluster.

Clean shutdown first stops the voter, joins the reservation worker, drains and
joins the history writer, syncs exact history (including the verified finality
floor), then syncs a marker containing
its digest. Startup checks the digest and durably consumes the marker before
any new signature or history mutation. A crash in that interval therefore falls
back to the reservation. A shutdown before uncertain recovery completes cannot
create a clean marker. Clean vote-history recovery permits normal new voting,
but leader production always skips the previous reservation because vote
history is not a complete record of blocks the leader may have signed.
Every leader slot is checked before building, including trailing window slots.

The directory lock prevents two instances of this implementation owning one
history directory. It does not fence copies on other machines/directories.
The storage contract assumes successful file and directory sync are honored;
media loss, intentional rollback or compromised signing keys require operator
recovery. Format signatures establish integrity, not freshness against rollback.

Validation includes race tests for incomplete history, repeated crashes, clean
marker consumption/mismatch, missing/corrupt records, incompatible domains,
unacknowledged and uncertain writes, H/H+1 for all five vote types and restoration,
renewal retry with finality advancement, window-spanning skips, and leader gates.
Writer tests additionally cover snapshot isolation from mutable state, continued
voting while disk I/O is blocked, coalescing with complete retained decisions,
shutdown ordering, terminal write failures and killing a subprocess with both
an in-flight and pending unwritten snapshot. The reservation refuses voting in
the uncertain range after restart. The process-kill test does not simulate
power loss to the disk or a filesystem rollback.

These are software tests, not a host power-loss qualification or
formal proof of the full consensus protocol. `BenchmarkVoteHistoryPersistence`
compares the actual serialization/write paths with 32 recorded notarizations;
it does not measure block replay or full validator FAST participation.

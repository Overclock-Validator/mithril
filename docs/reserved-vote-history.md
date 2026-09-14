# Reserved vote history (experimental, opt-in)

Use `--reserved-vote-history` to keep detailed per-vote history with atomic
write-and-rename, without explicitly syncing each vote. An independent signed
reservation is synced before its slot range can be used. The default remains
synchronous history. `--wait-to-vote-slot N` is an additional inclusive minimum;
it cannot override recovery, finality, execution or ParentReady checks.

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

Clean shutdown first stops the voter, joins the reservation worker, syncs exact
history (including the verified finality floor), then syncs a marker containing
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
These are deterministic software tests, not a host power-loss qualification or
formal proof of the full consensus protocol. `BenchmarkVoteHistoryPersistence`
compares the actual serialization/write paths with 32 recorded notarizations;
it does not measure block replay or full validator FAST participation.

# Shred-spool completion publication

`MarkComplete` runs on the block-delivery path. Previously it wrote a 16-byte
`complete.idx` record while holding the spool mutex. This write uses no fsync,
but ordinary filesystem writes can still wait. Earlier live observations included
32–40 ms completion-to-delivery delays; one separate detailed trace localized a
37.8 ms pause to the journal write on an already-adopted own-leader block. That
own-leader example did not delay its vote. Do not equate all completion-to-delivery
stalls with journal I/O without the finer trace.

Completion publication now updates the in-memory map and makes a nonblocking
submission to one writer with a 256-record queue. The writer never takes the
spool mutex. A full queue drops only the persistent completion hint; in-memory
completeness remains available. This bounds pending memory and prevents storage
backpressure from directly reaching completion publication. No worker-count or
validator configuration changes are required.

## Recovery and ownership contract

The spool is a disposable verified-shred cache, not vote history, account state,
or a durable checkpoint. Completion hints can be lost on an unexpected stop or
queue overflow. Recovery then reassembles/re-repairs; a hint never replaces the
assembler's coverage and block validation. The packet checksums and journal/file
formats are unchanged. There is no new fsync or power-loss durability guarantee.

Deletion and corrupt-tail truncation are different: they must not race an older
queued completion. Their tombstone goes through the same ordered writer, and the
caller waits for it **before changing the slot file**. Thus an older queued hint
cannot be written after the tombstone and resurrect completeness for a replacement
partial file. These uncommon operations can still wait for storage while holding
the spool mutex; this change does not remove every source of spool contention.

After a short/error write, the worker stops appending hints and truncates the
journal to zero. This avoids appending behind a partial record and removes old
completion hints before an invalidation is acknowledged. If truncation also fails,
the invalidation fails and the slot-file mutation is refused; a later attempt can
retry. Losing all cached completion hints is an acceptable repair-cost fallback.

`Close` excludes further mutations, flushes packet buffers, retries current
completion hints if the queue overflowed, and drains/closes the journal worker
before returning. The next opener therefore preserves the existing clean-handoff
contract when storage succeeds. A stuck disk can still delay shutdown. The worker
must not outlive ownership of the spool directory. No voting-resume, signing-bound,
checkpoint-coverage or consensus-safety rule changes.

## Validation and measurements

Tests block the writer and overflow the queue while asserting that completions
remain available, then verify clean-close recovery. A replacement-file test keeps
the old completion write blocked and verifies that replacement cannot proceed
before its tombstone. Fault tests inject partial writes and failed truncation,
verify refusal to mutate the slot, then retry and check that no stale hint returns
on restart. Existing checksum/torn-tail, retention, handoff, receiver shutdown and
invalid-block tests remain covered.

Native full turbine/blockstream race suites, vet and the combined validator build
passed on the Ryzen 9700X (Zen 5), Go 1.26.4. The test process used GOMAXPROCS=2,
Nice=19 and a 200% CPU quota on the active validator host.

Run the identical `BenchmarkShredSpoolMarkComplete` file on both source revisions.
Each sample opens a fresh spool, appends a packet outside the timer, times one
completion, then closes/drains outside the timer. Thus no already-complete dedupe
or queue-overflow drop is measured. Three runs of 300 iterations:

| Component | Before | Candidate |
|---|---|---|
| Median run p50 | 2.805 µs | 0.170 µs |
| Median run p99 | 6.201 µs | 0.581 µs |

This measures ordinary storage, not injected tail latency, total CPU work, replay
or FAST inclusion. Disk work moves to the worker; it does not disappear. The
baseline is the exact previously deployed combined validator, SHA256
`a58366704680154628ff0a4c6b4027a3e5b79f1909eb39bffeec326c008f0b68`, not the full
branch versus alpenglow-dev. Exact source copies, native outputs, test-exclusion
windows and binary-verified traces remain at
`/srv/mithril-spool-async-20260915` on the validator host.

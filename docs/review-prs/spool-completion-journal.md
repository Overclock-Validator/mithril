# turbine: keep completion journal writes off block delivery

`MarkComplete` previously wrote a 16-byte spool-journal record while holding the spool mutex on the delivery path. Even without fsync, that write can stall. Update live completeness immediately and send the repair-cache hint to a single bounded writer without waiting for disk or queue space.

Only completion hints may be dropped on queue overflow. `Close` retries current hints after overflow and drains the worker before handing the directory to another opener. Deletion and corrupt-tail truncation retain an ordered, acknowledged tombstone before changing a slot file; pending older completions cannot resurrect a replaced file's status. Short/error writes disable further hints and clear the disposable journal. If clearing also fails, the file mutation is refused. These rare invalidation paths can still block on I/O.

The spool remains disposable and unsynced. Packet checksums and journal/file formats are unchanged. Missing hints after a crash require reassembly/repair; they do not authorize a block, vote, or checkpoint. No signing-state or voting-recovery contract changes. Detailed ownership and recovery guarantees: `docs/shred-spool-completion.md`.

Full native turbine/blockstream race suites, vet and combined build passed. Tests cover a blocked writer, bounded-queue overflow, clean-close recovery, replacement ordering, partial writes, failed journal truncation and retry, alongside existing torn-tail, shutdown and invalid-block tests.

Identical native benchmark on the previous combined validator and candidate: Ryzen 9700X, Go 1.26.4, GOMAXPROCS=2, Nice=19, 200% CPU quota, three runs × 300 fresh-spool samples. Median run p50: **2.805 → 0.170 µs**; median run p99: **6.201 → 0.581 µs**. Packet setup and Close/drain are untimed, and no queue-overflow drop is measured. Disk work moves off publication rather than disappearing; this is neither whole-replay nor FAST evidence.

Earlier live completion-to-delivery outliers were 32–40 ms. A separate detailed 37.8 ms journal-write pause affected an already-adopted own block and did not delay its vote. The fresh ten-minute pre-deployment trace did not reproduce a long stall. Do not claim that every earlier outlier was journal I/O or that the candidate has proven a sustained FAST gain.

This branch is stacked on `7layer/review-streaming-preparation`; it is separate from the status-map experiment. Exact combined source, native outputs, probe verification and deployment metadata remain server-side at `/srv/mithril-spool-async-20260915`.

## Initial deployment measurement

Deployed September 15 at 18:06 UTC after a clean stop. Three advancing health
checks finished at one-slot vote lag before the existing loader and FAST monitor
resumed. The exact candidate SHA256 is
`d6966d72afa9cb7dbb6724d16e459402918975a17bdf0638136e1c68c339ba08`.
The status-map experiment and all other runtime settings remained unchanged, so
this trial changed only spool behavior.

Both bounded ten-minute spool traces exited successfully. The candidate recorded
2,522 completion calls: median 0.00945 ms, p99 0.02104 ms, maximum 0.14015 ms,
including probe overhead. Its background journal writer recorded three writes
above 1 ms, with a 39.334 ms maximum. No completion call exceeded 1 ms. Background
write records were not tagged as completion versus tombstone, so this does not
identify the particular slow record or prove a counterfactual per-block saving.
The blocked-writer tests establish isolation of completion publication. Other
spool I/O remains: one measured slot-file close took 29.824 ms.

The preceding trace had 2,524 completions and no long stalls either. Directly
comparing its 0.04212 ms p99 with the candidate's 0.02104 ms p99 is confounded by
unequal probe counts inside the measured function. Use the identical untraced
native benchmark for the ordinary-path component comparison.

Received-block comparison excludes startup, native-test intervals and incomplete
or nonmonotonic joins. Assembler completeness is reconstructed from completion
entry minus queue delay; the endpoint is local QUIC serialization, not receipt
by another validator. Locally adopted own blocks are excluded. Windows and sample
sizes differ and are not randomized A/B.

| Received blocks | Before n | Candidate n | Overall p99 before → candidate |
|---|---:|---:|---|
| Empty | 2,257 | 570 | 8.464 → 7.871 ms |
| At least 30,000 transactions | 546 | 209 | 123.954 → 148.555 ms |

No overall large-block tail improvement is established. Of the 209 large blocks,
204 had controls matched by leader, slot position, sender overlap and transaction
count/rounded CU within 10%, within one hour. Median per-candidate total difference
was +0.519 ms; controls can be reused. Two consecutive same-leader blocks around
80 seconds after restart spent 23.458 and 182.377 ms in remaining verification;
one had no early parsing or verification recorded. Their spool-to-delivery
intervals were at most 0.01765 ms, and no metadata-recovery warning appeared.
The deeper cause of this verification gap remains open; stage attribution alone
does not prove it is unrelated to all effects of the deployment.

Voting, continuous large-block production and the existing monitors remain active.

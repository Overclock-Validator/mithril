# Reclaiming obsolete small records

The 5 November 2026 mainnet stop at replay slot 465874941 (epoch 1078)
was the storage reserve working as configured. A fold through 465874940 needed
140748234 bytes with 68849156096 available and a 68719476736-byte reserve.
The last confirmed durable slot was 465874812.

The compactor punched pages inside each obsolete account's payload while
retaining every record header. Small obsolete accounts therefore stranded
physical pages indefinitely. A few undo references in almost every retired
fold prevented relocation, even when little live account data remained.

Sparse compaction now writes a `ManifestKindSparse` (3) marker listing only
the records still referenced by the current index or retained undo pointers.
It then punches complete pages between those records, including dead headers.
Retained records keep their exact file ID, offset, header and data. The
manifest's existing CRC protects the record list. Bank hashes, transaction
execution and the configured rewind horizon are unchanged.

The marker is written, fsynced, renamed and directory-synced before any hole is
punched. After an interrupted punch, the marker's retained offsets define the
remaining dead gaps, so retry does not read discarded headers. Older fold and
payload-only compact manifests still require contiguous record coverage before
conversion. Sparse manifests require ordered, nonoverlapping records, valid
record bounds and keys, and complete coverage of every undo reference.

Readers use the account index and rewind uses retained undo offsets. A sparse
record segment must never be scanned with the sequential appendvec parser.
Snapshot appendvecs and relocated compact files retain their old layouts.
Platforms without hole-punching support fail closed for this new manifest kind.
Use a binary with kind-3 support after conversion: previous binaries cannot
continue compaction of the converted store. Their compaction pin validation
rejects the new kind. Do not remove the markers or rebuild an index by scanning
these sparse files as ordinary appendvecs.

Validation includes a small-account regression that fails with the old
implementation, WAL-on/off recovery after the marker and after partial hole
punching, unchanged index locations, cold reads, rewind, reader exclusion,
inventory verification, and the full AccountsDB race suite. An incident-only
harness also tested independent copies of four production segments against the
original read-only index and 64-batch rewind pins; SHA-256 hashes of all retained
record bytes were identical before and after reclamation and retry.

Commands:

```
MITHRIL_TEST_INVENTORY_STRICT=1 go test -race ./pkg/accountsdb -count=1
go test -race ./cmd/mithril/node ./pkg/replay -run 'Compaction|Storage|Recovery|Recover|Rewind|Rooted|Durable|Shutdown' -count=1
```

Reference behavior: Agave's `accounts-db/src/accounts_db.rs:1846` removes
obsolete offsets when collecting records for shrink; Firedancer's
`src/flamenco/accdb/fd_accdb.c:1532` retires superseded account versions after
root advancement. Mithril additionally retains exact undo locations throughout
its rewind horizon. This change follows that same state-preservation rule with
an in-place reclamation mechanism; it does not change runtime execution.

- https://github.com/anza-xyz/agave/blob/master/accounts-db/src/accounts_db.rs#L1846
- https://github.com/firedancer-io/firedancer/blob/main/src/flamenco/accdb/fd_accdb.c#L1532

## Sparse page amplification and retry scheduling (9 November 2026)

The next stop occurred at replay slot 467164771 (epoch 1081), after a fold
through 467164770 needed 132394222 bytes with 68765868032 available and the
same 64 GiB reserve. The last confirmed durable slot was 467164642. The
reserve correctly stopped the write; it must not be reduced to hide growth.

The store had 514080 bootstrap files occupying 492.789 GiB, 5343 sparse
segments occupying 70.236 GiB, and 65 retained fold segments occupying
7.653 GiB. Read-only samples found many sparse segments retaining less than
1 MiB of account records but 5–15 MiB of physical pages. Each small live
record can retain a whole boundary page. Even a few undo references prevented
the old whole-file compactor from packing those live records together.

A second problem delayed further cleanup: sparse segments below ten percent
of their logical size joined the same slow queue as half a million bootstrap
files. Segments below eight MiB also lost their retry age. Kind-3 manifests
already contain only retained records, so their scan cost no longer follows
the original logical file size. They now stay in the age-ordered retry queue,
including small segments. Fresh segments still alternate with retries, and
every eighth cycle still gives bootstrap work priority.

When a kind-3 segment wastes enough physical space, compaction now packs its
current records into a normal compact output, while retaining the source for
any undo references. The savings calculation includes the source pages still
needed by undo, rather than assuming the whole source can be removed. It also
respects the remaining per-cycle move budget and checks the configured disk
reserve before allocating an output ID. A low-space result skips relocation
and continues with in-place reclamation.

Publication uses the existing relocation ordering:

1. Validate retained manifest offsets, headers, keys, record bounds, index
   locations, and every undo reference. Never sequentially scan sparse gaps.
2. Copy complete current records, including headers and alignment padding,
   into the packed output. Fsync its data and directory, then durably publish
   its compact manifest.
3. Drain old source readers, publish the index changes, and make them durable
   with WAL sync or an explicit index flush when the WAL is disabled.
4. Keep the source, discard cached index lookups, and run sparse reclamation.
   Its manifest is published before holes are punched. Every undo record
   remains at its original file ID and offset, even if it was also copied.

An interruption before the index move leaves the source authoritative. An
interruption after the move leaves harmless duplicate source bytes until the
next sparse pass. Recovery does not replay compact or sparse manifests as
folds. The change preserves account bytes, bank hashes, the durable frontier,
and the configured rewind horizon. It introduces no new storage format beyond
the kind-3 sparse markers described above; a binary lacking kind-3 support is
still unsuitable for this store. Reverting only the dense relocation and queue
changes is format-compatible, but restores their space-growth problems.

Validation covers ten WAL-on/off interruption and recovery cases (after output
manifest, after durable index publication, after source manifest, during hole
punching, and uninterrupted), cold reads of complete account structs, unchanged
bank hashes, rewind, a current record also pinned by undo, reader exclusion,
move/disk budget rejection before file-ID allocation, a 520000-file bootstrap
backlog, retry fairness across advancing folds, and strict inventory checking.
Existing in-place-only tests use a one-byte relocation budget so their exact
index-location assertions continue to test that operation independently.

Independent copies of four production segments also passed SHA-256 comparisons
of complete current and undo record bytes before and after relocation, reopening,
and retry. Each released approximately 10–13 MiB while copying 0.4–0.9 MiB.
The original production index was opened read-only for this check.

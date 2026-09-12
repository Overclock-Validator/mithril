# Native genesis validation

Validated on 2026-09-11, macOS arm64, Go 1.26.4, against `alpenglow-dev`
at `ea579cb4` and the pinned Agave bank helper described below.

## Persistence and recovery

Genesis initialization uses the existing AccountsDB account index, appendvecs,
stake-index records and fold manifests. `AccountsDbStoreGuard` provides exclusive
ownership shared by initialization, database opens and snapshot cleanup/build.
A successful open transfers ownership to the database; shutdown reports close
errors and releases the guard after both databases close. Initialization publishes
its ready marker only after durable accounts, index entries, bank hashes and
complete bootstrap metadata have been verified.

Genesis origin uses schema 6 with an explicit storage-format tag; child replay
uses schema 7. Unsupported database formats and schemas are rejected before
initialization or recovery can overwrite their artifacts. Existing snapshot-origin
schema 3 remains supported.

Genesis recovery checks the complete manifest sequence, filenames, segment CRCs,
allocation bounds, bank-hash coverage and selected index watermark before orphan
cleanup. Bank hashes are reconstructed from all validated manifests, including
already-applied folds: their separate WAL can be lost even when the account-index
watermark survives a process crash. Ordinary snapshot recovery retains its policy.
The existing manifest encoding is unchanged; publication refuses to replace an
already-decided manifest.

Offline epoch execution invalidates the legacy process-wide stake-index cache
before a boundary scan. The short-epoch test deliberately seeds that cache from a
different database and still matches the oracle. A stake-index append error
fences the session so it cannot retry with consumed pending entries.

## Passing checks

- Deterministic equivalent/reordered configurations, raw/archive round trips,
  encoding failures, invalid keys/BLS points, duplicate/reserved accounts,
  invalid allocations, overflow, profile tampering and archive validation.
- Live pinned Agave bank helper at `7e51da963aee49622a395f562386a6bd8ba0e717`:
  genesis decode/reserialization; complete initialized and frozen slot-0 accounts;
  capitalization, account-data length, AccountsLtHash, bank hash, recent
  blockhashes, epoch stakes and runtime parameters.
- Four-slot first replay, signed-shred UDP ingress, native production, and
  independent non-voting replay. Empty blocks and signed transfers match Agave;
  checkpoint at 2, reopen at 3, continued replay and checkpoint at 4 match.
  Cached independent Agave shred-codec results remain bound to the exact wire
  request. No live Agave validator cluster is involved.
- Sixty-six slots with `test_slots_per_epoch = 32`, compared with the live pinned
  bank helper. Checkpoints at 31/33/63/65/66 survive reopen; stopping during active
  reward distribution at 32/64 replays from the pre-boundary checkpoint. The final
  checkpoint resumes at 67. Current/future stakes and historical vote states are
  restored, including the empty future leader set after admission funding runs out.
- Cancellation, invalid metadata/sidecars, account corruption outside LtHash,
  actual subprocess exits around initialization/checkpoint publication, and the
  existing AccountsDB fold crash matrix.
- Concurrent ownership; transferred/closed/wrong-root guards; snapshot cleanup
  and build refusal against an owned store or a genesis-origin directory;
  incompatible schemas/backends; pinned key scans with ordering, bounds,
  concurrent insertion and cancellation; damaged/gapped manifests, missing
  selected manifests, mismatched watermarks, corrupt segments and allocation
  markers all fail without deleting the orphan sentinel.
- Deterministic recovery of missing advisory bank hashes for already-applied folds.
- CLI build, `genesis create`, both raw and archived `genesis init`, independent
  reopen, matching metadata, persisted account-index files, and schema 6.
- Race detector on genesis initialization, UDP replay, checkpoint/restart,
  epoch boundaries, store ownership and snapshot protection: all passed.
- Full affected packages passed, including `accountsdb`, `genesis`, `genesisinit`,
  `global`, `replay`, `snapshot`, `state`, `blockprod`, `turbine`, `runtime`,
  `rewards`, `util`, and the node commands. `git diff --check` passed.

The example still produces genesis hash
`78Gd7KGw6X5pcy397thaQsQHg7LNrW6xKJ3tY21fRU75` and completed slot-0 bank hash
`8XUvkNJB8dr1D4aWjeJtRXi1iQZQ7rGuvh5Ch1ZiFUQk`.

## Full suite and baseline failures

`go test ./... -count=1 -timeout=15m` completed. The final run passed `pkg/replay`
and retained only these failures, each reproduced on an untouched source archive
of `ea579cb4`:

- `conformance`: the address-lookup-table fixture directory is absent.
- `pkg/sealevel`: BPF-loader expectation failures followed by the existing
  `TestExecute_Tx_BpfLoader_Close_ProgramData_Success` nil-pointer panic.
- `pkg/lightbringer`: `TestManager_StartStop` expects a missing startup log line.
  Lightbringer code was left unchanged as requested.

The first full run also hit an existing replay timing assertion requiring a
single cached lookup to take a nonzero number of nanoseconds. One hundred runs
reproduced it on untouched `alpenglow-dev`. That assertion was removed; the test
retains its deterministic requested/working-set/durable-key and batch-call checks.
One hundred subsequent runs and the final full replay suite passed.

## Reproduction and limits

```sh
go test ./pkg/genesis ./pkg/genesisinit ./pkg/accountsdb ./pkg/state ./pkg/snapshot
go test ./pkg/replay -run 'Genesis' -count=1 -timeout=10m
go test -race ./pkg/genesisinit ./pkg/replay ./pkg/accountsdb ./pkg/snapshot \
  -run 'Genesis|Initialize|Initialization|InterruptedInitialization|CancellationAndDestinationOwnership|AccountsDb|SnapshotCleanupAndBuild' \
  -count=1 -timeout=10m
go test ./... -count=1 -timeout=15m
```

For live bank comparisons, set `MITHRIL_AGAVE_ORACLE` to the helper produced by
`scripts/genesis-oracle/run.sh`. Ordinary runs use the checked-in pinned fixtures.
Production genesis commands run entirely in Mithril.

These checks establish functional parity and recovery, not a storage performance
comparison. The epoch fixture exercises empty reward partitions, not nonzero
validator payouts. Live startup, consensus-based root selection, voting and full/
incremental snapshot production remain future work. The receiver does not vote.
Complete bank metadata and the coherent account/checkpoint boundary remain
available to the future snapshot exporter.

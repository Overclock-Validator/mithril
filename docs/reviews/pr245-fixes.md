# PR #245 fixes and memory investigation

Follow-up to [the review](pr245-alpenglow.md), 2026-09-07.
The comparison checkpoint is `18631a3a` on `7layer/pr245-alpenglow-review`;
the upstream Alpenglow baseline is `7e4e8af1`. The PR remains rebased in the
isolated worktree. No upstream branch has been pushed or changed.

## Fixed behavior

- Reject duplicate, symlink-aliased, and overlapping accounts roots and data
  directories before cleanup, database opening, or truncating shard writers.
  Resolve nonexistent suffixes through existing parents. Invalid configurations
  now produce an error and leave existing data intact.
- Move the O_DIRECT flag into Linux-only code. Buffered snapshot code now
  cross-builds for Darwin/arm64. Other platforms reject explicit direct I/O
  before opening a shard writer.
- Include every shard's coalesced snapshot `accounts/data` in Alpenglow
  compaction. Find live accounts through the canonical index, retaining their
  original Slot and values, including unaligned packed appendvec starts,
  missing final padding and direct-I/O gaps.
- Release closed disk-writer buffers and drained worker-pool references before
  shard sorting. Make writer close idempotent and reject writes after close;
  deferred cleanup also closes writers on bootstrap errors. This drops the
  writer's references to 32 MiB per disk without forcing a production GC.

## Compaction design and limits

An incremental assessment pass sums live record bytes. Once the dead fraction
passes the existing threshold, another incremental pass streams live records
into durable replacement segments. The source remains until all index moves
are durable. Writes use a 64 KiB copy buffer; each output contains at most
8,192 records, bounding manifest and index-batch memory independently of the
size of the source file. Scan and move budgets stop between records; one
indivisible account can exceed an otherwise empty cycle's budget.

Outputs use manifest kind 3 and `0.<fileId>` physical names, with each account's
original index Slot preserved. Opening the DB rebuilds the file-ID-to-path map
from CRC-validated manifests. Later compaction scans only each output's recorded
key range. The initial snapshot file still requires a walk over the whole
account index, spread across cycles. This avoids an allocation proportional to
a snapshot file's size and avoids sequential parsing across packed boundaries.

Ordering is data sync → directory sync → atomic durable manifest → index commit
(WAL sync, or explicit index flush with WAL disabled). Source removal also
flushes the index, including after an earlier ambiguous commit error. Reader
pins protect the switch and unlink. Existing fold/rewind pins apply on every
cycle and invalidate saved scan progress. Restart or error repeats assessment
of the remaining references; recovery removes undecided outputs, and decided
but unreferenced outputs are themselves reclaimable.

Partial evacuation temporarily needs space for both source and copied live
records. Net bytes reclaimed can therefore be negative before final unlink.
An in-horizon undo pointer pins the entire source file and can defer reclamation.
No mainnet-scale compaction throughput or wear claim is made here.

Legacy databases remain readable. Once these new replacement segments have
been created, an older binary without manifest-kind-3 addressing cannot read
them; downgrade requires a compatible binary or rebuilding from a snapshot.
The snapshot coalesced format itself is unchanged.

## Validation

All compilation, tests, profiles and timings ran on `allnodes-ryzen9700x-fra`,
Go 1.26.4, `GOEXPERIMENT=arenas`. The Mac was used for source edits and analysis.
Tests used `TMPDIR=/var/tmp` so direct-I/O checks exercised the NVMe filesystem.

Passed:

- AccountsDB, snapshot, config, state and node package suites, race checks, vet,
  Linux node/probe builds, and Darwin/arm64 snapshot cross-build.
- Alias rejection without truncation; writer cleanup/readback and repeated close.
- Two-shard mixed-slot compaction, bounded progress across reopen, cold individual
  and batch reads, threshold handling, fully dead deletion, and re-compaction
  of replacement outputs.
- Invalid index locations and truncated source records fail without unlinking.
- A new undo pin invalidates partial work; rewind restores original values.
  Concurrent reads remain correct while sources are evacuated and removed.
- Error injection and abrupt subprocess exit after data sync, manifest sync,
  index commit, and immediately before unlink, with the actual index WAL both
  enabled and disabled. The subprocess tests exit without CloseDb, so shutdown
  cannot hide a missing durability step. Recovery and readback pass afterward.

The same pinned full snapshot at slot 952183 and incremental through 1021126
from the review were rebuilt and reopened in single-directory buffered,
two-directory buffered, and single-directory O_DIRECT modes. All three read
**5,593,721 accounts** through individual and batch APIs and matched the
baseline digest:
`82fecea1d07acf1cbdefe89f8ae4e222a7e416b0eb2bca24f7eb4289f8253f6a`.
The supplied manifest bank hash also matched; it was not independently recomputed.

The host exposes one NVMe, so two directories test the layout rather than
physical multi-drive scaling. The earlier bounded node replay attempt remains
limited by the shared client's lack of version-1 transaction support; these
fixes do not establish replay-throughput or consensus validation.

## Memory explanation

The new snapshot pipeline reduced cumulative allocation but kept more memory
live at once. Separate diagnostic builds sampled allocations at 64 KiB and
forced GC at the unpack/index boundary and after index construction. They were
not used as elapsed-time evidence.

In the reviewed PR diagnostic run, the pre-index live heap was 133.8 MiB versus
58.6 MiB for the baseline: roughly **43.3 MiB of retained tar buffers plus
32 MiB of writer staging buffers** accounted for the difference. After indexing,
the writer still retained its 32 MiB. Cumulative sampled allocation through
indexing fell from approximately 5,816 to 4,624 MiB (about 20%). Fewer allocated
bytes over time does not imply a smaller peak live heap.

The fix removes writer buffers from both phase-boundary profiles. After-index
retained heap was 56.9 MiB, versus 90.6 MiB in the reviewed PR run. However,
`sync.Pool` retention is GC-dependent: the fixed diagnostic run retained about
101.6 MiB of tar buffers before indexing. Releasing worker references does not
immediately empty the runtime's pool caches. The existing 45.8 MiB stake collector
is present in both versions. Overlapping read/sort/write work and GC headroom
also contribute to peak RSS; these profiles do not uniquely attribute every
byte of the process peak.

## Repeated measurements

Eight interleaved samples per version, reversing the order every pair, with
CPUs `2-7,10-15`, default Go parallelism, warm pinned archives and fresh output
DB rebuilds. The timer covers the production bootstrap API. Peak RSS covers
the probe process including close/reopen and cache initialization; full account
readback ran separately. No profiling, forced GC, build, race check, or test
node overlapped these samples. The pre-existing Lightbringer service remained
running, as in the original review.

| Metric (median) | Alpenglow baseline | Reviewed PR | With fixes |
| --- | ---: | ---: | ---: |
| Bootstrap elapsed | 1.1698 s | 1.0689 s | 1.0653 s |
| Process peak RSS | 248.5 MiB | 503.4 MiB | 469.0 MiB |

The fixed branch is **8.9% faster than the Alpenglow baseline**, winning all
eight paired samples (two-sided exact sign test p=0.0078125). Its elapsed time
is indistinguishable from the reviewed PR in this small experiment (four wins,
p=1). The observed RSS median is 6.8% lower than the reviewed PR, but RSS is noisy
(six of eight paired wins; sign-test p≈0.289), so this is not an established
process-peak reduction. The deterministic removal of writer references is
confirmed by tests and heap profiles. Peak RSS remains substantially higher
than baseline; the overall memory tradeoff has not been eliminated.

A useful next focused improvement is a byte-bounded tar buffer pool, evaluated
against throughput and peak memory across multiple corpus sizes. A phase-specific
peak metric would distinguish unpack pressure from index-pipeline pressure.
Neither broader optimization nor mainnet sizing is included in these fixes.

## Artifacts

Local task evidence:
`/Users/nikolashayes/Desktop/programming/Codex/review-results/pr245-fixes-zen5-2026-09-07`.
`measure.sh` contains the exact repeated-run command, `analyze.py` regenerates
`summary.json`, and `results/` plus `profiles/` retain raw logs and heap profiles.
The original diagnostic source generator is retained as `profile-original.py`;
`profile-fixed.py` applies identical checkpoints to the fixed source.

Remote task root:
`/var/tmp/mithril-pr245-fixes-20260907`.
The original reviewed sources/binaries and pinned archives remain in the prior
review root. The uninstrumented fixed binary and separate diagnostic binary are
retained. No node service, validator identity or stake configuration was changed.

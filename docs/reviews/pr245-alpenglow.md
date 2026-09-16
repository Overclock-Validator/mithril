# PR #245 review on Alpenglow

Reviewed 2026-09-07 on `allnodes-ryzen9700x-fra` (Ryzen 7 9700X).
Original PR: https://github.com/Overclock-Validator/mithril/pull/245
Original head: `46157e132edc37b580c279e75b7cbc42e9027548`.
Base fetched for this review: `origin/alpenglow-dev`, `7e4e8af1`.
Local review branch: `7layer/pr245-alpenglow-review`.

The PR was rebased in an isolated worktree. Its four commits are retained;
conflict resolution also ports the layout to Alpenglow's newer AccountsDB.
The existing checkouts and the upstream PR branch were not changed.

## Findings at the review checkpoint (`18631a3a`)

1. **P1 — Reject duplicate or aliased shard directories before opening writers.**
   `openShardBigFiles` opens an independent truncating writer for every supplied
   path. When two paths name the same directory, both writers begin at offset
   zero in the same `accounts/data` file. A deterministic reproduction wrote
   `first-account` to shard 0 and `other-account` to shard 1: both writes and
   close succeeded, but reading shard 0 returned `other-account`. Validate
   directory identity before cleanup/opening, including symlink aliases.
   This is in the original PR, whose writer implementation is unchanged by
   the rebase. Reproduction: `duplicate_shards_test.go` in the review artifacts.

2. **P2 — Teach Alpenglow compaction about coalesced snapshot storage.**
   The new `accounts/data` files are invisible to `CompactOnce`, which scans
   only primary-directory names matching `<slot>.<fileId>`. A completely dead
   snapshot file remained on disk and `FilesDeleted` was zero. This loses
   Alpenglow's existing bootstrap-data reclamation and leaves the original
   snapshot allocation permanently retained. A solution must preserve extent
   boundaries, account slots, rewind pins and crash safety; simply renaming
   `data` is insufficient because it contains appendvecs from different slots.
   This is an integration issue with `alpenglow-dev`, not a claim about the
   original `dev` branch. Reproduction: `compaction_shards_test.go`.

3. **P1 — Keep Linux-only O_DIRECT behind platform-specific code.**
   `pkg/snapshot/shard_writer.go:76` refers to `syscall.O_DIRECT` in an
   unconditional Go file, so even buffered mode cannot compile on macOS.
   The baseline snapshot package cross-build succeeds. Cross-building the PR
   snapshot package on Zen 5 with
   `GOOS=darwin GOARCH=arm64 CGO_ENABLED=0` fails with
   `undefined: syscall.O_DIRECT`. Put the flag/open implementation behind
   platform build constraints, with an explicit unsupported-direct-mode error
   on other systems while retaining buffered writes.

The reproductions intentionally fail at the review checkpoint and remain in
the original artifacts. The follow-up fixes are now on this branch; see
[PR #245 fixes and memory investigation](pr245-fixes.md) for implementation,
new passing regressions, durability checks, and updated measurements.

## Rebase integration

- Retain Alpenglow worker-error propagation, rejected-task accounting and
  status-cache retention; return pooled tar buffers on worker exits.
- Route batch account reads through the same file resolver as individual reads.
- Share the durable file-ID high-water mark across runtime writes, folds and
  compaction. Keep fold/compaction segments on the primary directory alongside
  their recovery manifests, using primary-shard IDs.
- Write bootstrap/high-water metadata for the coalesced namespace.
- Preserve legacy single-directory opening and ready-state validation.
- Preserve the corrected snapshot freshness reference and newer node settings.
- Replace hard-coded writer-test mount paths with `t.TempDir`; add a sharded
  snapshot → fold → reopen → recover → fold → rewind regression test.

## Validation and limits

All builds, tests, node runs and timings were executed on Zen 5 with Go 1.26.4
and `GOEXPERIMENT=arenas`. The Mac was used only for source editing and analysis.
Linux exposes one Samsung PM9A3 NVMe (960 GB decimal); PCI, sysfs and block-device
inventories all show one NVMe controller/device. Multiple directories here test
layout correctness, not multiple physical disks' aggregate bandwidth.

Both baseline and candidate pass the targeted AccountsDB, snapshot, config,
state and node suites. Candidate race checks and vet pass for those packages.
Both buffered and O_DIRECT writer readback/placement tests actually ran on ext4
with `TMPDIR=/var/tmp`, including oversized buffers. Sharded fold/restart/rewind
passes. The intentionally failing reproductions and macOS cross-build are reported
separately above.

Pinned corpus:

- Full slot 952183: `snapshot-952183-39vtNcVskxhNNHpmSFhQiUZfBvePGuWLLoZSNK2ZrSco.tar.zst`
- Incremental through 1021126:
  `incremental-snapshot-952183-1021126-8WPCAjnCZDUcCqYRYf3ThbBqZ6Qfsu3uKAmDUv7LMiWZ.tar.zst`
- Archives fetched from `https://rpc.ag.validator1.net`; checksums saved in
  `snapshot-sha256.txt` in the review artifacts.

After unpacking full + incremental snapshots and reopening, the baseline and
candidate (including two-directory buffered and one-directory O_DIRECT modes)
both read **5,593,721 indexed accounts** successfully and produced
identical canonical account digests:
`82fecea1d07acf1cbdefe89f8ae4e222a7e416b0eb2bca24f7eb4289f8253f6a`.
Every key was checked through both individual and batch APIs. The probe accounts
for the batch API's existing canonical zero-lamport tombstones and treats nil
and empty data equally. The reported manifest bank hash also matched; this is
not a separate recomputation of the bank hash.

## Full-node smoke test

The baseline and candidate were each launched for up to three minutes in
verifying mode, with their own
storage/log/ledger directories and no validator keys, to replay a bounded range
starting at 1021127 from RPC. The restored testnet returns RPC error -32015:
`Transaction version (1) is not supported by the requesting client`.
The shared `pkg/rpcclient/blockfetch.go` requests maximum version 0. This is
outside PR #245. Snapshot bootstrap and node initialization succeed, but the
requested replay range cannot be claimed as validated. RPC-only runs also do
not validate Alpenglow certificate-backed durable promotion. The running
Lightbringer instance was left untouched.

## Measurements

Buffered unpack of the same full + incremental corpus, eight alternating
pairs on CPUs `2-7,10-15` (Go's automatic parallelism; no host tuning or global
cache drops). Archives were warm, each output database was rebuilt, and the
full production bootstrap API—including manifest parsing, cleanup, unpack,
indexing and opening the DB—was timed. Full account readback ran separately.
No test nodes, builds or race checks ran during these samples; the existing
Lightbringer service remained active.

| Metric | Alpenglow baseline | Rebased PR |
| --- | ---: | ---: |
| Median bootstrap elapsed | 1.1533 s | 1.0446 s |
| Median process peak RSS | 257.0 MiB | 488.7 MiB |

The candidate used **9.4% less elapsed time**, winning all eight paired samples
(two-sided exact sign test p=0.0078125). **Peak RSS increased about 90%**. RSS is
measured for the probe process, including its close/reopen/cache initialization;
it is not a count of allocations or total memory including OS page cache.
Subsequent heap profiles confirmed retained tar and writer buffers as
contributors. See the follow-up report for measurements after earlier buffer
release; mainnet-scale memory requirements remain unmeasured.

Two-directory buffered and one-directory O_DIRECT builds also reopened and
validated every account against the same digest. These were correctness runs,
not repeated comparisons of those options. In particular, this does not prove
O_DIRECT is faster or measure scaling across physical drives. The corpus is
small (420 MiB compressed), and buffered completion is not a measurement of
fully fsynced disk throughput. No replay-throughput improvement is established.

## Reproduction artifacts

Local evidence is in
`/Users/nikolashayes/Desktop/programming/Codex/review-results/pr245-zen5-2026-09-07`:
`probe.go`, `build.sh`, `run-node.sh`, `measure.sh`, the two failing reproductions,
`analyze.py`, `summary.json`, and raw `results/` logs. `python3 analyze.py`
regenerates the summary without executing benchmarks.

The isolated remote test root is
`/var/tmp/mithril-pr245-20260907-01a07786` and retains sources, binaries, pinned
archives and databases for follow-up. Both bounded node processes have exited;
the pre-existing Lightbringer PID remains running. No validator keys were read,
no stake/vote operation was attempted, and no GitHub review, comment or push
was made.


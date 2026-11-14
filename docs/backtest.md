# mithril backtest

See [benchmarking.md](benchmarking.md) for what the recorded numbers mean, what to check in a result, and how to compare with other clients.

`mithril backtest` replays a Firedancer shredcap capture offline and reports
how long replay took, the way `firedancer-dev backtest` does. Given the same
capture and starting state, Mithril and Firedancer execute exactly the same
blocks.

The node uses no network and no RPC endpoint: no gossip, Turbine, repair or
RPC calls. It starts from local snapshot files or an existing AccountsDB, reads
every block from the capture, checks every block's bank hash against the
capture, and stops at the capture's last block. A backtest does not stop other
Mithril processes on the host (a normal `mithril run` does), so it can run
beside a live node.

## What you need

1. **The capture**: the `.pcapng` or `.pcapng.zst` file given to
   `firedancer-dev backtest`. It must hold every slot after the snapshot slot
   and a bank hash record for each of them; captures written by Firedancer's
   `blockstore2shredcap` or `shredb-export` do. Alpenglow blocks only: rooting
   follows the finalization certificates in block footers.
2. **The starting state**, one of:
   - the snapshot archives Firedancer started from: a full snapshot
     (`snapshot-<slot>-<hash>.tar.zst`) and, optionally, the incremental on top
     of it (`incremental-snapshot-<base>-<slot>-<hash>.tar.zst`); or
   - an existing Mithril AccountsDB at the capture's starting slot, or one that
     can rewind to it (`--accounts`).

The capture's first block must build on the snapshot slot. A capture that
starts later is refused with `snapshot too old for capture`; shreds at or below
the snapshot slot are skipped, as in Firedancer.

## Run it

Build with `make build`. Then, pinned to the 8 cores of one chiplet (on an
EPYC 9555P each CCD has 8 cores; check `lscpu -e` for which CPU numbers share
an L3 and leave out their SMT siblings):

```
GOMAXPROCS=8 taskset -c 0-7 ./mithril backtest \
  --capture shredcap.pcapng.zst \
  --snapshot snapshot-<slot>-<hash>.tar.zst \
  --incremental incremental-snapshot-<base>-<slot>-<hash>.tar.zst \
  --workdir /data/backtest-work
```

Every setting can come from a flag or from a backtest configuration file,
`mithril backtest --config backtest.toml`, laid out like the TOML
`firedancer-dev backtest` runs from (start from `backtest.example.toml`). A
flag overrides the file, relative paths in the file are relative to the file,
and an unknown key is an error. The file configures the backtest only: the
node configuration it runs with is generated in the work directory and cannot
be changed from here, so no file can turn on an RPC endpoint, compaction or the
verifier.

| Flag | File key | Default | Meaning | Firedancer |
|---|---|---|---|---|
| `--capture` | `[ledger_input] path` | required | shredcap file | `[development.ledger_input] path` |
| `--snapshot`, `--incremental` | `[paths] snapshot`, `incremental` | | snapshot archives to start from | `[paths] snapshots` |
| `--accounts` | `[paths] accounts` | | existing AccountsDB instead of a snapshot | `[paths] accounts` |
| `--workdir` | `[paths] workdir` | required | everything the backtest writes goes here | |
| `--cluster`, `--genesis-hash` | `[network] cluster`, `genesis_hash` | `mainnet` | `mainnet`, `testnet`, `devnet`, or a custom name with its genesis hash | genesis file / expected genesis hash |
| `--txpar` | `[layout] exec_workers` | 8 | transaction execution workers | `[layout] execrp_tile_count` |
| `--end-slot` | `[ledger_input] end_slot` | capture's last block | stop earlier | `[development.backtest] end_slot` |
| `--warmup` | `[backtest] warmup` | 30 (0 with `--cache cold`) | leading blocks left out of the timing table | none (Firedancer excludes none) |
| `--cache` | `[backtest] cache` | `normal` | AccountsDB page cache at replay start: `normal`, `cold` or `warm` (see below) | none (accounts are in memory) |
| `--measure-from` | `[backtest] measure_from` | | leave blocks before this slot out of the timing table | |
| `--phase-timers` | `[backtest] phase_timers` | off | time each transaction phase and instruction dispatch, as a live node does | |
| `--verify-shred-signatures` | `[backtest] verify_shred_signatures` | off | check every shred's leader signature as replay reads it | not done by its backtest |

Firedancer's `root_distance` has no counterpart: it delays the backtest
tile's mocked finalization, an extra rooting source Mithril does not have.
Both replay paths also root from the finalization certificates in block
footers (see the differences below). The summary names the configuration file
it ran with.

The default Go interpreter loop is the recommended path. `MITHRIL_SBPF_ASM_LOOP=1`
selects the optional amd64 assembly loop instead; it is off by default, was
faster on a Zen 4 desktop CPU and made no measurable difference on a Zen 5
one. The summary states which loop ran.

Exit status is 0 when every block matched the capture's bank hash and replay
reached the last block, and 1 otherwise. A mismatch prints
`Bank hash mismatch! slot=<slot> expected=<hash>, got=<hash>` and stops at that
slot, as Firedancer does; a slot without a bank hash record in the capture also
stops the run.

## Repeat runs and disk

The work directory is reused: the imported capture (`corpus/`) is kept for
as long as the capture's contents (SHA-256) are unchanged, and
the AccountsDB is rewound rather than rebuilt. Only an import that passed the
capture checks (a bank hash for every block, an unbroken parent chain) is
kept: a rejected capture leaves no import, so running it again reports the
same error.

- With `--accounts`, a store that has replayed past the capture's start is
  rewound to it when that slot is a retained fold boundary, and refused
  otherwise. The rewind changes that store without asking, so never point
  `--accounts` at the store of a node that is running.
- With `--snapshot`, the first run builds the AccountsDB in
  `<workdir>/accounts` and replays from the snapshot slot. Later runs rewind to
  the first fold boundary after the snapshot slot (normally within about 128
  slots), so they replay slightly fewer blocks; the summary prints the measured
  slot range, and `--measure-from` makes runs cover the same blocks.

A run that was killed leaves the store past the slot its state file records.
The next run plans from the store's fold manifests, as node startup recovers
from them, so it rewinds as above. After every run the timing rows must cover
exactly the planned blocks, the first planned slot through the last with none
missing; otherwise the backtest exits with status 1, says what was planned and
what was replayed, and prints no summary.

The work directory must be new, empty, or one a backtest created
(`BACKTEST.json`); anything else is refused. All paths are inside it, or the
`--accounts` store.

Disk, for mainnet: the AccountsDB built from a snapshot is about 550 GB; the
imported capture takes about 0.5 GB per 1,000 blocks. Each rewind moves the
undone folds under `accounts/rewound/` (a few GB per 1,000 blocks). In the work
directory's own store a backtest deletes them before rewinding; under an
`--accounts` store they accumulate and are safe to delete while no node runs.

## Output

```
Backtest summary
  config / commit / go / GOMAXPROCS / usable CPUs / txpar / phase timers / shred signatures / MITHRIL_SBPF_ASM_LOOP / memory limit / shred version / cache
  blocks                 1000 replayed, 30 warm-up excluded, 970 measured (slots A..B)
  measured span          970 blocks in ... s: ... blocks/s, ... ms/block; ... slots advanced: ... slots/s
  avg consumed CU/block  14.01 M
  interval        mean ms    p50 ms    p90 ms    p99 ms    max ms    CU/ns
  BlockInterval     ...
  SlotReplay        ...
  TxLoop            ...
BACKTEST_SUMMARY snapshot_slot=... end_slot=... slots=... txns=... snap_bytes_s=... replay_init_s=... load_total_s=... replay_s=... slot_exec_s=... sec_per_slot=... tps=... total_s=...
MITHRIL_EXTRA import_s=... shutdown_s=...
```

The measured span runs from the completion of the block before the first
measured one to the completion of the last, and covers everything replay did
in that time. `BlockInterval` is each block's completion minus the previous
block's, so the intervals add up to the span: its mean is exactly the span's
ms/block, and with no warm-up the span is `replay_s` and the mean is
`sec_per_slot`. SlotReplay and TxLoop are parts of a block's work and do not
add up to the span. "Slots advanced" counts skipped slots too; blocks/s counts
only replayed blocks, as Firedancer's `slots` does.

The table above the `BACKTEST_SUMMARY` line is Mithril's own: warm-up blocks
(and blocks before `--measure-from`) are left out, and CU/ns is total consumed
CU over total nanoseconds of that interval, not a mean of per-block ratios;
higher is better. Consumed CU makes blocks of different size comparable but
does not measure host CPU work.

### The BACKTEST_SUMMARY line

`BACKTEST_SUMMARY` is the line Firedancer's backtest prints at the end
(`finish()` in `src/discof/backtest/fd_backtest_tile.c`, last checked at
firedancer main `a99731d6f28d`). Mithril prints the same fields under the same
names and formats, each measured over the same span, so the two lines can be
compared field by field. Like Firedancer's, it covers every replayed block: no
warm-up is excluded, and the start (snapshot or checkpoint) slot is not
counted.

Firedancer prints two other things that are not mirrored. Its "Backtest
playback done" line has an `elapsed` that starts at snapshot completion, so
its `sec/slot` includes `replay_init_s` and differs from `sec_per_slot`. Its
per-slot "Bank hash matches!" line splits each slot into switch (previous
slot's completion to this one's preparation), begin (preparation to the first
transaction scheduled), exec (first scheduled to last finished) and finish
(last finished to completion). Mithril's per-block intervals are in
`replay_timings.jsonl` instead and are not divided the same way.

All times are wall clock. Firedancer's boot is its `boot_timestamp_nanos`,
taken when the command loads its configuration (`fd_config.c`), so in effect
process start, before its host configuration stages run; Mithril's is process
start.

| Field | Firedancer's span | Mithril's span |
|---|---|---|
| `snapshot_slot` | the snapshot slot replay starts from | the slot replay starts from: the snapshot slot, or the checkpoint an AccountsDB was rewound to |
| `end_slot` | last replayed slot | last replayed slot |
| `slots` | completed slots after the snapshot slot | replayed blocks after the start slot |
| `txns` | per completed slot, vote and non-vote, succeeded and failed | every transaction in the replayed blocks |
| `snap_bytes_s` | boot to snapshot loaded (a duration, despite the name) | node start to AccountsDB ready (built from the snapshot, or opened and rewound) |
| `replay_init_s` | snapshot loaded to the start slot complete | AccountsDB ready to the replay loop about to fetch its first block |
| `load_total_s` | boot to the start slot complete | node start to the replay loop about to fetch its first block |
| `replay_s` | start slot complete to finish | replay loop ready to the last block's completion; includes reading and assembling blocks from the shred spool, as Firedancer's includes reading the capture |
| `slot_exec_s` | first replayed slot's preparation start to the last slot's completion | first block's start to the last block's completion; the start is where streaming execution opened the block's bank, or its SlotReplay start if it was not streamed or its stream was discarded |
| `sec_per_slot` | `replay_s / slots` | `replay_s / slots` |
| `tps` | `txns / replay_s`, one decimal | `txns / replay_s`, one decimal |
| `total_s` | boot to finish | process start to the last block's completion |

`MITHRIL_EXTRA` has what Firedancer has no field for. `import_s` runs from
process start to node start: hashing the capture (SHA-256, so an earlier
import is reused only for the same capture contents; about a second per
500 MB), the capture import (skipped when an earlier import is reused), and
preparing the work directory (restoring the shred spool, writing the node
configuration). Firedancer reads the capture while replaying,
so its decoding time is inside its `replay_s`, while Mithril imports it first.
So `total_s = import_s + load_total_s + replay_s`, and `load_total_s` covers
state loading only, as Firedancer's does. `shutdown_s` is the node's shutdown
after the last block, which Firedancer's `total_s` also leaves out (it exits at
the last slot).

Not identical, and why:

- **Per-slot start.** Firedancer's slot begins at preparation, when replay
  creates its bank. Mithril's SlotReplay begins once the block is assembled,
  so a streamed block's earlier work falls outside it (`ReplayWork` adds it
  back). `slot_exec_s` starts the first block where its streamed bank opened,
  the counterpart of Firedancer's preparation start, so only per-block
  SlotReplay figures keep the later start.
- **Signature verification.** Both verify transaction signatures inside
  `replay_s` and `slot_exec_s`, but at different points (see below).
- **Footer certificates and rooting.** Both verify footer certificates and
  root from them; Firedancer's backtest adds mocked finalization from the
  capture's root records.
- **Account state.** Firedancer keeps accounts in its in-memory accdb sized by
  its configuration; Mithril reads an on-disk AccountsDB through its caches.
  Our published numbers come from repeated replays of the same blocks with the
  OS page cache warm; the first run after a snapshot build is colder and slower.
- **Threads.** Firedancer's `execrp_tile_count` against Mithril's
  `exec_workers` (`--txpar`); other work (signature verification, assembly)
  uses separate threads on each side.

### What runs inside the timed intervals

Mithril, from its source:

- Transaction signature verification runs while blocks are assembled from
  shreds, before SlotReplay starts; only waiting for outstanding results
  (`SignatureVerificationJoin`) is inside SlotReplay. It is inside the
  block-to-block time.
- The entry hash chain is checked during block assembly, outside SlotReplay.
- Footer finalization certificates are verified (BLS) when a block is observed,
  before SlotReplay; inside the block-to-block time.
- Inside SlotReplay: account loading, transaction execution (TxLoop), bank
  hash computation, the footer and capture bank hash comparisons, and the
  in-memory commit. Folding rooted slots to disk runs on a background worker.
- Per-transaction phase timers are off by default in backtest, unlike a live
  node (`mithril run` keeps them on). On the Zen 4 corpus they cost about
  0.9% of SlotReplay (two runs per arm, 0.7% run-to-run spread). With them
  off, 19 per-transaction fields of the timing rows read zero (for example
  `IxLoop`, `CalcAndDeductFees`, `ExecIxNativeProgram*`); pass
  `--phase-timers` when you want that detail.

Firedancer, read from its source (main at `a99731d6f28d`) and not measured:

- Per-slot timings run from slot preparation to slot completion.
- Transaction signatures are verified as scheduler tasks
  (`FD_SCHED_TT_TXN_SIGVERIFY`) on the same exec tiles as execution,
  interleaved with it within the slot (`fd_replay_tile.c`, `fd_execrp_tile.c`).
  They are inside the per-slot span, where Mithril's are mostly outside
  SlotReplay; both are inside `slot_exec_s` and `replay_s`.
- Footer certificates are verified (`fd_alpenglow_cert_verify`) as part of
  replaying the block, on the exec tiles and inline; a failure marks the block
  dead. That is inside the per-slot span, where Mithril's is before SlotReplay.
- Shred signatures are not checked: the backtest tile parses capture shreds
  for structure only.
- Where its entry hash checks fall was not established here.

### Cache modes

Firedancer keeps accounts in memory. Mithril reads an on-disk AccountsDB, so
how much of it the OS page cache holds when replay starts changes the result.
`--cache` sets it:

| Mode | What it does | Represents |
|---|---|---|
| `normal` (default) | evicts the AccountsDB files from the page cache just before replay, then leaves the warm-up blocks out | a node that has been replaying the preceding blocks, with nothing cached from earlier runs over these same blocks |
| `cold` | evicts the same way and measures from the first block (warm-up 0 unless set) | a node right after a restart |
| `warm` | evicts nothing | the page cache as earlier runs over the same blocks left it: warmer than a running node, but the least noisy, so the one for comparing two builds |

Eviction uses `posix_fadvise(DONTNEED)` on every file under the AccountsDB
(Linux only; no root needed) after startup and rewind, so it lands inside
`replay_init_s`; the settings line reports how many files and bytes and how
long it took. In-process caches always start empty, since every run is a new
process. For `normal`, use a warm-up long enough for the per-block times to
settle: compare the first and second halves of the measured blocks, and
lengthen the warm-up if the first half is slower.

## Differences from Firedancer's backtest

- **Rooting.** Both root from the finalization certificates carried in block
  footers, as a running node does. Firedancer's backtest tile adds a second
  source: a mocked fast-finalization certificate for each of the capture's
  root records, `root_distance` slots behind replay. Mithril has no such
  source, so roots, and with them folds to disk, can land at different points.
  Captures without footer certificates cannot be rooted by Mithril and are not
  supported.
- **Start state.** Firedancer loads the snapshot on every run. Mithril builds
  its AccountsDB once and rewinds it on later runs.
- **Read-ahead.** Firedancer's backtest tile reads the capture at most 30
  slots ahead of the last completed slot. Mithril imports the capture before
  the timed run, then loads shreds from its spool into block assembly up to
  256 slots ahead of the slot being replayed.
- **Shred signatures.** Neither backtest checks them by default: Firedancer
  feeds capture shreds straight to replay without its shred verification stage
  (read from source), and Mithril trusts its shred spool the way it trusts
  shreds it verified on arrival. Every block's bank hash is still compared
  with the capture, which catches corruption; a forged capture with matching
  hashes would not be. `--verify-shred-signatures` checks each shred against
  its slot leader when replay reads it (one signature check per FEC set, cached)
  and stops with exit status 1 on the first failure. It adds that work to the
  timed run, so leave it off when comparing timings with Firedancer.
- **RPC.** Neither uses one. Mithril's offline mode takes the genesis hash
  from the cluster setting (`--cluster`, or `--genesis-hash` for a custom
  cluster), still refusing an AccountsDB from another chain, and runs no tip
  polling.

# mithril backtest

`mithril backtest` replays a Firedancer shredcap capture offline and reports
how long replay took, the way `firedancer-dev backtest` does. Given the same
capture and starting state, Mithril and Firedancer execute exactly the same
blocks.

The node opens no network sockets except a loopback listener inside the
process, starts from local snapshot files or an existing AccountsDB, reads
every block from the capture, checks every block's bank hash against the
capture, and stops at the capture's last block.

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
GOMAXPROCS=8 MITHRIL_SBPF_ASM_LOOP=1 taskset -c 0-7 ./mithril backtest \
  --capture shredcap.pcapng.zst \
  --snapshot snapshot-<slot>-<hash>.tar.zst \
  --incremental incremental-snapshot-<base>-<slot>-<hash>.tar.zst \
  --workdir /data/backtest-work
```

No configuration file is needed or read. Options:

| Flag | Default | Meaning |
|---|---|---|
| `--capture` | required | shredcap file |
| `--snapshot`, `--incremental` | | snapshot archives to start from |
| `--accounts` | | existing AccountsDB instead of a snapshot |
| `--workdir` | required | everything the backtest writes goes here |
| `--cluster` | `mainnet` | `mainnet`, `testnet`, `devnet`, or a custom name with `--genesis-hash` |
| `--txpar` | 8 | transaction execution workers |
| `--end-slot` | capture's last block | stop earlier |
| `--warmup` | 30 | leading blocks left out of the timing table |
| `--measure-from` | | leave blocks before this slot out of the timing table |
| `--verify-shred-signatures` | off | check every shred's leader signature as replay reads it |

`MITHRIL_SBPF_ASM_LOOP=1` selects the amd64 assembly interpreter loop; without
it the Go loop runs. The summary states which.

Exit status is 0 when every block matched the capture's bank hash and replay
reached the last block, and 1 otherwise. A mismatch prints
`Bank hash mismatch! slot=<slot> expected=<hash>, got=<hash>` and stops at that
slot, as Firedancer does; a slot without a bank hash record in the capture also
stops the run.

## Repeat runs and disk

The work directory is reused: the imported capture (`corpus/`) is kept, and
the AccountsDB is rewound rather than rebuilt.

- With `--accounts`, a store that has replayed past the capture's start is
  rewound to it when that slot is a retained fold boundary, and refused
  otherwise.
- With `--snapshot`, the first run builds the AccountsDB in
  `<workdir>/accounts` and replays from the snapshot slot. Later runs rewind to
  the first fold boundary after the snapshot slot (normally within about 128
  slots), so they replay slightly fewer blocks; the summary prints the measured
  slot range, and `--measure-from` makes runs cover the same blocks.

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
  commit / go / GOMAXPROCS / usable CPUs / txpar / MITHRIL_SBPF_ASM_LOOP / memory limit / shred version
  blocks                 1000 replayed, 30 warm-up excluded, 970 measured (slots A..B)
  avg consumed CU/block  13.95 M
  interval       mean ms    p50 ms    p90 ms    p99 ms    CU/ns
  SlotReplay       ...
  TxLoop           ...
FD_COMPARABLE slots=1000 slot_exec_s=... sec_per_slot=... tps=... outside_s=...
```

CU/ns is total consumed CU over total nanoseconds of that interval, not a mean
of per-block ratios; higher is better. Consumed CU makes blocks of different
size comparable but does not measure host CPU work.

### How the numbers line up with Firedancer's

From Firedancer's `src/discof/backtest/fd_backtest_tile.c` (read from source,
not measured). Firedancer prints, per slot,
`Bank hash matches! ... (switch, begin, exec, finish)` in ms, and at the end a
`BACKTEST_SUMMARY` line.

| Firedancer | Its span | Mithril |
|---|---|---|
| per-slot `exec` | first transaction scheduled to last transaction finished | `TxLoop` (transaction execution loop) |
| per-slot `begin + exec + finish` | slot preparation start to slot completion | `SlotReplay` (complete block to bank hash and commit); start points differ, see below |
| per-slot `switch` | previous slot's completion to this slot's preparation | time between blocks (block-to-block loop time minus SlotReplay) |
| `slot_exec_s` | first replayed slot's preparation start to last slot's completion | `slot_exec_s`: first block's SlotReplay plus the block-to-block time of every later block |
| `replay_s` | start slot (snapshot) complete to finish | not printed; longer than `slot_exec_s` by the time before the first block starts |
| `sec_per_slot` | `replay_s / slots` | `slot_exec_s / slots` |
| `tps` | all transactions (votes and failures included) / `replay_s` | all transactions / `slot_exec_s` |
| `load_total_s` | boot to start slot complete | part of `outside_s` (process start to finish minus `slot_exec_s`) |

Where the spans are not the same (SlotReplay's start point, `replay_s` against
`slot_exec_s`, and therefore `sec_per_slot` and `tps`), compare per-slot
numbers on the same slots rather than the totals.

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

Firedancer, read from its source and not measured: per-slot timings start at
slot preparation and end at slot completion; where its signature
verification, entry checks and certificate handling fall relative to that span
was not established here.

## Differences from Firedancer's backtest

- **Rooting.** Firedancer roots slots from the capture's root records, a
  configurable distance behind replay, and tells replay with a mocked
  fast-finalization certificate. Mithril roots from the finalization
  certificates carried in block footers, as a running node does, so folds to
  disk can happen at different points. Captures without footer certificates
  cannot be rooted and are not supported.
- **Start state.** Firedancer loads the snapshot on every run. Mithril builds
  its AccountsDB once and rewinds it on later runs.
- **Shred signatures.** Neither backtest checks them by default: Firedancer
  feeds capture shreds straight to replay without its shred verification stage
  (read from source), and Mithril trusts its shred spool the way it trusts
  shreds it verified on arrival. Every block's bank hash is still compared
  with the capture, which catches corruption; a forged capture with matching
  hashes would not be. `--verify-shred-signatures` checks each shred against
  its slot leader when replay reads it (one signature check per FEC set, cached)
  and stops with exit status 1 on the first failure. It adds that work to the
  timed run, so leave it off when comparing timings with Firedancer.
- **RPC.** The node's only RPC endpoint is the loopback listener in the
  backtest process. It answers `getGenesisHash` from the cluster setting and
  rejects everything else; the summary counts the rejected calls (background
  tip polling).

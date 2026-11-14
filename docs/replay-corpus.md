# Replay corpora and Firedancer shred captures

A replay corpus is a fixed stretch of blocks stored as shreds, used to replay
exactly the same blocks again and again for benchmarks and comparisons. It is a
directory holding:

- `shreds/s<slot>.shreds`: shred spool files, one per slot;
- `complete.idx`: the spool's completion index;
- `manifest.json`: the checkpoint slot the replay starts from, the slots to
  replay in order, and the expected bank hash of each.

The node replaying a corpus must already hold the state of the checkpoint slot
in its AccountsDB.

## A corpus from a node run

A node's shred spool (`<blockstore>/catchup-spool`) holds only a window around
replay: slots behind replay are deleted as it advances. To keep every block a
run replays, start the node with

```bash
export MITHRIL_SHRED_ARCHIVE_DIR=/path/to/archive
```

Slots that fall behind replay then move into that directory instead of being
deleted (slots dropped for space or discarded as invalid are still deleted).
It holds every verified shred of every slot replay passed, including
abandoned forks, so it grows by roughly the size of the blocks replayed:
check free space for a long run. After stopping the node:

```bash
mithril corpus from-spool OUTDIR \
  --spool /path/to/archive --spool <blockstore>/catchup-spool \
  --bank-hashes <accountsdb>/bankhash.log \
  --from-slot SNAPSHOT_SLOT+1 --to-slot LAST_FINAL_SLOT \
  --checkpoint SNAPSHOT_SLOT \
  --capture OUTFILE.pcapng.zst
```

The corpus is the chain of shred-header parents ending at the highest complete
slot with a logged bank hash within the range, so abandoned forks are left
out. The bank hashes are the ones the node computed, which it checked against
each block's footer as it replayed. The newest slots of a run may not be final
when it stops, so set `--to-slot` some slots before the end. `--checkpoint`
refuses a chain that does not start right after the snapshot slot (a slot in
between is missing, for example because the archive was not on yet).
`--capture` also writes the corpus as a shredcap capture of rooted blocks
only, ready for `mithril backtest` or another client.

## Firedancer shredcap captures

`firedancer-dev backtest` replays shredcap captures: pcapng files, usually
zstd-compressed, that carry one shred per UDP packet plus bank hash and root
slot records. Mithril reads and writes the same format, so both clients can
replay the same blocks.

```
# A capture into a corpus (.pcapng or .pcapng.zst):
mithril corpus import-shredcap CAPTURE OUTDIR [--from-slot N] [--to-slot N]

# A corpus into a capture that firedancer-dev backtest accepts:
mithril corpus export-shredcap CORPUS OUTFILE.pcapng.zst [--snapshot-slot N]
```

Import keeps the capture's rooted slots when it has root records, and otherwise
the chain of shred parents ending at the highest complete slot with a bank
hash. It reports slots it had to leave out and slots completed by recovering
data shreds from coding shreds, including a slot the capture holds only as
coding shreds. Export writes data shreds only, in index
order, as backtest requires, recovering any the corpus never received.

A capture is trusted input. Import does not check leader signatures on the
shreds, and the bank hashes it records come from the same file. Use captures
you made or have reason to trust.

Not yet verified: a capture exported by Mithril has not been run through
`firedancer-dev` itself. The layout follows Firedancer's `shredcap.md`,
`fd_backtest_tile.c` and `contrib/blockstore/blockstore2shredcap.c` (last
checked at firedancer main `a99731d6f28d`).

## Offline replay

`mithril run --offline` (or `network.offline = true`) replays only what is in
the local shred spool. It starts no gossip client, Turbine tree, repair or
Votor listener, binds the shred receiver to loopback, and takes rooting from
block footer certificates only. It makes no RPC calls: `network.rpc` is
ignored, the startup genesis check uses the cluster setting, and no tip is
polled. It refuses to start unless the configuration has
`bootstrap.mode = "accountsdb"` or local snapshot files (`--snapshot`),
verifying consensus mode, a finite replay range (`replay.end_slot` or
`replay.num_slots`), `turbine.shred_version`, and `block.rpc_fallback` off, and
says which is missing. Unlike a normal `mithril run`, offline replay does not
stop other Mithril processes on the host, so it can run beside a live node.

`mithril backtest` wraps all of this in one command for a shredcap capture:
see [backtest.md](backtest.md).

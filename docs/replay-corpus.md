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
data shreds from coding shreds. Export writes data shreds only, in index
order, as backtest requires, recovering any the corpus never received.

A capture is trusted input. Import does not check leader signatures on the
shreds, and the bank hashes it records come from the same file. Use captures
you made or have reason to trust.

Not yet verified: a capture exported by Mithril has not been run through
`firedancer-dev` itself. The layout follows Firedancer's `shredcap.md`,
`fd_backtest_tile.c` and `blockstore2shredcap.c`.

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
says which is missing.

`mithril backtest` wraps all of this in one command for a shredcap capture:
see [backtest.md](backtest.md).

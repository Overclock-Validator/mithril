# replaybench

A standalone harness for replaying real mainnet blocks offline and measuring
how long replay takes. It runs the production block-replay path
(`configureBlock` + `ProcessBlock`) against a scratch AccountsDB seeded from a
pre-packaged bundle — no live node, gossip, or network, and no snapshot or full
AccountsDB on disk.

Runs are deterministic and self-checking: every bundled block carries the
transaction metadata captured from mainnet, so replay either reproduces the
recorded per-transaction results and final bankhash or fails loudly on the first
divergence. That makes it usable both as a **performance benchmark** and as an
**offline correctness gate** for changes to the runtime, VM, or account paths.

## How it works

`replaybench` has two subcommands:

- **`export`** — run against a stopped instance that has already replayed up to
  some slot `B`. It packages everything needed to replay blocks `B+1..B+n`
  standalone: the state file, the union of accounts those blocks touch (their
  transaction keys, resolved address-lookup-table keys and the lookup tables
  themselves, programs and programdata, sysvars, feature gates, and the staked
  vote accounts) read at their slot-`B` values, plus the blocks themselves
  fetched from RPC with their transaction metadata. The result is a portable
  bundle directory.

- **`run`** — seed a fresh single-shard AccountsDB from a bundle, restore global
  replay state the way the node's resume path does, replay each block in order,
  and report per-block and total timing plus the final bankhash.

### Constraints

The replay window must stay **within a single epoch** and **outside the epoch
rewards-distribution period**. Epoch boundaries and the rewards period require
the full stake set, which the bundle intentionally does not carry.

## Building

```bash
go build -o replaybench ./cmd/replaybench
```

Requires the Go toolchain pinned in `go.mod`.

## Exporting a bundle

Export reads a local AccountsDB directly, so the instance that owns it must be
stopped. Blocks are fetched from RPC and paced by `-rps`; a rate-limited export
can be re-run and resumes from the blocks already written to the bundle.

```bash
replaybench export -accounts /path/to/accountsdb -n 100 -out /data/bench-bundle
```

| flag | default | meaning |
|------|---------|---------|
| `-accounts` | — | AccountsDB directory of the stopped instance (same as the node's storage config) |
| `-n` | 100 | number of blocks past the last replayed slot to bundle |
| `-out` | — | bundle output directory |
| `-rpc` | mainnet-beta | RPC endpoint to fetch blocks from |
| `-rps` | 2 | max block fetches per second (0 = unpaced) |

The bundle is portable: export once on a machine with a synced instance, then
copy the bundle directory to wherever the benchmark runs.

## Running a bundle

```bash
replaybench run -bundle /data/bench-bundle -db /tmp/scratch-db
```

| flag | default | meaning |
|------|---------|---------|
| `-bundle` | — | bundle directory |
| `-db` | — | scratch AccountsDB directory (wiped and recreated each run) |
| `-tx-parallelism` | 0 | parallel transaction workers (0 = sequential) |
| `-cpuprofile` | — | write a CPU profile of the replay loop to this file |
| `-memprofile` | — | write a heap profile after the replay loop to this file |

Sequential and parallel runs produce the same final bankhash.

### Reading the output

```
=== replaybench summary ===
blocks:          100 (slots ..NNNNNNN)
transactions:    NNNNN
compute units:   NNNNNNNN
exec time:       N.NNNs
txs/sec:         NNNNN
blocks/sec:      N.NN
final bankhash:  <base58>
```

- **`final bankhash`** is the correctness signal. The recorded transaction
  metadata is validated as each block replays, so a run that finishes has
  reproduced mainnet's per-transaction behavior; the printed bankhash is the
  cumulative check. A divergence aborts the run instead of printing a wrong
  hash.
- **`exec time`** / **`txs/sec`** are the performance signals.

## Benchmarking a change end-to-end

Build the tool from the base revision and from the change, replay the **same
bundle** with each, and compare:

```bash
git worktree add /tmp/mith-before <base-ref>
git worktree add /tmp/mith-after  <change-ref>
( cd /tmp/mith-before && go build -o /tmp/replaybench-before ./cmd/replaybench )
( cd /tmp/mith-after  && go build -o /tmp/replaybench-after  ./cmd/replaybench )

/tmp/replaybench-before run -bundle /data/bench-bundle -db /tmp/scratch-before
/tmp/replaybench-after  run -bundle /data/bench-bundle -db /tmp/scratch-after
```

- The two `final bankhash` values **must be identical** (and, via the bundled
  metadata, both match mainnet). Identical hashes mean the change is
  behavior-preserving.
- The `exec time` delta is the real-workload speedup or regression.

If the harness itself is not present on the base revision, cherry-pick the
commit that adds it onto the base worktree first — it only adds files under
`cmd/replaybench` and `pkg/replay/bench*.go` and does not touch the runtime, so
it applies cleanly.

Pass `-cpuprofile` to the `after` run to capture a representative profile for
profile-guided optimization.

## Companion microbenchmarks

For finer-grained measurement of the VM itself, the repository carries Go
microbenchmarks alongside the replay harness:

- `pkg/sbpf` — `BenchmarkInterp*`: interpreter dispatch, memory access, calls,
  syscalls, and the per-execution VM lifecycle.
- `pkg/sealevel` — `BenchmarkMem*`: the memory syscalls
  (`sol_memcpy_`/`memmove_`/`memset_`/`memcmp_`).

Run them with [`benchstat`](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat)
across a base and a change:

```bash
( cd base   && go test -run=XXX -bench='BenchmarkInterp' -benchmem -count=10 ./pkg/sbpf/ )     > interp.before
( cd change && go test -run=XXX -bench='BenchmarkInterp' -benchmem -count=10 ./pkg/sbpf/ )     > interp.after
( cd base   && go test -run=XXX -bench='BenchmarkMem'    -benchmem -count=10 ./pkg/sealevel/ ) > mem.before
( cd change && go test -run=XXX -bench='BenchmarkMem'    -benchmem -count=10 ./pkg/sealevel/ ) > mem.after
benchstat interp.before interp.after
benchstat mem.before mem.after
```

A benchmark file added alongside a change does not exist on the base revision;
copy just the relevant `*_bench_test.go` files onto the base worktree before
running the "before" side (they depend only on exported API).

## Environment for stable numbers

On amd64 desktop/server parts, frequency boost and SMT introduce run-to-run
variance. Pin the governor and quiesce the machine (no validator or other heavy
process running):

```bash
sudo cpupower frequency-set -g performance
```

Where pinning is unavailable, `-count=10` combined with `benchstat`'s
significance testing absorbs most of the noise.

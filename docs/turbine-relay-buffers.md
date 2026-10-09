# Turbine relay buffer ownership

Each retransmit worker reuses a private peer-result slice. Weighted shuffle,
tree placement, address filtering and fanout are unchanged. The exported
`RetransmitPeers` API still returns independently allocated result storage.
Workers clear their entire scratch slice after each send so it cannot retain
addresses from an old cluster snapshot.

`SubmitFrom` copies a packet into exclusively owned storage before returning
to its caller. Canonical packets use a fixed-size, GC-reclaimable `sync.Pool`;
oversized inputs retain the independent allocation path. Queue admission
transfers ownership to the worker. The worker returns storage only after all
synchronous sends and retries finish, including error and no-peer paths.
Queue rejection returns storage immediately. Shutdown closes admission under
a short lock, joins workers, then drains remaining copies. The channel stays
open for concurrent submitters, which cannot enqueue after admission closes.

`packetBatchSender.Send` borrows both packet bytes and peer addresses only
until return, even on partial sends or errors. Implementations that retain
either must copy them. A pooled packet must never escape this lifetime.
The admission lock is not held during authentication, routing or socket I/O.

## Measurement

`BenchmarkRetransmitPipeline` measures deduplication, packet copying, queue
handoff, weighted routing and dispatch to a mock sender. Run with:

```sh
GOMAXPROCS=2 go test ./pkg/turbine -run '^$' \
  -bench '^BenchmarkRetransmitPipeline$' -benchmem -benchtime=20000x -count=3
```

On a Ryzen 7 9700X (Zen 5), Go 1.26.4, one producer and one relay worker,
the incremental comparison against the relay implementation at
`c40ac9e8ca8aa09a9f2a8759a231199b0f90346e` was:

| Gossip contacts | Before ns/shred | After ns/shred | Before B/shred | After B/shred | Allocations before → after |
|---|---:|---:|---:|---:|---:|
| 90 | 3,906 | 3,761 | 3,784 | ~729 | 8 → 6 |
| 512 | 23,789 | 22,056 | 3,784 | ~729 | 8 → 6 |

Times are medians of six samples per version, in baseline/candidate/candidate/
baseline order, three samples per round. Both versions use the same benchmark
and combined validator source; only the two relay production files change.
Each iteration submits a distinct 1,203-byte data shred with cached topology;
the benchmark checks that none are dropped and waits for workers to finish.
The fixture includes staked and zero-stake peers. Contact count does not mean
that every shred has that many recipients: tree position determines forwarding.

Allocation fell about 81%. The 4–7% median timing improvement is modest and
noisy on the shared validator host; one candidate sample was slower than every
baseline sample in its case. These are amortized in-memory pipeline times,
not network delivery latency or live FAST-score gains. Socket syscalls, parent
authentication and retransmitter signing are excluded from this fixture.

Routing tests compare against a full weighted permutation, including both
shuffle modes and missing/unroutable contacts. Ownership tests exercise caller
buffer reuse, queue pressure, send retries, concurrent routing and shutdown.
The full Turbine race suite passed locally and natively; native vet and the
combined validator build passed. Historical run artifacts are linked from [the evidence archive](streaming-preparation-evidence.md).

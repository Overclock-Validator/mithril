# Mithril Snapshot Integration

This document describes the snapshot download and streaming integration in Mithril, which uses the `solana-snapshot-finder-go` library for intelligent node discovery and selection.

## Overview

Mithril can bootstrap from Solana snapshots in two modes:

1. **HTTP Streaming** (default): Stream snapshots directly from network to processing pipeline, optionally saving to disk while streaming
2. **Disk Download**: Download snapshots to disk first, then process from disk

The integration automatically discovers the fastest RPC nodes with recent snapshots using a two-stage speed testing algorithm.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        mithril/cmd/mithril                       │
│                     (verify-live, node commands)                 │
└─────────────────────────────────┬───────────────────────────────┘
                                  │
┌─────────────────────────────────▼───────────────────────────────┐
│                     pkg/snapshotdl                               │
│  - SnapshotConfig (all tuning parameters)                        │
│  - GetSnapshotURL() → HTTP URL for full snapshot                 │
│  - GetIncrementalSnapshotURL() → HTTP URL for incremental        │
│  - DownloadSnapshot() → disk path (if disk download needed)      │
└─────────────────────────────────┬───────────────────────────────┘
                                  │
┌─────────────────────────────────▼───────────────────────────────┐
│                     pkg/snapshot                                 │
│  - BuildAccountsDbWithIncr() → main entry point                  │
│  - newSnapshotReaderWithSave() → HTTP or file reader             │
│  - readTarWithSave() / readTarIncrWithSave() → tar processing    │
│  - Cleanup logic for partial downloads                           │
└─────────────────────────────────┬───────────────────────────────┘
                                  │
┌─────────────────────────────────▼───────────────────────────────┐
│               solana-snapshot-finder-go (library)                │
│  - rpc.FetchClusterNodes() → discover RPC nodes                  │
│  - rpc.EvaluateNodesWithVersionsAndStats() → two-stage testing   │
│  - rpc.SortBestNodesWithStats() → rank by speed                  │
│  - snapshot.GetSnapshotURL() → get HTTP URL from node            │
└─────────────────────────────────────────────────────────────────┘
```

## Two-Stage Node Discovery Algorithm

The snapshot finder uses a two-stage algorithm to efficiently find the fastest nodes:

### Stage 1: Fast Parallel Triage

- Downloads a small amount of data from many nodes in parallel
- Quickly eliminates slow nodes to reduce the candidate pool
- Configuration:
  - `stage1_warm_kib`: Warmup data before timing (default: 512 KiB)
  - `stage1_window_kib`: Size of each measurement window (default: 512 KiB)
  - `stage1_windows`: Number of measurement windows (default: 4)
  - `stage1_timeout_ms`: Timeout per node (default: 3000 ms)
  - `stage1_concurrency`: Parallel downloads (default: 0 = auto, uses CPU cores)

### Stage 2: Sustained Speed Test

- Takes top candidates from stage 1
- Measures sustained download speed over longer period
- Detects nodes that start fast but slow down
- Configuration:
  - `stage2_top_k`: Number of candidates to test (default: 8)
  - `stage2_warm_sec`: Warmup duration (default: 2 seconds)
  - `stage2_measure_sec`: Measurement duration (default: 2 seconds)
  - `stage2_min_ratio`: Speed collapse threshold (default: 0.6)
  - `stage2_min_abs_mbs`: Minimum absolute speed (default: 0.0, disabled)

### Node Filtering

Before speed testing, nodes are filtered by:

- **Version**: `min_node_version` (default: "3.0.0"), `allowed_node_versions` (optional whitelist)
- **RTT**: `max_rtt_ms` (default: 200 ms)
- **TCP Connectivity**: `tcp_timeout_ms` (default: 1000 ms)
- **Snapshot Age**: `full_threshold` (default: 100000 slots), `incremental_threshold` (default: 1000 slots)

## Full Snapshot Selection Flow

```
1. Get reference slot from RPC endpoint
2. Discover all RPC nodes from cluster (getClusterNodes)
3. Filter nodes by version, RTT, snapshot age
4. Run two-stage speed test on remaining nodes
5. Rank nodes by measured download speed
6. Try ranked nodes in order until one succeeds:
   a. Get HTTP URL for snapshot
   b. Verify snapshot exists and is accessible
   c. Return URL for streaming (or download to disk)
```

**Fallback Resilience**: If the #1 fastest node's HTTP endpoint is down or snapshot was deleted, the system automatically tries the next best nodes. Configure with `max_snapshot_url_attempts` (default: 3).

## Incremental Snapshot Selection Flow

Incremental snapshots must match the base slot of the full snapshot. The selection strategy differs from full snapshots:

```
1. Try same source as full snapshot first (fastest, most likely match)
   └─ Check if incremental base slot == full snapshot slot

2. If that fails, search cluster for matching incrementals:
   a. Evaluate all nodes (same two-stage algorithm)
   b. Filter to nodes with incremental base slot matching full snapshot
   c. Filter by minimum speed (min_incremental_speed_mbs, default: 2 MB/s)
   d. Sort by freshness (highest end slot first), then by speed
   e. Try ranked nodes until one succeeds
```

**Key Difference**: For incrementals, freshness (end slot) is prioritized over pure speed, since we want the most recent incremental that matches the base slot.

## HTTP Streaming vs Disk Download

### Streaming Mode (Default)

When `max_full_snapshots = 0` (default):
- Snapshot data streams directly from HTTP to processing pipeline
- No disk space required for snapshot files
- Cannot resume if interrupted
- Fastest overall time-to-ready

### Streaming + Save Mode

When `max_full_snapshots > 0` and `storage.snapshots` is set:
- Uses `io.TeeReader` to write to disk while streaming
- Processing happens in parallel with disk write
- Snapshot files are saved for potential reuse
- Old snapshots automatically deleted when limit exceeded
- Slightly higher memory usage

### Disk Download Mode

When using `DownloadSnapshot()` instead of `GetSnapshotURL()`:
- Downloads complete snapshot to disk first
- Then processes from disk
- Useful for debugging or when you need the file

## Retry and Cleanup Behavior

### Partial Download Cleanup

If a download fails mid-way or is cancelled (Ctrl+C):
- Partial download files are automatically deleted
- Full snapshot: partial file deleted on cancellation
- Incremental: partial incremental deleted, full snapshot preserved (already complete)

### Incremental Retry with Re-Discovery

If an incremental download fails mid-way (not due to Ctrl+C):
1. Partial file is deleted
2. Source discovery runs again (cluster state may have changed)
3. Fresh sources are ranked and tried
4. Retry up to 3 times before failing

This handles cases where a source stops serving mid-download - the re-discovery may find newer/fresher incrementals from different nodes.

## Configuration Reference

All configuration options can be set in `config.toml` under the `[snapshot]` section:

```toml
[snapshot]
    # Maximum snapshots to keep on disk (controls both saving and retention)
    #   0 = Stream-only mode (don't save snapshots, saves disk space)
    #   1 = Save one snapshot, delete previous before downloading new
    #   2+ = Keep N snapshots, delete oldest when limit exceeded
    #
    # When set to 0, snapshots are streamed directly from the network and
    # processed without saving to disk. This saves significant disk space
    # but means you'll need to re-download if the process is interrupted.
    # max_full_snapshots = 1

    # Snapshots are saved to the path in [storage].snapshots

    # Enable verbose output showing detailed node discovery statistics
    # verbose = false

    # -------------------------------------------------------------------------
    # Stage 1: Fast Parallel Triage
    # -------------------------------------------------------------------------

    # Warmup data before timing (KiB)
    # stage1_warm_kib = 512

    # Size of each measurement window (KiB)
    # stage1_window_kib = 512

    # Number of measurement windows (total data = windows * window_kib)
    # stage1_windows = 4

    # Timeout for stage 1 testing per node (milliseconds)
    # stage1_timeout_ms = 3000

    # Number of concurrent downloads in stage 1 (0 = auto, uses CPU cores)
    # stage1_concurrency = 0

    # -------------------------------------------------------------------------
    # Stage 2: Sustained Speed Test
    # -------------------------------------------------------------------------

    # Number of top candidates from stage 1 to test in stage 2
    # stage2_top_k = 8

    # Warmup duration before measurement (seconds)
    # stage2_warm_sec = 2

    # Measurement duration (seconds)
    # stage2_measure_sec = 2

    # Minimum speed ratio (collapse if speed drops below this * peak)
    # stage2_min_ratio = 0.6

    # Minimum absolute speed (MB/s, 0 = disabled)
    # stage2_min_abs_mbs = 0.0

    # -------------------------------------------------------------------------
    # Node Filtering
    # -------------------------------------------------------------------------

    # Maximum RTT to consider a node (milliseconds, 0 = disabled)
    # max_rtt_ms = 200

    # TCP connection timeout for pre-check (milliseconds)
    # tcp_timeout_ms = 1000

    # Minimum Solana version required (e.g., "3.0.0", empty = no filter)
    # min_node_version = "3.0.0"

    # Allowed Solana versions (empty = all versions allowed)
    # allowed_node_versions = []

    # -------------------------------------------------------------------------
    # Snapshot Age Thresholds
    # -------------------------------------------------------------------------

    # Maximum age for full snapshots (slots)
    # full_threshold = 100000

    # Maximum age for incremental snapshots (slots)
    # incremental_threshold = 1000

    # Safety margin - warn if snapshot is this close to expiration (slots)
    # safety_margin_slots = 5000

    # -------------------------------------------------------------------------
    # Performance
    # -------------------------------------------------------------------------

    # Number of concurrent workers for node evaluation
    # worker_count = 100

    # -------------------------------------------------------------------------
    # Fallback Resilience
    # -------------------------------------------------------------------------

    # Maximum number of ranked snapshot sources to try before giving up
    # This is a resilience mechanism - if the #1 fastest node's HTTP endpoint
    # is down or snapshot was deleted, we try the next best ranked nodes.
    # Set to 0 to try all available nodes.
    # max_snapshot_url_attempts = 3

    # -------------------------------------------------------------------------
    # Incremental Snapshot Selection
    # -------------------------------------------------------------------------

    # Minimum download speed for incremental snapshot sources (MB/s)
    # Incrementals are ~1GB, so this filters out nodes that would take too long.
    # At 2 MB/s: ~8 minutes for 1GB (acceptable)
    # Set to 0 to disable speed filtering.
    # min_incremental_speed_mbs = 2.0
```

## Default Values

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_full_snapshots` | `1` | Save one snapshot (0=stream, 1+=save and retain N) |
| `verbose` | `false` | Quiet logging |
| `stage1_warm_kib` | `512` | 512 KiB warmup |
| `stage1_window_kib` | `512` | 512 KiB windows |
| `stage1_windows` | `4` | 4 windows = 2 MiB total |
| `stage1_timeout_ms` | `3000` | 3 second timeout |
| `stage1_concurrency` | `0` | Auto (CPU cores) |
| `stage2_top_k` | `8` | Test top 8 candidates |
| `stage2_warm_sec` | `2` | 2 second warmup |
| `stage2_measure_sec` | `2` | 2 second measurement |
| `stage2_min_ratio` | `0.6` | 60% speed collapse threshold |
| `stage2_min_abs_mbs` | `0.0` | Disabled |
| `max_rtt_ms` | `200` | 200ms max RTT |
| `tcp_timeout_ms` | `1000` | 1 second TCP check |
| `min_node_version` | `"3.0.0"` | Minimum Agave 3.0.0 |
| `full_threshold` | `100000` | ~11 hours old |
| `incremental_threshold` | `1000` | ~7 minutes old |
| `safety_margin_slots` | `5000` | Warn if close to expiration |
| `worker_count` | `100` | 100 concurrent workers |
| `max_snapshot_url_attempts` | `3` | Try top 3 nodes |
| `min_incremental_speed_mbs` | `2.0` | Minimum 2 MB/s |

## Verbose Mode Output

Enable `verbose = true` to see detailed statistics:

```
=== Snapshot Node Discovery Statistics ===
Total nodes discovered: 1847
TCP connectivity passed: 1623
Nodes with any snapshot: 892
Nodes with incremental: 445 (usable: 312)
After version filter: 756
After RTT filter (<200ms): 234
After age filters (full<100000, incr<200 slots): full=198, incr=156
Final eligible nodes: 156
==========================================
```

## Error Handling

The integration handles several failure modes:

1. **No nodes available**: Returns error with count of nodes tried
2. **All nodes too slow**: Falls back to slower nodes rather than failing
3. **Source stops mid-download**:
   - Deletes partial file
   - Re-runs discovery (may find fresher snapshots)
   - Retries up to 3 times
4. **Context cancellation (Ctrl+C)**:
   - Deletes partial download files
   - Exits immediately without retry

## Usage Examples

### Basic verify-live with defaults

```bash
mithril verify-live --rpc https://api.mainnet-beta.solana.com
```

### With custom config file

```bash
mithril verify-live --config my-config.toml
```

### Save snapshots while streaming

```toml
[storage]
    snapshots = "/data/snapshots"

[snapshot]
    # Keep 2 snapshots on disk
    max_full_snapshots = 2
```

### High-speed network tuning

```toml
[snapshot]
    # Test more candidates in stage 2
    stage2_top_k = 16

    # Longer measurement for more accurate speeds
    stage2_measure_sec = 5

    # Stricter speed requirement
    stage2_min_abs_mbs = 50.0

    # More retry attempts for resilience
    max_snapshot_url_attempts = 5
```

### Low-latency network (nearby nodes only)

```toml
[snapshot]
    # Only consider very low latency nodes
    max_rtt_ms = 50

    # Faster triage with shorter timeout
    stage1_timeout_ms = 1500
```

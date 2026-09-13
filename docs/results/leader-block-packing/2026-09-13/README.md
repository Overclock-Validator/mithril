# Leader block packing: standalone PR and live experiment results

Goal: keep Mithril's own 200ms leader slots supplied and reduce work in the
packing window, while enforcing the actual cluster cost/byte limits. This PR
extracts the live-tested leader changes onto current `alpenglow-dev` at
`33dde4050d9250557583395810799aaac2f54017`. It can be reviewed independently of
FEC PR #259 and the streaming transaction-verification follow-up.

## Fresh comparison against current alpenglow-dev

Ryzen 7 9700X (8 physical cores/16 threads), Go 1.26.4, GOMAXPROCS=8, affinity
CPUs 0–7, nice 10. One serial benchmark caller builds a bank containing **48,622
unique 198-byte one-signature, zero-instruction transactions**. Both versions
use identical in-memory accounts, 50M block-cost/20M writable-account budgets,
10MiB entry-byte limits and the same fixture/benchmark source. The copied test
harness is the only addition to the baseline; production code is unchanged.
Three paired rounds alternate order (base/PR, PR/base, base/PR); each reported
sample times three full banks. Tables show medians of the three sample means.
The validator continued running, so normal shared-host variation remains.

| Bank input path | alpenglow-dev | PR | Allocated MB/block, base → PR |
|---|---:|---:|---:|
| Wire bytes | 209.64 ms | **150.30 ms** | 259.27 → 223.45 |
| Already decoded | 189.73 ms | **132.40 ms** | 237.88 → 202.06 |
| Already decoded and statically prepared | — | **101.92 ms** | — → 173.67 |

The like-for-like wire path takes **28.3% less bank time**; the decoded path
30.2% less. The prepared bank phase additionally moves static work before
leadership. It does not eliminate that work from total CPU use. Its three
samples were 121.71, 101.92 and 101.60 ms. No whole-validator 2× claim is made.

Timing includes bank admission, execution, account publication, entry building,
Merkle hashing and final entry flush. It excludes signing, signature verification,
bank setup, actual AccountsDB, network/shred broadcast, consensus and replay
adoption. Thus this is not a promise to fit the same block inside every live
packing window. The offline correctness test independently round-trips all
emitted entries through shred generation and decoding, verifies fees/hash/order,
and rejects the next transaction at the block-cost boundary.

`run-native.sh` records the commands and baseline harness copying. Source and
binary hashes, topology, race/vet/build logs and every paired measurement are
included. `python3 summarize.py` rebuilds summary.json. The source archive was
captured before documentation-only additions; its Go/module files match the
published implementation. The live validator's PID stayed unchanged.

## What enabled the live result

- Decode owned TPU bytes once. Prepare immutable static transaction data before
  leadership; recheck feature compatibility and bank-dependent state on use.
- Reuse borrowed-account scratch during serialized bank execution, and omit
  detailed replay timing from that leader hot path.
- Format missing-bank-account diagnostics lazily before falling back to parent
  accounts; calculate writable keys once and avoid base58 loader comparisons.
- Calculate signature Merkle roots without retaining unused proof nodes.
- Remove scheduler heap dispatch/index-store overhead, preserving priority/FIFO.
- Expose bounded queue capacity and local completion reserve as tuning settings,
  keeping the 131,072-entry and 75ms defaults.
- Correct slot-time-dependent resource budgets. In the tested 200ms regime,
  RaiseBlockLimitsTo100m means **50M**, not 100M per block. The skipped initial
  42,827-transaction compute-budget block exceeded the real limit and is not a
  successful result. After correction, 42,338 such transactions finalized at
  49,916,502 cost units. The lighter readonly-pair workload then approached 48.6k.

Slot-budget reference: [Agave release 2e10d67 slot parameters](https://github.com/anza-xyz/agave/blob/2e10d67f909c23a9586e1b3d099f7b8ef810ccfe/runtime/src/slot_params.rs),
including following-epoch activation and scaling of both account and block costs.

## Live results: separate from the standalone benchmark

The live binaries combined these changes with FEC/producer and streaming
verification work from trial `15ad1845`. No new live traffic or deployment was
performed while preparing this PR. These sequential windows are observations,
not controlled attribution of each optimization.

**Best single finalized block:** slot **3073657**, **48,523 transactions**, one
signature each, zero instructions/execution CU, zero failures, **49,881,644 /
50,000,000 block-cost units**. Its local completion deadline margin was 49ms.
RPC blockhash: `H6iU43SrXp94LRvfpvhLDskboUJA4CupsEVFJcCRLANF`.

The cost-only ceiling is floor(50,000,000 / 1,028) = 48,638. Upfront loaded-data
reservation reduces the offline admission limit to 48,622; runtime timing and
background work can reduce live inclusion further. The best live block is 99
transactions below that offline admission count. We did not reach 50,000.

The later fully preloaded four-block test offered 200,000 transactions at 150k
transactions/sec before our leader window, with queue capacity 262,144 and a
60ms completion reserve. The four finalized blocks were:

| Slot | Included transactions | Cost units | Completion margin |
|---|---:|---:|---:|
| 3077076 | 26,687 | 27,434,236 | 39ms |
| 3077077 | 42,150 | 43,330,200 | 45ms |
| 3077078 | 41,387 | 42,545,836 | 41ms |
| 3077079 | 42,607 | 43,799,996 | 41ms |

Total **152,831**, all successful; the other 47,169 expired without inclusion.
The preceding timely-refill run included 124,988. The 22.3% observed increase
coincides with different queue size, prefill timing, process age and live
conditions; it is not an isolated software speedup. **Four maximum-size blocks
were not achieved.** First-block packing time was shorter; later starts waited
for local replay/adoption. A reserve reduction does not add an equal amount of
packing time to every slot. Buffered log output was too delayed to be a useful
refill trigger, so the subsequent test used RPC slot observations and finally
full prefill.

Small extracted finalized-validation records are in best-block/ and
four-block-prefill/. evidence-manifest.json hashes the original source records.
Full prior evidence archives remain identified by SHA-256:

- Best-block experiment: `f785aeaad00d5cc85e868bf1488e4ebb7adb0346f97308f0fc6d8ae538c943a5`.
- Four-block prefill: `e7a3f03b5f88f4110c9dc58d306f14142bee4c5ba2f33b5b348d5ea05ded525f`.

`live-preload.go.txt` is the original bounded live sender source (no key material).
Its RPC/genesis/identity/path constants are historical experiment settings, not
portable defaults. The offline tests reproduce its message construction and
capacity checks without funds or network. See [reproduction commands](../../../leader_block_packing.md).

## Validation and remaining work

Local and native Zen 5 race suites passed for accounts, blockprod/scheduler,
costmodel, merkletree, replay, fees, turbine, txfixture, config and node. Vet and
full Mithril builds passed on both machines. The full repository suite is not
claimed to pass: prior baseline/candidate trials reproduced unrelated BPF-loader
failures. Tests cover owned packet lifetimes, stale feature preparation,
current payer-state changes, fees-only/instruction failures, duplicates/age,
arena reuse, queue ordering/eviction, Merkle parity and slot-budget activation.

The discarded status-map allocation experiment showed no clear timing gain and
is not included. Reusing message hashes through local replay and shortening
replay/adoption transitions remain possible follow-ups, not completed gains.

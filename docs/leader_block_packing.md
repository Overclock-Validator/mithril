# Leader block packing and synthetic load tests

This change reduces work performed during a leader's available packing window.
The scheduler owns and decodes packet bytes once, and prepares static message
validation, instructions/account metadata, compute limits, message hash and cost
while transactions are queued. Each bank checks the immutable feature snapshot
before reuse. Account state, age, duplicates, strict fee-payer eligibility, rent,
execution and all resource budgets remain bank-dependent checks.

Leader execution reuses borrowed-account scratch and skips detailed replay
stage timers. Missing-current-bank lookup errors defer base58 formatting until
used, avoiding wasted work before parent lookup. Entry Merkle hashing retains
only the root-building scratch, and max-heap removal avoids heap interface dispatch
while preserving priority/FIFO order. Both priority heaps now track entry
indexes so consumption, eviction and expiry remove every buffer reference.
Repeated rebuffering reuses the caller's intact transaction without accumulating
duplicate heap references. The existing slot-local skip scanning policy remains
unchanged, including selection of newly arrived higher-priority transactions.

## Capacity and protocol limits

Live banks use slot-dependent budgets with slot-time feature gates taking effect
in the epoch after activation. For the September 13 cluster's 200ms regime with
RaiseBlockLimitsTo100m, the budget was 50M block-cost units, 20M writable-account
cost units, 50MB allocated-data growth and 10MiB entry bytes. The entry packer
reserves 48 bytes for the ending tick. A 100M per-slot budget would be incorrect
in this regime. Active limits are logged when a leader bank opens.

The readonly-pair fixture has one signature, one writable fee payer, two existing
readonly accounts, no instructions, and 198 wire bytes. Its observed actual cost
is 1,028 units: 720 signature, 300 write lock and eight loaded-account units. Its
program execution cost is zero. The theoretical cost-only ceiling is 48,638,
but upfront admission must fit the larger estimated loaded-data reservation:
the offline bank test includes 48,622 before rejecting the next transaction.
This workload is designed for signature/packing load, not application execution.

## Reproduce locally

All commands below are offline. They use deterministic test keys and in-memory
accounts; no RPC, faucet, funding or transaction submission occurs.

```sh
# The 200,000-message, eight-payer, two-blockhash workload used to prefill four slots.
go test ./pkg/tpu/txfixture -run '^TestReadonlyPair200KDistinctMessages$' -count=1

# Fill a 50M-cost bank, reject the next tx, check fees, and round-trip all entries
# through actual shred generation and decoding, preserving transaction order/hash.
go test ./pkg/blockprod -run '^TestReadonlyPairBlockCapacityAndShredRoundTrip$' -count=1

# Whole-bank construction: one serial caller; pre-signed unique transactions.
GOMAXPROCS=8 go test ./pkg/blockprod -run '^$' \
  -bench '^BenchmarkReadonlyPair(FullBlock|PreparedFullBlock)$' -benchtime=3x -count=3

# More representative instruction workloads and smaller component microbenchmarks.
GOMAXPROCS=8 go test ./pkg/blockprod -run '^$' \
  -bench '^BenchmarkWorkingBank(Decoded|Prepared)?HotAccounts$' -benchtime=2s -count=3
```

The whole-bank benchmark admits 48,622 transactions and includes execution,
account publication, entry batching/hash work and final entry flush. Signing and
bank setup are excluded. The prepared variant additionally does static
preparation before timing, modeling a queue ready before leadership. That work
is moved, not eliminated. Actual AccountsDB, signature verification, network,
consensus and the protocol deadline are outside this benchmark. The correctness
test's shred round-trip is also outside the timed benchmark.

`txfixture.ReadonlyPairWire` provides the same ordered-pair construction as the
live experiment: 128×127 unique messages per payer/blockhash. Repeated ordinals
need a different payer or blockhash. The 200k test verifies uniqueness across
all messages and decodes/verifies representative signatures and phase boundaries.

## Queue supply and completion reserve

```toml
[validator]
tpu_max_buffered_transactions = 0 # default 131072
block_completion_reserve_ms = 0  # default 75ms
```

The corresponding flags are `--tpu-max-buffered-transactions` and
`--leader-completion-reserve-ms`. The measured full-prefill trial used 262,144
queue entries and a 60ms reserve. Those are opt-in tuning values; defaults stay
unchanged. A larger queue uses additional memory for owned wire, decoded and
prepared objects. `BenchmarkReadonlyPairPreparationMemory` measured 728 allocated
bytes per preparation (9 allocations) on Go 1.26.4 arm64 for the 198-byte fixture.
That is 91 MiB of allocation volume for 131,072 preparations, or 182 MiB for
262,144, in addition to wire/decoded transactions and queue indexes. Allocation
volume includes temporary preparation storage: it is not retained heap or RSS.
More accounts/instructions increase the footprint; these are not worst-case caps.
Reproduce with `go test ./pkg/blockprod -run '^$' -bench '^BenchmarkReadonlyPairPreparationMemory$' -benchmem`.
A shorter reserve needs measured local finalization/broadcast
margin and does not change the protocol deadline. Shifting completion also
shifts later bank start times, so it does not add the same packing time to all
four blocks.

The live results showed why total supply and timing matter: 120k transactions
cannot fill four approximately 48.6k blocks. An 80k refill competed with ongoing
work. Preloading 200k into a larger queue improved the observed four-block total,
while the first block still had less usable time and later banks awaited local
replay/adoption. These observations do not isolate a single CPU bottleneck.

[Measured results, exact baselines and evidence](https://github.com/Overclock-Validator/mithril/blob/06ef067798c99947e8cc527450ad28430a9a7333/docs/results/leader-block-packing/2026-09-13/README.md)
include both the successful near-limit block and the still-underfilled four-slot
window. The archived live helper is historical experiment source with explicit
cluster/identity/path constants; the offline fixture is the portable reproduction.

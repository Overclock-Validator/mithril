# High-risk changes

Some parts of Mithril can diverge from the cluster in ways that ordinary testing will not catch: they run rarely,
they fail late, or they fail silently in fields the bank hash does not cover. Before changing code in one of these
areas, read its entry, say in the PR which entry applies, and run the verification it lists. When in doubt, treat a
change as high risk.

Each entry lists why the area is risky, what to check before changing it, and how to verify the change.

## 1. Epoch boundary logic

Packages: `pkg/replay` (boundary processing), `pkg/rewards`, `pkg/epochrewards`, `pkg/epochstakes`,
`pkg/rewardcerts`, `pkg/leaderschedule`, `pkg/features` (activation).

- **Why:** runs once per epoch (about every two days on mainnet), so normal replay and short corpora never exercise
  it. Errors in rewards, stake activation, stake history or epoch stakes can stay hidden until the boundary, and
  then affect every stake account and the next leader schedule.
- **Check:** points and inflation computation, commission, vote credits, partitioned reward distribution, stake
  activation and deactivation (warmup and cooldown, stake history), epoch stakes and leader schedule inputs, and
  feature activations that take effect at the boundary. Note which slot and epoch every value comes from.
- **Verify:** replay a range that crosses an epoch boundary and compare bank hashes. Compare reward and stake totals
  against Agave or Firedancer for that epoch. Run with the relevant features both on and off.

## 2. Feature gates

Packages: `pkg/features`, and every call site of `Features.IsActive`.

- **Why:** behaviour must switch exactly at the activation slot, and both paths must keep working for replay of
  older slots. A check on the wrong slot or bank (parent vs current) diverges only at activation.
- **Check:** which bank's feature set the call site reads; that the old path still exists; the activation slot
  semantics in Agave.
- **Verify:** tests with the feature on and off; replay across the activation slot if a corpus exists.

## 3. Fees, fee payers and rent states

Packages: `pkg/fees`, `pkg/rent`, `pkg/replay` (transaction loading and commit).

- **Why:** every transaction pays a fee, and failure paths still charge it. Fee formulas, payer validation order,
  rent-state transitions and rent-epoch normalization change through SIMDs. A mistake affects many accounts at once.
  Speculative replay also assumes a failed transaction's fee follows from its message alone (see 9).
- **Check:** error precedence (payer validation before account loading), fee-only failure semantics, durable-nonce
  rollback, rent-exempt minimum balances, rent-epoch handling.
- **Verify:** fee and failure-path tests; a corpus replay with bank hashes and exact fee totals; check raw stored
  fields such as rent epoch, not only the bank hash.

## 4. Account storage, versions and the accounts hash

Packages: `pkg/accounts`, `pkg/accountsdb`, `pkg/lthash`, `pkg/bankhash`, `pkg/replay` (publication and commit).

- **Why:** the accounts lattice hash covers lamports, data, owner, executable and key, so errors in which accounts
  are stored, deletion (zero lamports), or account bytes change the bank hash. Errors in fields the hash does not
  cover, such as rent epoch, do not, and can stay silent. Copy-on-write account objects and their versions are also
  what speculative replay validates against.
- **Check:** which accounts a transaction publishes; deletion and creation; that published account objects are never
  mutated in place or recycled while referenced; ownership of any reused buffer.
- **Verify:** corpus replay with bank hashes; raw stored-field comparison; durable reload and snapshot restore of the
  same state.

## 5. Virtual machine, loaders and the program ABI

Packages: `pkg/sbpf`, `pkg/sealevel` (loaders, serialization, CPI, syscalls, compute metering).

- **Why:** compute units, errors and account writes all reach consensus. Serialization layout, direct mapping,
  CPI account rules and syscall costs must match the reference exactly.
- **Check:** CU charges, error kinds and their order, account privilege checks in CPI, realloc limits.
- **Verify:** conformance fixtures, interpreter differential tests, corpus replay with exact CU per block.

## 6. Sysvars and block context

Packages: `pkg/replay`, `pkg/sealevel` (sysvars).

- **Why:** clock, slot hashes, recent blockhashes, the instructions sysvar and epoch-related sysvars are read by
  programs and by transaction-age checks. A value from the wrong slot breaks every later block.
- **Check:** when each sysvar is updated relative to the transaction loop, and from which parent.
- **Verify:** corpus replay including the first blocks after a restart and after an epoch boundary.

## 7. Transaction age, durable nonces and duplicate detection

Packages: `pkg/replay` (status cache), `pkg/txstatus`, `pkg/sealevel` (nonce).

- **Why:** deciding whether a transaction is processable is consensus. Errors are rare and depend on fork history.
- **Verify:** tests across forks and restarts; corpus replay.

## 8. Alpenglow consensus and finality

Packages: `pkg/alpenglow`, `pkg/consensus`, vote rewards in `pkg/replay`.

- **Why:** votes, certificates, the footer bank hash and finalization govern safety and liveness, not only state.
  Vote rewards also change state.
- **Verify:** consensus tests; a live run near the tip; footer checks on corpus replay.

## 9. Parallel replay and speculative execution

Packages: `pkg/replay` (dependency planner, plan runner, work-ahead).

- **Why:** a missing dependency edge gives nondeterministic divergence that may not reproduce. Speculative execution
  relies on invariants that changes elsewhere can break without touching replay code. These are documented at the top
  of the speculation code: storage versions, read sets, failed-transaction effects, cache isolation, determinism,
  commit-path parity.
- **Check:** whether your change, anywhere, affects one of those invariants (fees, loaders, storage, commit).
- **Verify:** verify mode (re-execute and compare every adopted early result); corpus replay with bank hashes; race
  tests for the scheduler.

## 10. Snapshots, state files and restarts

Packages: `pkg/snapshot`, `pkg/snapshotdl`, `pkg/state`, startup in `pkg/replay`.

- **Why:** wrong restored state diverges immediately or, worse, only when an affected account is next used. A crash
  between a rewind and a state save can leave the state file ahead of the store.
- **Verify:** restore from a snapshot and replay; restart mid-run; incremental snapshot paths.

## Lower risk (still verify)

Logging, metrics, RPC formatting, tooling and tests that do not change replay behaviour. Performance changes that
keep behaviour identical still need a corpus replay with bank hashes, because "identical" is the claim being tested.

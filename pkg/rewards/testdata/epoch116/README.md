# Epoch 115 → 116 inactive-stake regression

`inactive-stakes.json` is a reduced fixture from the Alpenglow failure at slot
6,264,001 on 2026-09-21, running Mithril `320ce8da`. It contains the 33 fully
cooled stake accounts that Mithril incorrectly rewrote, their vote accounts'
epoch-115 credits, and the saved StakeHistory sysvar. All 33 delegated stakes
had deactivation epoch 114 and zero effective/activating stake in epoch 115.

The source was the supplied `/mnt/mithril-accounts` checkpoint at slot 6,264,000
and `footer-bankhash-mismatch-slot-6264001.json` in the supplied logs. AccountsDB
was opened read-only. Account bytes are public chain data; no keys or validator
identity files are included.

To isolate the defect, `base_accounts_lt_hash` includes all correct slot effects
and the unchanged 33 accounts. The other 792 modified accounts were reconstructed
from the parent checkpoint and deterministic slot updates; all 825 reconstructed
accounts matched the diagnostic's individual SHA-256 data hashes. Combining
their deltas with the saved parent LtHash reproduced both the diagnostic LtHash
checksum and the original bad bank hash. Undoing only the 33 credit-only writes
then reproduced the exact expected footer hash:

| State | Bank hash |
| --- | --- |
| Original 33 erroneous writes | `CCY2QcFoGGAodDaB9RCAW2RUbbZm2xMo9dJw8tKHdDcG` |
| Preserve the 33 inactive accounts | `BBXWdsTHc3EdN8zXbcZZ8vwqtYjzggD3gp8CqkZuBvBm` |

`TestEpoch116InactiveStakeBankHash` checks each reconstructed erroneous account
against its recorded data hash, checks the bad bank hash, and then runs the
production streaming calculator, spool distributor and bank hasher. Before the
fix, that path emitted 33 zero-lamport writes and produced the bad hash. After
the fix it emits no writes for these stakes and produces the expected hash.
Other slot effects are held constant; this is not a full signed-shred replay or
a replay of later slots.

Reference behavior:

- [Agave ab655329, inflation_rewards/mod.rs:254–270](https://github.com/anza-xyz/agave/blob/ab6553293094e59dee7d3e7c928c7fa1023d0684/runtime/src/inflation_rewards/mod.rs#L254-L270)
  restricts skipped-reward credit advancement to effective or activating stake,
  preserving the explicit forced-update cases.
- [Firedancer 57d39904, fd_rewards.c:649–678](https://github.com/firedancer-io/firedancer/blob/57d39904e3886731b96b9174ff8763ee7c36e3ad/src/flamenco/rewards/fd_rewards.c#L649-L678)
  rejects an inactive credit-only update. Its
  [points calculation at lines 908–917](https://github.com/firedancer-io/firedancer/blob/57d39904e3886731b96b9174ff8763ee7c36e3ad/src/flamenco/rewards/fd_rewards.c#L908-L917)
  retains the inactive flag from the existing activation-status calculation.

Run with:

```sh
go test ./pkg/rewards -run TestEpoch116InactiveStakeBankHash -count=1 -v
```

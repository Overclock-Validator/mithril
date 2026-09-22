# Epoch 115 → 116 reward divergence

The supplied run (`320ce8da`, started 2026-09-18) passed the epoch-boundary bank
at slot **6,264,000**, then stopped at **6,264,001** with a footer bank-hash
mismatch. Both blocks had zero transactions. The mismatch was a deterministic
reward-account state divergence, not a transaction-execution panic.

At the boundary, rewards for epoch 115 were calculated and vote commissions
were paid. The following bank distributed one staking-reward partition with
724 stake records. Of those records, 33 paid zero lamports but advanced
`credits_observed` on stakes that had deactivated in epoch 114 and were fully
inactive in epoch 115. Agave leaves those accounts unchanged. The missing
effective-or-activating condition in `shouldForceCreditsOnly` caused the extra
writes and changed the LtHash.

Reconstructing all 825 modified accounts from the saved parent accounts and
the diagnostic's per-account hashes reproduced the original computed bank
hash. Removing only those 33 writes reproduced the expected footer hash
exactly. The [reduced fixture and regression](../pkg/rewards/testdata/epoch116/README.md)
preserve this evidence and link the Agave/Firedancer rules. The code retains
activation status from the existing calculation, so no extra stake scan is
needed. Credit rewinds, disabled inflation, activation-epoch updates and
fractional rewards on effective stakes retain their prior behavior.

## Checkpoint failure and recovery

The distribution decremented its remaining-partition counter to zero before
the failing bank's footer was verified. The promotion guard used only that
counter, so graceful shutdown released the epoch hold and persisted slot
6,264,000 with an active EpochRewards sysvar. The spool had already been
consumed. That checkpoint cannot resume distribution directly.

Promotion now waits for the successfully verified bank's immutable inactive
EpochRewards state. It also waits for that bank to pass the normal finality
and verification gates. The boundary through completion is committed in one
fold, including when the configured batch size is smaller than the rewards
window. A failed bank, a finality cutoff inside the window, or a failed fold
keeps the prior checkpoint. Memory remains bounded by the existing tail cap;
the completion fold can be larger than the usual batch, once per epoch.

The supplied data includes a retained fold and transaction-status checkpoint
at **6,263,999**, the last slot of epoch 115. Read-only validation confirmed its
57,677,519-byte transaction-status checkpoint, complete retained-root coverage,
selected block identity, inactive EpochRewards state, and all 122 account undo
targets for reverting the boundary bank. With the fixed binary, the existing
`run --rewind-to-slot 6263999` option can restore that boundary and recalculate
rewards, provided historical blocks/shreds remain available. Keep the normal
node configuration and signing-history recovery checks. A direct restart from
6,264,000 cannot recover the missing distribution bookkeeping.

The supplied local shred spool does not contain slots 6,264,000–6,264,001. On
2026-09-22 the configured primary RPC also reported 6,264,001 as pruned (first
available block 6,764,691). Replaying from the retained boundary therefore needs
another historical source; otherwise use the fixed binary with a fresh
snapshot. Original accounts, ledger, logs and signing history were preserved;
the fix was not deployed to a live validator during diagnosis.

## Validation

The branch was fetched and fast-forwarded from `320ce8da` to `be29319f` before
implementation. The reduced production-path regression failed on `be29319f`
with the incident's exact bad hash and passed after the fix. Additional tests
cover inactive, activating, effective and cooling stakes, forced credit-update
exceptions, failed distribution, finality lag, distribution generations,
atomic completion folds and failed-commit retry.

Passed locally with Go 1.27.1 on linux/amd64:

```sh
go test -race ./pkg/rewards ./pkg/replay ./pkg/accountsdb ./pkg/bankhash -count=1
GOMAXPROCS=2 go test -race -p 2 -count=1 \
  ./pkg/alpenglow ./pkg/consensus ./pkg/replay ./pkg/rewards \
  ./pkg/turbine ./pkg/sigverify ./pkg/blockprod/... \
  ./cmd/mithril/node ./cmd/mithril/configcmd
go vet ./pkg/rewards ./pkg/replay ./pkg/accountsdb ./pkg/bankhash
go build ./cmd/mithril
git diff --check
```

The CI regression job now includes `pkg/rewards`, so the incident fixture runs
on pull requests. These checks do not establish live post-restart catch-up or
later-slot parity; the original node was not restarted.

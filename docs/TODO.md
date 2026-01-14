# TODO / Known Issues

Identified on branch `perf/reward-distribution-optimizations` at commit `3b2ad67`
dev HEAD at time of identification: `a25b2e3`
Date: 2026-01-13

---

## Failing Tests

### 1. Address Lookup Table Tests - `InstrErrUnsupportedProgramId`

**File:** `pkg/sealevel/address_lookup_table_test.go`
**Test:** `TestExecute_AddrLookupTable_Program_Test_Create_Lookup_Table_Idempotent` (and likely all other ALT tests)

**Root Cause:** `AddressLookupTableAddr` and `StakeProgramAddr` were accidentally removed from `resolveNativeProgramById` switch in `pkg/sealevel/native_programs_common.go`.

| Program | Removed In | Commit Date | Commit Message |
|---------|------------|-------------|----------------|
| `AddressLookupTableAddr` | `d47c16b` | May 16, 2025 | "many optimisations and changes" |
| `StakeProgramAddr` | `e890f9e` | Jul 26, 2025 | "snapshot download, stake program migration, refactoring" |

**Fix:** Add these cases back to the switch in `resolveNativeProgramById`:
```go
case a.StakeProgramAddr:
    return StakeProgramExecute, a.StakeProgramAddrStr, nil
case a.AddressLookupTableAddr:
    return AddressLookupTableExecute, a.AddressLookupTableProgramAddrStr, nil
```

---

### 2. Bank Hash Test - Nil Pointer Dereference

**File:** `pkg/replay/hash_test.go`
**Test:** `Test_Compute_Bank_Hash`

**Error:**
```
panic: runtime error: invalid memory address or nil pointer dereference
pkg/replay/hash.go:227 - shouldIncludeEah(0x0, 0x0)
```

**Root Cause:** Test passes `nil` for the first argument to `shouldIncludeEah`, which dereferences it without a nil check.

**Fix:** Either add nil check in `shouldIncludeEah` or fix the test to pass valid arguments.

---

## Agave/Firedancer Parity Issues

### 3. Missing "Burned Rewards" Semantics in Reward Distribution

**File:** `pkg/rewards/rewards.go` (lines 180-230)

**Problem:** Mithril does not implement "burn" semantics for per-account failures during partitioned reward distribution. This diverges from both Agave and Firedancer.

**Current Mithril behavior:**
- `GetAccount` error → panic (aborts replay)
- `UnmarshalStakeState` error → silent skip (reward lost, not counted)
- `MarshalStakeStakeInto` error → panic (aborts replay)
- Lamport overflow → panic (aborts replay)

**Agave behavior** (`distribution.rs:260`):
- `build_updated_stake_reward` returns `DistributionError::UnableToSetState` or `AccountNotFound`
- Caller logs error and adds to `lamports_burned`
- Continues processing remaining accounts

**Firedancer behavior** (`fd_rewards.c:958`):
- `distribute_epoch_reward_to_stake_acc` returns non-zero on decode/non-stake/etc.
- Caller increments `lamports_burned` and continues

**Failure scenarios that should burn (not panic):**
- Account missing / not found
- Stake state decode fails (including short/invalid data)
- Account isn't a stake account
- Lamport add overflows
- `set_state`/encode fails (e.g., data too small)

**Fix required:**
1. Add `lamports_burned` tracking to reward distribution
2. Change panics to log + burn + continue
3. `epochRewards.Distribute()` should receive `distributedLamports` (successful) separately from burned amount
4. Ensure `SysvarEpochRewards.DistributedRewards` advances correctly (may need to include burned in total)

**Note:** The current silent skip on `UnmarshalStakeState` error reduces `distributedLamports` but doesn't track it as burned, which may cause `SysvarEpochRewards` to diverge from Agave/FD.

---

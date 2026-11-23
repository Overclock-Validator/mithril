# SIMD-0460 parity cases from Agave v4.4.1

This maps Agave's VASA and account-data mapping tests to Mithril tests on
`smcio/direct-mapping-write-handler`. Agave sources are pinned to
[v4.4.1](https://github.com/anza-xyz/agave/tree/v4.4.1). The activation-relevant
combination is VASA enabled with account-data direct mapping disabled. Tests
also run the legacy and direct-mapping combinations to catch regressions.

| Agave source and case | Mithril coverage |
| --- | --- |
| [`serialization.rs`: account count](https://github.com/anza-xyz/agave/blob/v4.4.1/program-runtime/src/serialization.rs) | `TestAgave0460SerializeAccountCountLimit`: 255 allowed, 256 and 255 plus duplicate rejected, VASA and pointer flag combinations |
| `serialization.rs`: ABI v1/v0 round trip and rent epoch mask | `TestAgave0460SerializeRoundTripMixedAccounts`, `TestSerializeParametersAlignedZeroPadding`: mixed lengths, two duplicates, account fields, padding, pointer trailer, masked rent epoch, no-write deserialization |
| `serialization.rs`: access violation handler | `TestAgaveV441DirectMappedAccessViolationHandler`, `TestAgaveV441CopiedAccountAccessAndGrowth`, `TestAgaveV441FailedWriteClampsRegionBeforeFault`: permission checks, copy-on-write, growth on store, account and transaction caps |
| `serialization.rs`: initial account region excludes resize allowance | `TestAgaveVasaInitialAccountRegionExcludesResizeAffordance`, `TestAgaveVasaAccountRegionCannotSpanInstructionTail` |
| [`cpi.rs`: serialized data and AccountInfo](https://github.com/anza-xyz/agave/blob/v4.4.1/program-runtime/src/cpi.rs) | `TestAgaveCPICallerAccountFromAccountInfo`, `TestAgaveCPIGetSerializedDataRejectsPastReserved`, `TestAgaveCPIRejectsAccountInfoLengthPointerInInput` |
| `cpi.rs`: caller/callee lamports, owner, writable and readonly data updates | `TestAgaveCPIUpdateCallerLamportsOwner`, `TestAgaveCPIUpdateCallerDataAndReallocLimit`, `TestAgaveCPIUpdateCalleeLamportsOwner`, `TestAgaveCPIUpdateCalleeDataWritable`, `TestAgaveCPIUpdateCalleeDataExternalOwner` |
| [`programs.rs`: account writes and reads beyond current length](https://github.com/anza-xyz/agave/blob/v4.4.1/programs/sbf/tests/programs.rs) | `TestAgaveSBFAccountDataWritePermissions`, `TestAgaveSBFReadBeyondCurrentLength`, `TestAgaveSBFAccountAccessViolationDepletesComputeMeter` |
| `programs.rs`: realloc and CPI updates | `TestAgaveSBFReallocWithoutCPI` (including shrink and regrow zeroing), `TestAgaveCPICalleeResizeExternalToCaller`, `TestAgaveCPICallerGrowthThenCalleeShrink`, `TestAgaveCPINestedAccountUpdates` |
| `programs.rs`: ownership change and caller access | `TestAgaveCPIOwnershipChangeWritePermissions`, `TestAgaveCPIRegainedOwnershipAllowsGrowth` |
| `programs.rs`: invalid AccountInfo pointers and RC boxes | `TestAgaveCPIRejectsWrongAccountInfoPointers` (four pointer fields, C and Rust), `TestAgaveCPIRejectsAccountInfoLengthPointerInInput`, `TestAgaveCPIRejectsRCBoxInExternalAccountData` (lamports and data RC) |
| `programs.rs`: stale cloned external data | `TestAgaveCPIStaleClonedExternalData` (callee-only change, caller-before-CPI change, same change on both sides) |
| `programs.rs`: 16 memory syscalls at account edges | `TestAgaveSBFMemorySyscallsAtAccountBoundaries`: 16 operations, empty and 1,024-byte accounts, ABI v0 and v1, three mapping modes; includes deprecated loader's zero-length exception |
| [`mem_ops.rs`: nonoverlapping ranges](https://github.com/anza-xyz/agave/blob/v4.4.1/program-runtime/src/mem_ops.rs) | `TestAgaveMemOpsIsNonoverlapping` |
| [SBPF v0.24.0 stack mapping](https://github.com/anza-xyz/sbpf/blob/v0.24.0/src/memory_region.rs) | `TestAgaveVasaLegacyStackCompactionAtVmAccess` |
| [`instruction_accounts.rs`: resize error order](https://github.com/anza-xyz/agave/blob/v4.4.1/transaction-context/src/instruction_accounts.rs) | `TestAgaveAccountResizeErrorPrecedence` and the CPI external-owner matrix |

The Go cases exercise Mithril's serializer, VM, account runtime, and CPI
translation. They do not run Agave's bank-backed SBF ELF fixtures. In
particular, the Agave SDK's immediate `AccountInfo::realloc` rejection of an
invalid intermediate size followed by an undo, its 1,024-instruction growth
loop, and system-program reassignment transactions are not literal ports.
The validator-observable limits, ownership rules, resize accounting, and
memory accesses used by those cases are tested at their Mithril runtime
boundaries.

Run the affected tests with:

```sh
go test ./pkg/sealevel ./pkg/sbpf ./pkg/features
```

package features

var StopTruncatingStringsInSyscalls = newGate("StopTruncatingStringsInSyscalls", "16FMCmgLzCNNz6eTwGanbyN2ZxvTBSLuQ6DZhgeMshg")
var EnablePartitionedEpochReward = newGate("EnablePartitionedEpochReward", "9bn2vTJUsUcnpiZWbu2woSKtTGW3ErZC9ERv88SDqQjK")
var EnablePartitionedEpochRewardsSuperfeature = newGate("EnablePartitionedEpochRewardsSuperfeature", "PERzQrt5gBD1XEe2c9XdFWqwgHY3mr7cYWbm5V772V8")
var LastRestartSlotSysvar = newGate("LastRestartSlotSysvar", "HooKD5NC9QNxk25QuzCssB8ecrEzGt6eXEPBUxWp1LaR")
var Libsecp256k1FailOnBadCount = newGate("Libsecp256k1FailOnBadCount", "8aXvSuopd1PUj7UhehfXJRg6619RHp8ZvwTyyJHdUYsj")
var Libsecp256k1FailOnBadCount2 = newGate("Libsecp256k1FailOnBadCount2", "54KAoNiUERNoWWUhTWWwXgym94gzoXFVnHyQwPA18V9A")
var EnableBpfLoaderSetAuthorityCheckedIx = newGate("EnableBpfLoaderSetAuthorityCheckedIx", "5x3825XS7M2A3Ekbn5VGGkvFoAg5qrRWkTrY4bARP1GL")
var LoosenCpiSizeRestriction = newGate("LoosenCpiSizeRestriction", "GDH5TVdbTPUpRnXaRyQqiKUa7uZAbZ28Q2N9bhbKoMLm")
var IncreaseTxAccountLockLimit = newGate("IncreaseTxAccountLockLimit", "9LZdXeKGeBV6hRLdxS1rHbHoEUsKqesCC2ZAPTPKJAbK")
var VoteStateAddVoteLatency = newGate("VoteStateAddVoteLatency", "7axKe5BTYBDD87ftzWbk5DfzWMGyRvqmWTduuo22Yaqy")
var AllowCommissionDecreaseAtAnyTime = newGate("AllowCommissionDecreaseAtAnyTime", "decoMktMcnmiq6t3u7g5BfgcQu91nKZr6RvMYf9z1Jb")
var CommissionUpdatesOnlyAllowedInFirstHalfOfEpoch = newGate("CommissionUpdatesOnlyAllowedInFirstHalfOfEpoch", "noRuG2kzACwgaY7TVmLRnUNPLKNVQE1fb7X55YWBehp")
var TimelyVoteCredits = newGate("TimelyVoteCredits", "tvcF6b1TRz353zKuhBjinZkKzjmihXmBAHJdjNYw1sQ")
var ReduceStakeWarmupCooldown = newGate("ReduceStakeWarmupCooldown", "GwtDQBghCTBgmX2cpEGNPxTEBUTQRaDMGTr5qychdGMj")
var StakeRaiseMinimumDelegationTo1Sol = newGate("StakeRaiseMinimumDelegationTo1Sol", "9onWzzvCzNC2jfhxxeqRgs5q7nFAAKpCUvkj6T6GJK9i")
var StakeRedelegateInstruction = newGate("StakeRedelegateInstruction", "2KKG3C6RBnxQo9jVVrbzsoSh41TDXLK7gBc9gduyxSzW")
var RequireRentExemptSplitDestination = newGate("RequireRentExemptSplitDestination", "D2aip4BBr8NPWtU9vLrwrBvbuaQ8w1zV38zFLxx4pfBV")
var DeprecateExecutableMetaUpdateInBpfLoader = newGate("DeprecateExecutableMetaUpdateInBpfLoader", "k6uR1J9VtKJnTukBV2Eo15BEy434MBg8bT6hHQgmU8v")
var RelaxAuthoritySignerCheckForLookupTableCreation = newGate("RelaxAuthoritySignerCheckForLookupTableCreation", "FKAcEvNgSY79RpqsPNUV5gDyumopH4cEHqUxyfm8b8Ap")
var DedupeConfigProgramSigners = newGate("DedupeConfigProgramSigners", "8kEuAshXLsgkUEdcFVLqrjCGGHVWFW99ZZpxvAzzMtBp")
var Ed25519PrecompileVerifyStrict = newGate("Ed25519PrecompileVerifyStrict", "ed9tNscbWLYBooxWA7FE2B5KHWs8A6sxfY8EzezEcoo")
var AbortOnInvalidCurve = newGate("AbortOnInvalidCurve", "FuS3FPfJDKSNot99ECLXtp3rueq36hMNStJkPJwWodLh")
var Blake3SyscallEnabled = newGate("Blake3SyscallEnabled", "HTW2pSyErTj4BV6KBM9NZ9VBUJVxt7sacNWcf76wtzb3")
var RemainingComputeUnitsSyscallEnabled = newGate("RemainingComputeUnitsSyscallEnabled", "5TuppMutoyzhUSfuYdhgzD47F92GL1g89KpCZQKqedxP")
var Curve25519SyscallEnabled = newGate("Curve25519SyscallEnabled", "7rcw5UtqgDTBBv2EcynNfYckgdAaH1MAsCjKgXMkN7Ri")
var SimplifyAltBn128SyscallErrorCodes = newGate("SimplityAltBn128SyscallErrorCodes", "JDn5q3GBeqzvUa7z67BbmVHVdE3EbUAjvFep3weR3jxX")
var EnableAltbn128CompressionSyscall = newGate("EnableAltbn128CompressionSyscall", "EJJewYSddEEtSZHiqugnvhQHiWyZKjkFDQASd7oKSagn")
var EnableAltBn128Syscall = newGate("EnableAltBn128Syscall", "A16q37opZdQMCbe5qJ6xpBB9usykfv8jZaMkxvZQi4GJ")
var DisableFeesSysvar = newGate("DisableFeesSysvar", "JAN1trEUEtZjgXYzNBYHU9DYd7GnThhXfFP7SzPXkPsG")
var DisableRentFeesCollection = newGate("DisableRentFeesCollection", "CJzY83ggJHqPGDq8VisV3U91jDJLuEaALZooBrXtnnLU")
var DeprecateUnusedLegacyVotePlumbing = newGate("DeprecateUnusedLegacyVotePlumbing", "6Uf8S75PVh91MYgPQSHnjRAPQq6an5BDv9vomrCwDqLe")
var RewardFullPriorityFee = newGate("RewardFullPriorityFee", "3opE3EzAKnUftUDURkzMgwpNgimBAypW1mNDYH4x4Zg7")
var StakeMinimumDelegationForRewards = newGate("StakeMinimumDelegationForRewards", "G6ANXD6ptCSyNd9znZm7j4dEczAJCfx7Cy43oBx3rKHJ")
var MoveStakeAndMoveLamportsIxs = newGate("MoveStakeAndMoveLamportsIxs", "7bTK6Jis8Xpfrs8ZoUfiMDPazTcdPcTWheZFJTA5Z6X4")
var GetSysvarSyscallEnabled = newGate("GetSysvarSyscallEnabled", "CLCoTADvV64PSrnR6QXty6Fwrt9Xc6EdxSJE4wLRePjq")
var AddNewReservedAccountKeys = newGate("AddNewReservedAccountKeys", "8U4skmMVnF6k2kMvrWbQuRUT3qQSiTYpSjqmhmgfthZu")
var EnableSecp256r1Precompile = newGate("EnableSecp256r1Precompile", "srremy31J5Y25FrAApwVb9kZcfXbusYMMsvTK9aWv5q")
var FixAltBn128MultiplicationInputLength = newGate("FixAltBn128MultiplicationInputLength", "bn2puAyxUx6JUabAxYdKdJ5QHbNNmKw8dCGuGCyRrFN")
var EnableTowerSyncIx = newGate("EnableTowerSyncIx", "tSynMCspg4xFiCj1v3TDb4c7crMR5tSBhLz4sF7rrNA")
var SkipRentRewrites = newGate("SkipRentRewrites", "CGB2jM8pwZkeeiXQ66kBMyBR6Np61mggL7XUsmLjVcrw")
var FullInflationVote = newGate("FullInflationVote", "BzBBveUDymEYoYzcMWNQCx3cd4jQs7puaVFHLtsbB6fm")
var FullInflationEnable = newGate("FullInflationEnable", "7XRJcS5Ud5vxGB54JbK9N2vBZVwnwdBNeJW1ibRgD9gx")
var FullInflationDevnetAndTestnet = newGate("FullInflationDevnetAndTestnet", "DT4n6ABDqs6w4bnfwrXT9rsprcPf6cdDga1egctaPkLC")
var PicoInflation = newGate("PicoInflation", "4RWNif6C2WCNiKVW7otP4G7dkmkHGyKQWRpuZ1pxKU5m")
var DisableAccountLoaderSpecialCase = newGate("DisableAccountLoaderSpecialCase", "EQUMpNFr7Nacb1sva56xn1aLfBxppEoSBH8RRVdkcD1x")
var EnableGetEpochStakeSyscall = newGate("EnableGetEpochStakeSyscall", "FKe75t4LXxGaQnVHdUKM6DSFifVVraGZ8LyNo7oPwy1Z")
var ReserveMinimalCUsForBuiltinInstructions = newGate("ReserveMinimalCUsForBuiltinInstructions", "C9oAhLxDBm3ssWtJx1yBGzPY55r2rArHmN1pbQn6HogH")
var RelaxIntraBatchAccountLocks = newGate("RelaxIntraBatchAccountLocks", "4WeHX6QoXCCwqbSFgi6dxnB6QsPo6YApaNTH7P4MLQ99")
var MaskOutRentEpochInVmSerialization = newGate("MaskOutRentEpochInVmSerialization", "RENtePQcDLrAbxAsP3k8dwVcnNYQ466hi2uKvALjnXx")
var RemoveAccountsExecutableFlagChecks = newGate("RemoveAccountsExecutableFlagchecks", "FXs1zh47QbNnhXcnB6YiAQoJ4sGB91tKF3UFHLcKT7PM")
var AccountsLtHash = newGate("AccountsLtHash", "LTHasHQX6661DaDD4S6A2TFi6QBuiwXKv66fB1obfHq")
var RemoveAccountsDeltaHash = newGate("RemoveAccountsDeltaHash", "LTdLt9Ycbyoipz5fLysCi1NnDnASsZfmJLJXts5ZxZz")
var EnableLoaderV4 = newGate("EnableLoaderV4", "2aQJYqER2aKyb3cZw22v4SL2xMX7vwXBRWfvS4pTrtED")
var EnableSbpfV1DeploymentAndExecution = newGate("EnableSbpfV1DeploymentAndExecution", "JE86WkYvTrzW8HgNmrHY7dFYpCmSptUpKupbo2AdQ9cG")
var EnableSbpfV2DeploymentAndExecution = newGate("EnableSbpfV2DeploymentAndExecution", "F6UVKh1ujTEFK3en2SyAL3cdVnqko1FVEXWhmdLRu6WP")
var EnableSbpfV3DeploymentAndExecution = newGate("EnableSbpfV3DeploymentAndExecution", "5cC3foj77CWun58pC51ebHFUWavHWKarWyR5UUik7dnC")
var DisableSbpfV0Execution = newGate("DisableSbpfV0Execution", "TestFeature11111111111111111111111111111111")
var ReenableSbpfV0Execution = newGate("ReenableSbpfV0Execution", "TestFeature21111111111111111111111111111111")
var FormalizeLoadedTransactionDataSize = newGate("FormalizeLoadedTransactionDataSize", "DeS7sR48ZcFTUmt5FFEVDr1v1bh73aAbZiZq3SYr8Eh8")
var EnableTxV1 = newGate("EnableTxV1", "txv1aq4pp281K9um3tnPgkfX8UqtFT6wcVW3hNezGLL")
var DefineLtdsFeeOnlySemantics = newGate("DefineLtdsFeeOnlySemantics", "LTDSzjZKFJMKHYpNycG1FrWwGGTaFFwqEFjB5GGLNVD")
var RelaxPostExecMinBalanceCheck = newGate("RelaxPostExecMinBalanceCheck", "BY4JhHLahVzS9ynfDz4exzGPbVXhFmJvEyMWsXbDBqME")
var RelaxFeePayerConstraint = newGate("RelaxFeePayerConstraint", "FEEXbxUuKobtrt1qNK5pjtzbPQhsppBTrNNG74xu4mai")
var RaiseBlockLimitsTo100m = newGate("RaiseBlockLimitsTo100m", "P1BCUMpAC7V2GRBRiJCNUgpMyWZhoqt3LKo712ePqsz")

// AlpenglowFeatureGateAddress is consensus-critical: Alpenglow runtime state
// PDAs are derived from this feature account. Keep it pinned to the feature ID
// deployed by the supported cluster, not an unreleased Agave branch.
const AlpenglowFeatureGateAddress = "A1pengvuM6JEcyNuTnMqepBKhwHE3N6PmUrdATGawhJS"

var Alpenglow = newGate("Alpenglow", AlpenglowFeatureGateAddress)
var AlpenglowDevContext = newGate("AlpenglowDevContext", "8KpruRFrT59jQ9NfFX9DU6j8a1hW7y6xchvZNQ5rxD4P")
var ValidatorAdmissionTicket = newGate("ValidatorAdmissionTicket", "VAT9huvhPjRN9cyrPytq9rwvEJ3J4ADtjdncgZRyANJ")
var ReduceSlotTimeTo350ms = newGate("ReduceSlotTimeTo350ms", "iBRL5RuWhw4yqaAZu96RUULHckHTZAoe2b77qaV38JZ")
var ReduceSlotTimeTo300ms = newGate("ReduceSlotTimeTo300ms", "iBRLL3k18HST852F1Mf3Lv83waTNQmmqvKDxvYGwQFL")
var ReduceSlotTimeTo250ms = newGate("ReduceSlotTimeTo250ms", "iBRLMc81UjRa8fn8A6eE8bJTnRbgQoPTynM51akENCV")
var ReduceSlotTimeTo200ms = newGate("ReduceSlotTimeTo200ms", "iBRLjhJnkmDZgNoZRDMW11d8ZV7HvsL3vAyRjZB5npW")
var IncreaseCpiAccountInfoLimit = newGate("IncreaseCpiAccountInfoLimit", "H6iVbVaDZgDphcPbcZwc5LoznMPWQfnJ1AM7L1xzqvt5")
var StaticInstructionLimit = newGate("StaticInstructionLimit", "64ixypL1HPu8WtJhNSMb9mSgfFaJvsANuRkTbHyuLfnx")
var PoseidonEnforcePadding = newGate("PoseidonEnforcePadding", "poUdAqRXXsNmfqAZ6UqpjbeYgwBygbfQLEvWSqVhSnb")
var FixAltBn128PairingLengthCheck = newGate("FixAltBn128PairingLengthCheck", "bnYzodLwmybj7e1HAe98yZrdJTd7we69eMMLgCXqKZm")
var DeprecateRentExemptionThreshold = newGate("DeprecateRentExemptionThreshold", "rent6iVy6PDoViPBeJ6k5EJQrkj62h7DPyLbWGHwjrC")
var ProvideInstructionDataOffsetInVmR2 = newGate("ProvideInstructionDataOffsetInVmR2", "5xXZc66h4UdB6Yq7FzdBxBiRAFMMScMLwHxk2QZDaNZL")
var SyscallParameterAddressRestrictions = newGate("SyscallParameterAddressRestrictions", "EDGMC5kxFxGk4ixsNkGt8bW7QL5hDMXnbwaZvYMwNfzF")
var VirtualAddressSpaceAdjustments = newGate("VirtualAddressSpaceAdjustments", "7VgiehxNxu53KdxgLspGQY8myE6f7UokaWa4jsGcaSz")
var AccountDataDirectMapping = newGate("AccountDataDirectMapping", "CR3dVN2Yoo95Y96kLSTaziWDAQT2MNEpiWh5cqVq2pNE")
var VoteStateV4 = newGate("VoteStateV4", "Gx4XFcrVMt4HUvPzTpTSVkdDVgcDSjKhDN1RqRS6KDuZ")
var BlsPubkeyManagementInVoteAccount = newGate("BlsPubkeyManagementInVoteAccount", "AnAP9zPV4KL7czAPQbFhpDKV2tx7g4UGNbK9wvXwjaRo")
var CommissionRateInBasisPoints = newGate("CommissionRateInBasisPoints", "Eg7tXEwMZzS98xaZ1YHUbdRHsaYZiCsSaR6sKgxreoaj")
var CustomCommissionCollector = newGate("CustomCommissionCollector", "3HcSrCTGXTUnrTueHi4DAwNuMxZSsm5xui2Ax3mgxHqf")
var BlockRevenueSharing = newGate("BlockRevenueSharing", "7MYx95UBiJufqnumyN7HfskJ9vKdcGMmhreVguqrE97K")
var VoteAccountInitializeV2 = newGate("VoteAccountInitializeV2", "VoteAccount1nitia1izeV211111111111111111111")
var RelaxProgramdataAccountCheckMigration = newGate("RelaxProgramdataAccountCheckMigration", "rexav5eNTUSNT1K2N7cfRjnthwhcP5BC25v2tA4rW4h")
var ReplaceSplTokenWithPToken = newGate("ReplaceSplTokenWithPToken", "ptokFjwyJtrwCa9Kgo9xoDS59V4QccBGEaRFnRPnSdP")
var CreateAccountAllowPrefund = newGate("CreateAccountAllowPrefund", "6sPDzwyARRExKH52LECxcGoqziH8G7SZofwuxi8Ja331")
var RemoveSimpleVoteFromCostModel = newGate("RemoveSimpleVoteFromCostModel", "2GCrNXbzmt4xrwdcKS2RdsLzsgu4V5zHAemW57pcHT6a")
var DisableZkElgamalProofProgram = newGate("DisableZkElgamalProofProgram", "zkdoVwnSFnSLtGJG7irJPEYUpmb4i7sGMGcnN6T9rnC")
var ReenableZkElgamalProofProgram = newGate("ReenableZkElgamalProofProgram", "zkexuyPRdyTVbZqEAREueqL2xvvoBhRgth9xGSc1tMN")
var AltBn128LittleEndian = newGate("AltBn128LittleEndian", "bn2oPgpkzQPT3tohMaAsMVGjhDmmDa4jCaVPqCFmtxM")
var EnableAltBn128G2Syscalls = newGate("EnableAltBn128G2Syscalls", "bn1hKNURMGQaQoEVxahcEAcqiX3NwRs6hgKKNSLeKxH")
var EnableBls12_381Syscall = newGate("EnableBls12_381Syscall", "b1sgUiJ3qu7hYm3tNDyyqZNQd6gLGJmJppnLNa93PCQ")
var UpgradeBpfStakeProgramToV5 = newGate("UpgradeBpfStakeProgramToV5", "STk5Xj8hdAx3sTzmtJ3QysKkq6X2A3yj73JtxttiRyk")
var DelayCommissionUpdates = newGate("DelayCommissionUpdates", "76dHtohc2s5dR3ahJyBxs7eJJVipFkaPdih9CLgTTb4B")
var ValidateChainedBlockId = newGate("ValidateChainedBlockId", "vcmrbYbiMVKaq1snKP6eCacNDcr6qZvpCNUjmk6gxvZ")
var DiscardUnexpectedDataCompleteShreds = newGate("DiscardUnexpectedDataCompleteShreds", "dcomRRWHXP1FVWPqi9Mm4oxJhF4ehC795SvAtUdA9os")
var LoaderV3MinimumExtendProgramSize = newGate("LoaderV3MinimumExtendProgramSize", "YbbRLkvenrocjGPGyoQE4wjnvYzTgfsk38NFmcYK7a5")

var AllFeatureGates = []FeatureGate{StopTruncatingStringsInSyscalls, EnablePartitionedEpochReward, EnablePartitionedEpochRewardsSuperfeature,
	LastRestartSlotSysvar, Libsecp256k1FailOnBadCount, Libsecp256k1FailOnBadCount2, EnableBpfLoaderSetAuthorityCheckedIx,
	LoosenCpiSizeRestriction, IncreaseTxAccountLockLimit, VoteStateAddVoteLatency, AllowCommissionDecreaseAtAnyTime,
	CommissionUpdatesOnlyAllowedInFirstHalfOfEpoch, TimelyVoteCredits, ReduceStakeWarmupCooldown,
	StakeRaiseMinimumDelegationTo1Sol, StakeRedelegateInstruction, RequireRentExemptSplitDestination,
	DeprecateExecutableMetaUpdateInBpfLoader, RelaxAuthoritySignerCheckForLookupTableCreation, DedupeConfigProgramSigners,
	Ed25519PrecompileVerifyStrict, AbortOnInvalidCurve, Blake3SyscallEnabled, RemainingComputeUnitsSyscallEnabled, Curve25519SyscallEnabled, SimplifyAltBn128SyscallErrorCodes,
	EnableAltbn128CompressionSyscall, EnableAltBn128Syscall, DisableFeesSysvar, DisableRentFeesCollection, DeprecateUnusedLegacyVotePlumbing,
	RewardFullPriorityFee, StakeMinimumDelegationForRewards, MoveStakeAndMoveLamportsIxs, GetSysvarSyscallEnabled,
	AddNewReservedAccountKeys, EnableSecp256r1Precompile, FixAltBn128MultiplicationInputLength, EnableTowerSyncIx, SkipRentRewrites,
	FullInflationVote, FullInflationEnable, FullInflationDevnetAndTestnet, PicoInflation, DisableAccountLoaderSpecialCase, EnableGetEpochStakeSyscall,
	ReserveMinimalCUsForBuiltinInstructions, RelaxIntraBatchAccountLocks, MaskOutRentEpochInVmSerialization, RemoveAccountsExecutableFlagChecks,
	AccountsLtHash, RemoveAccountsDeltaHash, EnableLoaderV4, EnableSbpfV1DeploymentAndExecution, EnableSbpfV2DeploymentAndExecution,
	EnableSbpfV3DeploymentAndExecution, DisableSbpfV0Execution, ReenableSbpfV0Execution, FormalizeLoadedTransactionDataSize,
	EnableTxV1, DefineLtdsFeeOnlySemantics, RelaxPostExecMinBalanceCheck, RelaxFeePayerConstraint, RaiseBlockLimitsTo100m, Alpenglow, AlpenglowDevContext,
	ValidatorAdmissionTicket, ReduceSlotTimeTo350ms, ReduceSlotTimeTo300ms, ReduceSlotTimeTo250ms, ReduceSlotTimeTo200ms, IncreaseCpiAccountInfoLimit,
	StaticInstructionLimit, PoseidonEnforcePadding, FixAltBn128PairingLengthCheck, DeprecateRentExemptionThreshold,
	ProvideInstructionDataOffsetInVmR2, SyscallParameterAddressRestrictions, VirtualAddressSpaceAdjustments, AccountDataDirectMapping,
	VoteStateV4, BlsPubkeyManagementInVoteAccount, CommissionRateInBasisPoints, CustomCommissionCollector, BlockRevenueSharing, VoteAccountInitializeV2,
	RelaxProgramdataAccountCheckMigration, ReplaceSplTokenWithPToken, CreateAccountAllowPrefund,
	RemoveSimpleVoteFromCostModel, DisableZkElgamalProofProgram, ReenableZkElgamalProofProgram, AltBn128LittleEndian, EnableAltBn128G2Syscalls,
	EnableBls12_381Syscall, UpgradeBpfStakeProgramToV5, DelayCommissionUpdates, ValidateChainedBlockId, DiscardUnexpectedDataCompleteShreds, LoaderV3MinimumExtendProgramSize}

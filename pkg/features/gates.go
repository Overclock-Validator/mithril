package features

import (
	"github.com/Overclock-Validator/mithril/pkg/base58"
)

var StopTruncatingStringsInSyscalls = FeatureGate{Name: "StopTruncatingStringsInSyscalls", Address: base58.MustDecodeFromString("16FMCmgLzCNNz6eTwGanbyN2ZxvTBSLuQ6DZhgeMshg")}
var EnablePartitionedEpochReward = FeatureGate{Name: "EnablePartitionedEpochReward", Address: base58.MustDecodeFromString("9bn2vTJUsUcnpiZWbu2woSKtTGW3ErZC9ERv88SDqQjK")}
var EnablePartitionedEpochRewardsSuperfeature = FeatureGate{Name: "EnablePartitionedEpochRewardsSuperfeature", Address: base58.MustDecodeFromString("PERzQrt5gBD1XEe2c9XdFWqwgHY3mr7cYWbm5V772V8")}
var LastRestartSlotSysvar = FeatureGate{Name: "LastRestartSlotSysvar", Address: base58.MustDecodeFromString("HooKD5NC9QNxk25QuzCssB8ecrEzGt6eXEPBUxWp1LaR")}
var Libsecp256k1FailOnBadCount = FeatureGate{Name: "Libsecp256k1FailOnBadCount", Address: base58.MustDecodeFromString("8aXvSuopd1PUj7UhehfXJRg6619RHp8ZvwTyyJHdUYsj")}
var Libsecp256k1FailOnBadCount2 = FeatureGate{Name: "Libsecp256k1FailOnBadCount2", Address: base58.MustDecodeFromString("54KAoNiUERNoWWUhTWWwXgym94gzoXFVnHyQwPA18V9A")}
var EnableBpfLoaderSetAuthorityCheckedIx = FeatureGate{Name: "EnableBpfLoaderSetAuthorityCheckedIx", Address: base58.MustDecodeFromString("5x3825XS7M2A3Ekbn5VGGkvFoAg5qrRWkTrY4bARP1GL")}
var LoosenCpiSizeRestriction = FeatureGate{Name: "LoosenCpiSizeRestriction", Address: base58.MustDecodeFromString("GDH5TVdbTPUpRnXaRyQqiKUa7uZAbZ28Q2N9bhbKoMLm")}
var IncreaseTxAccountLockLimit = FeatureGate{Name: "IncreaseTxAccountLockLimit", Address: base58.MustDecodeFromString("9LZdXeKGeBV6hRLdxS1rHbHoEUsKqesCC2ZAPTPKJAbK")}
var VoteStateAddVoteLatency = FeatureGate{Name: "VoteStateAddVoteLatency", Address: base58.MustDecodeFromString("7axKe5BTYBDD87ftzWbk5DfzWMGyRvqmWTduuo22Yaqy")}
var AllowCommissionDecreaseAtAnyTime = FeatureGate{Name: "AllowCommissionDecreaseAtAnyTime", Address: base58.MustDecodeFromString("decoMktMcnmiq6t3u7g5BfgcQu91nKZr6RvMYf9z1Jb")}
var CommissionUpdatesOnlyAllowedInFirstHalfOfEpoch = FeatureGate{Name: "CommissionUpdatesOnlyAllowedInFirstHalfOfEpoch", Address: base58.MustDecodeFromString("noRuG2kzACwgaY7TVmLRnUNPLKNVQE1fb7X55YWBehp")}
var TimelyVoteCredits = FeatureGate{Name: "TimelyVoteCredits", Address: base58.MustDecodeFromString("tvcF6b1TRz353zKuhBjinZkKzjmihXmBAHJdjNYw1sQ")}
var ReduceStakeWarmupCooldown = FeatureGate{Name: "ReduceStakeWarmupCooldown", Address: base58.MustDecodeFromString("GwtDQBghCTBgmX2cpEGNPxTEBUTQRaDMGTr5qychdGMj")}
var StakeRaiseMinimumDelegationTo1Sol = FeatureGate{Name: "StakeRaiseMinimumDelegationTo1Sol", Address: base58.MustDecodeFromString("9onWzzvCzNC2jfhxxeqRgs5q7nFAAKpCUvkj6T6GJK9i")}
var StakeRedelegateInstruction = FeatureGate{Name: "StakeRedelegateInstruction", Address: base58.MustDecodeFromString("2KKG3C6RBnxQo9jVVrbzsoSh41TDXLK7gBc9gduyxSzW")}
var RequireRentExemptSplitDestination = FeatureGate{Name: "RequireRentExemptSplitDestination", Address: base58.MustDecodeFromString("D2aip4BBr8NPWtU9vLrwrBvbuaQ8w1zV38zFLxx4pfBV")}
var DeprecateExecutableMetaUpdateInBpfLoader = FeatureGate{Name: "DeprecateExecutableMetaUpdateInBpfLoader", Address: base58.MustDecodeFromString("k6uR1J9VtKJnTukBV2Eo15BEy434MBg8bT6hHQgmU8v")}
var RelaxAuthoritySignerCheckForLookupTableCreation = FeatureGate{Name: "RelaxAuthoritySignerCheckForLookupTableCreation", Address: base58.MustDecodeFromString("FKAcEvNgSY79RpqsPNUV5gDyumopH4cEHqUxyfm8b8Ap")}
var DedupeConfigProgramSigners = FeatureGate{Name: "DedupeConfigProgramSigners", Address: base58.MustDecodeFromString("8kEuAshXLsgkUEdcFVLqrjCGGHVWFW99ZZpxvAzzMtBp")}
var Ed25519PrecompileVerifyStrict = FeatureGate{Name: "Ed25519PrecompileVerifyStrict", Address: base58.MustDecodeFromString("ed9tNscbWLYBooxWA7FE2B5KHWs8A6sxfY8EzezEcoo")}
var AbortOnInvalidCurve = FeatureGate{Name: "AbortOnInvalidCurve", Address: base58.MustDecodeFromString("FuS3FPfJDKSNot99ECLXtp3rueq36hMNStJkPJwWodLh")}
var Blake3SyscallEnabled = FeatureGate{Name: "Blake3SyscallEnabled", Address: base58.MustDecodeFromString("HTW2pSyErTj4BV6KBM9NZ9VBUJVxt7sacNWcf76wtzb3")}
var RemainingComputeUnitsSyscallEnabled = FeatureGate{Name: "RemainingComputeUnitsSyscallEnabled", Address: base58.MustDecodeFromString("5TuppMutoyzhUSfuYdhgzD47F92GL1g89KpCZQKqedxP")}
var Curve25519SyscallEnabled = FeatureGate{Name: "Curve25519SyscallEnabled", Address: base58.MustDecodeFromString("7rcw5UtqgDTBBv2EcynNfYckgdAaH1MAsCjKgXMkN7Ri")}
var SimplifyAltBn128SyscallErrorCodes = FeatureGate{Name: "SimplityAltBn128SyscallErrorCodes", Address: base58.MustDecodeFromString("JDn5q3GBeqzvUa7z67BbmVHVdE3EbUAjvFep3weR3jxX")}
var EnableAltbn128CompressionSyscall = FeatureGate{Name: "EnableAltbn128CompressionSyscall", Address: base58.MustDecodeFromString("EJJewYSddEEtSZHiqugnvhQHiWyZKjkFDQASd7oKSagn")}
var EnableAltBn128Syscall = FeatureGate{Name: "EnableAltBn128Syscall", Address: base58.MustDecodeFromString("A16q37opZdQMCbe5qJ6xpBB9usykfv8jZaMkxvZQi4GJ")}
var DisableFeesSysvar = FeatureGate{Name: "DisableFeesSysvar", Address: base58.MustDecodeFromString("JAN1trEUEtZjgXYzNBYHU9DYd7GnThhXfFP7SzPXkPsG")}
var DisableRentFeesCollection = FeatureGate{Name: "DisableRentFeesCollection", Address: base58.MustDecodeFromString("CJzY83ggJHqPGDq8VisV3U91jDJLuEaALZooBrXtnnLU")}
var DeprecateUnusedLegacyVotePlumbing = FeatureGate{Name: "DeprecateUnusedLegacyVotePlumbing", Address: base58.MustDecodeFromString("6Uf8S75PVh91MYgPQSHnjRAPQq6an5BDv9vomrCwDqLe")}
var RewardFullPriorityFee = FeatureGate{Name: "RewardFullPriorityFee", Address: base58.MustDecodeFromString("3opE3EzAKnUftUDURkzMgwpNgimBAypW1mNDYH4x4Zg7")}
var StakeMinimumDelegationForRewards = FeatureGate{Name: "StakeMinimumDelegationForRewards", Address: base58.MustDecodeFromString("G6ANXD6ptCSyNd9znZm7j4dEczAJCfx7Cy43oBx3rKHJ")}
var MoveStakeAndMoveLamportsIxs = FeatureGate{Name: "MoveStakeAndMoveLamportsIxs", Address: base58.MustDecodeFromString("7bTK6Jis8Xpfrs8ZoUfiMDPazTcdPcTWheZFJTA5Z6X4")}
var GetSysvarSyscallEnabled = FeatureGate{Name: "GetSysvarSyscallEnabled", Address: base58.MustDecodeFromString("CLCoTADvV64PSrnR6QXty6Fwrt9Xc6EdxSJE4wLRePjq")}
var AddNewReservedAccountKeys = FeatureGate{Name: "AddNewReservedAccountKeys", Address: base58.MustDecodeFromString("8U4skmMVnF6k2kMvrWbQuRUT3qQSiTYpSjqmhmgfthZu")}
var EnableSecp256r1Precompile = FeatureGate{Name: "EnableSecp256r1Precompile", Address: base58.MustDecodeFromString("srremy31J5Y25FrAApwVb9kZcfXbusYMMsvTK9aWv5q")}
var FixAltBn128MultiplicationInputLength = FeatureGate{Name: "FixAltBn128MultiplicationInputLength", Address: base58.MustDecodeFromString("bn2puAyxUx6JUabAxYdKdJ5QHbNNmKw8dCGuGCyRrFN")}
var EnableTowerSyncIx = FeatureGate{Name: "EnableTowerSyncIx", Address: base58.MustDecodeFromString("tSynMCspg4xFiCj1v3TDb4c7crMR5tSBhLz4sF7rrNA")}
var SkipRentRewrites = FeatureGate{Name: "SkipRentRewrites", Address: base58.MustDecodeFromString("CGB2jM8pwZkeeiXQ66kBMyBR6Np61mggL7XUsmLjVcrw")}
var FullInflationVote = FeatureGate{Name: "FullInflationVote", Address: base58.MustDecodeFromString("BzBBveUDymEYoYzcMWNQCx3cd4jQs7puaVFHLtsbB6fm")}
var FullInflationEnable = FeatureGate{Name: "FullInflationEnable", Address: base58.MustDecodeFromString("7XRJcS5Ud5vxGB54JbK9N2vBZVwnwdBNeJW1ibRgD9gx")}
var FullInflationDevnetAndTestnet = FeatureGate{Name: "FullInflationDevnetAndTestnet", Address: base58.MustDecodeFromString("DT4n6ABDqs6w4bnfwrXT9rsprcPf6cdDga1egctaPkLC")}
var PicoInflation = FeatureGate{Name: "PicoInflation", Address: base58.MustDecodeFromString("4RWNif6C2WCNiKVW7otP4G7dkmkHGyKQWRpuZ1pxKU5m")}
var DisableAccountLoaderSpecialCase = FeatureGate{Name: "DisableAccountLoaderSpecialCase", Address: base58.MustDecodeFromString("EQUMpNFr7Nacb1sva56xn1aLfBxppEoSBH8RRVdkcD1x")}
var EnableGetEpochStakeSyscall = FeatureGate{Name: "EnableGetEpochStakeSyscall", Address: base58.MustDecodeFromString("FKe75t4LXxGaQnVHdUKM6DSFifVVraGZ8LyNo7oPwy1Z")}
var ReserveMinimalCUsForBuiltinInstructions = FeatureGate{Name: "ReserveMinimalCUsForBuiltinInstructions", Address: base58.MustDecodeFromString("C9oAhLxDBm3ssWtJx1yBGzPY55r2rArHmN1pbQn6HogH")}
var RelaxIntraBatchAccountLocks = FeatureGate{Name: "RelaxIntraBatchAccountLocks", Address: base58.MustDecodeFromString("4WeHX6QoXCCwqbSFgi6dxnB6QsPo6YApaNTH7P4MLQ99")}
var MaskOutRentEpochInVmSerialization = FeatureGate{Name: "MaskOutRentEpochInVmSerialization", Address: base58.MustDecodeFromString("RENtePQcDLrAbxAsP3k8dwVcnNYQ466hi2uKvALjnXx")}
var RemoveAccountsExecutableFlagChecks = FeatureGate{Name: "RemoveAccountsExecutableFlagchecks", Address: base58.MustDecodeFromString("FXs1zh47QbNnhXcnB6YiAQoJ4sGB91tKF3UFHLcKT7PM")}
var AccountsLtHash = FeatureGate{Name: "AccountsLtHash", Address: base58.MustDecodeFromString("LTHasHQX6661DaDD4S6A2TFi6QBuiwXKv66fB1obfHq")}
var RemoveAccountsDeltaHash = FeatureGate{Name: "RemoveAccountsDeltaHash", Address: base58.MustDecodeFromString("LTdLt9Ycbyoipz5fLysCi1NnDnASsZfmJLJXts5ZxZz")}
var EnableLoaderV4 = FeatureGate{Name: "EnableLoaderV4", Address: base58.MustDecodeFromString("2aQJYqER2aKyb3cZw22v4SL2xMX7vwXBRWfvS4pTrtED")}
var EnableSbpfV1DeploymentAndExecution = FeatureGate{Name: "EnableSbpfV1DeploymentAndExecution", Address: base58.MustDecodeFromString("JE86WkYvTrzW8HgNmrHY7dFYpCmSptUpKupbo2AdQ9cG")}
var EnableSbpfV2DeploymentAndExecution = FeatureGate{Name: "EnableSbpfV2DeploymentAndExecution", Address: base58.MustDecodeFromString("F6UVKh1ujTEFK3en2SyAL3cdVnqko1FVEXWhmdLRu6WP")}
var EnableSbpfV3DeploymentAndExecution = FeatureGate{Name: "EnableSbpfV3DeploymentAndExecution", Address: base58.MustDecodeFromString("5cC3foj77CWun58pC51ebHFUWavHWKarWyR5UUik7dnC")}
var DisableSbpfV0Execution = FeatureGate{Name: "DisableSbpfV0Execution", Address: base58.MustDecodeFromString("TestFeature11111111111111111111111111111111")}
var ReenableSbpfV0Execution = FeatureGate{Name: "ReenableSbpfV0Execution", Address: base58.MustDecodeFromString("TestFeature21111111111111111111111111111111")}
var FormalizeLoadedTransactionDataSize = FeatureGate{Name: "FormalizeLoadedTransactionDataSize", Address: base58.MustDecodeFromString("DeS7sR48ZcFTUmt5FFEVDr1v1bh73aAbZiZq3SYr8Eh8")}
var EnableTxV1 = FeatureGate{Name: "EnableTxV1", Address: base58.MustDecodeFromString("txv1aq4pp281K9um3tnPgkfX8UqtFT6wcVW3hNezGLL")}
var DefineLtdsFeeOnlySemantics = FeatureGate{Name: "DefineLtdsFeeOnlySemantics", Address: base58.MustDecodeFromString("LTDSzjZKFJMKHYpNycG1FrWwGGTaFFwqEFjB5GGLNVD")}
var RelaxPostExecMinBalanceCheck = FeatureGate{Name: "RelaxPostExecMinBalanceCheck", Address: base58.MustDecodeFromString("BY4JhHLahVzS9ynfDz4exzGPbVXhFmJvEyMWsXbDBqME")}
var RelaxFeePayerConstraint = FeatureGate{Name: "RelaxFeePayerConstraint", Address: base58.MustDecodeFromString("FEEXbxUuKobtrt1qNK5pjtzbPQhsppBTrNNG74xu4mai")}
var RaiseBlockLimitsTo100m = FeatureGate{Name: "RaiseBlockLimitsTo100m", Address: base58.MustDecodeFromString("P1BCUMpAC7V2GRBRiJCNUgpMyWZhoqt3LKo712ePqsz")}

// AlpenglowFeatureGateAddress is consensus-critical: Alpenglow runtime state
// PDAs are derived from this feature account. Keep it pinned to the feature ID
// deployed by the supported cluster, not an unreleased Agave branch.
const AlpenglowFeatureGateAddress = "A1pengvuM6JEcyNuTnMqepBKhwHE3N6PmUrdATGawhJS"

var Alpenglow = FeatureGate{Name: "Alpenglow", Address: base58.MustDecodeFromString(AlpenglowFeatureGateAddress)}
var AlpenglowDevContext = FeatureGate{Name: "AlpenglowDevContext", Address: base58.MustDecodeFromString("8KpruRFrT59jQ9NfFX9DU6j8a1hW7y6xchvZNQ5rxD4P")}
var ValidatorAdmissionTicket = FeatureGate{Name: "ValidatorAdmissionTicket", Address: base58.MustDecodeFromString("VAT9huvhPjRN9cyrPytq9rwvEJ3J4ADtjdncgZRyANJ")}
var ReduceSlotTimeTo350ms = FeatureGate{Name: "ReduceSlotTimeTo350ms", Address: base58.MustDecodeFromString("iBRL5RuWhw4yqaAZu96RUULHckHTZAoe2b77qaV38JZ")}
var ReduceSlotTimeTo300ms = FeatureGate{Name: "ReduceSlotTimeTo300ms", Address: base58.MustDecodeFromString("iBRLL3k18HST852F1Mf3Lv83waTNQmmqvKDxvYGwQFL")}
var ReduceSlotTimeTo250ms = FeatureGate{Name: "ReduceSlotTimeTo250ms", Address: base58.MustDecodeFromString("iBRLMc81UjRa8fn8A6eE8bJTnRbgQoPTynM51akENCV")}
var ReduceSlotTimeTo200ms = FeatureGate{Name: "ReduceSlotTimeTo200ms", Address: base58.MustDecodeFromString("iBRLjhJnkmDZgNoZRDMW11d8ZV7HvsL3vAyRjZB5npW")}
var IncreaseCpiAccountInfoLimit = FeatureGate{Name: "IncreaseCpiAccountInfoLimit", Address: base58.MustDecodeFromString("H6iVbVaDZgDphcPbcZwc5LoznMPWQfnJ1AM7L1xzqvt5")}
var StaticInstructionLimit = FeatureGate{Name: "StaticInstructionLimit", Address: base58.MustDecodeFromString("64ixypL1HPu8WtJhNSMb9mSgfFaJvsANuRkTbHyuLfnx")}
var PoseidonEnforcePadding = FeatureGate{Name: "PoseidonEnforcePadding", Address: base58.MustDecodeFromString("poUdAqRXXsNmfqAZ6UqpjbeYgwBygbfQLEvWSqVhSnb")}
var FixAltBn128PairingLengthCheck = FeatureGate{Name: "FixAltBn128PairingLengthCheck", Address: base58.MustDecodeFromString("bnYzodLwmybj7e1HAe98yZrdJTd7we69eMMLgCXqKZm")}
var DeprecateRentExemptionThreshold = FeatureGate{Name: "DeprecateRentExemptionThreshold", Address: base58.MustDecodeFromString("rent6iVy6PDoViPBeJ6k5EJQrkj62h7DPyLbWGHwjrC")}
var ProvideInstructionDataOffsetInVmR2 = FeatureGate{Name: "ProvideInstructionDataOffsetInVmR2", Address: base58.MustDecodeFromString("5xXZc66h4UdB6Yq7FzdBxBiRAFMMScMLwHxk2QZDaNZL")}
var SyscallParameterAddressRestrictions = FeatureGate{Name: "SyscallParameterAddressRestrictions", Address: base58.MustDecodeFromString("EDGMC5kxFxGk4ixsNkGt8bW7QL5hDMXnbwaZvYMwNfzF")}
var VirtualAddressSpaceAdjustments = FeatureGate{Name: "VirtualAddressSpaceAdjustments", Address: base58.MustDecodeFromString("7VgiehxNxu53KdxgLspGQY8myE6f7UokaWa4jsGcaSz")}
var AccountDataDirectMapping = FeatureGate{Name: "AccountDataDirectMapping", Address: base58.MustDecodeFromString("CR3dVN2Yoo95Y96kLSTaziWDAQT2MNEpiWh5cqVq2pNE")}
var VoteStateV4 = FeatureGate{Name: "VoteStateV4", Address: base58.MustDecodeFromString("Gx4XFcrVMt4HUvPzTpTSVkdDVgcDSjKhDN1RqRS6KDuZ")}
var BlsPubkeyManagementInVoteAccount = FeatureGate{Name: "BlsPubkeyManagementInVoteAccount", Address: base58.MustDecodeFromString("AnAP9zPV4KL7czAPQbFhpDKV2tx7g4UGNbK9wvXwjaRo")}
var CommissionRateInBasisPoints = FeatureGate{Name: "CommissionRateInBasisPoints", Address: base58.MustDecodeFromString("Eg7tXEwMZzS98xaZ1YHUbdRHsaYZiCsSaR6sKgxreoaj")}
var CustomCommissionCollector = FeatureGate{Name: "CustomCommissionCollector", Address: base58.MustDecodeFromString("3HcSrCTGXTUnrTueHi4DAwNuMxZSsm5xui2Ax3mgxHqf")}
var BlockRevenueSharing = FeatureGate{Name: "BlockRevenueSharing", Address: base58.MustDecodeFromString("B1ockRevenueSharing111111111111111111111111")}
var VoteAccountInitializeV2 = FeatureGate{Name: "VoteAccountInitializeV2", Address: base58.MustDecodeFromString("VoteAccount1nitia1izeV211111111111111111111")}
var RelaxProgramdataAccountCheckMigration = FeatureGate{Name: "RelaxProgramdataAccountCheckMigration", Address: base58.MustDecodeFromString("rexav5eNTUSNT1K2N7cfRjnthwhcP5BC25v2tA4rW4h")}
var ReplaceSplTokenWithPToken = FeatureGate{Name: "ReplaceSplTokenWithPToken", Address: base58.MustDecodeFromString("ptokFjwyJtrwCa9Kgo9xoDS59V4QccBGEaRFnRPnSdP")}
var CreateAccountAllowPrefund = FeatureGate{Name: "CreateAccountAllowPrefund", Address: base58.MustDecodeFromString("6sPDzwyARRExKH52LECxcGoqziH8G7SZofwuxi8Ja331")}
var RemoveSimpleVoteFromCostModel = FeatureGate{Name: "RemoveSimpleVoteFromCostModel", Address: base58.MustDecodeFromString("2GCrNXbzmt4xrwdcKS2RdsLzsgu4V5zHAemW57pcHT6a")}
var DisableZkElgamalProofProgram = FeatureGate{Name: "DisableZkElgamalProofProgram", Address: base58.MustDecodeFromString("zkdoVwnSFnSLtGJG7irJPEYUpmb4i7sGMGcnN6T9rnC")}
var ReenableZkElgamalProofProgram = FeatureGate{Name: "ReenableZkElgamalProofProgram", Address: base58.MustDecodeFromString("zkexuyPRdyTVbZqEAREueqL2xvvoBhRgth9xGSc1tMN")}
var AltBn128LittleEndian = FeatureGate{Name: "AltBn128LittleEndian", Address: base58.MustDecodeFromString("bn2oPgpkzQPT3tohMaAsMVGjhDmmDa4jCaVPqCFmtxM")}
var EnableAltBn128G2Syscalls = FeatureGate{Name: "EnableAltBn128G2Syscalls", Address: base58.MustDecodeFromString("bn1hKNURMGQaQoEVxahcEAcqiX3NwRs6hgKKNSLeKxH")}
var EnableBls12_381Syscall = FeatureGate{Name: "EnableBls12_381Syscall", Address: base58.MustDecodeFromString("b1sgUiJ3qu7hYm3tNDyyqZNQd6gLGJmJppnLNa93PCQ")}
var UpgradeBpfStakeProgramToV5 = FeatureGate{Name: "UpgradeBpfStakeProgramToV5", Address: base58.MustDecodeFromString("STk5Xj8hdAx3sTzmtJ3QysKkq6X2A3yj73JtxttiRyk")}
var DelayCommissionUpdates = FeatureGate{Name: "DelayCommissionUpdates", Address: base58.MustDecodeFromString("76dHtohc2s5dR3ahJyBxs7eJJVipFkaPdih9CLgTTb4B")}
var ValidateChainedBlockId = FeatureGate{Name: "ValidateChainedBlockId", Address: base58.MustDecodeFromString("vcmrbYbiMVKaq1snKP6eCacNDcr6qZvpCNUjmk6gxvZ")}
var DiscardUnexpectedDataCompleteShreds = FeatureGate{Name: "DiscardUnexpectedDataCompleteShreds", Address: base58.MustDecodeFromString("dcomRRWHXP1FVWPqi9Mm4oxJhF4ehC795SvAtUdA9os")}

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
	EnableBls12_381Syscall, UpgradeBpfStakeProgramToV5, DelayCommissionUpdates, ValidateChainedBlockId, DiscardUnexpectedDataCompleteShreds}

package sealevel

import "github.com/Overclock-Validator/mithril/pkg/accounts"

type sysvarCache struct {
	RecentBlockHashes recentBlockhashesCache
	Rent              rentCache
	Clock             clockCache
	Fees              feesCache
	SlotHashes        slotHashesCache
	SlotHistory       slotHistoryCache
	EpochSchedule     epochScheduleCache
	EpochRewards      epochRewardsCache
	StakeHistory      stakeHistoryCache
	LastRestartSlot   lastRestartSlotCache
}

type recentBlockhashesCache struct {
	Acct   *accounts.Account
	Sysvar *SysvarRecentBlockhashes
}

type rentCache struct {
	Acct   *accounts.Account
	Sysvar *SysvarRent
}
type clockCache struct {
	Acct   *accounts.Account
	Sysvar *SysvarClock
}
type feesCache struct {
	Acct   *accounts.Account
	Sysvar *SysvarFees
}
type slotHashesCache struct {
	Acct   *accounts.Account
	Sysvar *SysvarSlotHashes
}

type slotHistoryCache struct {
	Acct   *accounts.Account
	Sysvar *SysvarSlotHistory
}

type epochScheduleCache struct {
	Acct   *accounts.Account
	Sysvar *SysvarEpochSchedule
}

type epochRewardsCache struct {
	Acct   *accounts.Account
	Sysvar *SysvarEpochRewards
}

type stakeHistoryCache struct {
	Acct   *accounts.Account
	Sysvar *SysvarStakeHistory
}

type lastRestartSlotCache struct {
	Acct   *accounts.Account
	Sysvar *SysvarLastRestartSlot
}

var SysvarCache = sysvarCache{}

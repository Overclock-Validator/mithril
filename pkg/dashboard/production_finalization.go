package dashboard

import (
	"math"
	"time"
)

// ProductionFinalizationDetail retains existing local operation durations for
// one successful terminal. They are not delivery or whole-slot latencies.
// BankPhases overlaps BankCommitNanoseconds; do not add the two breakdowns.
type ProductionFinalizationDetail struct {
	SlotEndingNanoseconds          uint64                            `json:"slot_ending_nanoseconds,string"`
	FreezeNanoseconds              uint64                            `json:"freeze_nanoseconds,string"`
	EntryDrainNanoseconds          uint64                            `json:"entry_drain_nanoseconds,string"`
	RewardCertificatesNanoseconds  uint64                            `json:"reward_certificates_nanoseconds,string"`
	BuildBlockNanoseconds          uint64                            `json:"build_block_nanoseconds,string"`
	BankCommitNanoseconds          uint64                            `json:"bank_commit_nanoseconds,string"`
	FooterBroadcastNanoseconds     uint64                            `json:"footer_broadcast_nanoseconds,string"`
	EndingTickBroadcastNanoseconds uint64                            `json:"ending_tick_broadcast_nanoseconds,string"`
	BankPhases                     *ProductionBankFinalizationDetail `json:"bank_phases"`
	WindowCompletion               *ProductionWindowCompletionDetail `json:"window_completion,omitempty"`
}

// ProductionWindowCompletionDetail stops at local final-shred submission.
// The signed margin is deadline minus completion, not peer delivery latency.
type ProductionWindowCompletionDetail struct {
	WindowStartSlot                     uint64    `json:"window_start_slot,string"`
	SlotDurationNanoseconds             uint64    `json:"slot_duration_nanoseconds,string"`
	ReadyAt                             time.Time `json:"ready_at"`
	NetworkCompletedAt                  time.Time `json:"network_completed_at"`
	ReadyToCompletionNanoseconds        uint64    `json:"ready_to_completion_nanoseconds,string"`
	FinalizationToCompletionNanoseconds uint64    `json:"finalization_to_completion_nanoseconds,string"`
	DeadlineMarginNanoseconds           int64     `json:"deadline_margin_nanoseconds,string"`
}

// ProductionBankFinalizationDetail is absent when the native commit did not
// supply its inner phase observations. Zero durations are measured values.
type ProductionBankFinalizationDetail struct {
	PrepareSysvarsNanoseconds     uint64 `json:"prepare_sysvars_nanoseconds,string"`
	FooterClockNanoseconds        uint64 `json:"footer_clock_nanoseconds,string"`
	VoteRewardsNanoseconds        uint64 `json:"vote_rewards_nanoseconds,string"`
	DistributeFeesNanoseconds     uint64 `json:"distribute_fees_nanoseconds,string"`
	CollectRentNanoseconds        uint64 `json:"collect_rent_nanoseconds,string"`
	IncineratorNanoseconds        uint64 `json:"incinerator_nanoseconds,string"`
	FinalizeSysvarsNanoseconds    uint64 `json:"finalize_sysvars_nanoseconds,string"`
	CompileAccountsNanoseconds    uint64 `json:"compile_accounts_nanoseconds,string"`
	LoadParentAccountsNanoseconds uint64 `json:"load_parent_accounts_nanoseconds,string"`
	BankHashNanoseconds           uint64 `json:"bank_hash_nanoseconds,string"`
}

func validProductionFinalization(t *ProductionTerminalDetail) bool {
	f := t.Finalization
	if f == nil {
		return true
	}
	if t.Outcome != "broadcast" || t.Bank == nil || t.Costs == nil {
		return false
	}
	for _, n := range []uint64{f.SlotEndingNanoseconds, f.FreezeNanoseconds, f.EntryDrainNanoseconds, f.RewardCertificatesNanoseconds,
		f.BuildBlockNanoseconds, f.BankCommitNanoseconds, f.FooterBroadcastNanoseconds, f.EndingTickBroadcastNanoseconds} {
		if n >= math.MaxInt64 {
			return false
		}
	}
	if b := f.BankPhases; b != nil {
		for _, n := range []uint64{b.PrepareSysvarsNanoseconds, b.FooterClockNanoseconds, b.VoteRewardsNanoseconds, b.DistributeFeesNanoseconds,
			b.CollectRentNanoseconds, b.IncineratorNanoseconds, b.FinalizeSysvarsNanoseconds, b.CompileAccountsNanoseconds, b.LoadParentAccountsNanoseconds, b.BankHashNanoseconds} {
			if n >= math.MaxInt64 {
				return false
			}
		}
	}
	if w := f.WindowCompletion; w != nil {
		if w.WindowStartSlot%4 != 0 || w.WindowStartSlot > t.Slot || t.Slot-w.WindowStartSlot >= 4 || w.SlotDurationNanoseconds == 0 ||
			w.SlotDurationNanoseconds > math.MaxInt64/4 || !validReplayDetailTime(w.ReadyAt) || !validReplayDetailTime(w.NetworkCompletedAt) ||
			w.ReadyToCompletionNanoseconds >= math.MaxInt64 || w.FinalizationToCompletionNanoseconds > w.ReadyToCompletionNanoseconds {
			return false
		}
		budget := (t.Slot - w.WindowStartSlot + 1) * w.SlotDurationNanoseconds
		if w.DeadlineMarginNanoseconds != int64(budget)-int64(w.ReadyToCompletionNanoseconds) {
			return false
		}
	}
	return true
}

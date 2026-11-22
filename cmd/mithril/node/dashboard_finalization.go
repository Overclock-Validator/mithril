package node

import (
	"errors"
	"math"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/blockprod"
	"github.com/Overclock-Validator/mithril/pkg/dashboard"
)

func productionFinalizationMeasurement(o blockprod.LeaderFinalizationObservation) (*dashboard.ProductionFinalizationDetail, error) {
	values := []time.Duration{o.SlotEnding, o.Freeze, o.EntryDrain, o.RewardCertificates, o.BuildBlock, o.BankCommit, o.FooterBroadcast, o.EndingTickBroadcast}
	if o.BankPhasesKnown {
		b := o.Bank
		values = append(values, b.PrepareSysvars, b.FooterClock, b.VoteRewards, b.DistributeFees, b.CollectRent, b.Incinerator,
			b.FinalizeSysvars, b.CompileAccounts, b.LoadParentAccounts, b.BankHash)
	}
	for _, n := range values {
		if n < 0 || n == math.MaxInt64 {
			return nil, errors.New("source_invalid")
		}
	}
	f := &dashboard.ProductionFinalizationDetail{SlotEndingNanoseconds: uint64(o.SlotEnding), FreezeNanoseconds: uint64(o.Freeze), EntryDrainNanoseconds: uint64(o.EntryDrain),
		RewardCertificatesNanoseconds: uint64(o.RewardCertificates), BuildBlockNanoseconds: uint64(o.BuildBlock), BankCommitNanoseconds: uint64(o.BankCommit),
		FooterBroadcastNanoseconds: uint64(o.FooterBroadcast), EndingTickBroadcastNanoseconds: uint64(o.EndingTickBroadcast)}
	if o.BankPhasesKnown {
		b := o.Bank
		f.BankPhases = &dashboard.ProductionBankFinalizationDetail{PrepareSysvarsNanoseconds: uint64(b.PrepareSysvars), FooterClockNanoseconds: uint64(b.FooterClock),
			VoteRewardsNanoseconds: uint64(b.VoteRewards), DistributeFeesNanoseconds: uint64(b.DistributeFees), CollectRentNanoseconds: uint64(b.CollectRent), IncineratorNanoseconds: uint64(b.Incinerator),
			FinalizeSysvarsNanoseconds: uint64(b.FinalizeSysvars), CompileAccountsNanoseconds: uint64(b.CompileAccounts), LoadParentAccountsNanoseconds: uint64(b.LoadParentAccounts), BankHashNanoseconds: uint64(b.BankHash)}
	}
	if o.WindowCompletionKnown {
		w := o.WindowCompletion
		if w.SlotDuration <= 0 || w.SlotDuration > math.MaxInt64/4 || w.ReadyToCompletion < 0 || w.ReadyToCompletion == math.MaxInt64 ||
			w.FinalizationToCompletion < 0 || w.FinalizationToCompletion > w.ReadyToCompletion || w.ReadyAt.IsZero() || w.NetworkCompletedAt.IsZero() {
			return nil, errors.New("source_invalid")
		}
		f.WindowCompletion = &dashboard.ProductionWindowCompletionDetail{WindowStartSlot: w.WindowStartSlot, SlotDurationNanoseconds: uint64(w.SlotDuration),
			ReadyAt: w.ReadyAt.UTC(), NetworkCompletedAt: w.NetworkCompletedAt.UTC(), ReadyToCompletionNanoseconds: uint64(w.ReadyToCompletion),
			FinalizationToCompletionNanoseconds: uint64(w.FinalizationToCompletion), DeadlineMarginNanoseconds: int64(w.DeadlineMargin)}
	}
	return f, nil
}

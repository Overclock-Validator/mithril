package replay

import (
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/duration"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/safemath"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/wide"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/tidwall/btree"
)

const nsPerSlot = 400000000
const maxAllowableDriftFast = 25
const maxAllowableDriftSlow = 150

func updateClockSysvar(clock *sealevel.SysvarClock, block *block.Block, epochSchedule *sealevel.SysvarEpochSchedule) error {
	return updateClockSysvarForMode(clock, block, epochSchedule, false)
}

func updateClockSysvarForMode(clock *sealevel.SysvarClock, block *block.Block, epochSchedule *sealevel.SysvarEpochSchedule, alpenglowClock bool) error {
	epochOld := clock.Epoch
	epochNew := block.Epoch

	if epochOld != epochNew && epochOld+1 != epochNew {
		return fmt.Errorf("unexpected epoch transition in Clock sysvar: clock epoch %d, block epoch %d at slot %d", epochOld, epochNew, block.Slot)
	}

	if alpenglowClock {
		// Alpenglow banks populate timestamp fields from the block footer after
		// execution. At bank start, transactions only see updated slot/epoch
		// fields while timestamp fields are preserved from the parent bank.
	} else if global.CalcUnixTimeForClockSysvar() {
		firstSlotInEpoch := epochSchedule.FirstSlotInEpoch(clock.Epoch)
		epochStartTimestamp := clock.EpochStartTimestamp
		timestampEstimate := getTimestampEstimate(block.Slot, firstSlotInEpoch, epochStartTimestamp, epochSchedule)
		if timestampEstimate > clock.UnixTimestamp {
			clock.UnixTimestamp = timestampEstimate
		}
	} else {
		clock.UnixTimestamp = block.UnixTimestamp
	}

	clock.Slot = block.Slot
	clock.Epoch = epochNew
	clock.LeaderScheduleEpoch = epochSchedule.LeaderScheduleEpoch(clock.Slot)

	if epochOld != epochNew {
		clock.EpochStartTimestamp = clock.UnixTimestamp
	}

	return nil
}

func updateClockSysvarFromAlpenglowFooter(clock *sealevel.SysvarClock, block *block.Block, epochSchedule *sealevel.SysvarEpochSchedule) error {
	footerUnixTimestamp, ok, err := alpenglowFooterUnixTimestamp(block)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	epochNew := block.Epoch
	parentEpoch := epochSchedule.GetEpoch(block.ParentSlot)
	epochStartTimestamp := clock.EpochStartTimestamp
	if block.Slot == 0 || parentEpoch != epochNew {
		epochStartTimestamp = footerUnixTimestamp
	}

	clock.Slot = block.Slot
	clock.Epoch = epochNew
	clock.LeaderScheduleEpoch = epochSchedule.LeaderScheduleEpoch(clock.Slot)
	clock.EpochStartTimestamp = epochStartTimestamp
	clock.UnixTimestamp = footerUnixTimestamp
	return nil
}

// alpenglowFooterUnixTimestamp returns the footer clock timestamp in seconds.
// Agave stores block_producer_time_nanos on the wire and divides by 1e9 for Clock::unix_timestamp.
func alpenglowFooterUnixTimestamp(block *block.Block) (int64, bool, error) {
	if block == nil {
		return 0, false, nil
	}
	if block.UnixTimestamp != 0 {
		return block.UnixTimestamp, true, nil
	}
	if block.FooterProducerTimeNanos == 0 {
		return 0, false, nil
	}
	if block.FooterProducerTimeNanos > uint64(^uint64(0)>>1) {
		return 0, false, fmt.Errorf("footer producer time nanos %d overflows i64", block.FooterProducerTimeNanos)
	}
	return int64(block.FooterProducerTimeNanos / 1_000_000_000), true, nil
}

type tsEntry struct {
	pubkey    solana.PublicKey
	slot      uint64
	timestamp int64
}

func getTimestampEstimate(slot uint64, epochStartTimestampSlot uint64, epochStartTimestamp int64, epochSchedule *sealevel.SysvarEpochSchedule) int64 {
	slotsPerEpoch := epochSchedule.SlotsPerEpoch
	voteAccts := global.VoteCache()

	recentTimestamps := make([]*tsEntry, 0, len(voteAccts))
	for addr, voteAcct := range voteAccts {
		lastTs := voteAcct.LastTimestamp()
		slotDelta := safemath.SaturatingSubU64(slot, lastTs.Slot)
		if slotDelta <= slotsPerEpoch {
			recentTimestamps = append(recentTimestamps, &tsEntry{pubkey: addr, slot: lastTs.Slot, timestamp: lastTs.Timestamp})
		}
	}

	slotDuration := duration.NewDurationFromNanos(nsPerSlot)
	epoch := epochSchedule.GetEpoch(slot)
	stakes := global.EpochStakes(epoch)
	timestampEstimate, err := calculateStakeWeightedTimestamp(recentTimestamps, stakes, slot, slotDuration, epochStartTimestampSlot, epochStartTimestamp)
	if err != nil {
		panic(err)
	}

	return timestampEstimate
}

func calculateStakeWeightedTimestamp(
	recentTimestamps []*tsEntry,
	stakes map[solana.PublicKey]uint64,
	slot uint64,
	slotDuration duration.Duration,
	epochStartSlot uint64,
	epochStartTimestamp int64) (int64, error) {

	var stakePerTimestamp btree.Map[int64, wide.Uint128]
	var totalStake wide.Uint128

	for _, r := range recentTimestamps {
		offset := slotDuration.SaturatingMul(uint32(safemath.SaturatingSubU64(slot, r.slot)))
		estimate := safemath.SaturatingAddI64(r.timestamp, int64(offset.Secs))

		stake := stakes[r.pubkey]
		add := wide.Uint128FromUint64(stake)

		if cur, ok := stakePerTimestamp.Get(estimate); ok {
			stakePerTimestamp.Set(estimate, cur.Add(add))
		} else {
			stakePerTimestamp.Set(estimate, add)
		}
		totalStake = totalStake.Add(add)
	}

	if totalStake.Eq(wide.Uint128{}) {
		return 0, fmt.Errorf("total stake == 0")
	}

	halfTotalStake := totalStake.Div(wide.Uint128FromUint64(2))
	var acc wide.Uint128
	var estimate int64

	it := stakePerTimestamp.Iter()
	if !it.First() {
		return 0, fmt.Errorf("no stakes")
	}
	for {
		ts := it.Key()
		stake := it.Value()
		acc = acc.Add(stake)
		if acc.Gt(halfTotalStake) {
			estimate = ts
			break
		}
		if !it.Next() {
			estimate = ts
			break
		}
	}

	pohEstimateOffset := slotDuration.SaturatingMul(uint32(safemath.SaturatingSubU64(slot, epochStartSlot)))

	estimateOffset := duration.NewDurationFromSecs(
		safemath.SaturatingSubU64(uint64(estimate), uint64(epochStartTimestamp)),
	)

	maxDriftFast := pohEstimateOffset.SaturatingMul(maxAllowableDriftFast).Div(100)
	maxDriftSlow := pohEstimateOffset.SaturatingMul(maxAllowableDriftSlow).Div(100)

	if estimateOffset.Gt(pohEstimateOffset) &&
		estimateOffset.SaturatingSub(pohEstimateOffset).Gt(maxDriftSlow) {
		estimate = safemath.SaturatingAddI64(
			safemath.SaturatingAddI64(epochStartTimestamp, int64(pohEstimateOffset.Secs)),
			int64(maxDriftSlow.Secs),
		)
	} else if estimateOffset.Lt(pohEstimateOffset) &&
		pohEstimateOffset.SaturatingSub(estimateOffset).Gt(maxDriftFast) {
		estimate = safemath.SaturatingSubI64(
			safemath.SaturatingAddI64(epochStartTimestamp, int64(pohEstimateOffset.Secs)),
			int64(maxDriftFast.Secs),
		)
	}

	return estimate, nil
}

// finalizeBankSysvars applies the bank-end updates that are shared by replay
// and local production.  The account overlay and the immutable bank sysvar
// snapshot are replaced together before either bank hashing or chain-tip
// publication, so no consumer can observe the pre-finalization bytes.
func finalizeBankSysvars(slotCtx *sealevel.SlotCtx) error {
	if slotCtx == nil {
		return fmt.Errorf("missing slot context while finalizing bank sysvars")
	}

	recentAcct, err := slotCtx.GetAccount(sealevel.SysvarRecentBlockHashesAddr)
	if err != nil {
		return fmt.Errorf("get RecentBlockhashes sysvar: %w", err)
	}
	var recent sealevel.SysvarRecentBlockhashes
	if bankSysvars := slotCtx.BankSysvars(); bankSysvars != nil {
		if cached, ok := bankSysvars.RecentBlockhashes(); ok {
			recent = append(sealevel.SysvarRecentBlockhashes(nil), cached...)
		}
	}
	if recent == nil {
		recent.MustUnmarshalWithDecoder(bin.NewBinDecoder(recentAcct.Data))
	}
	slotCtx.LatestEvictedBlockhash = recent.PushLatest(slotCtx.Blockhash, slotCtx.FeeRateGovernor.LamportsPerSignature)
	recentAcct.Data = recent.MustMarshal()

	historyAcct, err := slotCtx.GetAccount(sealevel.SysvarSlotHistoryAddr)
	if err != nil {
		return fmt.Errorf("get SlotHistory sysvar: %w", err)
	}
	var history sealevel.SysvarSlotHistory
	hasCachedHistory := false
	if bankSysvars := slotCtx.BankSysvars(); bankSysvars != nil {
		if cached, ok := bankSysvars.SlotHistory(); ok {
			history = cached
			history.Bits.Bits.Blocks = append([]uint64(nil), cached.Bits.Bits.Blocks...)
			hasCachedHistory = true
		}
	}
	if !hasCachedHistory {
		history.MustUnmarshalWithDecoder(bin.NewBinDecoder(historyAcct.Data))
	}
	history.Add(slotCtx.Slot)
	history.SetNextSlot(slotCtx.Slot + 1)
	historyAcct.Data = history.MustMarshal()

	if err := slotCtx.SetAccount(recentAcct.Key, recentAcct); err != nil {
		return fmt.Errorf("write RecentBlockhashes sysvar: %w", err)
	}
	if err := slotCtx.SetAccount(historyAcct.Key, historyAcct); err != nil {
		return fmt.Errorf("write SlotHistory sysvar: %w", err)
	}

	bankSysvars := slotCtx.BankSysvars()
	if bankSysvars == nil {
		bankSysvars, err = sealevel.NewBankSysvars(slotCtx.Slot, recentAcct, historyAcct)
	} else {
		bankSysvars, err = bankSysvars.WithAccounts(recentAcct, historyAcct)
	}
	if err != nil {
		return fmt.Errorf("update finalized bank sysvar snapshot: %w", err)
	}
	if err := slotCtx.PublishBankSysvars(bankSysvars); err != nil {
		return err
	}

	// The legacy singleton remains an ordered-replay bootstrap/checkpoint aid.
	// Never publish speculative producer state into it.
	if slotCtx.Replay {
		recentForLegacy := append(sealevel.SysvarRecentBlockhashes(nil), recent...)
		historyForLegacy := history
		historyForLegacy.Bits.Bits.Blocks = append([]uint64(nil), history.Bits.Bits.Blocks...)
		sealevel.SysvarCache.RecentBlockHashes.Sysvar = &recentForLegacy
		sealevel.SysvarCache.RecentBlockHashes.Acct = recentAcct.Clone()
		sealevel.SysvarCache.SlotHistory.Sysvar = &historyForLegacy
		sealevel.SysvarCache.SlotHistory.Acct = historyAcct.Clone()
	}
	return nil
}

func collectSysvarAcctsForAdh(slotCtx *sealevel.SlotCtx) []*accounts.Account {
	sysvarPubkeys := []solana.PublicKey{sealevel.SysvarClockAddr, sealevel.SysvarRecentBlockHashesAddr, sealevel.SysvarSlotHashesAddr, sealevel.SysvarSlotHistoryAddr}
	var sysvarAccts []*accounts.Account

	for _, pk := range sysvarPubkeys {
		acct, err := slotCtx.GetAccount(pk)
		if err != nil {
			panic(fmt.Sprintf("unable to get sysvar account for ADH: %s", pk))
		}

		sysvarAccts = append(sysvarAccts, acct)
	}
	return sysvarAccts
}

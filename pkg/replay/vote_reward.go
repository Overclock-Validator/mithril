package replay

import (
	"fmt"
	"math"
	"math/big"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/rewardcerts"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

var agMigrationEpochCredit = sealevel.EpochCredits{
	Epoch:       math.MaxUint64,
	Credits:     math.MaxUint64,
	PrevCredits: math.MaxUint64,
}

// alpenglowNsPerSlot is the fixed slot duration used when deriving reward/final
// LastTimestamp from footer producer time (200ms).
const alpenglowNsPerSlot = 200_000_000

// ApplyAlpenglowVoteRewards updates vote account state from validated footer reward and final certs.
func ApplyAlpenglowVoteRewards(
	slotCtx *sealevel.SlotCtx,
	block *b.Block,
	epochSchedule *sealevel.SysvarEpochSchedule,
	skipRaw, notarRaw, finalCertRaw []byte,
	shredVersion uint16,
) error {
	if len(skipRaw) == 0 && len(notarRaw) == 0 && len(finalCertRaw) == 0 {
		return nil
	}
	if !alpenglowClockFeatureActive(slotCtx.Features) {
		return nil
	}

	var rewardDetails *metrics.VoteRewardDetails
	if slotCtx.Replay {
		rewardDetails = &metrics.GlobalBlockReplay.VoteRewardDetails
	}
	var statePreparationDuration time.Duration

	var rewardValidators map[solana.PublicKey]struct{}
	var rewardSlot uint64
	var rewardEpoch uint64
	currentEpoch := block.Epoch
	var inflationState EpochInflationState
	var rewardEpochStakes map[solana.PublicKey]uint64
	var totalStake uint64
	var migrationEpoch uint64
	var leaderVote solana.PublicKey
	var leaderVoteOK bool

	if len(skipRaw) > 0 || len(notarRaw) > 0 {
		var stateStart time.Time
		if rewardDetails != nil {
			stateStart = time.Now()
		}
		var ok bool
		rewardSlot, ok = rewardcerts.RewardSlotForLeader(block.Slot)
		if !ok {
			return fmt.Errorf("slot %d vote rewards: invalid reward slot offset", block.Slot)
		}
		rewardEpoch = epochSchedule.GetEpoch(rewardSlot)
		if rewardDetails != nil {
			statePreparationDuration += time.Since(stateStart)
		}

		verifierMaterial, err := loadVoteRewardVerifierMaterial(rewardEpoch, shredVersion, rewardDetails)
		if err != nil {
			return fmt.Errorf("slot %d vote rewards: %w", block.Slot, err)
		}

		validated, validationTimings, err := verifierMaterial.validateRewardCertificates(
			block.Slot,
			skipRaw,
			notarRaw,
			rewardDetails != nil,
		)
		if rewardDetails != nil {
			if validationTimings.Skip > 0 {
				rewardDetails.SkipCertificateValidation.AddTiming(validationTimings.Skip)
			}
			if validationTimings.Notar > 0 {
				rewardDetails.NotarCertificateValidation.AddTiming(validationTimings.Notar)
			}
		}
		if err != nil {
			return fmt.Errorf("slot %d validate reward certs: %w", block.Slot, err)
		}
		if validated != nil {
			rewardValidators = validated.Validators
			rewardSlot = validated.RewardSlot
			if rewardDetails != nil {
				atomic.AddUint64(&rewardDetails.RewardValidators, uint64(len(rewardValidators)))
			}
		}

		if rewardDetails != nil {
			stateStart = time.Now()
		}
		inflationAcct, err := loadEpochInflationAccountStateForReplay(slotCtx)
		if err != nil {
			return fmt.Errorf("slot %d vote rewards: %w", block.Slot, err)
		}
		// Vote rewards are credited to the processing bank's epoch, so their
		// reward rate comes from that epoch too.  The stake distribution still
		// comes from rewardSlot (eight slots earlier).  These epochs differ for
		// the first eight banks after an epoch boundary.
		inflationState, err = voteRewardInflationState(inflationAcct, currentEpoch, rewardEpoch)
		if err != nil {
			return fmt.Errorf("slot %d vote rewards: %w", block.Slot, err)
		}

		migrationEpoch, err = alpenglowMigrationEpoch(block, epochSchedule)
		if err != nil {
			return fmt.Errorf("slot %d vote rewards: %w", block.Slot, err)
		}

		rewardEpochStakes = verifierMaterial.snapshot.Stakes
		totalStake = verifierMaterial.snapshot.TotalStake
		// The leader reward belongs to the current block's selected vote account,
		// while validator rewards use rewardSlot (eight slots earlier). At an epoch
		// boundary, a compatibility fallback must therefore use the leader epoch.
		leaderVoteAccounts := verifierMaterial.snapshot.VoteAccounts
		leaderEpoch := epochSchedule.GetEpoch(block.Slot)
		if leaderEpoch != rewardEpoch {
			if leaderStakes, ok := global.EpochStakesSnapshot(leaderEpoch); ok {
				leaderVoteAccounts = leaderStakes.VoteAccounts
			} else {
				leaderVoteAccounts = nil
			}
		}
		leaderVote, err = leaderVotePubkey(block.Slot, leaderVoteAccounts, block.Leader)
		if err != nil {
			return fmt.Errorf("slot %d vote rewards: %w", block.Slot, err)
		}
		leaderVoteOK = true

		if totalStake == 0 {
			for _, stake := range rewardEpochStakes {
				totalStake += stake
			}
		}
		if rewardDetails != nil {
			statePreparationDuration += time.Since(stateStart)
		}
	}

	var finalSigners map[solana.PublicKey]struct{}
	var finalSlot uint64
	if len(finalCertRaw) > 0 {
		var decodeStart time.Time
		if rewardDetails != nil {
			decodeStart = time.Now()
		}
		fc, err := rewardcerts.DecodeFinalCertificate(finalCertRaw)
		if rewardDetails != nil {
			rewardDetails.FinalCertificateDecode.AddTimingSince(decodeStart)
		}
		if err != nil {
			return fmt.Errorf("slot %d decode final cert: %w", block.Slot, err)
		}
		finalEpoch := epochSchedule.GetEpoch(fc.Slot)
		verifierMaterial, err := loadVoteRewardVerifierMaterial(finalEpoch, shredVersion, rewardDetails)
		if err != nil {
			return fmt.Errorf("slot %d final cert: %w", block.Slot, err)
		}
		var validationStart time.Time
		if rewardDetails != nil {
			validationStart = time.Now()
		}
		validatedFinal, err := rewardcerts.ValidateDecodedBlockFinalCertificateWithVerifier(
			fc,
			finalEpoch,
			verifierMaterial.verifier,
		)
		if rewardDetails != nil {
			rewardDetails.FinalCertificateValidation.AddTimingSince(validationStart)
		}
		if err != nil {
			return fmt.Errorf("slot %d validate final cert: %w", block.Slot, err)
		}
		if validatedFinal != nil {
			finalSigners = validatedFinal.Signers
			finalSlot = validatedFinal.FinalSlot
			if rewardDetails != nil {
				atomic.AddUint64(&rewardDetails.FinalSigners, uint64(len(finalSigners)))
			}
		}
	}

	if len(rewardValidators) == 0 && len(finalSigners) == 0 {
		return nil
	}

	var stateStart time.Time
	if rewardDetails != nil {
		stateStart = time.Now()
	}
	producerTimeNanos, ok, err := alpenglowFooterProducerTimeNanos(block)
	if err != nil {
		return fmt.Errorf("slot %d vote rewards: footer producer time: %w", block.Slot, err)
	}
	if !ok {
		return fmt.Errorf("slot %d vote rewards: missing footer producer time for LastTimestamp", block.Slot)
	}
	var rewardSlotTimestampNs int64
	if len(rewardValidators) > 0 {
		rewardSlotTimestampNs = calcSlotTimestampNanos(rewardSlot, block.Slot, producerTimeNanos)
	}
	var finalSlotTimestampNs int64
	if len(finalSigners) > 0 {
		finalSlotTimestampNs = calcSlotTimestampNanos(finalSlot, block.Slot, producerTimeNanos)
	}
	var accountMutationStart time.Time
	if rewardDetails != nil {
		statePreparationDuration += time.Since(stateStart)
		rewardDetails.StatePreparation.AddTiming(statePreparationDuration)
		accountMutationStart = time.Now()
	}

	var leaderRewardAccum uint64
	var voteAccountsUpdated int
	var leaderUpdatedInUnion bool

	for votePubkey := range unionVotePubkeys(rewardValidators, finalSigners) {
		// Read the live in-slot account (reflecting same-slot transaction writes such as a
		// Vote Withdraw) and fall back to the persisted parent state otherwise. Rewards are
		// then a normal in-slot account write: SetAccount + RecordModifiedAcct, and the
		// end-of-slot batch lt-hash computes -h(parent)+h(final) once from the true parent.
		acct, err := loadAccountLiveOrParentForReplay(slotCtx, votePubkey)
		if err != nil {
			mlog.Log.Infof("slot %d vote rewards: skip missing vote account %s: %v", block.Slot, votePubkey, err)
			continue
		}
		_, inReward := rewardValidators[votePubkey]
		_, inFinal := finalSigners[votePubkey]

		var applied bool
		if inReward {
			applied, err = applyVoteRewardToAccount(
				acct,
				rewardSlot,
				rewardSlotTimestampNs,
				currentEpoch,
				migrationEpoch,
				inflationState,
				totalStake,
				rewardEpochStakes[votePubkey],
				&leaderRewardAccum,
			)
			if err != nil {
				mlog.Log.Infof("slot %d vote rewards: skip %s: %v", block.Slot, votePubkey, err)
				continue
			}
		}
		if inFinal {
			if err := applyFinalCertToAccount(acct, finalSlot, finalSlotTimestampNs); err != nil {
				mlog.Log.Infof("slot %d vote rewards: skip %s: %v", block.Slot, votePubkey, err)
				continue
			}
			applied = true
		}
		if !applied {
			continue
		}
		if err := slotCtx.SetAccount(votePubkey, acct); err != nil {
			return fmt.Errorf("slot %d vote rewards: set account %s: %w", block.Slot, votePubkey, err)
		}
		slotCtx.RecordModifiedAcct(votePubkey)
		voteAccountsUpdated++
		if votePubkey == leaderVote {
			leaderUpdatedInUnion = true
		}
	}

	if leaderRewardAccum > 0 {
		if !leaderVoteOK {
			return fmt.Errorf("slot %d vote rewards: leader vote account not found for %s", block.Slot, block.Leader)
		}
		// Read the live account so the leader reward stacks on top of any update the union loop
		// (or a same-slot transaction) already applied to the leader's vote account, mirroring
		// single-map Occupied/Vacant handling.
		acct, err := loadAccountLiveOrParentForReplay(slotCtx, leaderVote)
		if err != nil {
			return fmt.Errorf("slot %d vote rewards: load leader vote %s: %w", block.Slot, leaderVote, err)
		}
		applied, err := applyLeaderVoteReward(acct, currentEpoch, migrationEpoch, leaderRewardAccum)
		if err != nil {
			return fmt.Errorf("slot %d vote rewards: leader vote %s: %w", block.Slot, leaderVote, err)
		}
		if !applied {
			return fmt.Errorf("slot %d vote rewards: leader vote %s: apply returned false", block.Slot, leaderVote)
		}
		if err := slotCtx.SetAccount(leaderVote, acct); err != nil {
			return fmt.Errorf("slot %d vote rewards: set leader vote %s: %w", block.Slot, leaderVote, err)
		}
		slotCtx.RecordModifiedAcct(leaderVote)
		if !leaderUpdatedInUnion {
			voteAccountsUpdated++
		}
	}

	if rewardDetails != nil {
		rewardDetails.AccountMutation.AddTimingSince(accountMutationStart)
		atomic.AddUint64(&rewardDetails.VoteAccountsUpdated, uint64(voteAccountsUpdated))
	}
	return nil
}

func loadAccountLiveOrParentForReplay(slotCtx *sealevel.SlotCtx, pubkey solana.PublicKey) (*accounts.Account, error) {
	if slotCtx == nil {
		return nil, fmt.Errorf("missing slot context")
	}
	if acct, err := slotCtx.GetAccount(pubkey); err == nil && acct != nil {
		return acct, nil
	}
	acct, err := slotCtx.GetAccountFromAccountsDb(pubkey)
	if err != nil {
		return nil, err
	}
	return acct.Clone(), nil
}

func unionVotePubkeys(rewardValidators, finalSigners map[solana.PublicKey]struct{}) map[solana.PublicKey]struct{} {
	if len(rewardValidators) == 0 {
		return finalSigners
	}
	if len(finalSigners) == 0 {
		return rewardValidators
	}
	out := make(map[solana.PublicKey]struct{}, len(rewardValidators)+len(finalSigners))
	for pk := range rewardValidators {
		out[pk] = struct{}{}
	}
	for pk := range finalSigners {
		out[pk] = struct{}{}
	}
	return out
}

func alpenglowMigrationEpoch(block *b.Block, epochSchedule *sealevel.SysvarEpochSchedule) (uint64, error) {
	if block.Features == nil {
		return 0, fmt.Errorf("missing feature set")
	}
	slot, ok := block.Features.ActivationSlot(features.Alpenglow)
	if !ok {
		return 0, fmt.Errorf("alpenglow feature not active")
	}
	return epochSchedule.GetEpoch(slot), nil
}

func leaderVotePubkey(
	leaderSlot uint64,
	voteAccounts map[solana.PublicKey]*epochstakes.VoteAccount,
	leaderNode solana.PublicKey,
) (solana.PublicKey, error) {
	if scheduledNode, scheduledVote, ok := global.LeaderForSlotWithVoteAccount(leaderSlot); ok {
		if scheduledNode != leaderNode {
			return solana.PublicKey{}, fmt.Errorf(
				"scheduled leader %s does not match block leader %s for slot %d",
				scheduledNode, leaderNode, leaderSlot,
			)
		}
		return scheduledVote, nil
	}

	var leaderVote solana.PublicKey
	found := false
	for pk, va := range voteAccounts {
		if va != nil && va.NodePubkey == leaderNode {
			if found {
				return solana.PublicKey{}, fmt.Errorf(
					"leader vote account is ambiguous for node %s without vote-keyed schedule metadata",
					leaderNode,
				)
			}
			leaderVote = pk
			found = true
		}
	}
	if !found {
		return solana.PublicKey{}, fmt.Errorf("leader vote account not found for %s", leaderNode)
	}
	return leaderVote, nil
}

func applyVoteRewardToAccount(
	acct *accounts.Account,
	rewardSlot uint64,
	rewardSlotTimestampNs int64,
	currentEpoch, migrationEpoch uint64,
	inflation EpochInflationState,
	totalStake, validatorStake uint64,
	leaderRewardAccum *uint64,
) (bool, error) {
	versioned, err := sealevel.UnmarshalVersionedVoteState(acct.Data)
	if err != nil {
		return false, err
	}
	if versioned.Type != sealevel.VoteStateVersionV4 {
		return false, fmt.Errorf("unsupported vote state version %d", versioned.Type)
	}

	maybeUpdateVotesV4(&versioned.V4, rewardSlot, rewardSlotTimestampNs)
	validatorReward, leaderReward := calculateAlpenglowReward(inflation, totalStake, validatorStake)
	*leaderRewardAccum += leaderReward
	if validatorReward > 0 {
		incrementAlpenglowCredits(&versioned.V4.EpochCredits, migrationEpoch, currentEpoch, validatorReward)
	}

	if err := sealevel.WriteVersionedVoteStateInPlace(acct.Data, versioned); err != nil {
		return false, err
	}
	return true, nil
}

func applyLeaderVoteReward(
	acct *accounts.Account,
	currentEpoch, migrationEpoch, leaderReward uint64,
) (bool, error) {
	versioned, err := sealevel.UnmarshalVersionedVoteState(acct.Data)
	if err != nil {
		return false, err
	}
	if versioned.Type != sealevel.VoteStateVersionV4 {
		return false, fmt.Errorf("unsupported vote state version %d", versioned.Type)
	}
	incrementAlpenglowCredits(&versioned.V4.EpochCredits, migrationEpoch, currentEpoch, leaderReward)
	if err := sealevel.WriteVersionedVoteStateInPlace(acct.Data, versioned); err != nil {
		return false, err
	}
	return true, nil
}

// calcSlotTimestampNanos returns producer_ns - duration(targetSlot+1..=bankSlot).
// Alpenglow community cluster uses 200ms slots for this derivation.
func calcSlotTimestampNanos(targetSlot, bankSlot uint64, producerTimeNanos int64) int64 {
	if targetSlot >= bankSlot {
		return producerTimeNanos
	}
	slots := bankSlot - targetSlot
	duration := int64(slots) * int64(alpenglowNsPerSlot)
	if producerTimeNanos < duration {
		return 0
	}
	return producerTimeNanos - duration
}

func maybeUpdateVotesV4(vs *sealevel.VoteState4, slot uint64, slotTimestampNs int64) {
	latest := slot
	if last, ok := lastVotedSlotV4(vs); ok && last > latest {
		latest = last
	}
	if vs.RootSlot != nil && *vs.RootSlot > latest {
		latest = *vs.RootSlot
	}
	vs.Votes.Clear()
	vs.Votes.SetBaseCap(1)
	vs.Votes.PushBack(sealevel.LandedVote{
		Latency: 0,
		Lockout: sealevel.VoteLockout{Slot: latest, ConfirmationCount: 1},
	})

	// Advance last_timestamp when the derived slot wall-clock second is strictly newer.
	timestamp := slotTimestampNs / 1_000_000_000
	if timestamp > vs.LastTimestamp.Timestamp {
		vs.LastTimestamp = sealevel.BlockTimestamp{Slot: slot, Timestamp: timestamp}
	}
}

func applyFinalCertToAccount(acct *accounts.Account, finalSlot uint64, finalSlotTimestampNs int64) error {
	versioned, err := sealevel.UnmarshalVersionedVoteState(acct.Data)
	if err != nil {
		return err
	}
	if versioned.Type != sealevel.VoteStateVersionV4 {
		return fmt.Errorf("unsupported vote state version %d", versioned.Type)
	}
	maybeUpdateRootV4(&versioned.V4, finalSlot)
	maybeUpdateVotesV4(&versioned.V4, finalSlot, finalSlotTimestampNs)
	return sealevel.WriteVersionedVoteStateInPlace(acct.Data, versioned)
}

func maybeUpdateRootV4(vs *sealevel.VoteState4, slot uint64) {
	latestRoot := slot
	if vs.RootSlot != nil && *vs.RootSlot > latestRoot {
		latestRoot = *vs.RootSlot
	}
	vs.RootSlot = &latestRoot
}

func lastVotedSlotV4(vs *sealevel.VoteState4) (uint64, bool) {
	if vs.Votes.Len() == 0 {
		return 0, false
	}
	return vs.Votes.At(vs.Votes.Len() - 1).Lockout.Slot, true
}

func calculateAlpenglowReward(inflation EpochInflationState, totalStake, validatorStake uint64) (validatorReward, leaderReward uint64) {
	if totalStake == 0 || validatorStake == 0 || inflation.SlotsPerEpoch == 0 {
		return 0, 0
	}
	num := new(big.Int).SetUint64(inflation.MaxPossibleValidatorReward)
	num.Mul(num, new(big.Int).SetUint64(validatorStake))
	den := new(big.Int).SetUint64(inflation.SlotsPerEpoch)
	den.Mul(den, new(big.Int).SetUint64(totalStake))
	if den.Sign() == 0 {
		return 0, 0
	}
	rewardLamports := new(big.Int).Div(num, den).Uint64()
	validatorReward = rewardLamports / 2
	leaderReward = rewardLamports - validatorReward
	return validatorReward, leaderReward
}

// voteRewardInflationState deliberately accepts both epochs to make the
// boundary rule explicit: delayed certificates use rewardEpoch's stake view,
// but rewards are credited using the processing bank epoch's inflation budget.
func voteRewardInflationState(
	state EpochInflationAccountState,
	processingEpoch, rewardEpoch uint64,
) (EpochInflationState, error) {
	inflation, ok := state.epochState(processingEpoch)
	if !ok {
		return EpochInflationState{}, fmt.Errorf(
			"missing epoch inflation for processing epoch %d (reward slot epoch %d)",
			processingEpoch,
			rewardEpoch,
		)
	}
	return inflation, nil
}

func incrementAlpenglowCredits(epochCredits *[]sealevel.EpochCredits, migrationEpoch, epoch, newCredits uint64) {
	if newCredits == 0 {
		return
	}
	if epoch == migrationEpoch {
		ensureMigrationMarker(epochCredits)
	}

	if len(*epochCredits) == 0 {
		*epochCredits = append(*epochCredits, sealevel.EpochCredits{Epoch: epoch, Credits: newCredits, PrevCredits: 0})
		return
	}

	last := &(*epochCredits)[len(*epochCredits)-1]
	if isMigrationMarker(*last) {
		var finalTowerCredits uint64
		if len(*epochCredits) >= 2 {
			prev := (*epochCredits)[len(*epochCredits)-2]
			finalTowerCredits = prev.Credits
		}
		*epochCredits = append(*epochCredits, sealevel.EpochCredits{
			Epoch:       epoch,
			Credits:     newCredits + finalTowerCredits,
			PrevCredits: finalTowerCredits,
		})
		trimEpochCreditsHistory(epochCredits)
		return
	}

	if last.Epoch == epoch {
		last.Credits += newCredits
		return
	}

	if last.Credits == last.PrevCredits {
		last.Epoch = epoch
		last.Credits += newCredits
		return
	}

	finalCredits := last.Credits
	*epochCredits = append(*epochCredits, sealevel.EpochCredits{
		Epoch:       epoch,
		Credits:     newCredits + finalCredits,
		PrevCredits: finalCredits,
	})
	trimEpochCreditsHistory(epochCredits)
}

func ensureMigrationMarker(epochCredits *[]sealevel.EpochCredits) {
	for i := len(*epochCredits) - 1; i >= 0; i-- {
		if isMigrationMarker((*epochCredits)[i]) {
			return
		}
	}
	*epochCredits = append(*epochCredits, agMigrationEpochCredit)
}

func isMigrationMarker(entry sealevel.EpochCredits) bool {
	return entry.Epoch == agMigrationEpochCredit.Epoch &&
		entry.Credits == agMigrationEpochCredit.Credits &&
		entry.PrevCredits == agMigrationEpochCredit.PrevCredits
}

func trimEpochCreditsHistory(epochCredits *[]sealevel.EpochCredits) {
	for len(*epochCredits) > sealevel.MaxEpochCreditsHistory {
		*epochCredits = (*epochCredits)[1:]
	}
}

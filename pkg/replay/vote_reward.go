package replay

import (
	"fmt"
	"math"
	"math/big"
	"runtime"
	"sync"
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
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

// Vote-account reward updates split across workers once a block rewards at
// least two workers' worth of accounts (about 660 per mainnet block today).
const voteAccountsPerWorker = 64

// maxVoteAccountWorkers caps the workers; tests set it to 1 for the serial reference.
var maxVoteAccountWorkers = 8

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
	if slotCtx.Features == nil || !(slotCtx.Features.IsActive(features.Alpenglow) || slotCtx.Features.IsActive(features.AlpenglowDevContext)) {
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
		// The pre-consensus preflight may already have validated these exact inputs
		// in the background; otherwise validate here as before.
		validatedFinal, prefetched := takeFinalCertPrefetch(block.Slot, finalCertRaw, finalEpoch, verifierMaterial)
		if !prefetched {
			validatedFinal, err = rewardcerts.ValidateDecodedBlockFinalCertificateWithVerifier(
				fc,
				finalEpoch,
				verifierMaterial.verifier,
			)
		}
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

	leaderRewardAccum, voteAccountsUpdated, leaderUpdatedInUnion, err := applyVoteAccountUpdates(slotCtx, &voteAccountUpdate{
		slot:                  block.Slot,
		rewardValidators:      rewardValidators,
		finalSigners:          finalSigners,
		rewardSlot:            rewardSlot,
		rewardSlotTimestampNs: rewardSlotTimestampNs,
		finalSlot:             finalSlot,
		finalSlotTimestampNs:  finalSlotTimestampNs,
		currentEpoch:          currentEpoch,
		migrationEpoch:        migrationEpoch,
		inflation:             inflationState,
		totalStake:            totalStake,
		stakes:                rewardEpochStakes,
		leaderVote:            leaderVote,
	})
	if err != nil {
		return err
	}

	if leaderRewardAccum > 0 {
		if !leaderVoteOK {
			return fmt.Errorf("slot %d vote rewards: leader vote account not found for %s", block.Slot, block.Leader)
		}
		// Read the live account so the leader reward stacks on top of any update the union loop
		// (or a same-slot transaction) already applied to the leader's vote account, mirroring
		// single-map Occupied/Vacant handling.
		acct, parent, err := loadAccountLiveOrParentForReplay(slotCtx, leaderVote)
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
		if parent != nil {
			recordParentImage(slotCtx, leaderVote, parent)
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

// voteAccountUpdate is the per-block input for crediting vote accounts.
type voteAccountUpdate struct {
	slot                                        uint64
	rewardValidators, finalSigners              map[solana.PublicKey]struct{}
	rewardSlot, finalSlot                       uint64
	rewardSlotTimestampNs, finalSlotTimestampNs int64
	currentEpoch, migrationEpoch                uint64
	inflation                                   EpochInflationState
	totalStake                                  uint64
	stakes                                      map[solana.PublicKey]uint64
	leaderVote                                  solana.PublicKey
}

// applyVoteAccountUpdates credits every rewarded or final-certificate vote
// account and records it as modified. It returns the leader's accumulated
// reward, the number of accounts written and whether the leader's vote account
// was one of them.
func applyVoteAccountUpdates(slotCtx *sealevel.SlotCtx, u *voteAccountUpdate) (uint64, int, bool, error) {
	var leaderRewardAccum uint64
	var voteAccountsUpdated int
	var leaderUpdatedInUnion bool

	// updateVoteAccount applies the reward and/or final-certificate credit to one
	// vote account and stores it. It returns the account, nil when it is not
	// written.
	updateVoteAccount := func(votePubkey solana.PublicKey, leaderAccum *uint64) (*creditedVoteAccount, error) {
		// Read the live in-slot account (reflecting same-slot transaction writes such as a
		// Vote Withdraw) and fall back to the persisted parent state otherwise. Rewards are
		// then a normal in-slot account write: SetAccount + RecordModifiedAcct, and the
		// end-of-slot batch lt-hash computes -h(parent)+h(final) once from the true parent.
		acct, parent, err := loadAccountLiveOrParentForReplay(slotCtx, votePubkey)
		if err != nil {
			mlog.Log.Infof("slot %d vote rewards: skip missing vote account %s: %v", u.slot, votePubkey, err)
			return nil, nil
		}
		_, inReward := u.rewardValidators[votePubkey]
		_, inFinal := u.finalSigners[votePubkey]

		var applied bool
		if inReward && inFinal {
			applied, err = applyVoteRewardAndFinalCertToAccount(
				acct, u.rewardSlot, u.rewardSlotTimestampNs, u.finalSlot, u.finalSlotTimestampNs,
				u.currentEpoch, u.migrationEpoch, u.inflation, u.totalStake,
				u.stakes[votePubkey], leaderAccum,
			)
			if err != nil {
				mlog.Log.Infof("slot %d vote rewards: skip %s: %v", u.slot, votePubkey, err)
				return nil, nil
			}
		} else if inReward {
			applied, err = applyVoteRewardToAccount(
				acct,
				u.rewardSlot,
				u.rewardSlotTimestampNs,
				u.currentEpoch,
				u.migrationEpoch,
				u.inflation,
				u.totalStake,
				u.stakes[votePubkey],
				leaderAccum,
			)
			if err != nil {
				mlog.Log.Infof("slot %d vote rewards: skip %s: %v", u.slot, votePubkey, err)
				return nil, nil
			}
		}
		if inFinal && !inReward {
			if err := applyFinalCertToAccount(acct, u.finalSlot, u.finalSlotTimestampNs); err != nil {
				mlog.Log.Infof("slot %d vote rewards: skip %s: %v", u.slot, votePubkey, err)
				return nil, nil
			}
			applied = true
		}
		if !applied {
			return nil, nil
		}
		if err := slotCtx.SetAccount(votePubkey, acct); err != nil {
			return nil, fmt.Errorf("slot %d vote rewards: set account %s: %w", u.slot, votePubkey, err)
		}
		return &creditedVoteAccount{key: votePubkey, parent: parent}, nil
	}

	// Vote accounts are independent: each is read, updated and stored on its own,
	// through the sharded in-slot overlay. The only shared value is the leader's
	// reward sum, and its wrapping uint64 additions commute, so per-worker partial
	// sums give the serial result. Parent images go to the unsharded parent set,
	// so they are recorded after the join. This runs after the block's
	// transactions, when replay's workers are otherwise idle.
	union := unionVotePubkeys(u.rewardValidators, u.finalSigners)
	votePubkeys := make([]solana.PublicKey, 0, len(union))
	for votePubkey := range union {
		votePubkeys = append(votePubkeys, votePubkey)
	}
	workers := 1
	if len(votePubkeys) >= 2*voteAccountsPerWorker {
		workers = min(runtime.GOMAXPROCS(0), maxVoteAccountWorkers, len(votePubkeys)/voteAccountsPerWorker)
	}
	type voteAccountChunk struct {
		leaderReward uint64
		written      []creditedVoteAccount
		err          error
	}
	chunks := make([]voteAccountChunk, workers)
	updateChunk := func(w int) {
		chunk := &chunks[w]
		keys := votePubkeys[w*len(votePubkeys)/workers : (w+1)*len(votePubkeys)/workers]
		chunk.written = make([]creditedVoteAccount, 0, len(keys))
		for _, votePubkey := range keys {
			credited, err := updateVoteAccount(votePubkey, &chunk.leaderReward)
			if err != nil {
				chunk.err = err
				return
			}
			if credited != nil {
				chunk.written = append(chunk.written, *credited)
			}
		}
	}
	if workers == 1 {
		updateChunk(0)
	} else {
		var wg sync.WaitGroup
		for w := 1; w < workers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				updateChunk(w)
			}(w)
		}
		updateChunk(0)
		wg.Wait()
	}
	for i := range chunks {
		if chunks[i].err != nil {
			return 0, 0, false, chunks[i].err
		}
	}
	for i := range chunks {
		leaderRewardAccum += chunks[i].leaderReward
		for _, update := range chunks[i].written {
			if update.parent != nil {
				recordParentImage(slotCtx, update.key, update.parent)
			}
			slotCtx.RecordModifiedAcct(update.key)
			voteAccountsUpdated++
			if update.key == u.leaderVote {
				leaderUpdatedInUnion = true
			}
		}
	}
	return leaderRewardAccum, voteAccountsUpdated, leaderUpdatedInUnion, nil
}

// creditedVoteAccount is a written vote account, with the parent value its
// new value was derived from when that came from the account source.
type creditedVoteAccount struct {
	key    solana.PublicKey
	parent *accounts.Account
}

// loadAccountLiveOrParentForReplay returns a mutable copy of the account as
// the bank currently sees it: the in-slot value when this slot wrote it, else
// the parent value, which it then also returns as read so the caller can
// record it as the account's parent image (see recordParentImage). It only
// reads the bank, so it is safe for concurrent use.
func loadAccountLiveOrParentForReplay(slotCtx *sealevel.SlotCtx, pubkey solana.PublicKey) (acct, parent *accounts.Account, err error) {
	if slotCtx == nil {
		return nil, nil, fmt.Errorf("missing slot context")
	}
	if acct, err := slotCtx.GetAccount(pubkey); err == nil && acct != nil {
		return acct, nil, nil
	}
	parent, err = slotCtx.GetAccountFromAccountsDb(pubkey)
	if err != nil {
		return nil, nil, err
	}
	return parent.Clone(), parent, nil
}

// recordParentImage keeps parent, read from the account source for an
// account the bank is about to write, as that account's parent image when the
// bank has none yet. ensureParentAccountsForModified would otherwise read and
// copy it again for the LtHash, the only reader of parent images of accounts
// no transaction loaded; without the LtHash it is skipped, as that function
// is. parent must never be mutated: callers modify a copy.
func recordParentImage(slotCtx *sealevel.SlotCtx, pubkey solana.PublicKey, parent *accounts.Account) {
	if slotCtx.ParentAccts == nil || slotCtx.Features == nil || !slotCtx.Features.IsActive(features.AccountsLtHash) {
		return
	}
	key := [32]byte(pubkey)
	if _, err := slotCtx.ParentAccts.GetAccount(&key); err == nil {
		return
	}
	_ = slotCtx.ParentAccts.SetAccount(&key, parent)
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

// Decoding into a new VoteStateVersions allocates every version layout (about
// 6.6 KB) for a 344-byte V4 state, once or twice per credited vote account in
// every block. The reward path decodes into pooled zeroed values instead; none
// is retained past the in-place write.
var voteStateScratch = sync.Pool{New: func() any { return new(sealevel.VoteStateVersions) }}

// decodeVoteStateScratch matches sealevel.UnmarshalVersionedVoteState. The
// caller returns the value to voteStateScratch once it is done with it.
func decodeVoteStateScratch(data []byte) (*sealevel.VoteStateVersions, error) {
	versioned := voteStateScratch.Get().(*sealevel.VoteStateVersions)
	*versioned = sealevel.VoteStateVersions{}
	if err := versioned.UnmarshalWithDecoder(bin.NewBinDecoder(data)); err != nil {
		voteStateScratch.Put(versioned)
		return nil, sealevel.InstrErrInvalidAccountData
	}
	return versioned, nil
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
	versioned, err := decodeVoteStateScratch(acct.Data)
	if err != nil {
		return false, err
	}
	defer voteStateScratch.Put(versioned)
	if versioned.Type != sealevel.VoteStateVersionV4 {
		return false, fmt.Errorf("unsupported vote state version %d", versioned.Type)
	}

	applyVoteRewardToState(&versioned.V4, rewardSlot, rewardSlotTimestampNs,
		currentEpoch, migrationEpoch, inflation, totalStake, validatorStake, leaderRewardAccum)

	if err := sealevel.WriteVersionedVoteStateInPlace(acct.Data, versioned); err != nil {
		return false, err
	}
	return true, nil
}

func applyVoteRewardToState(
	state *sealevel.VoteState4,
	rewardSlot uint64, rewardSlotTimestampNs int64,
	currentEpoch, migrationEpoch uint64,
	inflation EpochInflationState,
	totalStake, validatorStake uint64,
	leaderRewardAccum *uint64,
) {
	maybeUpdateVotesV4(state, rewardSlot, rewardSlotTimestampNs)
	validatorReward, leaderReward := calculateAlpenglowReward(inflation, totalStake, validatorStake)
	*leaderRewardAccum += leaderReward
	if validatorReward > 0 {
		incrementAlpenglowCredits(&state.EpochCredits, migrationEpoch, currentEpoch, validatorReward)
	}
}

// applyVoteRewardAndFinalCertToAccount applies overlapping certificate updates in
// reward-then-finalization order.
func applyVoteRewardAndFinalCertToAccount(
	acct *accounts.Account,
	rewardSlot uint64, rewardSlotTimestampNs int64,
	finalSlot uint64, finalSlotTimestampNs int64,
	currentEpoch, migrationEpoch uint64,
	inflation EpochInflationState,
	totalStake, validatorStake uint64,
	leaderRewardAccum *uint64,
) (bool, error) {
	versioned, err := decodeVoteStateScratch(acct.Data)
	if err != nil {
		return false, err
	}
	defer voteStateScratch.Put(versioned)
	if versioned.Type != sealevel.VoteStateVersionV4 {
		return false, fmt.Errorf("unsupported vote state version %d", versioned.Type)
	}
	leaderBefore := *leaderRewardAccum
	applyVoteRewardToState(&versioned.V4, rewardSlot, rewardSlotTimestampNs,
		currentEpoch, migrationEpoch, inflation, totalStake, validatorStake, leaderRewardAccum)
	maybeUpdateRootV4(&versioned.V4, finalSlot)
	maybeUpdateVotesV4(&versioned.V4, finalSlot, finalSlotTimestampNs)
	if err := sealevel.WriteVersionedVoteStateInPlace(acct.Data, versioned); err == nil {
		return true, nil
	}

	// Finalization keeps the single vote and credit history sizes unchanged and
	// can only grow the root option. Thus a successful combined write also means
	// the intermediate reward state would fit. On failure, the writer leaves data
	// untouched: replay the original sequence to retain its exact error, partial
	// account write and leader accumulation behavior for undersized accounts.
	*leaderRewardAccum = leaderBefore
	if _, err := applyVoteRewardToAccount(acct, rewardSlot, rewardSlotTimestampNs,
		currentEpoch, migrationEpoch, inflation, totalStake, validatorStake, leaderRewardAccum); err != nil {
		return false, err
	}
	if err := applyFinalCertToAccount(acct, finalSlot, finalSlotTimestampNs); err != nil {
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
	versioned, err := decodeVoteStateScratch(acct.Data)
	if err != nil {
		return err
	}
	defer voteStateScratch.Put(versioned)
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

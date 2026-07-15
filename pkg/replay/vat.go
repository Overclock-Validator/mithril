package replay

import (
	"fmt"
	"math"
	"sort"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

const (
	// MaxAlpenglowVoteAccounts is the SIMD-0357 validator admission cap.
	MaxAlpenglowVoteAccounts = 2000
	// VATToBurnPerEpochLamports is the fixed Validator Admission Ticket (1.6 SOL).
	VATToBurnPerEpochLamports = 1_600_000_000
)

type vatVoteCandidate struct {
	votePubkey solana.PublicKey
	stake      uint64
	lamports   uint64
	meta       epochstakes.VoteAccount
}

// useAlpenglowVAT reports whether Alpenglow VAT collection/filtering is active.
func useAlpenglowVAT(alpenglowReplayMode bool, f *features.Features) bool {
	return useAlpenglowClockSemantics(alpenglowReplayMode, f)
}

func minimumVoteAccountBalanceForVAT(rent *sealevel.SysvarRent, alpenglowVATActive bool) uint64 {
	minimum := rent.MinimumBalance(sealevel.VoteStateV3Size)
	if alpenglowVATActive {
		return minimum + VATToBurnPerEpochLamports
	}
	return minimum
}

func loadRentSysvar(acctsDb *accountsdb.AccountsDb, slotCtx *sealevel.SlotCtx) (sealevel.SysvarRent, error) {
	if sealevel.SysvarCache.Rent.Sysvar != nil {
		return *sealevel.SysvarCache.Rent.Sysvar, nil
	}
	rentAcct, err := slotCtx.GetAccount(sealevel.SysvarRentAddr)
	if err != nil {
		rentAcct, err = acctsDb.GetAccount(slotCtx.Slot, sealevel.SysvarRentAddr)
		if err != nil {
			return sealevel.SysvarRent{}, fmt.Errorf("load rent sysvar: %w", err)
		}
	}
	var rent sealevel.SysvarRent
	decoder := bin.NewBinDecoder(rentAcct.Data)
	if err := rent.UnmarshalWithDecoder(decoder); err != nil {
		return sealevel.SysvarRent{}, fmt.Errorf("decode rent sysvar: %w", err)
	}
	return rent, nil
}

func hasNonZeroBLSPubkey(voteState *sealevel.VoteStateVersions) bool {
	if voteState == nil {
		return false
	}
	bls := voteState.BlsPubkeyCompressed()
	if bls == nil {
		return false
	}
	var zero [48]byte
	return *bls != zero
}

func filterVoteAccountsForVAT(
	acctsDb *accountsdb.AccountsDb,
	slot uint64,
	effectiveStakes map[solana.PublicKey]uint64,
	voteCache map[solana.PublicKey]*sealevel.VoteStateVersions,
	minimumBalance uint64,
	maxAccounts int,
) (map[solana.PublicKey]uint64, map[solana.PublicKey]*epochstakes.VoteAccount, uint64) {
	lamportsByVote := make(map[solana.PublicKey]uint64, len(effectiveStakes))
	for votePubkey := range effectiveStakes {
		voteAcct, err := acctsDb.GetAccount(slot, votePubkey)
		if err != nil {
			continue
		}
		lamportsByVote[votePubkey] = voteAcct.Lamports
	}
	return selectVATVoteAccounts(effectiveStakes, voteCache, lamportsByVote, minimumBalance, maxAccounts)
}

func selectVATVoteAccounts(
	effectiveStakes map[solana.PublicKey]uint64,
	voteCache map[solana.PublicKey]*sealevel.VoteStateVersions,
	lamportsByVote map[solana.PublicKey]uint64,
	minimumBalance uint64,
	maxAccounts int,
) (map[solana.PublicKey]uint64, map[solana.PublicKey]*epochstakes.VoteAccount, uint64) {
	candidates := buildVATCandidates(effectiveStakes, voteCache, lamportsByVote, minimumBalance)
	selected := capVATCandidatesByStake(candidates, maxAccounts)

	filteredStakes := make(map[solana.PublicKey]uint64, len(selected))
	filteredVoteAccts := make(map[solana.PublicKey]*epochstakes.VoteAccount, len(selected))
	var totalStake uint64
	for _, candidate := range selected {
		filteredStakes[candidate.votePubkey] = candidate.stake
		meta := candidate.meta
		filteredVoteAccts[candidate.votePubkey] = &meta
		totalStake += candidate.stake
	}
	return filteredStakes, filteredVoteAccts, totalStake
}

func buildVATCandidates(
	effectiveStakes map[solana.PublicKey]uint64,
	voteCache map[solana.PublicKey]*sealevel.VoteStateVersions,
	lamportsByVote map[solana.PublicKey]uint64,
	minimumBalance uint64,
) []vatVoteCandidate {
	candidates := make([]vatVoteCandidate, 0, len(effectiveStakes))
	for votePubkey, stake := range effectiveStakes {
		if stake == 0 {
			continue
		}
		voteState := voteCache[votePubkey]
		if !hasNonZeroBLSPubkey(voteState) {
			continue
		}
		lamports, ok := lamportsByVote[votePubkey]
		if !ok || lamports < minimumBalance {
			continue
		}
		candidates = append(candidates, vatVoteCandidate{
			votePubkey: votePubkey,
			stake:      stake,
			lamports:   lamports,
			meta: epochstakes.VoteAccount{
				Lamports:            lamports,
				NodePubkey:          voteState.NodePubkey(),
				BlsPubkeyCompressed: voteState.BlsPubkeyCompressed(),
			},
		})
	}
	return candidates
}

func capVATCandidatesByStake(candidates []vatVoteCandidate, maxAccounts int) []vatVoteCandidate {
	if len(candidates) <= maxAccounts {
		return candidates
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].stake != candidates[j].stake {
			return candidates[i].stake > candidates[j].stake
		}
		return candidates[i].votePubkey.String() < candidates[j].votePubkey.String()
	})
	cutoffStake := candidates[maxAccounts].stake
	selected := make([]vatVoteCandidate, 0, maxAccounts)
	for _, candidate := range candidates {
		if candidate.stake > cutoffStake {
			selected = append(selected, candidate)
		}
	}
	return selected
}

func burnAlpenglowVAT(
	acctsDb *accountsdb.AccountsDb,
	prevSlotCtx *sealevel.SlotCtx,
	slot uint64,
	votePubkeys []solana.PublicKey,
	burnAmount uint64,
) ([]*accounts.Account, []*accounts.Account, uint64, error) {
	if len(votePubkeys) == 0 {
		return nil, nil, 0, nil
	}

	updated := make([]*accounts.Account, 0, len(votePubkeys)+1)
	parentUpdated := make([]*accounts.Account, 0, len(votePubkeys)+1)
	var totalBurned uint64

	for _, votePubkey := range votePubkeys {
		parentAcct, err := loadAccountForVATBurn(acctsDb, prevSlotCtx, votePubkey)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("load vote account %s for VAT burn: %w", votePubkey, err)
		}
		newAcct, err := debitVoteAccountForVAT(parentAcct, burnAmount)
		if err != nil {
			return nil, nil, 0, err
		}
		parentUpdated = append(parentUpdated, parentAcct)
		updated = append(updated, newAcct)
		totalBurned += burnAmount
	}

	parentIncinerator, err := loadAccountForVATBurn(acctsDb, prevSlotCtx, a.IncineratorAddr)
	if err != nil {
		parentIncinerator = &accounts.Account{
			Key:       a.IncineratorAddr,
			Owner:     a.SystemProgramAddr,
			RentEpoch: math.MaxUint64,
		}
	}
	newIncinerator := creditIncineratorForVAT(parentIncinerator, totalBurned)
	parentUpdated = append(parentUpdated, parentIncinerator)
	updated = append(updated, newIncinerator)

	if !acctsDb.RootedDurable {
		if err := acctsDb.StoreAccounts(updated, slot, nil); err != nil {
			return nil, nil, 0, fmt.Errorf("store VAT burn accounts: %w", err)
		}
	}

	return updated, parentUpdated, totalBurned, nil
}

func loadAccountForVATBurn(acctsDb *accountsdb.AccountsDb, prevSlotCtx *sealevel.SlotCtx, pubkey solana.PublicKey) (*accounts.Account, error) {
	acct, err := prevSlotCtx.GetAccount(pubkey)
	if err == nil {
		return acct.Clone(), nil
	}
	acct, err = acctsDb.GetAccount(prevSlotCtx.Slot, pubkey)
	if err != nil {
		return nil, err
	}
	return acct.Clone(), nil
}

func debitVoteAccountForVAT(parentAcct *accounts.Account, burnAmount uint64) (*accounts.Account, error) {
	if parentAcct.Lamports < burnAmount {
		return nil, fmt.Errorf("vote account %s balance %d is below VAT burn %d", parentAcct.Key, parentAcct.Lamports, burnAmount)
	}
	newAcct := parentAcct.Clone()
	newAcct.Lamports -= burnAmount
	return newAcct, nil
}

func creditIncineratorForVAT(parentIncinerator *accounts.Account, totalBurned uint64) *accounts.Account {
	newIncinerator := parentIncinerator.Clone()
	newIncinerator.Lamports += totalBurned
	return newIncinerator
}

func filterVoteCacheForVAT(
	voteCache map[solana.PublicKey]*sealevel.VoteStateVersions,
	allowed map[solana.PublicKey]struct{},
) map[solana.PublicKey]*sealevel.VoteStateVersions {
	if allowed == nil {
		return voteCache
	}
	filtered := make(map[solana.PublicKey]*sealevel.VoteStateVersions, len(allowed))
	for votePubkey := range allowed {
		if voteState, ok := voteCache[votePubkey]; ok {
			filtered[votePubkey] = voteState
		}
	}
	return filtered
}

func applyAlpenglowVATAtEpochBoundary(
	acctsDb *accountsdb.AccountsDb,
	prevSlotCtx *sealevel.SlotCtx,
	block *block.Block,
	scanResult *BoundaryStakeScanResult,
	f *features.Features,
	alpenglowReplayMode bool,
) (effectiveStakes map[solana.PublicKey]uint64, voteAccts map[solana.PublicKey]*epochstakes.VoteAccount, totalStake uint64, vatAllowed map[solana.PublicKey]struct{}) {
	effectiveStakes = scanResult.EffectiveStakes
	totalStake = scanResult.TotalEffectiveStake
	if !useAlpenglowVAT(alpenglowReplayMode, f) {
		return effectiveStakes, nil, totalStake, nil
	}

	rent, err := loadRentSysvar(acctsDb, prevSlotCtx)
	if err != nil {
		panic(err)
	}
	minimumBalance := minimumVoteAccountBalanceForVAT(&rent, true)
	voteCache := global.VoteCache()

	filteredStakes, filteredVoteAccts, filteredTotal := filterVoteAccountsForVAT(
		acctsDb,
		prevSlotCtx.Slot,
		effectiveStakes,
		voteCache,
		minimumBalance,
		MaxAlpenglowVoteAccounts,
	)

	votePubkeys := make([]solana.PublicKey, 0, len(filteredStakes))
	vatAllowed = make(map[solana.PublicKey]struct{}, len(filteredStakes))
	for votePubkey := range filteredStakes {
		votePubkeys = append(votePubkeys, votePubkey)
		vatAllowed[votePubkey] = struct{}{}
	}

	updated, parentUpdated, totalBurned, err := burnAlpenglowVAT(acctsDb, prevSlotCtx, block.Slot, votePubkeys, VATToBurnPerEpochLamports)
	if err != nil {
		panic(fmt.Sprintf("alpenglow VAT burn failed: %s", err))
	}
	block.EpochUpdatedAccts = append(block.EpochUpdatedAccts, updated...)
	block.ParentEpochUpdatedAccts = append(block.ParentEpochUpdatedAccts, parentUpdated...)

	for _, acct := range updated {
		if meta, ok := filteredVoteAccts[acct.Key]; ok {
			meta.Lamports = acct.Lamports
		}
	}

	mlog.Log.Infof(
		"Alpenglow VAT: admitted %d validators, burned %d lamports (%.4f SOL)",
		len(filteredStakes),
		totalBurned,
		float64(totalBurned)/float64(solana.LAMPORTS_PER_SOL),
	)

	return filteredStakes, filteredVoteAccts, filteredTotal, vatAllowed
}

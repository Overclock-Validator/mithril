package replay

import (
	"context"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/genesis"
	"github.com/Overclock-Validator/mithril/pkg/leaderschedule"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// GenesisReplayBootstrap holds a verified, completed slot-0 parent. Its private
// state authorizes the one lifecycle exception at genesis: SlotHashes does not
// exist until the first child. Snapshot/resume callers retain their strict
// missing-sysvar checks. It does not invent a consensus block or shred identity.
type GenesisReplayBootstrap struct {
	metadata genesis.BankMetadata
	sysvars  *sealevel.BankSysvars
	features *features.Features
	leaders  *leaderschedule.LeaderSchedule
}

func NewGenesisReplayBootstrap(ctx context.Context, g *genesis.Genesis) (*GenesisReplayBootstrap, error) {
	bank, err := genesis.ConstructInitialBank(ctx, g)
	if err != nil {
		return nil, err
	}
	var sysvars []*accounts.Account
	active := make(map[solana.PublicKey]bool)
	for _, a := range bank.Frozen.Accounts {
		if sealevel.IsBankSysvarAccount(a.Key) {
			sysvars = append(sysvars, a.Account.Clone())
		}
		if a.Owner == addresses.FeatureAddr {
			active[a.Key] = true
		}
	}
	view, err := sealevel.NewBankSysvars(0, sysvars...)
	if err != nil {
		return nil, err
	}
	f := features.NewFeaturesDefault()
	for _, gate := range features.AllFeatureGates {
		if active[gate.Address] {
			f.EnableFeature(gate, 0)
		}
	}
	votes := make(map[solana.PublicKey]*epochstakes.VoteAccount)
	stakes := make(map[solana.PublicKey]uint64)
	for _, epoch := range bank.Frozen.Metadata.EpochStakes {
		if epoch.Epoch != 0 {
			continue
		}
		for _, vote := range epoch.Votes {
			key := solana.MustPublicKeyFromBase58(vote.VoteAccount)
			votes[key] = &epochstakes.VoteAccount{NodePubkey: solana.MustPublicKeyFromBase58(vote.Node)}
			stakes[key] = vote.Stake
		}
	}
	schedule, ok := view.EpochSchedule()
	if !ok {
		return nil, fmt.Errorf("genesis is missing EpochSchedule")
	}
	leaders := leaderschedule.New(votes, stakes, &schedule, 0, schedule.SlotsPerEpoch, 4)
	return &GenesisReplayBootstrap{metadata: bank.Frozen.Metadata, sysvars: view, features: f, leaders: leaders}, nil
}

// ConfigureFirstBlock supplies execution state from genesis, leaving entries,
// footer, and any independently established consensus identities untouched.
// The caller must use the AccountsDB verified by genesisinit.Open for this
// genesis. Later blocks derive their state from the preceding replay result.
func (g *GenesisReplayBootstrap) ConfigureFirstBlock(block *b.Block) (*sealevel.BankSysvars, error) {
	if g == nil || g.sysvars == nil || g.leaders == nil || block == nil || block.Slot != 1 || block.SourceParentSlot != 0 {
		return nil, fmt.Errorf("genesis replay requires slot 1 with parent slot 0")
	}
	// An assembled Alpenglow block must explicitly name the genesis certificate
	// parent (0, zero ID). This ID is neither the genesis hash nor the bank hash.
	// Entry-only bank fixtures have no wire identity and retain their separate path.
	if block.AlpenglowParentBlockID != ([32]byte{}) ||
		((block.HasAlpenglowBlockID || block.TransactionSignaturesVerified()) && !block.HasAlpenglowParentBlockID) {
		return nil, fmt.Errorf("genesis replay requires the explicit zero genesis certificate parent")
	}
	m := g.metadata
	block.ParentSlot = 0
	block.ParentBankhash = solana.MustHashFromBase58(m.BankHash)
	block.BlockHeight = 1
	block.Epoch = 0
	block.AcctsLtHash = new(lthash.LtHash).InitWithHash(m.AccountsLtHash)
	block.Features = g.features.Clone()
	block.LastBlockhash = solana.MustHashFromBase58(m.LastBlockhash)
	block.PrevNumSignatures = m.SignatureCount
	block.InitialPreviousLamportsPerSignature = m.LamportsPerSignature
	block.PrevFeeRateGovernor = &sealevel.FeeRateGovernor{
		TargetLamportsPerSignature: m.Fees.TargetLamportsPerSig, TargetSignaturesPerSlot: m.Fees.TargetSigsPerSlot,
		MinLamportsPerSignature: m.Fees.MinLamportsPerSig, MaxLamportsPerSignature: m.Fees.MaxLamportsPerSig,
		BurnPercent: m.Fees.BurnPercent, LamportsPerSignature: m.LamportsPerSignature,
	}
	var found bool
	block.Leader, found = g.leaders.LeaderForSlot(block.Slot)
	if !found {
		return nil, fmt.Errorf("genesis leader schedule has no leader at slot %d", block.Slot)
	}
	block.EpochStakesPerVoteAcct = make(map[solana.PublicKey]uint64)
	block.VoteTimestamps = make(map[solana.PublicKey]sealevel.BlockTimestamp)
	for _, epoch := range m.EpochStakes {
		if epoch.Epoch != 0 {
			continue
		}
		block.TotalEpochStake = epoch.TotalStake
		for _, vote := range epoch.Votes {
			block.EpochStakesPerVoteAcct[solana.MustPublicKeyFromBase58(vote.VoteAccount)] = vote.Stake
			// ValidateProfile requires the canonical, never-voted genesis V4
			// state, whose last vote timestamp is (slot 0, timestamp 0).
			block.VoteTimestamps[solana.MustPublicKeyFromBase58(vote.VoteAccount)] = sealevel.BlockTimestamp{}
		}
	}
	return g.sysvars, nil
}

// LeaderForSlot supplies the genesis epoch's schedule to signature-verifying
// ingress. It returns false outside the schedule; it does not assume that one
// bootstrap validator leads every future epoch.
func (g *GenesisReplayBootstrap) LeaderForSlot(slot uint64) (solana.PublicKey, bool) {
	if g == nil || g.leaders == nil {
		return solana.PublicKey{}, false
	}
	return g.leaders.LeaderForSlot(slot)
}

// FirstChildSlotHashes authorizes leader setup's genesis-only missing sysvar.
// The exact verified parent view, frozen hash and child position must agree.
// Install this account only in the child; its parent before-image remains absent.
func (g *GenesisReplayBootstrap) FirstChildSlotHashes(slot, parentSlot uint64, parentHash solana.Hash, parent *sealevel.BankSysvars) (*accounts.Account, error) {
	return g.slotHashesAccount(&b.Block{Slot: slot, ParentSlot: parentSlot, ParentBankhash: parentHash}, parent)
}

func (g *GenesisReplayBootstrap) slotHashesAccount(block *b.Block, parent *sealevel.BankSysvars) (*accounts.Account, error) {
	if g == nil || g.sysvars == nil || block == nil || parent != g.sysvars || block.Slot != 1 || block.ParentSlot != 0 || block.ParentBankhash != solana.MustHashFromBase58(g.metadata.BankHash) {
		return nil, fmt.Errorf("missing SlotHashes is only valid for the verified genesis parent")
	}
	rent, ok := parent.Rent()
	if !ok {
		return nil, fmt.Errorf("genesis parent is missing Rent")
	}
	const size = 8 + sealevel.SlotHashesMaxEntries*40
	return &accounts.Account{Key: sealevel.SysvarSlotHashesAddr, Owner: addresses.SysvarOwnerAddr,
		Lamports: rent.MinimumBalance(size), Data: make([]byte, size)}, nil
}

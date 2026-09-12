package replay

import (
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/genesis"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGenesisParentExceptionIsBoundToVerifiedBank(t *testing.T) {
	_, err := new(GenesisReplayBootstrap).ConfigureFirstBlock(&b.Block{Slot: 1})
	require.Error(t, err)
	g, _, err := genesis.Build(t.Context(), replayGenesisConfig())
	require.NoError(t, err)
	seed, err := NewGenesisReplayBootstrap(t.Context(), g)
	require.NoError(t, err)
	block := &b.Block{Slot: 1}
	parent, err := seed.ConfigureFirstBlock(block)
	require.NoError(t, err)
	require.False(t, block.HasAlpenglowParentBlockID)
	require.False(t, block.HasAlpenglowBlockID)
	require.False(t, block.HasAlpenglowLastChainedRoot)
	_, present := parent.SlotHashes()
	require.False(t, present)
	_, err = seed.slotHashesAccount(block, parent)
	require.NoError(t, err)
	other, err := NewGenesisReplayBootstrap(t.Context(), g)
	require.NoError(t, err)
	_, err = other.slotHashesAccount(block, parent)
	require.Error(t, err)
	block.ParentBankhash[0] ^= 1
	_, err = seed.slotHashesAccount(block, parent)
	require.Error(t, err)
	_, err = seed.ConfigureFirstBlock(&b.Block{Slot: 2})
	require.Error(t, err)
	_, err = seed.ConfigureFirstBlock(&b.Block{Slot: 1, SourceParentSlot: 1})
	require.Error(t, err)
	_, err = seed.slotHashesAccount(&b.Block{Slot: 1}, &sealevel.BankSysvars{})
	require.Error(t, err)
}

func TestGenesisWireParentMustNameGenesisCertificate(t *testing.T) {
	g, _, err := genesis.Build(t.Context(), replayGenesisConfig())
	require.NoError(t, err)
	seed, err := NewGenesisReplayBootstrap(t.Context(), g)
	require.NoError(t, err)
	for name, parentID := range map[string]solana.Hash{
		"bank hash":    solana.MustHashFromBase58(seed.metadata.BankHash),
		"genesis hash": solana.MustHashFromBase58(seed.metadata.GenesisHash),
		"unrelated ID": {1},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := seed.ConfigureFirstBlock(&b.Block{Slot: 1, HasAlpenglowBlockID: true,
				HasAlpenglowParentBlockID: true, AlpenglowParentBlockID: parentID})
			require.ErrorContains(t, err, "genesis certificate parent")
		})
	}
	_, err = seed.ConfigureFirstBlock(&b.Block{Slot: 1, HasAlpenglowBlockID: true})
	require.ErrorContains(t, err, "explicit zero")
	missingHeader := &b.Block{Slot: 1}
	missingHeader.MarkTransactionSignaturesVerified()
	_, err = seed.ConfigureFirstBlock(missingHeader)
	require.ErrorContains(t, err, "explicit zero")
	_, err = seed.ConfigureFirstBlock(&b.Block{Slot: 1, HasAlpenglowBlockID: true, HasAlpenglowParentBlockID: true})
	require.NoError(t, err)
	leader, ok := seed.LeaderForSlot(1)
	require.True(t, ok)
	require.Equal(t, seed.metadata.Leader, leader.String())
	schedule, present := seed.sysvars.EpochSchedule()
	require.True(t, present)
	_, ok = seed.LeaderForSlot(schedule.SlotsPerEpoch)
	require.False(t, ok)
}

func TestGenesisClockProfilePreservesDeployedBehavior(t *testing.T) {
	g, _, err := genesis.Build(t.Context(), replayGenesisConfig())
	require.NoError(t, err)
	seed, err := NewGenesisReplayBootstrap(t.Context(), g)
	require.NoError(t, err)
	block := &b.Block{Slot: 1}
	parent, err := seed.ConfigureFirstBlock(block)
	require.NoError(t, err)
	schedule, ok := parent.EpochSchedule()
	require.True(t, ok)
	clock, ok := parent.Clock()
	require.True(t, ok)
	initialTime := clock.UnixTimestamp
	block.Slot, block.ParentSlot = 3, 2
	require.NoError(t, updateClockSysvarForMode(&clock, block, &schedule, true))
	require.Equal(t, initialTime+1, clock.UnixTimestamp)
	clock.UnixTimestamp = initialTime
	block.Features.EnableFeature(features.Alpenglow, 0)
	require.NoError(t, updateClockSysvarForMode(&clock, block, &schedule, true))
	require.Equal(t, initialTime, clock.UnixTimestamp)
}

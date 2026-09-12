package blockprod

import (
	"os"
	"testing"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/genesis"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestGenesisLeaderParentException(t *testing.T) {
	f, err := os.Open("../../examples/genesis.toml")
	require.NoError(t, err)
	defer f.Close()
	cfg, err := genesis.ParseConfig(f)
	require.NoError(t, err)
	g, _, err := genesis.Build(t.Context(), cfg)
	require.NoError(t, err)
	seed, err := replay.NewGenesisReplayBootstrap(t.Context(), g)
	require.NoError(t, err)
	next := &b.Block{Slot: 1}
	view, err := seed.ConfigureFirstBlock(next)
	require.NoError(t, err)
	parent := ParentContext{ReplayGeneration: 1, ParentSlot: 0, ParentBankhash: next.ParentBankhash, HasParentBlockID: true, GenesisParent: seed,
		BankSysvars: view, Features: next.Features, PrevFeeGovernor: next.PrevFeeRateGovernor, VoteTimestamps: next.VoteTimestamps}
	ctx, err := NewLeaderSlotCtx(1, 0, nil, parent, nil)
	require.NoError(t, err)
	require.Error(t, view.ValidateForExecution(), "slot-0 absence must remain explicit")
	require.NoError(t, ctx.BankSysvars().ValidateForExecution())
	before, err := ctx.GetParentAccount(sealevel.SysvarSlotHashesAddr)
	require.NoError(t, err)
	require.Zero(t, before.Lamports)
	require.NoError(t, replay.PrepareLeaderSlotSysvars(ctx, next, true))
	a, err := ctx.GetAccount(sealevel.SysvarSlotHashesAddr)
	require.NoError(t, err)
	require.Len(t, a.Data, 8+40*sealevel.SlotHashesMaxEntries)
	hashes, ok := ctx.BankSysvars().SlotHashes()
	require.True(t, ok)
	require.Len(t, hashes, 1)
	require.Equal(t, next.ParentBankhash, hashes[0].Hash)
	for _, name := range []string{"hash", "identity", "presence", "view", "slot", "unverified"} {
		t.Run(name, func(t *testing.T) {
			bad := parent
			slot := uint64(1)
			switch name {
			case "hash":
				bad.ParentBankhash[0] ^= 1
			case "identity":
				bad.ParentBlockID = solana.Hash{1}
			case "presence":
				bad.HasParentBlockID = false
			case "view":
				bad.BankSysvars, _ = view.Derive(0)
			case "slot":
				slot = 2
			case "unverified":
				bad.GenesisParent = new(replay.GenesisReplayBootstrap)
			}
			_, err := NewLeaderSlotCtx(slot, 0, nil, bad, nil)
			require.Error(t, err)
		})
	}
	ordinary := parent
	ordinary.GenesisParent = nil
	ctx, err = NewLeaderSlotCtx(1, 0, nil, ordinary, nil)
	require.NoError(t, err)
	require.ErrorContains(t, replay.PrepareLeaderSlotSysvars(ctx, next, true), "no SlotHashes")
	require.False(t, sameReplayParentSnapshot(parent, ordinary))
}

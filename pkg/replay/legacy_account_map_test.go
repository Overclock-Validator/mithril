package replay

import (
	"math"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestLegacyAccountMapFeatureGate(t *testing.T) {
	for _, tc := range []struct {
		name          string
		featureActive bool
	}{
		{"legacy_bank", false}, {"feature_active_bank", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := newSimd186SlotCtx()
			if tc.featureActive {
				ctx.Features.EnableFeature(features.DisableAccountLoaderSpecialCase, 0)
			}
			payer, readOnlyProgram, passedProgram, writableProgram := testPubkey(10), testPubkey(11), testPubkey(12), testPubkey(13)
			keys := []solana.PublicKey{payer, readOnlyProgram, passedProgram, writableProgram, sealevel.SysvarInstructionsAddr}
			tx := &solana.Transaction{Message: solana.Message{AccountKeys: keys, Instructions: []solana.CompiledInstruction{
				{ProgramIDIndex: 1, Accounts: []uint16{2}}, {ProgramIDIndex: 2}, {ProgramIDIndex: 3},
			}}}
			instructions := []sealevel.Instruction{
				{ProgramId: readOnlyProgram, Accounts: []sealevel.AccountMeta{{Pubkey: passedProgram}}},
				{ProgramId: passedProgram}, {ProgramId: writableProgram},
			}
			metas := make([]*solana.AccountMeta, len(keys))
			overrides := make(map[solana.PublicKey]*accounts.Account)
			for i, k := range keys {
				metas[i] = &solana.AccountMeta{PublicKey: k, IsSigner: i == 0, IsWritable: i == 0 || i == 3}
				overrides[k] = &accounts.Account{Key: k, Owner: addresses.NativeLoaderAddr, Executable: i > 0 && i < 4, Lamports: 19, Data: []byte{byte(i)}}
			}
			sysvar := &accounts.Account{Key: sealevel.SysvarInstructionsAddr, Lamports: 1, Data: []byte{7, 8}}
			result, _, size, err := loadAndValidateTxAcctsSimd186(ctx, metas, tx, instructions, sysvar, math.MaxUint32, overrides)
			require.NoError(t, err)
			require.Equal(t, uint32(4*(txAcctBaseSize+1)), size)
			for i, k := range keys[:4] {
				wantDummy := !tc.featureActive && i == 1
				require.Equal(t, wantDummy, result.Accounts[i].IsDummy)
				require.Equal(t, !wantDummy, result.Shared[i])
				if wantDummy {
					require.Equal(t, k, result.Accounts[i].Key)
					require.Equal(t, overrides[k].Owner, result.Accounts[i].Owner)
					require.True(t, result.Accounts[i].Executable)
					require.Zero(t, result.Accounts[i].Lamports)
				} else {
					require.Same(t, overrides[k], result.Accounts[i])
				}
			}
			require.Same(t, sysvar, result.Accounts[4])
			require.False(t, result.Shared[4])
		})
	}
}

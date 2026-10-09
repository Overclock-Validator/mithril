package sealevel

import (
	"bytes"
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	bin "github.com/gagliardetto/binary"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestExecutionRejectsOtherBankProgramCache(t *testing.T) {
	for _, kind := range []string{"unbound", "same-slot-fork", "different-features"} {
		t.Run(kind, func(t *testing.T) {
			w := expandedProgramWorkloads(t)[0]
			run := workloadRunner(t, w, false)
			ctx, err := run()
			require.NoError(t, err)
			poison := &accountsdb.ProgramCacheEntry{DeploymentSlot: 1336} // nil executable: using it would panic
			f := features.NewFeaturesDefault()
			switch kind {
			case "same-slot-fork":
				poison.BindSource([]byte("another fork's program"), f)
			case "different-features":
				f.EnableFeature(features.VirtualAddressSpaceAdjustments, 0)
				poison.BindSource(w.elf, f)
			}
			ctx.SlotCtx.AccountsDb.AddProgramToCache(w.program, poison)
			next, err := run()
			require.NoError(t, err)
			w.check(t, next)
			require.Equal(t, ctx.ComputeMeter.Used(), next.ComputeMeter.Used())
		})
	}
}

func TestWarmCacheCannotBypassDeploymentSlot(t *testing.T) {
	key, dataKey := benchPubkey(80), benchPubkey(81)
	var encoded bytes.Buffer
	state := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram, Program: UpgradeableLoaderStateProgram{ProgramDataAddress: dataKey}}
	require.NoError(t, state.MarshalWithEncoder(bin.NewBinEncoder(&encoded)))
	program := accounts.Account{Key: key, Owner: a.BpfLoaderUpgradeableAddr, Executable: true, Lamports: 10000000, Data: encoded.Bytes()}
	tx := NewTransactionAccounts([]accounts.Account{program})
	ctx := newBenchExecCtx(tx, 100)
	initializeLegacyBankFixture(t, ctx)
	ctx.SlotCtx.Slot = 100
	var data bytes.Buffer
	state = UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData, ProgramData: UpgradeableLoaderStateProgramData{Slot: 100}}
	require.NoError(t, state.MarshalWithEncoder(bin.NewBinEncoder(&data)))
	require.NoError(t, ctx.Accounts.SetAccount((*[32]byte)(&dataKey), &accounts.Account{Key: dataKey, Owner: a.BpfLoaderUpgradeableAddr, Lamports: 10000000, Data: data.Bytes()}))
	// The cache describes a previous bank. Its older deployment slot must not
	// hide this bank's deployment, which cannot be invoked in the same slot.
	ctx.SlotCtx.AccountsDb.AddProgramToCache(dataKey, &accountsdb.ProgramCacheEntry{DeploymentSlot: 99})
	err := ctx.ProcessInstruction(nil, nil, []uint64{0})
	require.ErrorIs(t, err, InstrErrInvalidAccountData)
}

func TestWarmCacheCannotBypassLoaderV4Retraction(t *testing.T) {
	key := benchPubkey(82)
	state := LoaderV4State{Slot: 99, Status: LoaderV4StatusRetracted}
	program := accounts.Account{Key: key, Owner: a.LoaderV4Addr, Executable: true, Lamports: 10000000, Data: state.Marshal()}
	tx := NewTransactionAccounts([]accounts.Account{program})
	ctx := newBenchExecCtx(tx, 100)
	initializeLegacyBankFixture(t, ctx)
	ctx.SlotCtx.Slot = 100
	require.NoError(t, ctx.Accounts.SetAccount((*[32]byte)(&key), &program))
	ctx.SlotCtx.AccountsDb.AddProgramToCache(key, &accountsdb.ProgramCacheEntry{DeploymentSlot: 99})
	err := ctx.ProcessInstruction(nil, nil, []uint64{0})
	require.ErrorIs(t, err, InstrErrUnsupportedProgramId)
}

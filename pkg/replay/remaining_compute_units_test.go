package replay

import (
	"crypto/sha256"
	"encoding/binary"
	"math"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/programs/system"
	"github.com/stretchr/testify/require"
)

func TestRemainingComputeUnitsPreservesSuccessfulNonceAdvance(t *testing.T) {
	slotCtx, cleanup := newCommitTestSlotCtx()
	defer cleanup()
	slotCtx.Features.EnableFeature(features.RemainingComputeUnitsSyscallEnabled, 0)
	slotCtx.Features.EnableFeature(features.DisableAccountLoaderSpecialCase, 0)
	slotCtx.Features.EnableFeature(features.RemoveAccountsDeltaHash, 0)
	slotCtx.AccountsDb = &accountsdb.AccountsDb{}
	slotCtx.AccountsDb.InitCaches()
	t.Cleanup(slotCtx.AccountsDb.ProgramCache.Close)
	t.Cleanup(slotCtx.AccountsDb.VoteAcctCache.Close)
	t.Cleanup(slotCtx.AccountsDb.CommonAcctsCache.Close)

	payer := txfixture.PayerPubkey()
	nonceKey := solana.PublicKey{0xD6}
	programKey := solana.PublicKey{0xD7}
	initialNonce := [32]byte{0xAA}
	nonceState := sealevel.NonceStateVersions{
		Type: sealevel.NonceVersionCurrent,
		Current: sealevel.NonceData{
			IsInitialized: true,
			Authority:     payer,
			DurableNonce:  initialNonce,
			FeeCalculator: sealevel.FeeCalculator{LamportsPerSignature: 5000},
		},
	}
	nonceData, err := nonceState.Marshal()
	require.NoError(t, err)
	require.NoError(t, slotCtx.SetAccount(nonceKey, &accounts.Account{
		Key: nonceKey, Lamports: 10_000_000, Owner: addresses.SystemProgramAddr,
		Data: nonceData, RentEpoch: math.MaxUint64,
	}))
	require.NoError(t, slotCtx.SetAccount(sealevel.SysvarRecentBlockHashesAddr, &accounts.Account{
		Key: sealevel.SysvarRecentBlockHashesAddr, Lamports: 42_706_560,
		Owner: addresses.SysvarOwnerAddr, RentEpoch: math.MaxUint64,
		Data: sealevel.SysvarCache.RecentBlockHashes.Sysvar.MustMarshal(),
	}))
	require.NoError(t, slotCtx.SetAccount(addresses.BpfLoader2Addr, &accounts.Account{
		Key: addresses.BpfLoader2Addr, Lamports: 1, Owner: addresses.NativeLoaderAddr,
		Executable: true, RentEpoch: math.MaxUint64,
	}))
	require.NoError(t, slotCtx.SetAccount(programKey, &accounts.Account{
		Key: programKey, Lamports: 1_000_000, Owner: addresses.BpfLoader2Addr,
		Executable: true, RentEpoch: math.MaxUint64,
	}))

	// A cached SBF program keeps this regression independent of an external ELF:
	// call sol_remaining_compute_units; mov64 r0, 0; exit.
	text := []sbpf.Slot{
		sbpf.Slot(sbpf.OpCall) | sbpf.Slot(sbpf.SymbolHash("sol_remaining_compute_units"))<<32,
		sbpf.Slot(sbpf.OpMov64Imm),
		sbpf.Slot(sbpf.OpExit),
	}
	textBytes := make([]byte, len(text)*sbpf.SlotSize)
	for i, instruction := range text {
		binary.LittleEndian.PutUint64(textBytes[i*sbpf.SlotSize:], uint64(instruction))
	}
	program := &sbpf.Program{Text: text, TextBytes: textBytes, TextVA: sbpf.VaddrProgram}
	require.NoError(t, program.Verify())
	slotCtx.AccountsDb.AddProgramToCache(programKey, &accountsdb.ProgramCacheEntry{Program: program})

	tx, err := solana.NewTransaction([]solana.Instruction{
		system.NewAdvanceNonceAccountInstruction(nonceKey, solana.SysVarRecentBlockHashesPubkey, payer).Build(),
		solana.NewInstruction(programKey, nil, nil),
	}, solana.Hash(txfixture.TestBlockhash()), solana.TransactionPayer(payer))
	require.NoError(t, err)
	payerPrivateKey := txfixture.PayerPrivateKey()
	_, err = tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
		if key == payer {
			return &payerPrivateKey
		}
		return nil
	})
	require.NoError(t, err)

	var sigverify sync.WaitGroup
	feeInfo, _, err := ProcessTransaction(slotCtx, &sigverify, tx, nil, nil, nil, false)
	sigverify.Wait()
	require.NoError(t, err)
	require.Equal(t, uint64(5000), feeInfo.TotalFee)
	nonceAfter, err := slotCtx.GetAccount(nonceKey)
	require.NoError(t, err)
	decoded, err := sealevel.UnmarshalNonceStateVersions(nonceAfter.Data)
	require.NoError(t, err)
	expectedNonce := sha256.Sum256(append([]byte("DURABLE_NONCE"), slotCtx.LastBlockhash[:]...))
	require.Equal(t, expectedNonce, decoded.State().DurableNonce)
	require.Contains(t, slotCtx.ModifiedAccts, nonceKey)
}

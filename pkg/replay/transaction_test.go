package replay

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
)

func makeSlotCtx(acctList []*accounts.Account) *sealevel.SlotCtx {
	f := features.NewFeaturesDefault()
	memAccts := accounts.NewMemAccounts()
	for _, acct := range acctList {
		memAccts.SetAccountWithoutLock(acct.Key, acct)
	}
	return &sealevel.SlotCtx{
		Accounts:    memAccts,
		ParentAccts: accounts.NewMemAccounts(),
		Slot:        405551840,
		Epoch:       938,
		Features:    f,
		FeeRateGovernor: &sealevel.FeeRateGovernor{
			LamportsPerSignature:     5000,
			PrevLamportsPerSignature: 5000,
		},
		AcctMapsMu:      &sync.Mutex{},
		ModifiedAccts:   make(map[solana.PublicKey]bool),
		WritableAccts:   make(map[solana.PublicKey]bool),
		VoteTimestampMu: &sync.Mutex{},
		VoteTimestamps:  make(map[solana.PublicKey]sealevel.BlockTimestamp),
	}
}

func makeVoteTx(payer, voteAcct solana.PublicKey) *solana.Transaction {
	voteProgramPk := solana.PublicKeyFromBytes(a.VoteProgramAddr[:])
	tx := &solana.Transaction{
		Signatures: []solana.Signature{testSig(0x01)},
	}
	tx.Message.Header.NumRequiredSignatures = 1
	tx.Message.Header.NumReadonlyUnsignedAccounts = 1
	tx.Message.AccountKeys = []solana.PublicKey{payer, voteAcct, voteProgramPk}
	tx.Message.Instructions = []solana.CompiledInstruction{
		{
			ProgramIDIndex: 2,
			Accounts:       []uint16{0, 1},
			Data:           []byte{},
		},
	}
	return tx
}

func checkPreBalances(slotCtx *sealevel.SlotCtx, tx *solana.Transaction, txMeta *rpc.TransactionMeta) *string {
	if txMeta == nil {
		return nil
	}

	f := slotCtx.Features
	instrs, acctMetasPerInstr, err := instrsAndAcctMetasFromTx(tx, f)
	if err != nil {
		msg := fmt.Sprintf("instrsAndAcctMetasFromTx: %s", err)
		return &msg
	}
	_ = acctMetasPerInstr

	computeBudgetLimits, err := sealevel.ComputeBudgetExecuteInstructions(instrs, f)
	if err != nil {
		msg := fmt.Sprintf("ComputeBudgetExecuteInstructions: %s", err)
		return &msg
	}
	_ = computeBudgetLimits

	// Load accounts the same way ProcessTransaction does
	instrsAcct := sealevel.MakeInstructionsSysvarAccount(instrs)
	transactionAccts, _, err := loadAndValidateTxAccts(slotCtx, acctMetasPerInstr, tx, instrs, instrsAcct, computeBudgetLimits.LoadedAccountBytes)
	if err != nil {
		msg := fmt.Sprintf("loadAndValidateTxAccts: %s", err)
		return &msg
	}

	// Run the pre-balance check (same logic as transaction.go:395-416)
	for count := uint64(0); count < uint64(len(tx.Message.AccountKeys)); count++ {
		txAcct, err := transactionAccts.GetAccount(count)
		if err != nil {
			msg := fmt.Sprintf("unable to get tx acct %d", count)
			return &msg
		}

		if !isNativeProgram(txAcct.Key) && !txAcct.IsDummy {
			if txAcct.Lamports != txMeta.PreBalances[count] {
				msg := fmt.Sprintf("tx %s pre-balance divergence: lamport balance for %s was %d but onchain lamport balance was %d",
					tx.Signatures[0], txAcct.Key, txAcct.Lamports, txMeta.PreBalances[count])
				return &msg
			}
		}
		transactionAccts.Unlock(count)
	}
	return nil
}

func TestPreBalanceDivergenceDetected(t *testing.T) {
	payerPk := testPk(0x76)
	voteAcctPk := testPk(0x8a)

	mithrilLamports := uint64(51_492_474_108)
	onchainLamports := uint64(51_492_669_108)

	payerAcct := &accounts.Account{
		Key:       payerPk,
		Lamports:  mithrilLamports,
		Owner:     a.SystemProgramAddr,
		RentEpoch: math.MaxUint64,
	}
	voteAcct := &accounts.Account{
		Key:       voteAcctPk,
		Lamports:  14_572_195_217_575,
		Owner:     a.VoteProgramAddr,
		RentEpoch: math.MaxUint64,
		Data:      make([]byte, 3762),
	}
	voteProgramAcct := &accounts.Account{
		Key:        solana.PublicKeyFromBytes(a.VoteProgramAddr[:]),
		Lamports:   1,
		Owner:      a.NativeLoaderAddr,
		Executable: true,
	}

	slotCtx := makeSlotCtx([]*accounts.Account{payerAcct, voteAcct, voteProgramAcct})
	tx := makeVoteTx(payerPk, voteAcctPk)

	txMeta := &rpc.TransactionMeta{
		Fee:          5000,
		PreBalances:  []uint64{onchainLamports, 14_572_195_217_575, 1},
		PostBalances: []uint64{onchainLamports - 5000, 14_572_195_217_575, 1},
	}

	divergence := checkPreBalances(slotCtx, tx, txMeta)
	if divergence == nil {
		t.Fatal("expected pre-balance divergence to be detected, but check passed")
	}

	if !strings.Contains(*divergence, "pre-balance divergence") {
		t.Fatalf("expected 'pre-balance divergence' in message, got: %s", *divergence)
	}
	if !strings.Contains(*divergence, payerPk.String()) {
		t.Fatalf("expected payer pubkey in message, got: %s", *divergence)
	}
	if !strings.Contains(*divergence, fmt.Sprintf("%d", mithrilLamports)) {
		t.Fatalf("expected mithril lamports in message, got: %s", *divergence)
	}
	if !strings.Contains(*divergence, fmt.Sprintf("%d", onchainLamports)) {
		t.Fatalf("expected onchain lamports in message, got: %s", *divergence)
	}

	t.Logf("divergence correctly detected: %s", *divergence)
}

func TestPreBalanceMatchPasses(t *testing.T) {
	payerPk := testPk(0x76)
	voteAcctPk := testPk(0x8a)

	lamports := uint64(51_492_669_108)

	payerAcct := &accounts.Account{
		Key:       payerPk,
		Lamports:  lamports,
		Owner:     a.SystemProgramAddr,
		RentEpoch: math.MaxUint64,
	}
	voteAcct := &accounts.Account{
		Key:       voteAcctPk,
		Lamports:  14_572_195_217_575,
		Owner:     a.VoteProgramAddr,
		RentEpoch: math.MaxUint64,
		Data:      make([]byte, 3762),
	}
	voteProgramAcct := &accounts.Account{
		Key:        solana.PublicKeyFromBytes(a.VoteProgramAddr[:]),
		Lamports:   1,
		Owner:      a.NativeLoaderAddr,
		Executable: true,
	}

	slotCtx := makeSlotCtx([]*accounts.Account{payerAcct, voteAcct, voteProgramAcct})
	tx := makeVoteTx(payerPk, voteAcctPk)

	txMeta := &rpc.TransactionMeta{
		Fee:          5000,
		PreBalances:  []uint64{lamports, 14_572_195_217_575, 1},
		PostBalances: []uint64{lamports - 5000, 14_572_195_217_575, 1},
	}

	divergence := checkPreBalances(slotCtx, tx, txMeta)
	if divergence != nil {
		t.Fatalf("expected no divergence, but got: %s", *divergence)
	}
}

func TestPreBalanceDivergenceAmountIs195000(t *testing.T) {
	payerPk := testPk(0x76)
	voteAcctPk := testPk(0x8a)

	mithrilLamports := uint64(51_492_474_108)
	onchainLamports := uint64(51_492_669_108)
	expectedDiff := onchainLamports - mithrilLamports

	if expectedDiff != 195_000 {
		t.Fatalf("expected difference of 195,000 lamports, got %d", expectedDiff)
	}
	if expectedDiff%5000 != 0 {
		t.Fatalf("expected difference to be a multiple of 5,000 (vote tx fee), got %d", expectedDiff)
	}
	t.Logf("divergence = %d lamports = %d * 5,000 (vote tx fee)", expectedDiff, expectedDiff/5000)

	payerAcct := &accounts.Account{
		Key:       payerPk,
		Lamports:  mithrilLamports,
		Owner:     a.SystemProgramAddr,
		RentEpoch: math.MaxUint64,
	}
	voteAcct := &accounts.Account{
		Key:       voteAcctPk,
		Lamports:  14_572_195_217_575,
		Owner:     a.VoteProgramAddr,
		RentEpoch: math.MaxUint64,
		Data:      make([]byte, 3762),
	}
	voteProgramAcct := &accounts.Account{
		Key:        solana.PublicKeyFromBytes(a.VoteProgramAddr[:]),
		Lamports:   1,
		Owner:      a.NativeLoaderAddr,
		Executable: true,
	}

	slotCtx := makeSlotCtx([]*accounts.Account{payerAcct, voteAcct, voteProgramAcct})
	tx := makeVoteTx(payerPk, voteAcctPk)

	txMeta := &rpc.TransactionMeta{
		Fee:          5000,
		PreBalances:  []uint64{onchainLamports, 14_572_195_217_575, 1},
		PostBalances: []uint64{onchainLamports - 5000, 14_572_195_217_575, 1},
	}

	divergence := checkPreBalances(slotCtx, tx, txMeta)
	if divergence == nil {
		t.Fatal("expected pre-balance divergence to be detected")
	}

	if mithrilLamports >= onchainLamports {
		t.Fatal("test assumes mithril has fewer lamports than onchain")
	}
}

func TestNativeProgramSkippedInPreBalanceCheck(t *testing.T) {
	payerPk := testPk(0x76)
	voteAcctPk := testPk(0x8a)
	voteProgramPk := solana.PublicKeyFromBytes(a.VoteProgramAddr[:])

	lamports := uint64(51_492_669_108)

	payerAcct := &accounts.Account{
		Key:       payerPk,
		Lamports:  lamports,
		Owner:     a.SystemProgramAddr,
		RentEpoch: math.MaxUint64,
	}
	voteAcct := &accounts.Account{
		Key:       voteAcctPk,
		Lamports:  14_572_195_217_575,
		Owner:     a.VoteProgramAddr,
		RentEpoch: math.MaxUint64,
		Data:      make([]byte, 3762),
	}
	voteProgramAcct := &accounts.Account{
		Key:        voteProgramPk,
		Lamports:   1,
		Owner:      a.NativeLoaderAddr,
		Executable: true,
	}

	slotCtx := makeSlotCtx([]*accounts.Account{payerAcct, voteAcct, voteProgramAcct})
	tx := makeVoteTx(payerPk, voteAcctPk)

	txMeta := &rpc.TransactionMeta{
		Fee:          5000,
		PreBalances:  []uint64{lamports, 14_572_195_217_575, 999_999},
		PostBalances: []uint64{lamports - 5000, 14_572_195_217_575, 999_999},
	}

	divergence := checkPreBalances(slotCtx, tx, txMeta)
	if divergence != nil {
		t.Fatalf("native program account should be skipped in pre-balance check, but got: %s", *divergence)
	}
}

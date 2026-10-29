package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

func resetUnchangedStatsForTest() {
	for i := range unchangedStats {
		unchangedStats[i].mu.Lock()
		unchangedStats[i].stats = nil
		unchangedStats[i].mu.Unlock()
	}
}

// Role keys learn per (program, first data byte, position) and so predict
// accounts never seen before that fill a learned role.
func TestWorkAheadUnchangedByRole(t *testing.T) {
	savedOn, savedRole, savedValue := workAheadUnchanged, workAheadUnchangedByRole, workAheadValueCheck
	workAheadUnchanged, workAheadUnchangedByRole, workAheadValueCheck = true, true, true
	defer func() {
		workAheadUnchanged, workAheadUnchangedByRole, workAheadValueCheck = savedOn, savedRole, savedValue
		resetUnchangedStatsForTest()
	}()
	resetUnchangedStatsForTest()

	program, payer, config, pool := testPk(0x81), testPk(0x82), testPk(0x83), testPk(0x84)
	commit := func(poolChanged bool) {
		poolAfter := &accounts.Account{Key: pool, Lamports: 7}
		if poolChanged {
			poolAfter = &accounts.Account{Key: pool, Lamports: 8}
		}
		metas := []sealevel.AccountMeta{{Pubkey: payer}, {Pubkey: config}, {Pubkey: pool}}
		execCtx := &sealevel.ExecutionCtx{TransactionContext: &sealevel.TransactionCtx{
			AllInstructions: []sealevel.Instruction{{ProgramId: program, Data: []byte{7, 1, 2}, Accounts: metas}},
			Accounts: sealevel.TransactionAccounts{
				Accounts:  []*accounts.Account{{Key: payer, Lamports: 1}, {Key: config, Lamports: 5}, poolAfter},
				Touched:   []bool{true, true, true},
				Originals: []*accounts.Account{nil, {Key: config, Lamports: 5}, {Key: pool, Lamports: 7}},
				AcctMetas: []*sealevel.AccountMeta{{Pubkey: payer, IsWritable: true}, {Pubkey: config, IsWritable: true}, {Pubkey: pool, IsWritable: true}},
			},
		}}
		learnUnchanged(execCtx)
	}
	for range 3 {
		commit(false)
	}
	commit(true)

	// keys: 0 payer, 1..3 accounts, 4 program
	tx := func(data byte, a1, a2, extra solana.PublicKey) *solana.Transaction {
		return &solana.Transaction{Message: solana.Message{
			AccountKeys:  []solana.PublicKey{testPk(0x90), a1, a2, extra, program},
			Instructions: []solana.CompiledInstruction{{ProgramIDIndex: 4, Data: []byte{data, 9}, Accounts: []uint16{0, 1, 2}}},
		}}
	}
	newConfig, newPool, unlisted := testPk(0x85), testPk(0x86), testPk(0x87)
	sameRole := tx(7, newConfig, newPool, unlisted)
	if !predictsUnchangedByRole(sameRole, newConfig) {
		t.Fatal("an unseen account in a role learned unchanged predicts unchanged")
	}
	if predictsUnchangedByRole(sameRole, newPool) {
		t.Fatal("the role that last changed must not predict unchanged")
	}
	if !predictsUnchangedByRole(sameRole, unlisted) {
		t.Fatal("an account no instruction lists predicts unchanged")
	}
	if predictsUnchangedByRole(tx(8, newConfig, newPool, unlisted), newConfig) {
		t.Fatal("another instruction (first data byte) has no history")
	}
}

// Role keys follow the same rule as address keys about what counts as
// unchanged: without the value check, only an account the transaction never
// borrowed for writing, because only that leaves its version in place.
func TestWorkAheadUnchangedByRoleVersionOnly(t *testing.T) {
	savedOn, savedRole, savedValue := workAheadUnchanged, workAheadUnchangedByRole, workAheadValueCheck
	workAheadUnchanged, workAheadUnchangedByRole, workAheadValueCheck = true, true, false
	defer func() {
		workAheadUnchanged, workAheadUnchangedByRole, workAheadValueCheck = savedOn, savedRole, savedValue
		resetUnchangedStatsForTest()
	}()
	resetUnchangedStatsForTest()
	program, payer, untouched, rewritten := testPk(0x91), testPk(0x92), testPk(0x93), testPk(0x94)
	data := []byte{0x2A}
	for range 3 {
		learnUnchanged(&sealevel.ExecutionCtx{TransactionContext: &sealevel.TransactionCtx{
			AllInstructions: []sealevel.Instruction{{ProgramId: program, Data: data, Accounts: []sealevel.AccountMeta{{Pubkey: payer}, {Pubkey: untouched}, {Pubkey: rewritten}}}},
			Accounts: sealevel.TransactionAccounts{
				Accounts:  []*accounts.Account{{Key: payer, Lamports: 1}, {Key: untouched, Lamports: 5}, {Key: rewritten, Lamports: 7}},
				Touched:   []bool{true, false, true},
				AcctMetas: []*sealevel.AccountMeta{{Pubkey: payer, IsWritable: true}, {Pubkey: untouched, IsWritable: true}, {Pubkey: rewritten, IsWritable: true}},
			},
		}})
	}
	if !predictsUnchanged(roleKey(program, data, 1)) {
		t.Fatal("the role of an account never borrowed for writing is predicted unchanged")
	}
	if predictsUnchanged(roleKey(program, data, 2)) {
		t.Fatal("the role of an account written back is not predicted unchanged without the value check")
	}
	// A different account in the learned role is predicted from the role alone.
	fresh := testPk(0x95)
	tx := &solana.Transaction{Message: solana.Message{
		AccountKeys:  []solana.PublicKey{payer, fresh, rewritten, program},
		Instructions: []solana.CompiledInstruction{{ProgramIDIndex: 3, Accounts: []uint16{0, 1, 2}, Data: data}},
	}}
	if !predictsUnchangedByRole(tx, fresh) || predictsUnchangedByRole(tx, rewritten) {
		t.Fatal("prediction must follow the role, for an account never seen before as well")
	}
}

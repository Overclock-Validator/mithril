package replay

import (
	"encoding/binary"
	"testing"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func hintTestTx(sig byte, keys ...solana.PublicKey) *solana.Transaction {
	return &solana.Transaction{
		Signatures: []solana.Signature{{sig}},
		Message: solana.Message{
			Header:      solana.MessageHeader{NumRequiredSignatures: 1},
			AccountKeys: keys,
		},
	}
}

func hintTestAccounts(tx *solana.Transaction) plannerTransactionAccounts {
	builder := newPlannerTransactionAccountBuilder(len(tx.Message.AccountKeys))
	for _, key := range tx.Message.AccountKeys {
		builder.add(key, true)
	}
	return builder.finish()
}

func writableKeys(a plannerTransactionAccounts) map[solana.PublicKey]bool {
	out := make(map[solana.PublicKey]bool)
	for _, access := range a.writable() {
		out[access.publicKey] = true
	}
	return out
}

func TestReplayHintsNarrowTheGraphToRealWrites(t *testing.T) {
	payerA, payerB, payerC, payerD := solana.PublicKey{1}, solana.PublicKey{2}, solana.PublicKey{3}, solana.PublicKey{4}
	shared, other, nonce := solana.PublicKey{10}, solana.PublicKey{11}, solana.PublicKey{12}

	succeeded := hintTestTx(1, payerA, shared, other)
	failed := hintTestTx(2, payerB, shared)
	unknown := hintTestTx(3, payerC, shared, other)
	failedNonce := hintTestTx(4, payerD, nonce, shared, solana.SystemProgramID)
	advance := make([]byte, 4)
	binary.LittleEndian.PutUint32(advance, 4)
	failedNonce.Message.Instructions = []solana.CompiledInstruction{{ProgramIDIndex: 3, Accounts: []uint16{1}, Data: advance}}

	txs := []*solana.Transaction{succeeded, failed, unknown, failedNonce}
	accounts := make([]plannerTransactionAccounts, len(txs))
	for i, tx := range txs {
		accounts[i] = hintTestAccounts(tx)
	}
	use := &hintUse{hints: &blockHints{bySig: map[solana.Signature]txHint{
		{1}: {kcu: 301, touched: []solana.PublicKey{other}},
		{2}: {failed: true, kcu: 51},
		{4}: {failed: true, kcu: 2},
	}}}
	use.applyGraph(txs, accounts)

	require.Equal(t, map[solana.PublicKey]bool{payerA: true, other: true}, writableKeys(accounts[0]), "a success writes its payer and what it changed")
	require.Equal(t, map[solana.PublicKey]bool{payerB: true}, writableKeys(accounts[1]), "a failure writes only its payer")
	require.Equal(t, map[solana.PublicKey]bool{payerC: true, shared: true, other: true}, writableKeys(accounts[2]), "no hint keeps the declared accounts")
	require.Equal(t, map[solana.PublicKey]bool{payerD: true, nonce: true, solana.SystemProgramID: false}[payerD], writableKeys(accounts[3])[payerD])
	require.True(t, writableKeys(accounts[3])[nonce], "a failed nonce transaction still advances its nonce")
	require.False(t, writableKeys(accounts[3])[shared])
	for i := range accounts {
		require.Len(t, accounts[i].accounts, len(txs[i].Message.AccountKeys), "accounts are reclassified, never dropped")
	}

	// The real graph: the success and the failure no longer conflict on the
	// account both declared; the unhinted transaction still waits for the
	// success, which really changes an account it declares.
	plan := buildDependencyPlan(accounts)
	require.Zero(t, plan.inDegree[1], "a failure does not wait for a success it only shares declarations with")
	require.NotZero(t, plan.inDegree[2])

	sched := use.schedule(txs, planSchedule{})
	require.True(t, sched.priority)
	require.Equal(t, int32(301), sched.weight(0))
	require.Equal(t, int32(1), sched.weight(1), "a failure that is not executed weighs nothing")
	require.Equal(t, requestedKCU(unknown), sched.weight(2))
	require.True(t, use.knownFailure(failed))
	require.False(t, use.knownFailure(succeeded))
	require.False(t, use.knownFailure(unknown))
}

// A failed nonce transaction whose nonce account cannot be named from the
// message keeps its declared accounts: narrowing it could hide the advance.
func TestReplayHintsLeaveUnresolvedNonceTransactionsAlone(t *testing.T) {
	payer, shared := solana.PublicKey{1}, solana.PublicKey{10}
	tx := hintTestTx(1, payer, shared, solana.SystemProgramID)
	advance := make([]byte, 4)
	binary.LittleEndian.PutUint32(advance, 4)
	tx.Message.Instructions = []solana.CompiledInstruction{{ProgramIDIndex: 2, Accounts: []uint16{9}, Data: advance}}
	accounts := []plannerTransactionAccounts{hintTestAccounts(tx)}
	use := &hintUse{hints: &blockHints{bySig: map[solana.Signature]txHint{{1}: {failed: true}}}}
	use.applyGraph([]*solana.Transaction{tx}, accounts)
	require.True(t, writableKeys(accounts[0])[shared])
}

func TestReplayHintsAreBoundToTheBlock(t *testing.T) {
	store := &replayHints
	recorder := &hintRecorder{
		commits:  []hintCommit{{solana.Signature{1}, []solana.PublicKey{{9}}}, {solana.Signature{2}, []solana.PublicKey{{8}}}},
		outcomes: []hintOutcome{{solana.Signature{1}, false, 12}, {solana.Signature{2}, true, 3}, {solana.Signature{3}, true, 1}},
	}
	store.put(999_000_001, &recordedHints{blockhash: [32]byte{7}, recorder: recorder})
	hints := hintsFor(&b.Block{Slot: 999_000_001, Blockhash: [32]byte{7}})
	require.NotNil(t, hints)
	require.Same(t, hints, hintsFor(&b.Block{Slot: 999_000_001, Blockhash: [32]byte{7}}))
	require.Equal(t, txHint{kcu: 12, touched: []solana.PublicKey{{9}}}, hints.bySig[solana.Signature{1}])
	require.Equal(t, txHint{failed: true, kcu: 3}, hints.bySig[solana.Signature{2}], "a failure keeps no changed accounts")
	require.Equal(t, txHint{failed: true, kcu: 1}, hints.bySig[solana.Signature{3}])
	require.Nil(t, hintsFor(&b.Block{Slot: 999_000_001, Blockhash: [32]byte{8}}), "another block at the slot gets no hints")
	require.Nil(t, hintsFor(&b.Block{Slot: 999_000_002, Blockhash: [32]byte{7}}))
	store.put(999_000_003, &recordedHints{recorder: &hintRecorder{}})
	require.Nil(t, hintsFor(&b.Block{Slot: 999_000_003}), "hints without a block hash are never used")

	// The store is bounded: the oldest slot goes first.
	for slot := uint64(999_100_000); slot < 999_100_000+replayHintSlots; slot++ {
		store.put(slot, &recordedHints{blockhash: [32]byte{1}, recorder: &hintRecorder{}})
	}
	require.Nil(t, store.get(999_000_001))
	require.LessOrEqual(t, len(store.bySlot), replayHintSlots)
}

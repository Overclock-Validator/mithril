package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/Overclock-Validator/mithril/pkg/txstatus"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestTransactionResultsSeedIdentity(t *testing.T) {
	s := &state.MithrilState{ManifestParentSlot: 7, ManifestRecentBlockhashes: []state.BlockhashEntry{
		{Blockhash: (solana.Hash{7}).String()}, {Blockhash: (solana.Hash{6}).String()},
	}}
	parent, err := TransactionResultSeed(s, nil)
	require.NoError(t, err)
	require.Equal(t, txstatus.ResultBankID{Slot: 7, Blockhash: solana.Hash{7}}, parent)
	resume := &ResumeState{ParentSlot: 20, LastBlockhash: [32]byte{20}}
	parent, err = TransactionResultSeed(s, resume)
	require.NoError(t, err)
	require.Equal(t, txstatus.ResultBankID{Slot: 20, Blockhash: solana.Hash{20}}, parent)
	parent, err = TransactionResultSeed(nil, resume)
	require.NoError(t, err, "a reconciled resume parent does not need a second manifest parse")
	require.Equal(t, uint64(20), parent.Slot)
	resume.LastBlockhash = [32]byte{}
	_, err = TransactionResultSeed(s, resume)
	require.ErrorIs(t, err, txstatus.ErrResultLineage, "never fall back to a stale manifest identity")
	for _, invalid := range []*state.MithrilState{nil, {},
		{ManifestRecentBlockhashes: []state.BlockhashEntry{{Blockhash: "bad"}}},
		{ManifestRecentBlockhashes: []state.BlockhashEntry{{Blockhash: (solana.Hash{}).String()}}}} {
		_, err = TransactionResultSeed(invalid, nil)
		require.Error(t, err)
	}
}

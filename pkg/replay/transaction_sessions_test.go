package replay

import (
	"testing"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	consensusengine "github.com/Overclock-Validator/mithril/pkg/consensus"
	"github.com/Overclock-Validator/mithril/pkg/transactionhistory"
	"github.com/Overclock-Validator/mithril/pkg/txstatus"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type transactionSessionEngine struct {
	consensusengine.Engine
	voting bool
}

func (e transactionSessionEngine) Snapshot() consensusengine.Snapshot {
	var snapshot consensusengine.Snapshot
	if e.voting {
		snapshot.Voting = &consensusengine.VotingStats{}
	}
	return snapshot
}

func TestTransactionSessionsObserverGuards(t *testing.T) {
	store, err := transactionhistory.Open(t.TempDir(), [32]byte{1}, transactionhistory.DefaultLimits())
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	parent := txstatus.ResultBankID{Slot: 7, Blockhash: solana.Hash{7}}
	cache, err := txstatus.NewResultCache(parent)
	require.NoError(t, err)
	for _, mode := range []string{"", "validator", "Verifying", " verifying"} {
		_, err := NewTransactionHistorySession(store, mode)
		require.Error(t, err, mode)
		_, err = NewTransactionResultSession(cache, mode)
		require.Error(t, err, mode)
	}
	history, err := NewTransactionHistorySession(store, "verifying")
	require.NoError(t, err)
	defer requireHistorySessionStopped(t, history, nil)
	results, err := NewTransactionResultSession(cache, "verifying")
	require.NoError(t, err)
	valid := &ConsensusOpts{Engine: transactionSessionEngine{}}
	require.NoError(t, history.validateReplay(valid, nil))
	require.NoError(t, results.validateReplay(valid, nil))
	for _, opts := range []*ConsensusOpts{nil, {}, {Engine: transactionSessionEngine{voting: true}}} {
		require.Error(t, history.validateReplay(opts, nil))
		require.Error(t, results.validateReplay(opts, nil))
	}
	for _, fetch := range []*BlockFetchOpts{
		{LocalBlocks: make(chan *b.Block)},
		{LocalLeaderForSlot: func(uint64) bool { return false }},
	} {
		require.Error(t, history.validateReplay(valid, fetch))
		require.Error(t, results.validateReplay(valid, fetch))
	}
	history, err = NewTransactionHistorySession(nil, "validator")
	require.NoError(t, err)
	require.Nil(t, history)
	results, err = NewTransactionResultSession(nil, "validator")
	require.NoError(t, err)
	require.Nil(t, results)
	require.Nil(t, results.begin())
	require.NoError(t, results.acceptBlock(nil))
	results.Invalidate()
}

func TestTransactionResultsSessionRejectsUnsealedCapture(t *testing.T) {
	parent := txstatus.ResultBankID{Slot: 7, Blockhash: solana.Hash{7}}
	cache, err := txstatus.NewResultCache(parent)
	require.NoError(t, err)
	session, err := NewTransactionResultSession(cache, "verifying")
	require.NoError(t, err)
	require.NotNil(t, session.begin())
	block := &b.Block{Slot: 8, ParentSlot: 7, Blockhash: [32]byte{8}, LastBlockhash: [32]byte{7}}
	require.ErrorIs(t, session.acceptBlock(block), txstatus.ErrResultLineage)
	require.Nil(t, session.current)
	_, err = cache.Lookup(nil)
	require.ErrorIs(t, err, txstatus.ErrResultUnavailable)
}

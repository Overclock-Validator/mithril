package replay

import (
	"encoding/binary"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/txstatus"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// This mirrors the real post-execution order: retain account writes, publish
// accepted outcomes, then inspect rooted-durable backpressure before the next bank.
func TestTransactionResultsCapacityGatePreventsPermanentLatch(t *testing.T) {
	parent := txstatus.ResultBankID{Slot: 5, Blockhash: solana.Hash{5}}
	cache, err := txstatus.NewResultCache(parent)
	require.NoError(t, err)
	tail := newUnrootedTail(&fakeDurable{}, &fakeCommitter{}, unrootedTailHaltCap, 32, "")
	admitted := 0
	var firstFold txstatus.ResultBankID
	for slot := uint64(6); slot <= 6+unrootedTailHaltCap; slot++ {
		var hash solana.Hash
		binary.LittleEndian.PutUint64(hash[:8], slot)
		id := txstatus.ResultBankID{Slot: slot, Blockhash: hash}
		tail.Add(slot, nil, hash[:])
		err = cache.Publish(txstatus.ResultBank{ID: id, Parent: parent, Complete: true})
		require.NoError(t, err, "accepted bank %d at slot %d before the capacity stop must not permanently invalidate optional statuses", admitted+1, slot)
		admitted++
		if admitted == 32 {
			firstFold = id
		}
		parent = id
		if tail.OverCap() {
			break
		}
	}
	require.Equal(t, unrootedTailHaltCap+1, admitted, "the existing post-execution cap permits exactly one completed-bank margin")
	require.True(t, tail.OverCap())
	_, err = cache.Lookup(nil)
	require.NoError(t, err)
	require.NoError(t, cache.Root(firstFold), "a later valid fold must still be able to advance the optional cache root")
	_, err = cache.Lookup(nil)
	require.NoError(t, err)
}

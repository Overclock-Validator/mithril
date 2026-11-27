package replay

import (
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestConeFollowsRealWritesBackToTheBoundary(t *testing.T) {
	a, b, c, d := solana.PublicKey{1}, solana.PublicKey{2}, solana.PublicKey{3}, solana.PublicKey{4}
	shared, pool, other := solana.PublicKey{10}, solana.PublicKey{11}, solana.PublicKey{12}
	txs := []*solana.Transaction{
		hintTestTx(1, a, pool),          // 0: changes pool
		hintTestTx(2, b, shared, other), // 1: declares shared, changes only other
		hintTestTx(3, c, shared, pool),  // 2: failed, moves only its fee
		hintTestTx(4, d, shared, pool),  // 3: target, reads shared and pool
	}
	hints := &blockHints{bySig: map[solana.Signature]txHint{
		{1}: {kcu: 100, touched: []solana.PublicKey{pool}},
		{2}: {kcu: 200, touched: []solana.PublicKey{other}},
		{3}: {failed: true, kcu: 50},
		{4}: {kcu: 300, touched: []solana.PublicKey{shared, pool}},
	}}
	fold := &coneFold{}
	fold.addBlock(txs[:2], hints)
	fold.addBlock(txs[2:], hints)
	require.Equal(t, []int{2, 4}, fold.blockEnd)

	count, kcu := fold.cone(3)
	require.Equal(t, 2, count, "only the transaction that really changed the pool is needed")
	require.Equal(t, int64(400), kcu)

	count, kcu = fold.cone(1)
	require.Equal(t, 1, count)
	require.Equal(t, int64(200), kcu)

	// Without hints every declared writable account counts as written.
	declared := &coneFold{}
	declared.addBlock(txs, nil)
	count, _ = declared.cone(3)
	require.Equal(t, 4, count)
}

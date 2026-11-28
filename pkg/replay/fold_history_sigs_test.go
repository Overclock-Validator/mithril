package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/history"
	"github.com/stretchr/testify/require"
)

func TestFoldHistoryBankSignatures(t *testing.T) {
	chunk := []accounts.SlotDelta{{Slot: 10}, {Slot: 12}}
	sigs := map[uint64][]uint64{10: {1, 2}, 12: {}}
	b := foldHistoryBank(chunk, nil, sigs, 12)
	require.Equal(t, []history.SlotSignatures{{Slot: 10, Prefixes: []uint64{1, 2}}, {Slot: 12, Prefixes: []uint64{}}}, b.Signatures)
	// A slot whose transactions are unknown leaves the whole fold unindexed.
	delete(sigs, 12)
	require.Nil(t, foldHistoryBank(chunk, nil, sigs, 12).Signatures)
}

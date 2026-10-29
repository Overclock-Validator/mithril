package block

import (
	"encoding/json"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestAccountPrefetchRefsNotSerialized(t *testing.T) {
	blk := &Block{Slot: 7}
	refs := &AccountPrefetchRefs{Keys: []solana.PublicKey{{1}}}
	blk.SetAccountPrefetchRefs(refs)
	copy := *blk
	require.Same(t, refs, copy.AccountPrefetchRefs())
	data, err := json.Marshal(blk)
	require.NoError(t, err)
	var decoded Block
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Nil(t, decoded.AccountPrefetchRefs())
}

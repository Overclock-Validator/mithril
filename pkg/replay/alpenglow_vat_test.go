package replay

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The boundary VAT burns from each admitted vote account and credits the
// incinerator. Every returned parent keeps its pre-burn balance and shares no
// data with its update or with the stored account, so later epoch work that
// mutates the updates cannot reach the parent records.
func TestApplyAlpenglowBoundaryVATOwnership(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "accounts"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "largest_file_id"), make([]byte, 8), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "bootstrap_high_file_id"), make([]byte, 8), 0o644))
	db, err := accountsdb.OpenDb(dir)
	require.NoError(t, err)
	db.InitCaches()
	t.Cleanup(db.CloseDb)

	voteA, voteB := solana.PublicKey{0x51}, solana.PublicKey{0x52}
	_, err = db.CommitBatch([]accounts.SlotDelta{{Slot: 10, Delta: []*accounts.Account{
		{Key: voteA, Lamports: 1000, Data: []byte("vote a"), Owner: a.VoteProgramAddr},
		{Key: voteB, Lamports: 500, Data: []byte("vote b"), Owner: a.VoteProgramAddr},
	}}}, 10, nil, nil)
	require.NoError(t, err)

	updated, parents, err := applyAlpenglowBoundaryVAT(db, 10, 100, map[solana.PublicKey]uint64{voteA: 1, voteB: 1})
	require.NoError(t, err)
	require.Len(t, updated, 3)
	require.Len(t, parents, 3)
	for i, want := range []struct {
		key             solana.PublicKey
		parent, updated uint64
	}{{voteA, 1000, 900}, {voteB, 500, 400}, {a.IncineratorAddr, 0, 200}} {
		assert.Equal(t, want.key, updated[i].Key)
		assert.Equal(t, want.parent, parents[i].Lamports)
		assert.Equal(t, want.updated, updated[i].Lamports)
	}
	for i := range 2 {
		updated[i].Data[0] ^= 0xff
		assert.NotEqual(t, updated[i].Data[0], parents[i].Data[0], "update %d shares data with its parent", i)
		stored, err := db.GetAccount(10, updated[i].Key)
		require.NoError(t, err)
		assert.Equal(t, parents[i].Data, stored.Data, "update %d shares data with the stored account", i)
	}
}

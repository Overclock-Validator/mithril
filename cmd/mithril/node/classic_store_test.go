package node

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/stretchr/testify/require"
)

// Startup refuses an old store (no bootstrap boundary) whose state file shows
// replay past the snapshot with no fold history: only classic replay wrote
// that. A store with fold history, a known boundary (fold recovery checks its
// files itself) or no recorded progress is let through.
func TestRefuseClassicStore(t *testing.T) {
	advanced := &state.MithrilState{SnapshotSlot: 100, LastSlot: 250}
	fresh := &state.MithrilState{SnapshotSlot: 100}
	for _, c := range []struct {
		name   string
		h      accountsdb.FoldHistory
		s      *state.MithrilState
		refuse bool
	}{
		{"no boundary, classic progress", accountsdb.FoldHistory{}, advanced, true},
		{"no boundary, no progress", accountsdb.FoldHistory{}, fresh, false},
		{"no boundary, no state file", accountsdb.FoldHistory{}, nil, false},
		{"folded", accountsdb.FoldHistory{Folded: true}, advanced, false},
		{"known boundary", accountsdb.FoldHistory{KnownBootstrap: true}, advanced, false},
	} {
		err := refuseClassicStore(c.h, c.s)
		if c.refuse {
			require.ErrorIs(t, err, accountsdb.ErrClassicStore, c.name)
		} else {
			require.NoError(t, err, c.name)
		}
	}
}

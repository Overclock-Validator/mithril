package blockprod

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSigningReservationGatesEveryLeaderSlotBeforeBuild(t *testing.T) {
	for slot := uint64(40); slot < 44; slot++ {
		var checked uint64
		l := &LeaderLoop{canSignSlot: func(s uint64) bool { checked = s; return false }}
		require.ErrorIs(t, l.startSlotLocked(slot), errParentNotReady)
		require.Equal(t, slot, checked)
		// All other builder dependencies are deliberately nil: rejection must happen
		// before accessing a working bank, executing or signing any shreds.
	}
}

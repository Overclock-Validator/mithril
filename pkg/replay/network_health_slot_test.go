package replay

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fakeCertifiedTip struct {
	slot uint64
	at   time.Time
}

func (f fakeCertifiedTip) AlpenglowCertifiedTip() (uint64, time.Time, bool) {
	return f.slot, f.at, !f.at.IsZero()
}

func TestNetworkHealthSlot(t *testing.T) {
	now := time.Now()
	rpc := func(slot uint64, fresh bool) func() (uint64, bool) {
		return func() (uint64, bool) { return slot, fresh }
	}
	for _, tc := range []struct {
		name      string
		certs     alpenglowCertifiedTip
		rpc       func() (uint64, bool)
		wantSlot  uint64
		wantFresh bool
	}{
		{"no sources", nil, rpc(0, false), 0, false},
		{"rpc only", nil, rpc(500, true), 500, true},
		{"certificates without rpc", fakeCertifiedTip{700, now}, rpc(0, false), 700, true},
		{"higher of both", fakeCertifiedTip{700, now}, rpc(705, true), 705, true},
		{"higher certificate", fakeCertifiedTip{710, now}, rpc(705, true), 710, true},
		{"stale certificate ignored", fakeCertifiedTip{900, now.Add(-healthTipMaxAge - time.Second)}, rpc(705, true), 705, true},
		{"stale certificate and no rpc", fakeCertifiedTip{900, now.Add(-healthTipMaxAge - time.Second)}, rpc(0, false), 0, false},
		{"no certificate yet", fakeCertifiedTip{}, rpc(0, false), 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			slot, fresh := networkHealthSlot(tc.certs, tc.rpc)
			require.Equal(t, tc.wantFresh, fresh)
			require.Equal(t, tc.wantSlot, slot)
		})
	}
}

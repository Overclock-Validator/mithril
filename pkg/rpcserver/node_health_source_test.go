package rpcserver

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/stretchr/testify/require"
)

func TestNodeHealthUsesObservedNetworkTip(t *testing.T) {
	local := global.WallClockSlot()
	server := &RpcServer{}
	server.SetSlotCtx(&sealevel.SlotCtx{Slot: local})
	for _, test := range []struct {
		name        string
		gap         uint64
		fresh       bool
		wantHealthy bool
	}{
		{"caught up", 0, true, true},
		{"catching up", 1000, true, false},
		{"stale network tip", 0, false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			server.SetHealthSlotSource(func() (uint64, bool) { return local + test.gap, test.fresh })
			value, err := server.GetHealth(t.Context(), nil)
			if test.wantHealthy {
				require.NoError(t, err)
				require.Equal(t, "ok", value)
			} else {
				var unhealthy *NodeUnhealthyError
				require.ErrorAs(t, err, &unhealthy)
			}
		})
	}
}

func TestNodeHealthRequiresNetworkTipSource(t *testing.T) {
	server := &RpcServer{}
	server.SetSlotCtx(&sealevel.SlotCtx{Slot: global.WallClockSlot()})
	_, err := server.GetHealth(t.Context(), nil)
	var unhealthy *NodeUnhealthyError
	require.ErrorAs(t, err, &unhealthy)
}

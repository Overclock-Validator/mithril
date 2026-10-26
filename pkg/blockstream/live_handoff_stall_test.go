package blockstream

import (
	"strings"
	"testing"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/stretchr/testify/require"
)

func TestNearTipHandoffWithoutRPCBlockFetch(t *testing.T) {
	for _, tt := range []struct {
		name       string
		source     BlockSourceType
		disableRPC bool
		nearTip    bool
		runLength  uint64
		parent     uint64
		wantReady  bool
	}{
		{"turbine without RPC", BlockSourceTurbine, true, true, 8, 100, true},
		{"turbine with RPC retains gap guard", BlockSourceTurbine, false, true, 8, 100, false},
		{"lightbringer retains gap guard", BlockSourceLightbringer, true, true, 8, 100, false},
		{"outside near tip", BlockSourceTurbine, true, false, 8, 100, false},
		{"incomplete runway", BlockSourceTurbine, true, true, 7, 100, false},
		{"wrong parent", BlockSourceTurbine, true, true, 8, 99, false},
	} {
		for _, entrypoint := range []string{"prepare", "maybe prepare"} {
			t.Run(tt.name+"/"+entrypoint, func(t *testing.T) {
				bs := NewBlockSource(&BlockSourceOpts{
					SourceType: tt.source, TurbineBindAddr: "127.0.0.1:0",
					LightbringerEndpoint: "localhost:1", StartSlot: 101,
					DisableRPCBlockFetch: tt.disableRPC,
				})
				// Reproduce the transition out of repair catchup: replay is
				// 30 slots behind, inside near-tip but beyond its half-window
				// handoff threshold. RPC cannot shrink this gap when disabled.
				bs.repairCatchupPending.Store(false)
				bs.isNearTip.Store(tt.nearTip)
				bs.liveStreamConnected.Store(true)
				bs.lastExecutedSlot.Store(100)
				bs.confirmedTip.Store(130)
				bs.liveLastStreamSlot.Store(130)
				bs.lastEmittedBlockSlot = 100
				bs.nextSlotToSend = 101
				require.False(t, bs.repairCatchupActive())
				require.Less(t, bs.liveHandoffMaxReplayGap(), uint64(30))
				for i := uint64(0); i < tt.runLength; i++ {
					slot, parent := 101+i, 100+i
					if i == 0 {
						parent = tt.parent
					}
					bs.bufferLiveStreamBlock(&b.Block{Slot: slot, SourceParentSlot: parent, FromLiveStream: true})
				}
				if tt.wantReady {
					reason := bs.liveHandoffWaitReason(101, 100)
					require.True(t, strings.HasPrefix(reason, "handoff-ready runway"), reason)
					require.NotContains(t, reason, "exceeds handoff arm threshold")
				}
				if entrypoint == "prepare" {
					blocks, slot, ready := bs.prepareLiveHandoff(101, 100)
					require.Equal(t, tt.wantReady, ready)
					if tt.wantReady {
						require.Equal(t, uint64(101), slot)
						require.Len(t, blocks, int(tt.runLength))
					}
				} else {
					bs.maybePrepareLiveHandoff()
					if tt.wantReady {
						require.Len(t, bs.resultQueue, int(tt.runLength))
						for slot := uint64(101); slot < 101+tt.runLength; slot++ {
							result := <-bs.resultQueue
							require.Equal(t, slot, result.slot)
							require.Equal(t, slot-1, result.block.SourceParentSlot)
							require.Equal(t, int32(-1), result.rpcIdx)
						}
					} else {
						require.Empty(t, bs.resultQueue)
					}
				}
				if tt.wantReady {
					require.Equal(t, uint64(101), bs.liveHandoffSlot.Load())
					require.False(t, bs.shouldUseRPCForSlot(101))
				} else {
					require.Zero(t, bs.liveHandoffSlot.Load())
					require.Len(t, bs.liveStagingBuffer, int(tt.runLength))
				}
			})
		}
	}
}

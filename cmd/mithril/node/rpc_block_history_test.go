package node

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/rpcserver"
	"github.com/stretchr/testify/require"
)

func TestRPCBlockHistoryOnlyStartsOnAlpenglow(t *testing.T) {
	for _, alpenglowMode := range []bool{false, true} {
		name := "classic"
		if alpenglowMode {
			name = "alpenglow"
		}
		t.Run(name, func(t *testing.T) {
			server := &rpcserver.RpcServer{}
			dir := filepath.Join(t.TempDir(), "rpc-block-history")
			require.NoError(t, enableRPCBlockHistory(server, alpenglowMode, dir, 10, 0))
			for slot := uint64(1); slot <= 1000; slot++ {
				require.NoError(t, server.RecordBlockHistory(&b.Block{Slot: slot}))
				if alpenglowMode && slot%10 == 0 {
					require.NoError(t, server.PrepareBlockHistory(slot))
					require.NoError(t, server.SetRootedBlockHistorySlot(slot))
				}
			}
			if !alpenglowMode {
				_, err := server.MinimumLedgerSlot(context.Background(), nil)
				require.ErrorContains(t, err, "no retained block history")
				_, err = os.Stat(dir)
				require.ErrorIs(t, err, os.ErrNotExist)
				return
			}
			first, err := server.GetFirstAvailableBlock(context.Background(), nil)
			require.NoError(t, err)
			require.Equal(t, uint64(991), first)
		})
	}
}

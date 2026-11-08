package node

import (
	"context"
	"path/filepath"
	"testing"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/rpcserver"
	"github.com/stretchr/testify/require"
)

func TestRPCBlockHistoryRetention(t *testing.T) {
	server := &rpcserver.RpcServer{}
	dir := filepath.Join(t.TempDir(), "rpc-block-history")
	require.NoError(t, enableRPCBlockHistory(server, dir, 10, 0))
	for slot := uint64(1); slot <= 1000; slot++ {
		require.NoError(t, server.RecordBlockHistory(&b.Block{Slot: slot}))
		if slot%10 == 0 {
			require.NoError(t, server.PrepareBlockHistory(slot))
			require.NoError(t, server.SetRootedBlockHistorySlot(slot))
		}
	}
	first, err := server.GetFirstAvailableBlock(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, uint64(991), first)
}

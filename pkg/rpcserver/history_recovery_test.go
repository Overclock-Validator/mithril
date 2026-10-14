package rpcserver

import (
	"reflect"
	"runtime"
	"testing"
	"time"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/stretchr/testify/require"
)

func TestHistoryRecoveryWaitsForReaders(t *testing.T) {
	var disabled *RpcServer
	disabled.SetHistoryRecoveryPending(true)
	disabled.SetHistoryRecoveryPending(false)

	_, registered := reflect.TypeOf(rpcHandler{}).MethodByName("SetHistoryRecoveryPending")
	require.False(t, registered, "the internal recovery gate must not be an RPC method")

	server := &RpcServer{}
	server.historyRecoveryMu.RLock()
	readerHeld := true
	defer func() {
		if readerHeld {
			server.historyRecoveryMu.RUnlock()
		}
	}()
	done := make(chan struct{})
	go func() {
		server.SetHistoryRecoveryPending(true)
		close(done)
	}()

	// Observe the waiting writer without releasing the in-flight reader.
	timeout := time.After(5 * time.Second)
	for server.historyRecoveryMu.TryRLock() {
		server.historyRecoveryMu.RUnlock()
		select {
		case <-timeout:
			t.Fatal("recovery did not wait for the in-flight reader")
		default:
			runtime.Gosched()
		}
	}
	select {
	case <-done:
		t.Fatal("recovery closed the gate before the in-flight reader finished")
	default:
	}
	server.historyRecoveryMu.RUnlock()
	readerHeld = false
	select {
	case <-done:
	case <-timeout:
		t.Fatal("recovery remained blocked after the reader finished")
	}
	require.True(t, server.historyRecoveryPending)
	server.SetHistoryRecoveryPending(false)
	require.False(t, server.historyRecoveryPending)
}

func TestBlockHistoryRecoveryGate(t *testing.T) {
	server := &RpcServer{}
	require.NoError(t, server.EnableBlockHistory(t.TempDir(), 100, 0))
	require.NoError(t, server.RecordBlockHistory(&b.Block{Slot: 2, ParentSlot: 1, BlockHeight: 2}))
	require.NoError(t, server.PrepareBlockHistory(2))
	require.NoError(t, server.SetRootedBlockHistorySlot(2))

	for _, pending := range []bool{false, true, false} {
		server.SetHistoryRecoveryPending(pending)
		minimum, minimumErr := server.MinimumLedgerSlot(t.Context(), jsonrpc.RawParams("[]"))
		first, firstErr := server.GetFirstAvailableBlock(t.Context(), jsonrpc.RawParams("[]"))
		block, blockErr := server.GetBlock(t.Context(), jsonrpc.RawParams(`[2,{"transactionDetails":"none"}]`))
		if pending {
			for _, err := range []error{minimumErr, firstErr, blockErr} {
				require.EqualError(t, err, "RPC history is unavailable during account recovery")
			}
			require.Zero(t, minimum)
			require.Zero(t, first)
			require.Zero(t, block)
			continue
		}
		require.NoError(t, minimumErr)
		require.NoError(t, firstErr)
		require.NoError(t, blockErr)
		require.Equal(t, uint64(2), minimum)
		require.Equal(t, uint64(2), first)
		require.Equal(t, uint64(2), block.BlockHeight)
	}
}

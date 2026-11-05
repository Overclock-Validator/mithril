package blockstream

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/rpcclient"
)

// The existing primary-RPC probe can pause the real emitter after a send.
func TestAcknowledgedBlockDoesNotSkipBufferedSuccessor(t *testing.T) {
	const slot = uint64(1000)
	started, release := make(chan struct{}), make(chan struct{})
	var startedOnce sync.Once
	releaseProbe := sync.OnceFunc(func() { close(release) })
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startedOnce.Do(func() { close(started) })
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":1000}`)
	}))
	defer primary.Close()
	bs := NewBlockSource(&BlockSourceOpts{SourceType: BlockSourceRpc, StartSlot: slot,
		RpcClient: rpcclient.NewRpcClient(primary.URL), BackupRpcEndpoints: []string{primary.URL}})
	bs.activeRpcIdx.Store(1)
	bs.slotsSinceFailover.Store(primaryRetryInterval - 1)
	done := make(chan struct{})
	go func() { bs.emitOrderedBlocks(); close(done) }()
	defer func() {
		releaseProbe()
		bs.Stop()
		close(bs.resultQueue)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("emitter did not stop")
		}
	}()
	bs.resultQueue <- fetchResult{slot: slot, block: &b.Block{Slot: slot}}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("primary probe did not start")
	}
	select {
	case block := <-bs.streamChan:
		if block.Slot != slot {
			t.Fatalf("received %d, want %d", block.Slot, slot)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first block was not delivered")
	}
	bs.reorderMu.Lock()
	bs.reorderBuffer[slot+1] = &b.Block{Slot: slot + 1}
	bs.reorderMu.Unlock()
	bs.SetLastExecutedSlot(slot)
	releaseProbe()
	select {
	case block := <-bs.streamChan:
		if block.Slot != slot+1 {
			t.Fatalf("received %d, want %d", block.Slot, slot+1)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("acknowledgement skipped the buffered next block")
	}
}

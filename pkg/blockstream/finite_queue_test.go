package blockstream

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/rpcclient"
	"github.com/gagliardetto/solana-go"
)

// These tests run the real RPC worker, scheduler, ordered emitter and Start
// teardown. Empty synthetic blocks test queue ownership, not bank execution.
func finiteQueueRPC(t *testing.T, beforeBlock func(uint64)) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     any               `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		var value any
		switch req.Method {
		case "getSlot":
			value = 20
		case "getBlock":
			var slot uint64
			if len(req.Params) != 2 || json.Unmarshal(req.Params[0], &slot) != nil || slot < 8 {
				t.Errorf("unexpected block request: %s", req.Params)
				return
			}
			if beforeBlock != nil {
				beforeBlock(slot)
			}
			if slot > 10 {
				http.Error(w, "end of fixture", http.StatusServiceUnavailable)
				return
			}
			value = map[string]any{"parentSlot": slot - 1, "blockhash": (solana.Hash{byte(slot)}).String(), "previousBlockhash": (solana.Hash{byte(slot - 1)}).String(), "blockHeight": slot, "blockTime": 223, "transactions": []any{}, "rewards": []any{map[string]any{"pubkey": (solana.PublicKey{1}).String(), "lamports": 1, "postBalance": 1, "rewardType": "Fee"}}}
		default:
			t.Errorf("unexpected RPC method %q", req.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": value}); err != nil {
			t.Error(err)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func startFiniteQueueSource(t *testing.T, endpoint string, end uint64) (*BlockSource, <-chan struct{}) {
	t.Helper()
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType: BlockSourceRpc, RpcClient: rpcclient.NewRpcClient(endpoint),
		StartSlot: 8, EndSlot: end, MaxRPS: 1000, MaxInflight: 1,
	})
	done := make(chan struct{})
	go func() {
		bs.Start()
		close(done)
	}()
	t.Cleanup(func() {
		bs.Stop()
		waitFiniteQueueDone(t, done)
	})
	return bs, done
}

func waitFiniteQueueDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("real BlockSource.Start failed to join workers and close output")
	}
}

func TestFiniteQueueLateReplayCallback(t *testing.T) {
	server := finiteQueueRPC(t, nil)
	// A closed stop case and closed send case race in select. Eight independent
	// finite lifetimes make the old failure reliable without changing scheduling
	// state or manually closing a production-owned channel in the test.
	for attempt := 0; attempt < 8; attempt++ {
		bs, done := startFiniteQueueSource(t, server.URL, 11)
		waitFiniteQueueDone(t, done)
		if bs.stopReasonEnum() != blockSourceStopReasonCompleted {
			t.Fatalf("stop reason = %s, want finite completion", bs.StopReason())
		}
		for slot := uint64(8); slot <= 10; slot++ {
			blk := bs.NextBlock()
			if blk == nil || blk.Slot != slot || blk.LastBlockhash != [32]byte{byte(slot - 1)} || blk.Blockhash != [32]byte{byte(slot)} {
				t.Fatalf("finite output at slot %d = %#v, want ordered fixture block", slot, blk)
			}
			func() {
				defer func() {
					if recovered := recover(); recovered != nil {
						t.Errorf("real finite source completed before replay callback at slot %d: %v", slot, recovered)
					}
				}()
				// Exactly the callback made by ReplayBlocks after consuming a bank.
				bs.SetLastExecutedSlot(blk.Slot)
			}()
		}
		if blk := bs.NextBlock(); blk != nil {
			t.Fatalf("extra finite output: slot %d", blk.Slot)
		}
		if !bs.isNearTip.Load() {
			t.Fatal("fixture did not enter the affected near-tip callback path")
		}
	}
}

func TestFiniteQueueIdleWorkerShutdown(t *testing.T) {
	var requests atomic.Int64
	server := finiteQueueRPC(t, func(uint64) { requests.Add(1) })
	bs, done := startFiniteQueueSource(t, server.URL, 8)
	waitFiniteQueueDone(t, done)
	if bs.NextBlock() != nil || requests.Load() != 0 {
		t.Fatal("empty finite range fetched or emitted a block")
	}
}

func TestFiniteQueueBufferedWorkerShutdown(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	var requests atomic.Int64
	server := finiteQueueRPC(t, func(uint64) {
		requests.Add(1)
		enterOnce.Do(func() { close(entered) })
		<-release
	})
	bs, done := startFiniteQueueSource(t, server.URL, math.MaxUint64)
	// Register after source cleanup, so failure releases the real HTTP request
	// before cleanup tries to join its worker.
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("worker never reached the real HTTP request")
	}
	waitForBlockSourceCondition(t, func() bool { return len(bs.workQueue) > 0 })
	bs.Stop()
	releaseOnce.Do(func() { close(release) })
	waitFiniteQueueDone(t, done)
	if got := requests.Load(); got != 1 {
		t.Fatalf("HTTP requests = %d, want only the one already in flight at Stop", got)
	}
}

func TestFiniteQueueUnboundedCancellation(t *testing.T) {
	server := finiteQueueRPC(t, nil)
	bs, done := startFiniteQueueSource(t, server.URL, math.MaxUint64)
	for slot := uint64(8); slot <= 10; slot++ {
		select {
		case blk := <-bs.streamChan:
			if blk == nil || blk.Slot != slot || blk.Blockhash != [32]byte{byte(slot)} {
				t.Fatalf("live output at slot %d = %#v, want ordered fixture block", slot, blk)
			}
			bs.SetLastExecutedSlot(blk.Slot)
		case <-time.After(3 * time.Second):
			t.Fatalf("no live output for slot %d", slot)
		}
	}
	bs.Stop()
	waitFiniteQueueDone(t, done)
	if bs.stopReasonEnum() == blockSourceStopReasonCompleted || bs.Stalled() {
		t.Fatalf("clean unbounded stop misclassified: %s", bs.StopReason())
	}
}

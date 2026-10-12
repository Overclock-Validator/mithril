package blockstream

import (
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/rpcclient"
)

func TestRateLimitedSourceStop(t *testing.T) {
	var requests atomic.Int64
	server := finiteQueueRPC(t, func(uint64) { requests.Add(1) })
	bs := NewBlockSource(&BlockSourceOpts{
		SourceType: BlockSourceRpc, RpcClient: rpcclient.NewRpcClient(server.URL),
		StartSlot: 8, EndSlot: math.MaxUint64, MaxRPS: 1, MaxInflight: 2,
	})
	done := make(chan struct{})
	go func() { bs.Start(); close(done) }()
	t.Cleanup(func() { bs.Stop(); waitFiniteQueueDone(t, done) })
	select {
	case blk := <-bs.streamChan:
		if blk == nil || blk.Slot != 8 {
			t.Fatalf("first block = %#v, want slot 8", blk)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("source did not fetch its first block")
	}
	// Two workers have reserved future tokens. Neither has an HTTP request in
	// flight, and the limiter's public configuration has not been changed.
	deadline := time.Now().Add(500 * time.Millisecond)
	for bs.rateLimiter.Tokens() > -1.5 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if tokens := bs.rateLimiter.Tokens(); tokens > -1.5 {
		t.Fatalf("queued tokens = %.2f, want two pending reservations", tokens)
	}
	before := requests.Load()
	start := time.Now()
	bs.Stop()
	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
		t.Error("Stop left Start waiting for rate-limited workers")
		waitFiniteQueueDone(t, done)
	}
	if after := requests.Load(); after != before {
		t.Errorf("getBlock requests after Stop = %d, want 0", after-before)
	}
	t.Logf("shutdown=%s requests_before=%d requests_after=%d", time.Since(start), before, requests.Load())
}

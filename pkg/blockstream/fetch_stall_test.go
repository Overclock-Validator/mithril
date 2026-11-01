package blockstream

import (
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/rpcclient"
)

// fetchStalledForCheck uses the real baseline callers' shared predicate until
// the exact-reason query exists. This keeps the baseline control behavioral,
// rather than failing compilation because a new symbol is absent.
func fetchStalledForCheck(bs *BlockSource) bool {
	if status, ok := any(bs).(interface{ FetchStalled() bool }); ok {
		return status.FetchStalled()
	}
	return bs.Stalled()
}

func TestFetchStalledDistinguishesHistoryAndSafetyStops(t *testing.T) {
	tests := []struct {
		name   string
		reason blockSourceStopReason
		legacy bool
		want   bool
	}{
		{name: "fetch timeout", reason: blockSourceStopReasonStalled, legacy: true, want: true},
		{name: "pruned history", reason: blockSourceStopReasonHistoryUnavailable, legacy: true},
		{name: "conflict", reason: blockSourceStopReasonAlpenglowConflict},
		{name: "invalid certificate", reason: blockSourceStopReasonInvalidAlpenglowCertificate},
		{name: "unexpected end", reason: blockSourceStopReasonUnexpectedLiveEnd},
		{name: "finite completion", reason: blockSourceStopReasonCompleted},
		{name: "operator stop"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var bs BlockSource
			bs.setStopReason(tt.reason, 100)
			bs.stallError.Store(tt.legacy)
			if got := fetchStalledForCheck(&bs); got != tt.want {
				t.Fatalf("exact fetch stall = %t, want %t for reason %d", got, tt.want, tt.reason)
			}
			if got := bs.Stalled(); got != tt.legacy {
				t.Fatalf("legacy stall flag changed: got %t, want %t", got, tt.legacy)
			}
		})
	}
}

func TestFetchStalledPreservesFirstStopReason(t *testing.T) {
	var bs BlockSource
	bs.setStopReason(blockSourceStopReasonHistoryUnavailable, 100)
	bs.setStopReason(blockSourceStopReasonStalled, 101)
	bs.stallError.Store(true)
	if fetchStalledForCheck(&bs) {
		t.Fatal("first history-unavailable stop was classified as a fetch timeout")
	}
	if bs.stopReasonEnum() != blockSourceStopReasonHistoryUnavailable {
		t.Fatal("first stop reason changed")
	}
}

func TestSchedulerFetchStallPublishesExactReason(t *testing.T) {
	bs := NewBlockSource(&BlockSourceOpts{
		RpcClient:  rpcclient.NewRpcClient("http://127.0.0.1:1"),
		SourceType: BlockSourceRpc,
		StartSlot:  100,
		EndSlot:    200,
	})
	// The scheduler takes its existing timeout branch before any work or RPC.
	// No source Start, client call, socket or live validator is run.
	bs.stallTimeout = time.Second
	bs.lastProgress.Store(time.Now().Add(-time.Minute).Unix())
	bs.scheduler()
	if !fetchStalledForCheck(bs) || !bs.Stalled() {
		t.Fatal("scheduler timeout did not publish the fetch-stall status")
	}
	if bs.stopReasonEnum() != blockSourceStopReasonStalled {
		t.Fatalf("scheduler reason = %d, want fetch stall", bs.stopReasonEnum())
	}
}

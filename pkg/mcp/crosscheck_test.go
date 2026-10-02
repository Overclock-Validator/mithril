package mcp

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestCompareSlots(t *testing.T) {
	cases := []struct {
		name             string
		mithril, ref, th uint64
		wantBehind       int64
		wantStatus       string
	}{
		{"in_sync within threshold", 1000, 1010, 150, 10, "in_sync"},
		{"behind over threshold", 1000, 1200, 150, 200, "behind"},
		{"exact threshold in_sync", 1000, 1150, 150, 150, "in_sync"},
		{"ahead", 1010, 1000, 150, -10, "ahead"},
		{"equal", 1000, 1000, 150, 0, "in_sync"},
		{"zero threshold one behind", 1000, 1001, 0, 1, "behind"},
		{"huge threshold no wrap", 1000, 1000, math.MaxUint64, 0, "in_sync"},
		{"extreme behind over huge threshold", 0, math.MaxUint64, uint64(math.MaxInt64) + 1, math.MaxInt64, "behind"},
		{"extreme ahead", math.MaxUint64, 0, 150, math.MinInt64, "ahead"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := compareSlots(c.mithril, c.ref, c.th, "confirmed")
			if got.SlotsBehind != c.wantBehind || got.Status != c.wantStatus {
				t.Errorf("compareSlots = behind %d status %q; want %d %q", got.SlotsBehind, got.Status, c.wantBehind, c.wantStatus)
			}
			if got.ReferenceCommitment != "confirmed" || got.MithrilView != "local_unfinalized_head" {
				t.Errorf("view semantics missing: %+v", got)
			}
		})
	}
}

func TestValidateCommitment(t *testing.T) {
	for _, ok := range []string{"processed", "confirmed", "finalized"} {
		if validateCommitment(ok) != nil {
			t.Errorf("%q should be valid", ok)
		}
	}
	for _, bad := range []string{"bogus", "", "Confirmed"} {
		if validateCommitment(bad) == nil {
			t.Errorf("%q should be invalid", bad)
		}
	}
}

func TestGetReferenceEpochInfoRetriesBoundedRead(t *testing.T) {
	var calls atomic.Uint32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			http.Error(w, "try again", http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      1,
			"result": map[string]any{
				"absoluteSlot": uint64(123),
				"blockHeight":  uint64(100),
				"epoch":        uint64(2),
			},
		})
	}))
	defer server.Close()

	client, err := newMithrilRPCClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	info, err := getReferenceEpochInfo(t.Context(), client, "confirmed", 3, 0)
	if err != nil {
		t.Fatal(err)
	}
	if info.AbsoluteSlot != 123 || calls.Load() != 2 {
		t.Fatalf("reference result = %+v after %d calls", info, calls.Load())
	}
}

func TestGetReferenceEpochInfoDoesNotRetryPermanentFailure(t *testing.T) {
	var calls atomic.Uint32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer server.Close()

	client, err := newMithrilRPCClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := getReferenceEpochInfo(t.Context(), client, "confirmed", 3, 0); err == nil {
		t.Fatal("permanent reference failure succeeded")
	}
	if calls.Load() != 1 {
		t.Fatalf("permanent failure made %d calls, want 1", calls.Load())
	}
}

func TestGetReferenceEpochInfoStopsWhenContextEnds(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "try again", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	client, err := newMithrilRPCClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	started := time.Now()
	if _, err := getReferenceEpochInfo(ctx, client, "confirmed", 3, time.Hour); err == nil {
		t.Fatal("canceled reference read succeeded")
	}
	if time.Since(started) > time.Second {
		t.Fatal("canceled reference read did not stop promptly")
	}
}

func TestSlotsBehindCheckSamplesMithrilAfterReferenceRetry(t *testing.T) {
	var referenceCalls atomic.Uint32
	reference := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if referenceCalls.Add(1) == 1 {
			http.Error(w, "try again", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"absoluteSlot":200,"blockHeight":180,"epoch":2}}`))
	}))
	defer reference.Close()

	mithril := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		slot := uint64(100)
		if referenceCalls.Load() >= 2 {
			slot = 200
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      1,
			"result": map[string]any{
				"absoluteSlot":     slot,
				"blockHeight":      uint64(180),
				"epoch":            uint64(2),
				"slotIndex":        uint64(10),
				"slotsInEpoch":     uint64(100),
				"transactionCount": uint64(1),
			},
		})
	}))
	defer mithril.Close()

	got, err := slotsBehindCheck(t.Context(), Config{RPCURL: mithril.URL}, reference.URL, "confirmed")
	if err != nil {
		t.Fatal(err)
	}
	if got.MithrilSlot != 200 || got.ReferenceSlot != 200 || got.Status != "in_sync" {
		t.Fatalf("comparison used stale local sample: %+v", got)
	}
}

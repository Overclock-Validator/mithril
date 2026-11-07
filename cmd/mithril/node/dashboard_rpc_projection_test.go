package node

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/rpcserver"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
)

func TestDashboardRPCPublicationThroughPublicReader(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := time.Now()
	rpc := &rpcserver.RpcServer{}
	rpc.SetSlotCtx(&sealevel.SlotCtx{Slot: 0})
	d := &nodeDashboard{runStartedAt: started, rpc: rpc, state: dashboard.Snapshot{
		RunID: "rpc-projection-test", StartedAt: started.UTC(), Mode: "verifying",
		Phase: "starting", RPCConfigured: true,
	}}
	dir, err := os.MkdirTemp("/tmp", "dashboard-rpc-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "node.sock")
	feed, err := dashboard.ListenFeed(ctx, socket, d.snapshot)
	if err != nil {
		t.Fatal(err)
	}
	defer feed.Close()
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := reserved.Addr().String()
	if err := reserved.Close(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- dashboard.ServePublic(ctx, socket, address, "https://rpc-test.invalid") }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("reader stopped: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("reader did not stop")
		}
	}()
	client := &http.Client{Timeout: time.Second}
	defer client.CloseIdleConnections()
	wait := func(wantSlot bool) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+"/api/v1/status", nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Host = "rpc-test.invalid"
			resp, err := client.Do(req)
			if err == nil {
				var result dashboard.PublicSnapshot
				err = json.NewDecoder(resp.Body).Decode(&result)
				resp.Body.Close()
				if err == nil && resp.StatusCode == http.StatusOK && result.SourceState == "fresh" && result.RPC != nil {
					r := result.RPC
					if wantSlot && r.PublishedSlot != nil && *r.PublishedSlot == 0 && r.PublishedAt != nil &&
						!r.PublishedAt.Before(started) && !r.PublishedAt.After(r.SampledAt) {
						return
					}
					if !wantSlot && r.PublishedSlot == nil && r.PublishedAt == nil {
						return
					}
				}
			}
			time.Sleep(25 * time.Millisecond)
		}
		t.Fatalf("published-slot state not observed: want slot %v", wantSlot)
	}
	wait(true)
	rpc.SetSlotCtx(nil)
	wait(false)
}

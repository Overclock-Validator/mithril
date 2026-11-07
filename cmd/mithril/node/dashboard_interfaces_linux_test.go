//go:build linux

package node

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
)

// This exercises the real diagnostics owner and Linux counters through the
// private Feed and reader. It does not start a validator or establish a chain.
func TestDashboardNativeInterfacesThroughReader(t *testing.T) {
	for _, role := range []string{"validator", "verifying"} {
		t.Run(role, func(t *testing.T) {
			start := time.Now().Add(-time.Minute).UTC()
			d := &nodeDashboard{networkDetails: true, state: dashboard.Snapshot{SchemaVersion: 1, RunID: "native-interface-qa", StartedAt: start, Mode: role, Phase: "starting"}}
			d.configureHostResources(true)
			d.observeRuntime(time.Now(), runtime.MemStats{}, nil)
			want, err := d.sampleNodeNetwork(time.Now())
			if err != nil || want.Detail.Interfaces == nil || want.Detail.Interfaces.State != "available" || len(want.Detail.Interfaces.Interfaces) == 0 {
				t.Fatalf("native owner did not capture available counters: %v", err)
			}
			for _, row := range want.Detail.Interfaces.Interfaces {
				if row.Failures == nil {
					t.Fatal("Linux sample omitted failure counters")
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			dir, err := os.MkdirTemp("/tmp", "if-wire-")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dir)
			socket := filepath.Join(dir, "node.sock")
			feed, err := dashboard.ListenFeedWithDetails(ctx, socket, "", func() dashboard.Snapshot {
				s := d.snapshot()
				s.SampledAt = time.Now().UTC()
				return s
			}, dashboard.DetailSamplers{NodeNetwork: d.sampleNodeNetwork})
			if err != nil {
				t.Fatal(err)
			}
			defer feed.Close()
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			address := listener.Addr().String()
			if err := listener.Close(); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				done <- dashboard.ServePublicWithOptions(ctx, socket, address, "https://interface.invalid", dashboard.PublicOptions{Resources: true, NetworkDetails: true})
			}()
			defer func() {
				cancel()
				select {
				case err := <-done:
					if err != nil {
						t.Errorf("reader shutdown: %v", err)
					}
				case <-time.After(3 * time.Second):
					t.Error("reader did not stop")
				}
				d.stopping()
			}()
			client := &http.Client{Timeout: time.Second}
			defer client.CloseIdleConnections()
			for ctx.Err() == nil {
				req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+"/api/v1/details/network", nil)
				req.Host = "interface.invalid"
				req.Header.Set("Mithril-Network-Pull-Requests", "1")
				req.Header.Set("Mithril-Network-Mtu", "1")
				response, err := client.Do(req)
				if err == nil {
					body, readErr := io.ReadAll(io.LimitReader(response.Body, dashboard.MaxNetworkDetailBytes+1))
					response.Body.Close()
					if readErr == nil && response.StatusCode == http.StatusOK {
						var got dashboard.PublicNodeNetworkDetail
						if json.Unmarshal(body, &got) != nil || got.State != "fresh" || !reflect.DeepEqual(got.Interfaces, want.Detail.Interfaces) || !reflect.DeepEqual(got.Binding, want.Detail.Binding) {
							t.Fatal("native capture changed between owner and public reader")
						}
						legacyReq, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+"/api/v1/details/network", nil)
						legacyReq.Host = "interface.invalid"
						legacyResponse, err := client.Do(legacyReq)
						if err != nil {
							t.Fatal(err)
						}
						legacyBody, err := io.ReadAll(io.LimitReader(legacyResponse.Body, dashboard.MaxNetworkDetailBytes+1))
						legacyResponse.Body.Close()
						expected := *want.Detail.Interfaces
						expected.Interfaces = append([]dashboard.InterfaceTrafficCounter(nil), expected.Interfaces...)
						for i := range expected.Interfaces {
							expected.Interfaces[i].MTU = 0
						}
						var legacy dashboard.PublicNodeNetworkDetail
						if err != nil || legacyResponse.StatusCode != http.StatusOK || json.Unmarshal(legacyBody, &legacy) != nil ||
							legacy.State != "fresh" || !reflect.DeepEqual(legacy.Interfaces, &expected) || !reflect.DeepEqual(legacy.Binding, want.Detail.Binding) {
							t.Fatal("legacy reader response changed other interface observations")
						}
						t.Logf("native owner/Feed/reader preserved %d interfaces for %s", len(got.Interfaces.Interfaces), role)
						return
					}
				}
				time.Sleep(20 * time.Millisecond)
			}
			t.Fatal("native reader did not become available")
		})
	}
}

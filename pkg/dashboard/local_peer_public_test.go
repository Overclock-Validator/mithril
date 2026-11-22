package dashboard

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestLocalPeerPublicOptIn(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "default_off", true: "enabled"}[enabled], func(t *testing.T) {
			path, _ := fixtureFeed(t) // no source: the public route must stay unavailable, not return empty peers
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() {
				done <- serveProfile(ctx, path, listener, "https://stats.example", PublicOptions{LocalPeers: enabled})
			}()
			t.Cleanup(func() {
				cancel()
				select {
				case err := <-done:
					if err != nil {
						t.Error(err)
					}
				case <-time.After(5 * time.Second):
					t.Error("reader did not stop")
				}
			})
			client := &http.Client{Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
			request := func(method, path, host, origin, body string) *http.Response {
				t.Helper()
				r, err := http.NewRequest(method, "http://"+listener.Addr().String()+path, strings.NewReader(body))
				if err != nil {
					t.Fatal(err)
				}
				r.Host = host
				if origin != "" {
					r.Header.Set("Origin", origin)
				}
				response, err := client.Do(r)
				if err != nil {
					t.Fatal(err)
				}
				return response
			}
			legacy := request("GET", "/api/v1/features", "stats.example", "", "")
			var old struct {
				SchemaVersion int  `json:"schema_version"`
				History       bool `json:"history"`
				Cluster       bool `json:"cluster"`
				NodeDisplay   bool `json:"node_display"`
			}
			decoder := json.NewDecoder(legacy.Body)
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&old); err != nil {
				t.Fatalf("legacy features shape changed: %v", err)
			}
			legacy.Body.Close()
			features := request("GET", "/api/v1/features?include=advanced_status", "stats.example", "", "")
			var f map[string]any
			if err := json.NewDecoder(features.Body).Decode(&f); err != nil {
				t.Fatal(err)
			}
			features.Body.Close()
			if got := f["local_peers"]; enabled && got != true || !enabled && got != nil {
				t.Fatalf("feature advertised incorrectly: %v", got)
			}
			want := http.StatusNotFound
			if enabled {
				want = http.StatusServiceUnavailable
			}
			response := request("GET", localPeerPublicPath, "stats.example", "", "")
			io.Copy(io.Discard, response.Body)
			response.Body.Close()
			if response.StatusCode != want {
				t.Fatalf("peer route: got %d, want %d", response.StatusCode, want)
			}
			if !enabled {
				return
			}
			for _, tc := range []struct {
				method, path, host, origin, body string
				status                           int
			}{
				{"GET", localPeerPublicPath, "wrong.example", "", "", 403},
				{"GET", localPeerPublicPath, "stats.example", "https://wrong.example", "", 403},
				{"GET", localPeerPublicPath + "?x=1", "stats.example", "", "", 400},
				{"GET", localPeerPublicPath + "/", "stats.example", "", "", 400},
				{"GET", localPeerPublicPath, "stats.example", "", "x", 400},
				{"POST", localPeerPublicPath, "stats.example", "", "", 400},
			} {
				res := request(tc.method, tc.path, tc.host, tc.origin, tc.body)
				io.Copy(io.Discard, res.Body)
				res.Body.Close()
				if res.StatusCode != tc.status {
					t.Errorf("%s %s: got %d, want %d", tc.method, tc.path, res.StatusCode, tc.status)
				}
			}
		})
	}
}

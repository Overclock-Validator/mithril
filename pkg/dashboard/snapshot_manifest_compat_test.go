package dashboard

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestSnapshotManifestNegotiation(t *testing.T) {
	now := time.Now().UTC()
	source := testSnapshot(now)
	fault := true
	source.Consensus = &Consensus{SampledAt: now, SafetyFault: &fault}
	source.SnapshotManifests = &SnapshotManifests{Attempt: 1, Full: &SnapshotManifestParse{Slot: 42, ParsedAt: now}, ReadyMarker: &SnapshotReadyMarker{Slot: 42, ObservedAt: now}}
	path := socketPath(t)
	feed, err := ListenFeed(context.Background(), path, func() Snapshot { return source })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	address, cancel, done := replayDetailPublicServer(t, path, PublicOptions{})
	t.Cleanup(func() { replayDetailStopPublic(t, cancel, done) })
	deadline := time.Now().Add(3 * time.Second)
	for {
		status, body, _ := replayDetailPublicRequest(t, address, "/api/v1/status?"+manifestProfileQuery, "stats.example", "")
		if status == http.StatusOK && strings.Contains(string(body), `"snapshot_manifests"`) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("enhanced status unavailable: %d %s", status, body)
		}
		time.Sleep(10 * time.Millisecond)
	}
	status, legacy, _ := replayDetailPublicRequest(t, address, "/api/v1/status", "stats.example", "")
	if status != http.StatusOK || strings.Contains(string(legacy), `"snapshot_manifests"`) || strings.Contains(string(legacy), `"safety_fault"`) {
		t.Fatalf("legacy status changed: %d %s", status, legacy)
	}
	status, v1, _ := replayDetailPublicRequest(t, address, "/api/v1/status?"+manifestProfileQuery, "stats.example", "")
	if status != http.StatusOK || !strings.Contains(string(v1), `"snapshot_manifests"`) || strings.Contains(string(v1), `"ready_marker"`) || strings.Contains(string(v1), `"safety_fault"`) {
		t.Fatalf("v1 manifest profile changed: %d %s", status, v1)
	}
	status, v2, _ := replayDetailPublicRequest(t, address, "/api/v1/status?"+readyProfileQuery, "stats.example", "")
	if status != http.StatusOK || !strings.Contains(string(v2), `"ready_marker"`) || strings.Contains(string(v2), `"safety_fault"`) {
		t.Fatalf("v2 marker unavailable: %d %s", status, v2)
	}
	status, v3, _ := replayDetailPublicRequest(t, address, "/api/v1/status?"+safetyProfileQuery, "stats.example", "")
	if status != http.StatusOK || len(v3) > MaxSnapshotBytes || !strings.Contains(string(v3), `"ready_marker"`) || !strings.Contains(string(v3), `"safety_fault":true`) {
		t.Fatalf("v3 fault or marker unavailable: %d %s", status, v3)
	}
	status, _, _ = replayDetailPublicRequest(t, address, "/api/v1/status?include=other", "stats.example", "")
	if status != http.StatusBadRequest {
		t.Fatalf("unexpected status query accepted: %d", status)
	}
	status, _, _ = replayDetailPublicRequest(t, address, "/api/v1/status?"+manifestProfileQuery, "attacker.example", "")
	if status != http.StatusForbidden {
		t.Fatalf("untrusted host accepted: %d", status)
	}
	addr := strings.TrimPrefix(address, "http://")
	dialer := websocket.Dialer{HandshakeTimeout: time.Second, NetDialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	}}
	for _, tc := range []struct {
		query   string
		present bool
		ready   bool
		fault   bool
	}{{"", false, false, false}, {"?" + manifestProfileQuery, true, false, false}, {"?" + readyProfileQuery, true, true, false}, {"?" + safetyProfileQuery, true, true, true}} {
		conn, _, err := dialer.Dial("ws://stats.example/websocket"+tc.query, http.Header{"Origin": {"https://stats.example"}})
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, body, err := conn.ReadMessage()
		conn.Close()
		if err != nil || !json.Valid(body) || len(body) > MaxSnapshotBytes+128 || strings.Contains(string(body), `"snapshot_manifests"`) != tc.present ||
			strings.Contains(string(body), `"ready_marker"`) != tc.ready || strings.Contains(string(body), `"safety_fault":true`) != tc.fault {
			t.Fatalf("websocket query %q: %s (%v)", tc.query, body, err)
		}
	}
}

func TestSafetyProfileWithoutManifestOverHTTPAndWebSocket(t *testing.T) {
	now := time.Now().UTC()
	source := testSnapshot(now)
	fault := true
	source.Consensus = &Consensus{SampledAt: now, SafetyFault: &fault}
	path := socketPath(t)
	feed, err := ListenFeed(context.Background(), path, func() Snapshot { return source })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	address, cancel, done := replayDetailPublicServer(t, path, PublicOptions{})
	t.Cleanup(func() { replayDetailStopPublic(t, cancel, done) })
	deadline := time.Now().Add(3 * time.Second)
	for {
		status, body, _ := replayDetailPublicRequest(t, address, "/api/v1/status?"+safetyProfileQuery, "stats.example", "")
		if status == http.StatusOK && strings.Contains(string(body), `"safety_fault":true`) {
			if len(body) > MaxSnapshotBytes || strings.Contains(string(body), `"snapshot_manifests"`) {
				t.Fatalf("unexpected v3 status: %s", body)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("v3 status unavailable: %d %s", status, body)
		}
		time.Sleep(10 * time.Millisecond)
	}
	addr := strings.TrimPrefix(address, "http://")
	dialer := websocket.Dialer{HandshakeTimeout: time.Second, NetDialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	}}
	conn, _, err := dialer.Dial("ws://stats.example/websocket?"+safetyProfileQuery, http.Header{"Origin": {"https://stats.example"}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, body, err := conn.ReadMessage()
	if err != nil || len(body) > MaxSnapshotBytes+128 || !strings.Contains(string(body), `"safety_fault":true`) || strings.Contains(string(body), `"snapshot_manifests"`) {
		t.Fatalf("unexpected v3 WebSocket: %s (%v)", body, err)
	}
}

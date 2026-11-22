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

// Synthetic end-to-end feed and reader check; run only with loopback isolation.
func TestFastExecutedFeedReaderWSAndGET(t *testing.T) {
	core := replayDetailLiveCore(0)
	genesis := "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d"
	core.Binding.GenesisHash = &genesis
	now := time.Now().UTC()
	value := FastExecuted{SchemaVersion: 1, Family: "recent_executed", Scope: "locally_executed", State: "fresh", Sequence: 1,
		SampledAt: now, Binding: FastExecutedBinding{RunID: core.Binding.RunID, GenesisHash: &genesis,
			NodeStartedAt: core.Binding.NodeStartedAt, Attempt: core.Binding.Attempt, Rewinds: core.Binding.Rewinds,
			AttemptStartedAt: core.Binding.AttemptStartedAt},
		Records: []ExecutedSlot{{At: now, Slot: 3, Bankhash: "11111111111111111111111111111111"}}}
	path := socketPath(t)
	feed, err := ListenFeedWithDetails(context.Background(), path, "", func() Snapshot {
		s := replayDetailLiveStatus(core)
		s.Replay.SampledAt = time.Now().UTC()
		s.Replay.Completed = &CompletedBlock{At: now, Slot: 3, Bankhash: value.Records[0].Bankhash}
		return s
	}, DetailSamplers{Replay: func(at time.Time) (ReplayDetailCore, error) {
		copy := core
		copy.CapturedAt = at.UTC()
		return copy, nil
	}, FastExecuted: func() (FastExecuted, bool) { return value, true }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	address, cancel, done := replayDetailPublicServer(t, path, PublicOptions{Resources: true, ReplayDetails: true, FastExecuted: true})
	t.Cleanup(func() { replayDetailStopPublic(t, cancel, done) })
	dialer := websocket.Dialer{HandshakeTimeout: time.Second, NetDialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", strings.TrimPrefix(address, "http://"))
	}}
	conn, _, err := dialer.Dial("ws://stats.example/websocket?"+advancedProfileQuery, http.Header{"Origin": {"https://stats.example"}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetReadDeadline(time.Now().Add(4 * time.Second)); err != nil {
		t.Fatal(err)
	}
	for {
		_, frame, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("waiting for fast hint: %v", err)
		}
		var hint struct {
			Topic string `json:"topic"`
			Key   string `json:"key"`
			Value any    `json:"value"`
		}
		if json.Unmarshal(frame, &hint) == nil && hint.Topic == "mithril" && hint.Key == "recent_executed" && hint.Value == true {
			break
		}
	}
	status, body, headers := replayDetailPublicRequest(t, address, "/api/v1/details/recent-executed", "stats.example", "https://stats.example")
	var got FastExecuted
	if status != http.StatusOK || json.Unmarshal(body, &got) != nil || got.Sequence != value.Sequence || len(got.Records) != 1 || got.Records[0].Slot != 3 || headers.Get(statusRemainingHeader) == "" || headers.Get(replayDetailRemainingHeader) == "" {
		t.Fatalf("invalid fast response: status=%d body=%s", status, body)
	}
	statusCode, _, statusHeaders := replayDetailPublicRequest(t, address, "/api/v1/status?"+advancedProfileQuery, "stats.example", "https://stats.example")
	if statusCode != http.StatusOK || statusHeaders.Get(statusRemainingHeader) == "" {
		t.Fatalf("invalid parent status: %d", statusCode)
	}
}

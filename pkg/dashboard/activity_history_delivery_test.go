package dashboard

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"golang.org/x/time/rate"
)

func TestActivityHistoryDeliveryAfterReplayAdvance(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Now()
		advancedAt := now.Add(1600 * time.Millisecond)
		core := replayDetailLiveCore(0)
		genesis := "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d"
		core.Binding.GenesisHash = &genesis
		status := replayDetailLiveStatus(core)
		parent := &replayDetailSource{}
		selected := &selectedWindowSource{gate: &parent.gate, gateMu: &parent.gateMu, revision: 1}
		parent.selected = selected
		source := &activityHistorySource{
			selected: selected, done: make(chan struct{}),
			sample: func(_ context.Context, at time.Time) (ActivityHistory, error) {
				row := ActivityHistoryInterval{AnchorSlot: 10, LastSlot: 20, StartTime: now.Unix() - 11,
					EndTime: now.Unix() - 1, Blocks: 10, Transactions: 100}
				observed := now
				if !at.Before(advancedAt) {
					row.LastSlot, row.StartTime, row.EndTime, row.Transactions = 21, now.Unix()-10, now.Unix(), 101
					observed = advancedAt
				}
				return ActivityHistory{SchemaVersion: 1, Binding: core.Binding, CapturedAt: at.UTC(),
					ObservedAt: observed.UTC(), Rows: []ActivityHistoryInterval{row}}, nil
			},
		}
		var gate atomic.Pointer[replayDetailGate]
		refreshStatus := func(at time.Time) {
			parent.observeStatus(status, at)
			g := replayGate(status, at)
			g.revision, g.stamp = 1, statusStamp{nodeEpoch: 1, familyEpoch: 1}
			gate.Store(&g)
		}
		refreshStatus(now)
		client := &http.Client{Transport: selectedEpochTransport(func(request *http.Request) (*http.Response, error) {
			w := httptest.NewRecorder()
			source.serveHTTP(w, request)
			return w.Result(), nil
		})}
		r := &activityHistoryReader{
			selected: &selectedWindowReader{gate: &gate, limit: rate.NewLimiter(16, 32), active: make(chan struct{}, 4)},
			client:   client, done: make(chan struct{}),
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go source.run(ctx)
		time.Sleep(1500 * time.Millisecond)
		go r.run(ctx)
		for range 121 {
			refreshStatus(time.Now())
			synctest.Wait()
			w := httptest.NewRecorder()
			r.serveHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/details/activity-history", nil), allowReplayStatusForTest)
			if w.Code != http.StatusOK {
				t.Fatalf("activity history unavailable at %s: HTTP %d", time.Since(now), w.Code)
			}
			h, err := decodeActivityHistory(w.Body.Bytes())
			if err != nil {
				t.Fatal(err)
			}
			if h.Frontier == 21 {
				delay := time.Since(advancedAt)
				t.Logf("advance reached the API after %s", delay)
				if delay > 2*time.Second || h.Rows[0].Transactions != 101 || h.Rows[0].EndTime-h.Rows[0].StartTime != 10 {
					t.Fatalf("unexpected delivery delay or interval: %s, %+v", delay, h.Rows[0])
				}
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatal("new transaction interval never reached the API")
	})
}

package dashboard

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

func TestFastExecutedPublicBurstAndSustainedRate(t *testing.T) {
	for _, viewers := range []int{3, 4} {
		t.Run(strconv.Itoa(viewers)+"_viewers", func(t *testing.T) {
			now := time.Now()
			value, gate := fastExecutedFixture(now)
			c := &collector{clockOrigin: now}
			c.detailGate.Store(&gate)
			frame := &frameSet{freshUntil: now.Add(StaleAfter), clockOrigin: now, clockUncertain: &c.clockUncertain}
			frame.fence.nodeEpoch, frame.fence.families[statusReplay] = 1, 1
			c.latest.Store(frame)
			body, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			r := newFastExecutedReader(&http.Client{Transport: &http.Transport{}}, &c.detailGate)
			defer r.client.CloseIdleConnections()
			r.latest.Store(&fastExecutedPublication{value: value, body: body, stamp: gate.stamp,
				freshUntil: now.Add(StaleAfter), invalidated: make(chan struct{})})
			configuredRate := r.limit.Limit()
			// Pause refill so even slow race builds exercise one synchronized burst.
			r.limit.SetLimitAt(now, 0)
			request := func() *httptest.ResponseRecorder {
				w := httptest.NewRecorder()
				r.serveHTTP(fastRecorder{w}, httptest.NewRequest(http.MethodGet, "/api/v1/details/recent-executed", nil), c)
				return w
			}
			start := make(chan struct{})
			responses := make(chan *httptest.ResponseRecorder, viewers)
			for range viewers {
				go func() { <-start; responses <- request() }()
			}
			close(start)
			for range viewers {
				w := <-responses
				if w.Code != http.StatusOK || !bytes.Equal(w.Body.Bytes(), body) ||
					w.Header().Get(statusRemainingHeader) == "" || w.Header().Get(replayDetailRemainingHeader) == "" {
					t.Errorf("%d-viewer cached burst: status=%d body=%s", viewers, w.Code, w.Body.String())
				}
			}
			if t.Failed() || viewers != 4 {
				return
			}
			if w := request(); w.Code != http.StatusTooManyRequests {
				t.Fatalf("fifth burst request escaped the bound: %d", w.Code)
			}
			// Restore the constructor's rate, then offer 20/s on a virtual clock.
			// A fresh burst is consumed first, so only sustained refill can admit.
			at := time.Now()
			r.limit.SetLimitAt(at, configuredRate)
			at = at.Add(time.Second)
			if !r.limit.AllowN(at, 4) {
				t.Fatal("burst did not recover after an idle second")
			}
			accepted := 0
			for i := 1; i <= 200; i++ {
				if r.limit.AllowN(at.Add(time.Duration(i)*50*time.Millisecond), 1) {
					accepted++
				}
			}
			if accepted < 119 || accepted > 120 {
				t.Fatalf("sustained admission changed: accepted %d/200 requests over ten seconds", accepted)
			}
			t.Logf("four cached viewers admitted; fifth rejected; sustained accepted=%d/200 over ten seconds", accepted)
		})
	}
}

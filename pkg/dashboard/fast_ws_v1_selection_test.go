package dashboard

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func nextFastV1Publication(t *testing.T, p *fastExecutedPublication, sequence uint64) *fastExecutedPublication {
	t.Helper()
	v := p.value
	v.Sequence = sequence
	body, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return &fastExecutedPublication{value: v, body: body, stamp: p.stamp, freshUntil: p.freshUntil, invalidated: make(chan struct{})}
}

func TestFastExecutedV1CurrentPublicationSelection(t *testing.T) {
	for _, scenario := range []string{"fresh replacement", "nil selection replaced", "expired selection replaced", "stable nil", "stable expired", "rewind", "different binding", "changed status stamp"} {
		t.Run(scenario, func(t *testing.T) {
			c, old := fastV1Fixture(t)
			next := nextFastV1Publication(t, old, old.value.Sequence+1)
			next.freshUntil = time.Now().Add(1500 * time.Millisecond)
			selected, want := old, next
			switch scenario {
			case "nil selection replaced":
				selected = nil
			case "expired selection replaced":
				old.freshUntil = time.Now().Add(-time.Millisecond)
			case "stable nil":
				c.fast.replace(nil)
				selected = nil
				want = nil
			case "stable expired":
				old.freshUntil = time.Now().Add(-time.Millisecond)
				want = nil
			case "rewind":
				g := *c.detailGate.Load()
				g.Rewinds++
				c.detailGate.Store(&g)
				want = nil
			case "different binding":
				g := *c.detailGate.Load()
				g.Rewinds++
				c.detailGate.Store(&g)
				next.value.Binding.Rewinds++
				want = nil
			case "changed status stamp":
				g := *c.detailGate.Load()
				g.stamp.familyEpoch++
				c.detailGate.Store(&g)
				frame := *c.latest.Load()
				frame.fence.families[statusReplay]++
				c.publishStatusFrame(&frame)
				next.stamp = g.stamp
				want = nil
			}
			if scenario != "stable nil" && scenario != "stable expired" && scenario != "rewind" {
				c.fast.replace(next)
			}
			got, until, settled := currentFastExecutedV1(c, selected, time.Now())
			if !settled || got != want {
				t.Fatalf("got=%p settled=%v want=%p", got, settled, want)
			}
			if got == nil && !until.IsZero() || got != nil && (!until.After(time.Now()) || until.After(got.freshUntil)) {
				t.Fatalf("invalid selected lease: %v", until)
			}
		})
	}
}

func TestFastExecutedV1WriterReselectsBeforeWrite(t *testing.T) {
	for _, scenario := range []string{"fresh replacement", "nil selection replaced", "source cleared", "rewind", "changed status stamp"} {
		t.Run(scenario, func(t *testing.T) {
			c, old := fastV1Fixture(t)
			next := nextFastV1Publication(t, old, old.value.Sequence+1)
			next.freshUntil = time.Now().Add(1500 * time.Millisecond)
			selected, want := old, next
			switch scenario {
			case "nil selection replaced":
				selected = nil
			case "source cleared":
				c.fast.replace(nil)
				want = nil
			case "rewind":
				g := *c.detailGate.Load()
				g.Rewinds++
				c.detailGate.Store(&g)
				want = nil
			case "changed status stamp":
				g := *c.detailGate.Load()
				g.stamp.familyEpoch++
				c.detailGate.Store(&g)
				frame := *c.latest.Load()
				frame.fence.families[statusReplay]++
				c.publishStatusFrame(&frame)
				next.stamp = g.stamp
				want = nil
			}
			if scenario != "source cleared" && scenario != "rewind" {
				c.fast.replace(next)
			}
			type outcome struct {
				p     *fastExecutedPublication
				until time.Time
				ok    bool
			}
			done := make(chan outcome, 1)
			challenge := fastExecutedChallenge{id: strings.Repeat("a", 32), receivedAt: time.Now()}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
				if err != nil {
					done <- outcome{}
					return
				}
				defer conn.Close()
				p, until, ok := writeFastExecutedV1(context.Background(), conn, c, challenge, selected)
				done <- outcome{p, until, ok}
			}))
			defer server.Close()
			conn := fastV1Connect(t, "ws"+strings.TrimPrefix(server.URL, "http"))
			_ = conn.SetReadDeadline(time.Now().Add(time.Second))
			_, body, readErr := conn.ReadMessage()
			select {
			case got := <-done:
				if want == nil {
					if got.ok || readErr == nil {
						t.Fatalf("genuine invalidation wrote: ok=%v err=%v", got.ok, readErr)
					}
					return
				}
				if !got.ok || got.p != want || readErr != nil {
					t.Fatalf("fresh replacement closed before write: ok=%v got=%p want=%p err=%v", got.ok, got.p, want, readErr)
				}
				var frame struct {
					Value FastExecuted `json:"value"`
					TTL   int64        `json:"expires_after_challenge_ms"`
				}
				if json.Unmarshal(body, &frame) != nil || frame.Value.Sequence != want.value.Sequence || frame.TTL != fastExecutedV1TTL(challenge.receivedAt, got.until) || !got.until.Equal(want.freshUntil) {
					t.Fatalf("wrong actual publication/lease: %s until=%v", body, got.until)
				}
			case <-time.After(time.Second):
				t.Fatal("writer did not settle")
			}
		})
	}
}

func TestFastExecutedV1SelectionBoundedChurn(t *testing.T) {
	c, first := fastV1Fixture(t)
	var publications [128]*fastExecutedPublication
	for i := range publications {
		publications[i] = nextFastV1Publication(t, first, uint64(i+2))
	}
	start, done := make(chan struct{}), make(chan struct{})
	go func() {
		<-start
		defer close(done)
		for _, p := range publications {
			c.fast.replace(p)
			runtime.Gosched()
		}
	}()
	close(start)
	for range 64 {
		got, until, settled := currentFastExecutedV1(c, first, time.Now())
		if got == nil && settled {
			t.Fatal("fresh same-binding churn became stable unavailability")
		}
		if got != nil && (got.stamp != first.stamp || !sameFastBinding(got.value.Binding, first.value.Binding) || until.After(got.freshUntil)) {
			t.Fatal("churn crossed binding or renewed a lease")
		}
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("bounded publisher did not finish")
	}
	got, _, settled := currentFastExecutedV1(c, first, time.Now())
	if !settled || got != publications[len(publications)-1] {
		t.Fatal("stable final publication did not recover")
	}
}

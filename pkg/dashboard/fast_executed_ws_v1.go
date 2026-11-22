package dashboard

import (
	"bytes"
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	fastExecutedV2Query     = "include=advanced_status&recent_executed=v2"
	fastExecutedV1Query     = "include=advanced_status&recent_executed=v1"
	fastExecutedV1AnchorFor = 2 * time.Second
)

type fastExecutedChallenge struct {
	id         string
	receivedAt time.Time // Server monotonic receipt; never replaced by write time.
}

// The v1 wire grammar is canonical and closed: no reordered/duplicate keys or whitespace.
func parseFastExecutedChallenge(body []byte, receivedAt time.Time) (fastExecutedChallenge, bool) {
	const prefix, suffix = `{"type":"challenge","id":"`, `"}`
	if len(body) != len(prefix)+32+len(suffix) || !bytes.HasPrefix(body, []byte(prefix)) || !bytes.HasSuffix(body, []byte(suffix)) {
		return fastExecutedChallenge{}, false
	}
	id := string(body[len(prefix) : len(prefix)+32])
	for _, ch := range id {
		if !(ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'f') {
			return fastExecutedChallenge{}, false
		}
	}
	return fastExecutedChallenge{id: id, receivedAt: receivedAt}, !receivedAt.IsZero()
}

// fastExecutedV1TTL assumes client monotonic rate >= .999 of server rate and
// <=1ms client clock precision. The 10ms margin covers precision/serialization;
// clients must withdraw on suspend/discontinuity and resynchronize every second.
func fastExecutedV1TTL(receivedAt, until time.Time) int64 {
	budget := min(until.Sub(receivedAt), StaleAfter)
	if budget <= 0 {
		return 0
	}
	milliseconds := (budget * 999 / 1000).Milliseconds() - 10
	return max(0, milliseconds)
}

func fastExecutedV1Admission(c *collector, p *fastExecutedPublication, now time.Time) (time.Time, bool) {
	if c.fast == nil || p == nil {
		return time.Time{}, false
	}
	remaining, ok := c.admitLiveDetail(statusReplay, p.stamp, func() (statusStamp, bool) {
		g := c.detailGate.Load()
		if g == nil {
			return statusStamp{}, false
		}
		now := time.Now()
		return g.stamp, c.fast.latest.Load() == p && p.freshUntil.After(now) && fastMatchesGate(p.value, g, now)
	}, now)
	until := minTime(now.Add(remaining), p.freshUntil)
	return until, ok && until.After(now)
}

// currentFastExecutedV1 reuses the bounded current-publication admission.
// A replacement may be retried only within the same source binding.
func currentFastExecutedV1(c *collector, selected *fastExecutedPublication, now time.Time) (*fastExecutedPublication, time.Time, bool) {
	if c.fast == nil {
		return nil, time.Time{}, true
	}
	p, remaining, settled := c.currentFastHint(selected)
	if p == nil || !settled {
		return nil, time.Time{}, settled
	}
	if selected != nil && (selected.stamp != p.stamp || !sameFastBinding(selected.value.Binding, p.value.Binding)) {
		return nil, time.Time{}, true
	}
	return p, minTime(now.Add(remaining), p.freshUntil), true
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func fastExecutedV1Envelope(challenge fastExecutedChallenge, ttl int64, body []byte) []byte {
	out := []byte(`{"type":"frame","topic":"mithril","key":"recent_executed_v1","challenge":"` + challenge.id + `","expires_after_challenge_ms":`)
	out = strconv.AppendInt(out, ttl, 10)
	out = append(out, `,"value":`...)
	out = append(out, body...)
	return append(out, '}')
}

func fastExecutedV2Envelope(challenge fastExecutedChallenge, ttl, sourceTTL int64, value []byte) []byte {
	body := fastExecutedV1Envelope(challenge, ttl, value)
	body = bytes.Replace(body, []byte(`"key":"recent_executed_v1"`), []byte(`"key":"recent_executed_v2"`), 1)
	return append(body[:len(body)-1], []byte(`,"source_expires_after_challenge_ms":`+strconv.FormatInt(sourceTTL, 10)+`}`)...)
}

// writeFastExecutedV1 has one Gorilla writer. Its watcher uses only concurrent-safe Close.
func writeFastExecutedV1(ctx context.Context, conn *websocket.Conn, c *collector, challenge fastExecutedChallenge, p *fastExecutedPublication) (*fastExecutedPublication, time.Time, bool) {
	return writeFastExecutedV2(ctx, conn, c, challenge, p, false)
}

func writeFastExecutedV2(ctx context.Context, conn *websocket.Conn, c *collector, challenge fastExecutedChallenge, p *fastExecutedPublication, separate bool) (*fastExecutedPublication, time.Time, bool) {
	now := time.Now()
	if now.Before(challenge.receivedAt) || !challenge.receivedAt.Add(fastExecutedV1AnchorFor).After(now) {
		return nil, time.Time{}, false
	}
	p, until, settled := currentFastExecutedV1(c, p, now)
	ttl := fastExecutedV1TTL(challenge.receivedAt, until)
	if !settled || p == nil || len(p.body) > 16<<10 || ttl < 1 {
		return nil, time.Time{}, false
	}
	deadline := minTime(until, challenge.receivedAt.Add(fastExecutedV1AnchorFor))
	body := fastExecutedV1Envelope(challenge, ttl, p.body)
	if separate {
		sourceTTL := fastExecutedV1TTL(challenge.receivedAt, p.freshUntil)
		if sourceTTL < 1 {
			return nil, time.Time{}, false
		}
		body = fastExecutedV2Envelope(challenge, ttl, sourceTTL, p.body)
	}
	if err := conn.SetWriteDeadline(deadline); err != nil {
		return nil, time.Time{}, false
	}
	done, joined := make(chan struct{}), make(chan struct{})
	var transition sync.Mutex
	writing := true
	valid := func() bool {
		_, admitted := fastExecutedV1Admission(c, p, time.Now())
		return admitted && deadline.After(time.Now())
	}
	go func() {
		defer close(joined)
		for {
			var statusChanged, leaseChanged <-chan struct{}
			if update := c.statusUpdate.Load(); update != nil {
				statusChanged = update.changed
			}
			watchUntil := deadline
			if frame := c.latest.Load(); frame != nil {
				at := time.Now()
				remaining, changed, _ := frame.statusRemaining(0, at)
				leaseChanged = changed
				watchUntil = minTime(watchUntil, at.Add(remaining))
			}
			// Subscribe before checking: an invalidation before registration
			// must not leave a blocked writer waiting on the replacement token.
			if !valid() {
				transition.Lock()
				if writing {
					_ = conn.Close()
				}
				transition.Unlock()
				return
			}
			timer := time.NewTimer(time.Until(watchUntil))
			select {
			case <-done:
				timer.Stop()
				return
			case <-statusChanged:
				timer.Stop()
				continue
			case <-leaseChanged:
				timer.Stop()
				continue
			case <-ctx.Done():
			case <-p.invalidated:
			case <-timer.C:
			}
			timer.Stop()
			transition.Lock()
			if writing {
				_ = conn.Close()
			}
			transition.Unlock()
			return
		}
	}()
	defer func() {
		transition.Lock()
		writing = false
		transition.Unlock()
		close(done)
		<-joined
	}()
	ok := valid() && conn.WriteMessage(websocket.TextMessage, body) == nil && valid()
	return p, until, ok
}

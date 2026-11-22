package dashboard

import (
	"context"
	"errors"
	"time"

	"github.com/gorilla/websocket"
	"golang.org/x/time/rate"
)

// streamFastExecutedV1 sends only v1 anchor/frame envelopes. Parent status stays
// on its independently leased HTTP path; legacy WebSocket sessions are unchanged.
func streamFastExecutedV1(ctx context.Context, conn *websocket.Conn, c *collector) {
	streamFastExecutedV2(ctx, conn, c, false)
}

func streamFastExecutedV2(ctx context.Context, conn *websocket.Conn, c *collector, separate bool) {
	defer conn.Close()
	conn.SetReadLimit(128)
	if conn.SetReadDeadline(time.Now().Add(3*time.Second)) != nil {
		return
	}
	controls := rate.NewLimiter(2, 4)
	conn.SetPongHandler(func(string) error {
		if !controls.Allow() {
			return errors.New("control frame rate exceeded")
		}
		return nil
	})
	conn.SetPingHandler(func(payload string) error {
		if !controls.Allow() {
			return errors.New("control frame rate exceeded")
		}
		return conn.WriteControl(websocket.PongMessage, []byte(payload), time.Now().Add(time.Second))
	})
	finished := make(chan struct{})
	pending := make(chan fastExecutedChallenge, 1)
	go func() {
		defer close(finished)
		defer conn.Close() // Also interrupts an ACK or payload blocked in WriteMessage.
		limiter := rate.NewLimiter(2, 2)
		var previous string
		for {
			kind, body, err := conn.ReadMessage()
			if err != nil || kind != websocket.TextMessage || !limiter.Allow() {
				return
			}
			challenge, ok := parseFastExecutedChallenge(body, time.Now())
			if !ok || challenge.id == previous || conn.SetReadDeadline(challenge.receivedAt.Add(3*time.Second)) != nil {
				return
			}
			previous = challenge.id
			select {
			case pending <- challenge:
			default:
				return
			}
		}
	}()
	defer func() { _ = conn.Close(); <-finished }()

	// The existing 16-viewer cap also bounds v1. Each viewer can receive at most
	// 12 cached payloads/s plus a four-frame burst; no node/source work is added.
	output := rate.NewLimiter(12, 4)
	var anchor fastExecutedChallenge
	var last *fastExecutedPublication
	var sent bool
	var fastChanged, statusChanged, leaseChanged <-chan struct{}
	anchorExpiry := time.NewTimer(fastExecutedV1AnchorFor) // Initial challenge is required.
	defer anchorExpiry.Stop()
	var expiry *time.Timer
	var expiryC <-chan time.Time
	defer func() {
		if expiry != nil {
			expiry.Stop()
		}
	}()
	write := func(body []byte) bool {
		if anchor.receivedAt.IsZero() || !anchor.receivedAt.Add(fastExecutedV1AnchorFor).After(time.Now()) {
			return false
		}
		return conn.SetWriteDeadline(anchor.receivedAt.Add(fastExecutedV1AnchorFor)) == nil && conn.WriteMessage(websocket.TextMessage, body) == nil
	}
	ack := func(challenge fastExecutedChallenge) bool {
		anchor = challenge
		anchorExpiry.Reset(time.Until(anchor.receivedAt.Add(fastExecutedV1AnchorFor)))
		return write([]byte(`{"type":"anchor","challenge":"` + anchor.id + `"}`))
	}
	send := func(force bool) bool {
		// A queued control wins before dispatching another payload; in-flight writes are unchanged.
		select {
		case challenge := <-pending:
			if !ack(challenge) {
				return false
			}
			force = true
		default:
		}
		if change := c.fast.changed.Load(); change != nil {
			fastChanged = change.changed
		}
		if update := c.statusUpdate.Load(); update != nil {
			statusChanged = update.changed
		}
		leaseChanged = nil
		if frame := c.latest.Load(); frame != nil {
			_, leaseChanged, _ = frame.statusRemaining(0, time.Now())
		}
		if expiry != nil {
			expiry.Stop()
		}
		expiryC = nil
		if anchor.receivedAt.IsZero() {
			return true
		}
		p, until, settled := currentFastExecutedV1(c, c.fast.latest.Load(), time.Now())
		if !settled {
			return false
		}
		if p == nil || fastExecutedV1TTL(anchor.receivedAt, until) < 1 {
			if force || !sent || last != nil {
				body := fastExecutedV1Envelope(anchor, 0, []byte("null"))
				if separate {
					body = fastExecutedV2Envelope(anchor, 0, 0, []byte("null"))
				}
				if !output.Allow() || !write(body) {
					return false
				}
			}
			last, sent = nil, true
			return true
		}
		if force || !sent || p != last {
			if !output.Allow() {
				return false
			}
			var ok bool
			p, until, ok = writeFastExecutedV2(ctx, conn, c, anchor, p, separate)
			if !ok {
				return false
			}
			last, sent = p, true
		}
		if expiry == nil {
			expiry = time.NewTimer(time.Until(until))
		} else {
			expiry.Reset(time.Until(until))
		}
		expiryC = expiry.C
		return true
	}
	if !send(false) {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-finished:
			return
		case <-anchorExpiry.C:
			return
		case challenge := <-pending:
			if !ack(challenge) || !send(true) {
				return
			}
		case <-fastChanged:
			if !send(false) {
				return
			}
		case <-statusChanged:
			if !send(false) {
				return
			}
		case <-leaseChanged:
			if !send(false) {
				return
			}
		case <-expiryC:
			if !send(false) {
				return
			}
		}
	}
}

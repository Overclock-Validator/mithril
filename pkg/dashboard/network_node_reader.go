package dashboard

import (
	"context"
	"crypto/sha256"
	"net/http"
	"time"
)

// collectNode uses the existing collector and bounded Unix transport. Only the
// enclosing run gate changes; receiver children still require the receiver gate.
func (r *networkDetailReader) collectNode(ctx context.Context, before networkDetailGate, body []byte, remaining time.Duration, started time.Time) {
	fail := func(reason string) {
		if ctx.Err() == nil {
			r.failure.Store(&networkDetailReaderFailure{gate: cloneNetworkDetailGate(before), reason: reason, nodeScoped: true})
		}
	}
	d, err := decodeNodeNetworkDetail(body)
	if err != nil {
		fail("source_invalid")
		return
	}
	if !sameNodeNetworkBinding(before.nodeBinding(), d.Binding) {
		fail("binding_changed")
		return
	}
	if !before.nodeStamp.valid() {
		fail("waiting_source")
		return
	}
	now := time.Now()
	freshUntil := started.Add(remaining)
	if !freshUntil.After(now) {
		fail("source_time_invalid")
		return
	}
	after := r.gate.Load()
	if after == nil || !sameNetworkNodeGateContext(before, *after) || !sameNodeNetworkBinding(after.nodeBinding(), d.Binding) {
		fail("binding_changed")
		return
	}
	if reason := after.nodeFailure(now); reason != "" {
		fail(reason)
		return
	}
	if d.Receiver != nil && (!before.stamp.valid() || before.stamp != after.stamp || after.failure(now) != "" || !after.matches(d.Receiver.Binding)) {
		d.Receiver, d.ReceiverReason = nil, "binding_changed"
		d.RepairGate = nil
	}
	digest := sha256.Sum256(body)
	collectedAt := now
	if current := r.latest.Load(); current != nil && current.node != nil && current.sourceDigest == digest {
		if current.nodeStamp == before.nodeStamp && current.stamp == after.stamp && !freshUntil.Before(current.sourceFreshUntil) {
			r.failure.Store(nil)
			return
		}
		if current.sourceFreshUntil.Before(freshUntil) {
			freshUntil = current.sourceFreshUntil
		}
		if !freshUntil.After(time.Now()) {
			fail("status_stale")
			return
		}
		collectedAt = current.publishedAt
	}
	prepared, err := preparePublicNodeNetwork(d, collectedAt, freshUntil)
	if err != nil {
		fail(err.Error())
		return
	}
	if !freshUntil.After(time.Now()) || ctx.Err() != nil {
		fail("source_time_invalid")
		return
	}
	latestGate := r.gate.Load()
	if latestGate == nil || !sameNetworkNodeGateContext(before, *latestGate) || latestGate.nodeFailure(time.Now()) != "" {
		fail("binding_changed")
		return
	}
	// A receiver transition during encoding does not invalidate independent
	// node observations; lookup will suppress the receiver on a stamp mismatch.
	receiverStamp := after.stamp
	if before.stamp != after.stamp {
		// This capture predates the replacement receiver gate. Keep its old
		// stamp so an identical refetch can restore the child after revalidation.
		receiverStamp = before.stamp
	}
	r.latest.Store(&publicNetworkDetailFamily{node: prepared, sourceDigest: digest, publishedAt: collectedAt,
		sourceFreshUntil: freshUntil, nodeStamp: before.nodeStamp, stamp: receiverStamp})
	r.failure.Store(nil)
}

func (r *networkDetailReader) lookupNode(gate networkDetailGate, family *publicNetworkDetailFamily, failure *networkDetailReaderFailure, now time.Time, extended, latest, wantGate bool) networkDetailReply {
	if reason := gate.nodeFailure(now); reason != "" {
		return networkDetailFailure(reason)
	}
	n := family.node
	if !sameNodeNetworkBinding(gate.nodeBinding(), n.binding) || gate.nodeStamp != family.nodeStamp {
		return networkDetailFailure("binding_changed")
	}
	if now.Before(family.publishedAt) {
		return networkDetailFailure("source_time_invalid")
	}
	reason := ""
	remaining := family.sourceFreshUntil.Sub(now).Milliseconds()
	if remaining < 1 {
		reason = "status_stale"
	}
	if failure != nil && sameNetworkNodeGateContext(failure.gate, gate) {
		reason = failure.reason
	}
	if n.receiver != nil && (gate.stamp != family.stamp || gate.failure(now) != "" || !gate.matches(*n.receiver)) {
		if reason != "" {
			return networkDetailFailure(reason)
		}
		if n.independentOnly == "" {
			return networkDetailFailure("binding_changed")
		}
		body := n.independentOnly
		if extended && n.extendedIndependentOnly != "" {
			body = n.extendedIndependentOnly
		}
		if latest && n.latestIndependentOnly != "" {
			body = n.latestIndependentOnly
		}
		return networkDetailReply{status: http.StatusOK, body: body, remainingMilliseconds: remaining,
			stamp: gate.nodeStamp, nodeScoped: true, family: family, failure: failure}
	}
	for i, candidate := range networkDetailPublicReasons {
		if candidate == reason {
			tail := n.tail
			if extended && n.extendedTail != "" {
				tail = n.extendedTail
			}
			if latest && n.latestTail != "" {
				tail = n.latestTail
			}
			if wantGate && n.gateTail != "" {
				tail = n.gateTail
			}
			reply := networkDetailReply{status: http.StatusOK, body: n.bodies[i], tail: tail, stamp: gate.nodeStamp,
				nodeScoped: true, family: family, failure: failure}
			if n.receiver != nil {
				reply.stamp, reply.nodeScoped = gate.stamp, false
			}
			if reason == "" {
				reply.remainingMilliseconds = remaining
			}
			return reply
		}
	}
	return networkDetailFailure("source_invalid")
}

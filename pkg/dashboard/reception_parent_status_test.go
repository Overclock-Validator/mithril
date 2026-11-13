package dashboard

import (
	"context"
	"net/http"
	"testing"
	"time"
)

// A cached live root must not outlive the status observation that identifies
// its node. Explicitly addressed historical records have different semantics.
func TestReceptionLiveRootRejectsExpiredStatus(t *testing.T) {
	input := receptionReaderInput(receptionChunkFixture(t, 1, 2, false))
	r, c, _ := receptionPublicFixture(t, []ReceptionHistoryChunkInput{input}, true)
	c.statusNodeEpoch = 1
	stamp := c.observeReceptionHistoryStatus("fresh", time.Now())
	receptionAcceptTest(t, r, r.last, time.Second)
	if err := r.publish(context.Background(), r.status(), true); err != nil {
		t.Fatal(err)
	}
	r.admitStatus = func(s receptionPublicSelection) (time.Duration, bool) { return c.admitReceptionHistory(r, s) }
	now := time.Now()
	c.clockOrigin = now
	frame := &frameSet{freshUntil: now.Add(time.Second), clockOrigin: now, clockUncertain: &c.clockUncertain}
	frame.fence.nodeEpoch, frame.fence.families[statusReception] = stamp.nodeEpoch, stamp.familyEpoch
	c.latest.Store(frame)
	if w := receptionPublicGet(r.publicHandler(), receptionHistoryPublicPath); w.Code != http.StatusOK || w.Header().Get(statusRemainingHeader) == "" || w.Header().Get(replayDetailRemainingHeader) == "" {
		t.Fatalf("fresh root lacked independently bounded leases: status %d", w.Code)
	}
	otherFrame := *frame
	otherFrame.fence.nodeEpoch++
	c.latest.Store(&otherFrame)
	if w := receptionPublicGet(r.publicHandler(), receptionHistoryPublicPath); w.Code == http.StatusOK && w.Header().Get(replayDetailRemainingHeader) != "" {
		t.Fatal("root was admitted with a different status binding")
	}
	expired := *frame
	expired.freshUntil = now.Add(-time.Millisecond)
	c.latest.Store(&expired)
	w := receptionPublicGet(r.publicHandler(), receptionHistoryPublicPath)
	if w.Code == http.StatusOK && w.Header().Get(replayDetailRemainingHeader) != "" {
		t.Fatalf("live root remained fresh after parent status expired: %s", w.Body.String())
	}
	path := receptionHistoryPublicPath + "/chunk/" + r.last.RecordingID + "/" + input.Descriptor.Digest
	if w := receptionPublicGet(r.publicHandler(), path); w.Code != http.StatusOK || w.Body.String() != input.Body {
		t.Fatal("expired parent incorrectly hid explicitly addressed history")
	}
}

package dashboard

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestReceptionLiveAdmissionHonorsAcceptedCapture(t *testing.T) {
	setup := func(t *testing.T) (*receptionHistoryReader, *collector, ReceptionHistoryRoot) {
		t.Helper()
		m := receptionCollectorMetadata()
		m.PublishedAt = time.Now().UTC()
		m.Sources = []ReceptionSource{{Generation: 1, StartedAt: m.PublishedAt, Kind: "live"}}
		root := receptionTestRoot(t, m)
		r, c := receptionCollectorForTest(t, root)
		c.statusNodeEpoch = 1
		c.observeReceptionHistoryStatus("fresh", time.Now())
		receptionAcceptTest(t, r, root, 2*time.Second)
		receptionInstallTest(t, r, m)
		now := time.Now()
		frame := &frameSet{freshUntil: now.Add(3 * time.Second), clockOrigin: c.clockOrigin, clockUncertain: &c.clockUncertain}
		frame.fence.nodeEpoch, frame.fence.families[statusReception] = 1, r.status().epoch
		c.latest.Store(frame)
		r.admitStatus = func(selected receptionPublicSelection) (time.Duration, bool) {
			return c.admitReceptionHistory(r, selected)
		}
		return r, c, root
	}
	t.Run("shorter lease before selection", func(t *testing.T) {
		r, _, root := setup(t)
		receptionAcceptTest(t, r, root, time.Second)
		response := receptionPublicGet(r.publicHandler(), receptionHistoryPublicPath)
		remaining, err := strconv.ParseInt(response.Header().Get(replayDetailRemainingHeader), 10, 64)
		if response.Code != http.StatusOK || err != nil || remaining < 1 || remaining > 1000 || response.Header().Get(statusRemainingHeader) == "" {
			t.Fatalf("shortened accepted lease not reflected: status=%d remaining=%q", response.Code, response.Header().Get(replayDetailRemainingHeader))
		}
	})
	t.Run("shorter lease after selection", func(t *testing.T) {
		r, c, root := setup(t)
		var acceptErr error
		r.admitStatus = func(selected receptionPublicSelection) (time.Duration, bool) {
			acceptErr = r.accept(context.Background(), r.status(), root, receptionReaderResponse{started: time.Now(), remaining: time.Second})
			return c.admitReceptionHistory(r, selected)
		}
		response := receptionPublicGet(r.publicHandler(), receptionHistoryPublicPath)
		if acceptErr != nil || response.Code != http.StatusServiceUnavailable || response.Header().Get(replayDetailRemainingHeader) != "" || response.Header().Get(statusRemainingHeader) != "" {
			t.Fatalf("shortened lease after selection admitted: status=%d accept=%v", response.Code, acceptErr)
		}
	})
	t.Run("source becomes stale before publication", func(t *testing.T) {
		r, _, root := setup(t)
		root.State = "stale"
		receptionAcceptTest(t, r, root, 0)
		response := receptionPublicGet(r.publicHandler(), receptionHistoryPublicPath)
		if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"state":"stale"`) ||
			response.Header().Get(replayDetailRemainingHeader) != "" || response.Header().Get(statusRemainingHeader) != "" {
			t.Fatalf("stale accepted source remained live: status=%d body=%s", response.Code, response.Body.String())
		}
	})
	t.Run("remaining lease after delayed admission", func(t *testing.T) {
		r, c, _ := setup(t)
		var deadline time.Time
		r.admitStatus = func(selected receptionPublicSelection) (time.Duration, bool) {
			deadline = selected.expires
			remaining, ok := c.admitReceptionHistory(r, selected)
			time.Sleep(120 * time.Millisecond)
			return remaining, ok
		}
		response := receptionPublicGet(r.publicHandler(), receptionHistoryPublicPath)
		if deadline.IsZero() {
			t.Fatal("live root did not reach status admission")
		}
		if response.Code == http.StatusOK && response.Header().Get(replayDetailRemainingHeader) == "" && strings.Contains(response.Body.String(), `"state":"stale"`) {
			return
		}
		remaining, err := strconv.ParseInt(response.Header().Get(replayDetailRemainingHeader), 10, 64)
		if response.Code != http.StatusOK || err != nil || remaining < 1 || remaining > time.Until(deadline).Milliseconds()+40 {
			t.Fatalf("delayed admission overstated the remaining lease: status=%d remaining=%q", response.Code, response.Header().Get(replayDetailRemainingHeader))
		}
	})
	t.Run("new publication after selection", func(t *testing.T) {
		r, c, root := setup(t)
		var publishErr error
		r.admitStatus = func(selected receptionPublicSelection) (time.Duration, bool) {
			replacement := root
			replacement.PublishedAt = root.PublishedAt.Add(time.Nanosecond)
			if publishErr = r.accept(context.Background(), r.status(), replacement, receptionReaderResponse{started: time.Now(), remaining: 2 * time.Second}); publishErr == nil {
				publishErr = r.publish(context.Background(), r.status(), true)
			}
			return c.admitReceptionHistory(r, selected)
		}
		response := receptionPublicGet(r.publicHandler(), receptionHistoryPublicPath)
		if publishErr != nil || response.Code != http.StatusServiceUnavailable || response.Header().Get(replayDetailRemainingHeader) != "" {
			t.Fatalf("replaced publication admitted: status=%d publish=%v", response.Code, publishErr)
		}
		r.admitStatus = func(selected receptionPublicSelection) (time.Duration, bool) {
			return c.admitReceptionHistory(r, selected)
		}
		if next := receptionPublicGet(r.publicHandler(), receptionHistoryPublicPath); next.Code != http.StatusOK {
			t.Fatalf("new publication was not available: status=%d body=%s", next.Code, next.Body.String())
		}
	})
}

func TestReceptionExplicitHistorySurvivesStatusLoss(t *testing.T) {
	input := receptionReaderInput(receptionChunkFixture(t, 1, 2, false))
	r, c, _ := receptionPublicFixture(t, []ReceptionHistoryChunkInput{input}, true)
	id := r.last.RecordingID
	c.observeReceptionHistoryStatus("unavailable", time.Now())
	for _, path := range []string{
		receptionHistoryPublicPath + "/chunk/" + id + "/" + input.Descriptor.Digest,
		receptionHistoryPublicPath + "/metadata/" + id + "/" + r.last.Metadata.Digest + "/sources/0",
		receptionHistoryPublicPath + "/cursor/" + id + "?after=0",
	} {
		if response := receptionPublicGet(r.publicHandler(), path); response.Code != http.StatusOK {
			t.Fatalf("same-identity status loss blocked explicit history %q: %d", path, response.Code)
		}
	}
}

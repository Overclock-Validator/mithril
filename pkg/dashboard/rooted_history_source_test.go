package dashboard

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func rootedHistoryLiveStatus(d RootedHistoryDetail) Snapshot {
	s := testSnapshot(time.Now().UTC())
	s.RunID, s.StartedAt, s.GenesisHash = d.Binding.RunID, d.Binding.NodeStartedAt, d.Binding.GenesisHash
	s.Replay = &Replay{Attempt: d.Binding.Attempt, Rewinds: d.Binding.Rewinds, StartedAt: d.Binding.AttemptStartedAt, Phase: "replaying"}
	return s
}

func rootedHistoryResponse(s *rootedHistorySource, method, target, body string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	s.serveHTTP(w, httptest.NewRequest(method, target, strings.NewReader(body)))
	return w
}

func TestRootedHistorySourceFencesAndOriginalDates(t *testing.T) {
	d := rootedHistoryDetailFixture()
	var sourceErr error
	deadline := time.Now().Add(RootedHistoryFreshFor)
	s := &rootedHistorySource{sample: func(now time.Time) (RootedHistoryCapture, error) {
		copy := d
		copy.CapturedAt = now.UTC()
		return RootedHistoryCapture{Detail: copy, FreshUntil: deadline}, sourceErr
	}}
	update := func() { s.update(rootedHistoryLiveStatus(d), time.Now()) }
	update()
	require.Empty(t, s.latest.Load().reason)
	first := s.latest.Load()
	require.Equal(t, d.ObservedAt, first.detail.ObservedAt)
	require.Equal(t, http.StatusOK, rootedHistoryResponse(s, "GET", "/details/slots", "").Code)
	for _, bad := range []struct{ method, path, body string }{
		{"POST", "/details/slots", ""}, {"GET", "/details/slots?", ""}, {"GET", "/details/slots?x=1", ""},
		{"GET", "/details/%73lots", ""}, {"GET", "/details/slots", "x"},
	} {
		require.Equal(t, http.StatusBadRequest, rootedHistoryResponse(s, bad.method, bad.path, bad.body).Code)
	}
	sourceErr = errors.New("private source error must not escape")
	update()
	require.Equal(t, "source_unavailable", s.latest.Load().reason)
	require.NotContains(t, rootedHistoryResponse(s, "GET", "/details/slots", "").Body.String(), "private source")
	sourceErr = nil
	d.ObservedAt = d.ObservedAt.Add(time.Second)
	update()
	require.Equal(t, "source_invalid", s.latest.Load().reason, "same bank cannot acquire a new observation date after an error")
	d = first.detail
	d.BankSlot, d.NextSlot = 21, 22
	bitmap, err := base64.StdEncoding.DecodeString(d.Bitmap)
	require.NoError(t, err)
	bitmap[21/8] |= 1 << (21 % 8)
	d.Bitmap = base64.StdEncoding.EncodeToString(bitmap)
	d.ObservedAt = d.ObservedAt.Add(-time.Hour)
	update()
	require.Empty(t, s.latest.Load().reason, "a new bank may have an earlier wall date")
	d = first.detail
	update()
	require.Equal(t, "binding_changed", s.latest.Load().reason, "same-generation rooted bank cannot regress")
	d.Binding.SourceGeneration++
	update()
	require.Equal(t, "binding_changed", s.latest.Load().reason, "generation cannot silently change within the same status attempt")
	d.Binding.Attempt++
	d.Binding.AttemptStartedAt = d.Binding.AttemptStartedAt.Add(time.Second)
	update()
	require.Empty(t, s.latest.Load().reason, "new attempt can start at an earlier root")
	// A fresh gate must not revive an expired source-copy lease.
	p := *s.latest.Load()
	p.freshUntil = time.Now().Add(-time.Millisecond)
	s.latest.Store(&p)
	require.Equal(t, http.StatusServiceUnavailable, rootedHistoryResponse(s, "GET", "/details/slots", "").Code)
}

func TestRootedHistorySourceRejectsBadCaptureClockAndClonesBinding(t *testing.T) {
	d := rootedHistoryDetailFixture()
	d.Epoch = &RootedHistoryEpoch{Number: 0, FirstSlot: 0, Slots: 32}
	genesis := d.Bankhash
	d.Binding.GenesisHash = &genesis
	s := &rootedHistorySource{}
	for _, mode := range []string{"wall-only", "expired", "extended", "wrong-copy-date", "valid"} {
		s.sample = func(now time.Time) (RootedHistoryCapture, error) {
			copy := d
			copy.CapturedAt = now.UTC()
			until := now.Add(RootedHistoryFreshFor)
			switch mode {
			case "wall-only":
				until = until.Round(0)
			case "expired":
				until = now.Add(-time.Second)
			case "extended":
				until = until.Add(time.Second)
			case "wrong-copy-date":
				copy.CapturedAt = copy.CapturedAt.Add(-time.Second)
			}
			return RootedHistoryCapture{Detail: copy, FreshUntil: until}, nil
		}
		s.update(rootedHistoryLiveStatus(d), time.Now())
		if mode == "valid" {
			require.Empty(t, s.latest.Load().reason)
		} else {
			require.Equal(t, "source_time_invalid", s.latest.Load().reason, mode)
		}
	}
	original := *s.lastGood.detail.Binding.GenesisHash
	genesis = "changed"
	require.Equal(t, original, *s.lastGood.detail.Binding.GenesisHash)
	genesis = original
	d.Epoch.Slots = 64
	require.Equal(t, uint64(32), s.lastGood.detail.Epoch.Slots, "publication must detach the optional child")
	require.NoError(t, validateRootedHistoryDetail(d), "mutation must reach the same-bank guard")
	s.update(rootedHistoryLiveStatus(d), time.Now())
	require.Equal(t, "source_invalid", s.latest.Load().reason, "same-bank geometry cannot change")
}

func TestRootedHistoryFeedRealUnixAndDefaultOff(t *testing.T) {
	d := rootedHistoryDetailFixture()
	var calls atomic.Int64
	path := socketPath(t)
	feed, err := ListenFeedWithDetails(context.Background(), path, "", func() Snapshot { return rootedHistoryLiveStatus(d) }, DetailSamplers{
		RootedHistory: func(now time.Time) (RootedHistoryCapture, error) {
			calls.Add(1)
			copy := d
			copy.CapturedAt = now.UTC()
			return RootedHistoryCapture{Detail: copy, FreshUntil: now.Add(RootedHistoryFreshFor)}, nil
		},
	})
	require.NoError(t, err)
	t.Cleanup(feed.Close)
	c := newCollector(path)
	t.Cleanup(c.client.CloseIdleConnections)
	for range 8 {
		resp, err := c.client.Get("http://node/details/slots")
		require.NoError(t, err)
		body, err := io.ReadAll(io.LimitReader(resp.Body, MaxRootedHistoryBytes+1))
		require.NoError(t, resp.Body.Close())
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Equal(t, "no-store", resp.Header.Get("Cache-Control"))
		require.NotEmpty(t, resp.Header.Get(replayDetailRemainingHeader))
		parsed, err := decodeRootedHistoryDetail(body)
		require.NoError(t, err)
		require.Equal(t, d.Bitmap, parsed.Bitmap)
		require.Equal(t, d.ObservedAt, parsed.ObservedAt)
	}
	require.Equal(t, int64(1), calls.Load(), "viewers must not invoke the source")
	path = socketPath(t)
	off, err := ListenFeed(context.Background(), path, func() Snapshot { return rootedHistoryLiveStatus(d) })
	require.NoError(t, err)
	t.Cleanup(off.Close)
	c2 := newCollector(path)
	t.Cleanup(c2.client.CloseIdleConnections)
	resp, err := c2.client.Get("http://node/details/slots")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	require.Nil(t, off.rooted)
}

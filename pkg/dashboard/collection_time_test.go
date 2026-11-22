package dashboard

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestCollectorDatesFetchedReceptionAtReceipt(t *testing.T) {
	path := socketPath(t)
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// This sample is necessarily newer than the poll's invocation time.
		at := time.Now().UTC()
		s := receptionFixture(at)
		s.BlockSource.Reception.ObservedAt = &at
		if err := json.NewEncoder(w).Encode(s); err != nil {
			t.Error(err)
		}
	})}
	done := make(chan struct{})
	go func() { defer close(done); server.Serve(listener) }()
	t.Cleanup(func() { server.Close(); listener.Close(); <-done })
	c := newCollector(path)
	c.public = true
	defer c.client.CloseIdleConnections()
	for range 5 {
		c.poll(context.Background(), time.Now().UTC())
		var p PublicSnapshot
		if err := json.Unmarshal(c.latest.Load().publicJSON, &p); err != nil {
			t.Fatal(err)
		}
		if p.Input == nil || p.Input.Reception == nil || p.Input.Reception.Slot == nil || *p.Input.Reception.Slot != 0 ||
			p.CollectorAt.Before(p.Input.Reception.SampledAt) {
			t.Fatalf("fresh post-request reception was lost or dated before collection: collector=%s input=%+v", p.CollectorAt, p.Input)
		}
	}
}

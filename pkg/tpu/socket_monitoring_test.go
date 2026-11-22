package tpu

import (
	"context"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/identity"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
)

func TestTPUOwnedSocketObservation(t *testing.T) {
	var absent *TPU
	if o := absent.ObserveSocketMemory(); o.Reason != metrics.SocketNotProvided || o.Known {
		t.Fatal("nil TPU fabricated socket counters")
	}
	holder, err := identity.NewEphemeralHolder()
	if err != nil {
		t.Fatal(err)
	}
	svc, err := Start(context.Background(), Config{Name: "socket-observation", ListenAddr: "127.0.0.1:0", Identity: holder})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = svc.Stop(nil) })
	before := svc.ListenAddr().String()
	o := svc.ObserveSocketMemory()
	if o.Reason == metrics.SocketNotProvided || svc.ListenAddr().String() != before || !o.Known && o.Memory != (metrics.SocketMemory{}) {
		t.Fatal("actual owner read changed the listener or fabricated unavailable counters")
	}
	if err := svc.Stop(nil); err != nil {
		t.Fatal(err)
	}
	if o := svc.ObserveSocketMemory(); o.Known || o.Memory != (metrics.SocketMemory{}) {
		t.Fatal("stopped socket retained successful kernel accounting")
	}
}

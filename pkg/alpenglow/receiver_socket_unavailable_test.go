package alpenglow

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/metrics"
)

func TestReceiverSocketMemoryUnavailable(t *testing.T) {
	var absent *Receiver
	want := metrics.SocketObservation{Reason: metrics.SocketNotProvided}
	if got := absent.ObserveSocketMemory(); got != want {
		t.Fatalf("nil receiver: got %+v, want %+v", got, want)
	}
	r, err := NewReceiver(ReceiverConfig{BindAddr: "127.0.0.1:0"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	if got := r.ObserveSocketMemory(); got != want {
		t.Fatalf("live receiver: got %+v, want %+v", got, want)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if got := r.ObserveSocketMemory(); got != want {
		t.Fatalf("closed receiver: got %+v, want %+v", got, want)
	}
}

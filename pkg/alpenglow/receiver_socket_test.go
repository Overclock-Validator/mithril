package alpenglow

import (
	"crypto/ed25519"
	"crypto/rand"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/identity"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
)

func TestReceiverSocketObservationLifetime(t *testing.T) {
	var absent *Receiver
	if got := absent.ObserveSocketMemory(); got != (metrics.SocketObservation{Reason: metrics.SocketNotProvided}) {
		t.Fatal("nil receiver fabricated socket data")
	}
	holder, err := identity.NewEphemeralHolder()
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewReceiver(ReceiverConfig{BindAddr: "127.0.0.1:0", Identity: holder}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	addr := r.Addr().(*net.UDPAddr)
	want := metrics.SocketObservation{Reason: metrics.SocketNotProvided}
	if got := r.ObserveSocketMemory(); got != want {
		t.Fatalf("live receiver: got %+v, want %+v", got, want)
	}
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Swap(key); err != nil {
		t.Fatal(err)
	}
	if r.Addr().String() != addr.String() {
		t.Fatal("certificate identity swap changed the Votor address")
	}
	if got := r.ObserveSocketMemory(); got != want {
		t.Fatalf("receiver after identity swap: got %+v, want %+v", got, want)
	}
	var readers sync.WaitGroup
	for range 4 {
		readers.Go(func() {
			for range 100 {
				if got := r.ObserveSocketMemory(); got != want {
					t.Errorf("concurrent observation: got %+v, want %+v", got, want)
					return
				}
			}
		})
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	readers.Wait()
	if got := r.ObserveSocketMemory(); got != want {
		t.Fatal("closed receiver retained an available observation")
	}
	if err := r.Close(); err != nil {
		t.Fatal("repeated close changed the existing idempotent contract", err)
	}
	// The listener may release its UDP port asynchronously.
	deadline := time.Now().Add(2 * time.Second)
	for {
		rebound, err := net.ListenUDP("udp", addr)
		if err == nil {
			if err := rebound.Close(); err != nil {
				t.Fatal(err)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("receiver close did not release its socket", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

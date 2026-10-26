package packet

import "testing"

func TestOwnedFromCopiesWireAndRecordsSource(t *testing.T) {
	wire := []byte{1, 2, 3}
	pkt := OwnedFrom(wire, SourceRelayer)
	wire[0] = 9
	if got := pkt.Data(); len(got) != 3 || got[0] != 1 {
		t.Fatalf("data = %v, want an independent copy", got)
	}
	if pkt.Source() != SourceRelayer || pkt.Source().String() != "relayer" {
		t.Fatalf("source = %v", pkt.Source())
	}
	if Owned(wire).Source() != SourceQUIC {
		t.Fatalf("Owned must default to the QUIC source")
	}
	pool := NewPool(1)
	buf, slot, ok := pool.Acquire()
	if !ok {
		t.Fatalf("Acquire failed")
	}
	if FromPool(pool, buf, 0, slot).Source() != SourceQUIC {
		t.Fatalf("pooled packets come from the QUIC listener")
	}
}

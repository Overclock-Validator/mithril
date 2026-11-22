package gossip

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"math"
	"net"
	"testing"
	"time"
	"unsafe"
)

func TestPeerDatasetSource(t *testing.T) {
	at := time.Now()
	var off *peerDataset
	if newPeerDataset(false, 1, at) != nil || newPeerDataset(true, 0, at) != nil {
		t.Fatal("disabled or exhausted generation created an owner")
	}
	if n := testing.AllocsPerRun(10, func() { off.observeAccepted(contactRecord{}, at); off.publish(at); off.retire() }); n != 0 {
		t.Fatal("disabled source allocated", n)
	}
	if _, ok := off.snapshot(); ok {
		t.Fatal("disabled source fabricated a publication")
	}

	t.Run("signed gate and original expiry", func(t *testing.T) {
		o := newPeerDataset(true, 1, at.Add(-time.Second))
		key := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{7}, ed25519.SeedSize))
		info, err := NewContactInfo(Pubkey(key.Public().(ed25519.PublicKey)), 4321,
			&net.UDPAddr{IP: net.ParseIP("203.0.113.1"), Port: 8000}, nil)
		if err != nil {
			t.Fatal(err)
		}
		info.Outset, info.Wallclock = 2, 10
		value, err := signCrdsContactInfo(info, key)
		if err != nil {
			t.Fatal(err)
		}
		for i, forged := range []bool{true, false} {
			signed := value
			if forged {
				signed.Signature[0] ^= 1
			}
			packet, err := encodePushMessage(info.Pubkey, []CrdsValue{signed})
			if err != nil {
				t.Fatal(err)
			}
			_, err = decodePacketWithContactHandler(packet, func(r contactRecord) { o.observeAccepted(r, at) })
			if err != nil {
				t.Fatal(err)
			}
			o.publish(at.Add(time.Duration(i) * time.Nanosecond))
			s, ok := o.snapshot()
			if !ok || forged && len(s.Rows) != 0 || !forged && (len(s.Rows) != 1 || s.Rows[0].Pubkey != info.Pubkey) {
				t.Fatal("existing signature gate did not bound the observer")
			}
		}
		r := contactRecordFromInfo(t, info, key)
		o.observeAccepted(r, at.Add(5*time.Minute)) // Equal advert cannot refresh.
		o.observeAccepted(contactRecord{Pubkey: r.Pubkey, Outset: 1, Wallclock: 20}, at.Add(6*time.Minute))
		entry := o.rows[r.Pubkey]
		if entry.receivedAt != at || !entry.row.ReceivedAt.Equal(at.UTC()) || entry.receivedAt == entry.receivedAt.Round(0) {
			t.Fatal("original monotonic receipt was replaced or converted to UTC")
		}
		// UTC metadata is not the expiry clock, even across a wall discontinuity.
		entry.row.ReceivedAt = at.Add(time.Hour).UTC()
		o.rows[r.Pubkey] = entry
		o.publish(at.Add(peerExpirationWindow + time.Nanosecond))
		s, _ := o.snapshot()
		if len(s.Rows) != 0 || s.AcceptedAdverts != 3 {
			t.Fatal("repeat renewed expiry or occurrences became unique identities")
		}
	})

	t.Run("prefix race and delayed publication", func(t *testing.T) {
		o := newPeerDataset(true, 2, at)
		r := contactRecord{Pubkey: Pubkey{1}, Outset: 1, Wallclock: 1}
		o.observeAccepted(r, at)
		r.Wallclock++
		o.observeAccepted(r, at.Add(time.Second))
		o.publish(at.Add(time.Second / 2))
		s, _ := o.snapshot()
		if !s.Limited || len(s.Rows) != 0 {
			t.Fatal("newer replacement silently erased an earlier capture prefix")
		}
		o.publish(at.Add(2 * time.Second))
		s, _ = o.snapshot()
		if s.Limited || len(s.Rows) != 1 {
			t.Fatal("next complete capture did not recover the row")
		}
		s.Rows[0].Pubkey[0] = 99
		o.install(&PeerDatasetSnapshot{PublicationSequence: 1, SampledAt: at.Add(time.Second)}) // A slow earlier copy finishes late.
		o.publish(at.Add(time.Second))
		current, ok := o.snapshot()
		if !ok || len(current.Rows) != 1 || current.Rows[0].Pubkey[0] != 1 || !current.SampledAt.Equal(at.Add(2*time.Second)) {
			t.Fatal("old publication or caller mutation replaced the cached generation")
		}
		o.retire()
		o.observeAccepted(r, at.Add(3*time.Second))
		o.publish(at.Add(3 * time.Second))
		o.install(&current)
		if _, ok := o.snapshot(); ok || len(o.rows) != 0 {
			t.Fatal("retired owner was revived")
		}
		next := newPeerDataset(true, 3, at.Add(4*time.Second))
		next.publish(at.Add(4 * time.Second))
		fresh, ok := next.snapshot()
		if !ok || fresh.Generation != 3 || fresh.PublicationSequence != 1 || len(fresh.Rows) != 0 {
			t.Fatal("new Run inherited prior contacts")
		}
	})

	t.Run("publication order and wall rollback", func(t *testing.T) {
		o := newPeerDataset(true, 5, at)
		o.publish(at)
		first, ok := o.snapshot()
		if !ok || first.PublicationSequence != 1 || first.SampledAt != at {
			t.Fatal("first publication lost its sequence or original clock")
		}
		o.publish(at)
		o.publish(at.Add(-time.Nanosecond))
		if o.publicationSequence != 1 {
			t.Fatal("duplicate or out-of-order capture consumed a sequence")
		}
		nextAt := at.Add(time.Second)
		o.publish(nextAt)
		second, ok := o.snapshot()
		if !ok || second.PublicationSequence != 2 || second.SampledAt != nextAt ||
			second.SampledAt == second.SampledAt.Round(0) || second.SampledAt.Sub(first.SampledAt) != time.Second {
			t.Fatal("publication did not retain its original monotonic sample")
		}

		// Exercise the post-copy install boundary with synthetic wall dates.
		// Production reserves the sequence after its original-clock comparison;
		// this test neither changes the host clock nor fabricates monotonic bits.
		rollback := second
		rollback.PublicationSequence = 3
		rollback.SampledAt = at.UTC().Add(-time.Hour)
		o.mu.Lock()
		o.publicationSequence = rollback.PublicationSequence
		o.mu.Unlock()
		o.install(&rollback)
		current, ok := o.snapshot()
		if !ok || current.PublicationSequence != 3 || !current.SampledAt.Equal(rollback.SampledAt) {
			t.Fatal("ordered publication was rejected because its wall date regressed")
		}
		duplicate := rollback
		duplicate.SampledAt = at.Add(time.Hour)
		duplicate.AcceptedAdverts = 99
		o.install(&duplicate)
		o.install(&second)
		if o.latest.Load() != &rollback {
			t.Fatal("duplicate or delayed install replaced the newer sequence")
		}

		// A newer reserved capture fences an older install even before the newer
		// sorted copy is installed, rather than using only the cached high-water.
		o.mu.Lock()
		o.publicationSequence = 5
		o.mu.Unlock()
		delayed := rollback
		delayed.PublicationSequence = 4
		o.install(&delayed)
		if o.latest.Load() != &rollback {
			t.Fatal("an install older than the reserved capture replaced the cache")
		}
	})

	t.Run("publication sequence exhaustion", func(t *testing.T) {
		o := newPeerDataset(true, 6, at)
		o.publicationSequence = math.MaxUint64 - 1
		o.observeAccepted(contactRecord{Pubkey: Pubkey{1}, Wallclock: 1}, at)
		o.publish(at)
		last, ok := o.snapshot()
		if !ok || last.PublicationSequence != math.MaxUint64 || len(last.Rows) != 1 {
			t.Fatal("last representable publication was lost")
		}
		o.publish(at.Add(time.Second))
		if _, ok := o.snapshot(); ok || !o.retired || o.rows != nil || o.publicationSequence != math.MaxUint64 {
			t.Fatal("publication sequence wrapped or exhaustion retained a live cache")
		}
		o.install(&last)
		o.observeAccepted(contactRecord{Pubkey: Pubkey{2}, Wallclock: 1}, at.Add(2*time.Second))
		o.publish(at.Add(2 * time.Second))
		if _, ok := o.snapshot(); ok || o.rows != nil {
			t.Fatal("exhausted publication owner was revived")
		}
	})

	t.Run("capacity and saturation", func(t *testing.T) {
		o := newPeerDataset(true, 4, at)
		for i := uint32(1); i <= PeerDatasetCapacity; i++ {
			r := contactRecord{Wallclock: 1}
			binary.LittleEndian.PutUint32(r.Pubkey[:4], i)
			o.observeAccepted(r, at)
		}
		extra := contactRecord{Pubkey: Pubkey{0, 0, 1}, Wallclock: 1}
		o.observeAccepted(extra, at)
		known := contactRecord{Pubkey: Pubkey{1}, Wallclock: 2}
		if n := testing.AllocsPerRun(100, func() { known.Wallclock++; o.observeAccepted(known, at) }); n != 0 {
			t.Fatal("known-identity replacement allocated", n)
		}
		o.publish(at)
		s, _ := o.snapshot()
		if len(o.rows) != PeerDatasetCapacity || len(s.Rows) != PeerDatasetCapacity || s.CapacityRefusals != 1 || !s.Limited {
			t.Fatal("capacity refusal or coverage was hidden")
		}
		o.publish(at.Add(peerExpirationWindow))
		s, _ = o.snapshot()
		if !s.Limited {
			t.Fatal("refusal coverage cleared before its full window")
		}
		o.publish(at.Add(peerExpirationWindow + time.Nanosecond))
		s, _ = o.snapshot()
		if s.Limited || len(s.Rows) != 0 {
			t.Fatal("original expiry did not bound coverage and retained rows")
		}
		o.accepted = math.MaxUint64
		o.observeAccepted(known, at.Add(peerExpirationWindow+2*time.Nanosecond))
		o.publish(at.Add(peerExpirationWindow + 3*time.Nanosecond))
		s, _ = o.snapshot()
		if !s.CountersSaturated || !s.Limited || s.AcceptedAdverts != math.MaxUint64 {
			t.Fatal("counter wrapped or advertised complete coverage")
		}
	})

	// Representation model only: a conservative two-slot-per-row map allowance
	// plus controls, one cached detached copy, and two further copy overlaps.
	// This is not a measured map-allocation, lock-latency or RSS guarantee.
	row, entry := unsafe.Sizeof(PeerDatasetRow{}), unsafe.Sizeof(peerDatasetEntry{})
	table := 2*PeerDatasetCapacity*(unsafe.Sizeof(Pubkey{})+entry+32) + unsafe.Sizeof(peerDataset{})
	copyBytes := PeerDatasetCapacity*row + unsafe.Sizeof(PeerDatasetSnapshot{})
	if table+copyBytes > 2<<20 || table+3*copyBytes > 4<<20 {
		t.Fatal("typed source holder target exceeded", table, copyBytes)
	}
	t.Logf("row=%d entry=%d modeled retained=%d peak=%d; not runtime memory or lock-cost proof", row, entry, table+copyBytes, table+3*copyBytes)
}

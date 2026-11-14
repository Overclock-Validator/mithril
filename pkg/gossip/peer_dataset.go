package gossip

import (
	"bytes"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// PeerDatasetCapacity bounds retained local signed-contact identities. This is
// not the cluster population, a routing limit or a reachability assertion.
const PeerDatasetCapacity = 4096

// PeerDatasetRow contains only accepted self-reported contact metadata. The
// public identity is not an endpoint; no signed bytes or socket data escape.
type PeerDatasetRow struct {
	PeerObservation
	Outset, Wallclock uint64
}

// PeerDatasetSnapshot is one detached local contact observation. Limited means
// known loss in this run's retained window, not incomplete cluster discovery.
// Counters count advert occurrences, not unique identities or atomic categories.
// They are copied when publication acquires the metadata lock, so concurrent
// receipts after SampledAt can already be reflected in these independent totals.
type PeerDatasetSnapshot struct {
	Generation uint64
	// PublicationSequence orders captures within this Run, independently of UTC.
	// Rejected delayed installs can leave gaps; zero is never published.
	PublicationSequence uint64
	StartedAt           time.Time
	SampledAt           time.Time
	AcceptedAdverts     uint64
	RetainedAdverts     uint64
	CapacityRefusals    uint64
	CountersSaturated   bool
	Limited             bool
	Rows                []PeerDatasetRow
}

type peerDatasetEntry struct {
	row        PeerDatasetRow
	receivedAt time.Time // Original same-process clock, never the row's UTC copy.
}

// peerDataset belongs to one Run, not a Client reused by another Run. Only the
// existing diagnostics loop may publish; request handlers must read its cache.
type peerDataset struct {
	mu                              sync.Mutex
	startedAt                       time.Time
	generation                      uint64
	rows                            map[Pubkey]peerDatasetEntry
	accepted, retained, refused     uint64
	saturated, timeInvalid, retired bool
	lastRefusal                     time.Time
	publicationSequence             uint64
	lastPublicationAt               time.Time // Original clock, including its monotonic component.
	latest                          atomic.Pointer[PeerDatasetSnapshot]
}

func newPeerDataset(enabled bool, generation uint64, startedAt time.Time) *peerDataset {
	if !enabled || generation == 0 || !peerDatasetTimeValid(startedAt) {
		return nil
	}
	return &peerDataset{startedAt: startedAt, generation: generation,
		rows: make(map[Pubkey]peerDatasetEntry, PeerDatasetCapacity)}
}

func peerDatasetTimeValid(at time.Time) bool {
	return !at.IsZero() && at.Year() >= 1 && at.Year() <= 9999
}

func (o *peerDataset) increment(counter *uint64) {
	if *counter == math.MaxUint64 {
		o.saturated = true
		return
	}
	*counter++
}

// observeAccepted must be called only after the existing signature, compatible
// shred-version and routing-admission checks. It never verifies or routes again.
// Nil is the default-off path: no lock, clock, allocation or signature work.
func (o *peerDataset) observeAccepted(r contactRecord, receivedAt time.Time) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.retired {
		return
	}
	if r.Pubkey == (Pubkey{}) || !peerDatasetTimeValid(receivedAt) || receivedAt.Before(o.startedAt) ||
		!peerDatasetTimeValid(receivedAt.Add(peerExpirationWindow)) {
		o.timeInvalid = true
		return
	}
	o.increment(&o.accepted)
	old, exists := o.rows[r.Pubkey]
	if exists && (r.Outset < old.row.Outset || r.Outset == old.row.Outset && r.Wallclock <= old.row.Wallclock) {
		return // Equal or older signed adverts never renew receipt or expiry.
	}
	if !exists && len(o.rows) == PeerDatasetCapacity {
		o.increment(&o.refused)
		if o.lastRefusal.IsZero() || receivedAt.After(o.lastRefusal) {
			o.lastRefusal = receivedAt
		}
		return // No capacity-time eviction scan on the contact reception path.
	}
	o.rows[r.Pubkey] = peerDatasetEntry{row: PeerDatasetRow{PeerObservation: PeerObservation{
		Pubkey: r.Pubkey, Version: r.Version, ShredVersion: r.ShredVer, ReceivedAt: receivedAt.UTC(),
		Repair:    r.ServeRepairAddr.ok && r.ServeRepairAddr.port != 0,
		TVU:       r.TVUAddr.ok && r.TVUAddr.port != 0,
		Alpenglow: r.Sockets[socketTagAlpenglow].ok && r.Sockets[socketTagAlpenglow].port != 0,
	}, Outset: r.Outset, Wallclock: r.Wallclock}, receivedAt: receivedAt}
	o.increment(&o.retained)
}

// publish is reserved for the existing ten-second owner cycle. Expiry and a
// bounded copy hold the lock; sorting never holds up contact reception.
func (o *peerDataset) publish(now time.Time) {
	if o == nil {
		return
	}
	o.mu.Lock()
	if o.retired {
		o.mu.Unlock()
		return
	}
	if !peerDatasetTimeValid(now) || now.Before(o.startedAt) {
		o.timeInvalid = true
		o.latest.Store(nil)
		o.mu.Unlock()
		return
	}
	if !o.lastPublicationAt.IsZero() && !now.After(o.lastPublicationAt) {
		o.mu.Unlock()
		return // A delayed/repeated publisher cannot renew or remove newer data.
	}
	if o.publicationSequence == math.MaxUint64 {
		o.retired, o.rows = true, nil
		o.latest.Store(nil)
		o.mu.Unlock()
		return // Exhaustion permanently retires this Run's publication owner.
	}
	// Reserve before sorting outside the lock, so a delayed install cannot
	// replace a later capture even when the wall clock has moved backwards.
	o.publicationSequence++
	o.lastPublicationAt = now
	s := &PeerDatasetSnapshot{Generation: o.generation, PublicationSequence: o.publicationSequence,
		StartedAt: o.startedAt, SampledAt: now,
		AcceptedAdverts: o.accepted, RetainedAdverts: o.retained, CapacityRefusals: o.refused,
		CountersSaturated: o.saturated, Limited: o.saturated || o.timeInvalid,
		Rows: make([]PeerDatasetRow, 0, len(o.rows))}
	if !o.lastRefusal.IsZero() && (now.Before(o.lastRefusal) || now.Sub(o.lastRefusal) <= peerExpirationWindow) {
		s.Limited = true
	}
	for key, entry := range o.rows {
		if now.Before(entry.receivedAt) {
			s.Limited = true // A newer replacement may have displaced this prefix's row.
			continue         // A concurrent later receipt is outside this capture's prefix.
		}
		if now.Sub(entry.receivedAt) > peerExpirationWindow {
			delete(o.rows, key)
			continue
		}
		s.Rows = append(s.Rows, entry.row)
	}
	o.mu.Unlock()
	sort.Slice(s.Rows, func(i, j int) bool { return bytes.Compare(s.Rows[i].Pubkey[:], s.Rows[j].Pubkey[:]) < 0 })
	o.install(s)
}

func (o *peerDataset) install(s *PeerDatasetSnapshot) {
	o.mu.Lock()
	current := o.latest.Load()
	if !o.retired && s.PublicationSequence != 0 && s.PublicationSequence == o.publicationSequence &&
		(current == nil || s.PublicationSequence > current.PublicationSequence) {
		o.latest.Store(s)
	}
	o.mu.Unlock()
}

// snapshot copies only the immutable published cache, never the reception map.
// Its original clocks are not renewed by readers. Future transport must also
// enforce the Run generation and source-publication lease.
func (o *peerDataset) snapshot() (PeerDatasetSnapshot, bool) {
	if o == nil {
		return PeerDatasetSnapshot{}, false
	}
	s := o.latest.Load()
	if s == nil {
		return PeerDatasetSnapshot{}, false
	}
	copy := *s
	copy.Rows = append([]PeerDatasetRow{}, s.Rows...)
	if o.latest.Load() != s {
		return PeerDatasetSnapshot{}, false
	}
	return copy, true
}

// retire permanently fences this owner. Reentering Run requires a new owner
// and nonwrapping generation, never clearing this flag to reuse old contacts.
func (o *peerDataset) retire() {
	if o == nil {
		return
	}
	o.mu.Lock()
	o.retired, o.rows = true, nil
	o.latest.Store(nil)
	o.mu.Unlock()
}

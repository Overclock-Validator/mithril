package progtrace

// Lookup records the admission history before an actual program-cache read.
// Ambiguous forbids eviction attribution: a cache mutation overlapped the
// lookup, or the remembered publication itself overlapped another mutation.
// This is conservative across keys; it never adds a cache read or changes the
// cache's admission/access policy.
type Lookup struct {
	T             int64
	PriorAdmit    int64
	PriorSource   uint64
	InvalidatedAt int64
	Known         bool
	Ambiguous     bool
	r             *recorder
	sequence      uint64
}

func BeginLookup(key [32]byte) Lookup {
	r := current.Load()
	if r == nil {
		return Lookup{}
	}
	r.mu.Lock()
	if r.closing.Load() {
		r.mu.Unlock()
		return Lookup{}
	}
	a, known := r.admitted[key]
	o := Lookup{T: Now(), PriorAdmit: a.at, PriorSource: a.source,
		InvalidatedAt: a.invalidatedAt, Known: known && a.at != 0,
		Ambiguous: r.cacheActive != 0 || a.ambiguous, r: r,
		sequence: r.cacheSequence}
	r.mu.Unlock()
	return o
}

func EndLookup(o Lookup) Lookup {
	if o.r != nil {
		o.r.mu.Lock()
		o.Ambiguous = o.Ambiguous || o.r.closing.Load() || o.r.cacheActive != 0 || o.sequence != o.r.cacheSequence
		o.r.mu.Unlock()
	}
	o.r = nil
	return o
}

// Mutation brackets only the existing cache operation. No program-cache lock
// is acquired here. Operations that overlap have deliberately ambiguous
// history; timestamps sampled at returns do not establish their linearization
// order inside the cache.
type Mutation struct {
	r        *recorder
	sequence uint64
	overlaps bool
}

func BeginMutation() Mutation {
	r := current.Load()
	if r == nil {
		return Mutation{}
	}
	r.mu.Lock()
	if r.closing.Load() {
		r.mu.Unlock()
		return Mutation{}
	}
	r.cacheSequence++
	m := Mutation{r: r, sequence: r.cacheSequence, overlaps: r.cacheActive != 0}
	r.cacheActive++
	r.mu.Unlock()
	return m
}

func (m Mutation) On() bool { return m.r != nil }

// EndMutation updates history from the actual Set/Delete/Clear result. A
// refused Set removes a predecessor, so it is an invalidation as well. The
// caller samples at immediately after the cache operation, before hashing or
// formatting a record. Source digests are prepared before that operation.
func EndMutation(m Mutation, key [32]byte, source uint64, at int64, outcome string) {
	if m.r == nil {
		return
	}
	r := m.r
	r.mu.Lock()
	defer r.mu.Unlock()
	ambiguous := m.overlaps || m.sequence != r.cacheSequence
	if outcome == "clear" {
		for k, a := range r.admitted {
			a.invalidatedAt = max(a.invalidatedAt, at)
			a.ambiguous = a.ambiguous || ambiguous
			r.admitted[k] = a
		}
	} else {
		a, exists := r.admitted[key]
		if outcome == "admitted" {
			if exists && (a.at > at || a.invalidatedAt > at) {
				a.ambiguous = true
				r.admitted[key] = a
			} else if exists || len(r.admitted) < maxTrackedKeys {
				r.admitted[key] = admission{at: at, source: source, ambiguous: ambiguous}
			} else {
				r.untrack++
			}
		} else if exists {
			a.invalidatedAt = max(a.invalidatedAt, at)
			a.ambiguous = a.ambiguous || ambiguous
			r.admitted[key] = a
		}
	}
	r.cacheActive--
	r.cacheSequence++
}

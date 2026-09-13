package gossip

import (
	"bytes"
	"crypto/sha256"
	"sort"
	"sync"
	"time"
)

// Agave accepts pushed values within a 15-second wallclock window.
const contactPushWindow = uint64(15 * time.Second / time.Millisecond)

const crdsMessageHeaderSize = 4 + 32 + 8

type relayContact struct {
	record    contactRecord
	hash      [32]byte
	forwarded uint64
}

// contactRelay retains at most the existing gossip peer limit. Each push tick
// relays at most one additional datagram per peer, rotating through contacts.
type contactRelay struct {
	mu       sync.Mutex
	contacts map[Pubkey]relayContact
	sequence uint64
}

func recentContact(wallclock, now, maxAge uint64) bool {
	if wallclock > now {
		return wallclock-now < contactPushWindow
	}
	return now-wallclock <= maxAge
}

// accept is called only after signature and shred-version verification.
func (r *contactRelay) accept(record contactRecord, now uint64) bool {
	if !recentContact(record.Wallclock, now, uint64(peerExpirationWindow/time.Millisecond)) {
		return false
	}
	if len(record.data)+len(record.signature)+crdsMessageHeaderSize > packetDataSize {
		return false
	}
	h := sha256.New()
	h.Write(record.signature[:])
	h.Write(record.data)
	var hash [32]byte
	copy(hash[:], h.Sum(nil))
	r.mu.Lock()
	defer r.mu.Unlock()
	old, exists := r.contacts[record.Pubkey]
	if exists {
		// ContactInfo orders restarts first, then wallclock, then the hash of
		// the complete signed value, matching Agave's CRDS replacement rule.
		if record.Outset < old.record.Outset ||
			(record.Outset == old.record.Outset && record.Wallclock < old.record.Wallclock) ||
			(record.Outset == old.record.Outset && record.Wallclock == old.record.Wallclock && bytes.Compare(hash[:], old.hash[:]) <= 0) {
			return false
		}
	} else if len(r.contacts) >= maxKnownGossipPeers {
		var oldest Pubkey
		oldestWallclock := ^uint64(0)
		for key, contact := range r.contacts {
			if contact.record.Wallclock < oldestWallclock {
				oldest, oldestWallclock = key, contact.record.Wallclock
			}
		}
		delete(r.contacts, oldest)
	}
	if r.contacts == nil {
		r.contacts = make(map[Pubkey]relayContact)
	}
	// The receive loop reuses its datagram buffer on the next packet.
	record.data = bytes.Clone(record.data)
	r.contacts[record.Pubkey] = relayContact{record: record, hash: hash, forwarded: old.forwarded}
	return true
}

func (r *contactRelay) values(now uint64) []CrdsValue {
	r.mu.Lock()
	defer r.mu.Unlock()
	var keys []Pubkey
	for key, contact := range r.contacts {
		if !recentContact(contact.record.Wallclock, now, uint64(peerExpirationWindow/time.Millisecond)) {
			delete(r.contacts, key)
			continue
		}
		if recentContact(contact.record.Wallclock, now, contactPushWindow) {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := r.contacts[keys[i]], r.contacts[keys[j]]
		if a.forwarded != b.forwarded {
			return a.forwarded < b.forwarded
		}
		return bytes.Compare(keys[i][:], keys[j][:]) < 0
	})
	var values []CrdsValue
	size := crdsMessageHeaderSize
	for _, key := range keys {
		contact := r.contacts[key]
		record := contact.record
		n := len(record.signature) + len(record.data)
		if size+n > packetDataSize {
			break
		}
		values = append(values, CrdsValue{Signature: record.signature, Data: record.data})
		size += n
		r.sequence++
		contact.forwarded = r.sequence
		r.contacts[key] = contact
	}
	return values
}

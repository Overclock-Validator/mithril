package watch

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/gagliardetto/solana-go"
)

// Kind says why a key is watched.
type Kind uint8

const (
	// Wallet keys are matched in transactions and own token accounts.
	Wallet Kind = 1 << iota
	// TokenAccount keys were enumerated or discovered as owned by a wallet.
	TokenAccount
	// Program keys match transactions that name the program.
	Program
	// Mint keys match transactions that name the mint.
	Mint
)

// Snapshot is what replay reads: one filter for matching transactions and
// one over wallets for discovering the token accounts they own.
type Snapshot struct {
	Match  *Filter
	Owners *Filter
}

// Registry holds watch registrations and publishes immutable snapshots.
// Tenant 0 is the operator; other tenants are reserved for multiple
// customers and share entries for the same key.
type Registry struct {
	mu      sync.Mutex
	keys    map[solana.PublicKey]map[uint32]Kind
	sigs    map[solana.Signature]uint64 // last block height the signature can land
	cur     atomic.Pointer[Snapshot]
	refused atomic.Uint64
}

// MaxKeys caps the number of distinct watched keys. Discovery can otherwise
// grow without bound when a busy address that owns many token accounts is
// watched as a wallet.
var MaxKeys = 100_000

// Refused counts registrations rejected because the registry was full.
func (r *Registry) Refused() uint64 { return r.refused.Load() }

// NewRegistry returns an empty registry; its snapshot matches nothing.
func NewRegistry() *Registry {
	r := &Registry{keys: map[solana.PublicKey]map[uint32]Kind{}, sigs: map[solana.Signature]uint64{}}
	r.cur.Store(&Snapshot{})
	return r
}

// Current returns the snapshot replay should use for the next transaction.
func (r *Registry) Current() *Snapshot {
	if r == nil {
		return nil
	}
	return r.cur.Load()
}

// Key is one registration.
type Key struct {
	Tenant uint32
	Key    solana.PublicKey
	Kind   Kind
}

// AddKeys registers keys and republishes once. It returns the keys that were
// not yet watched for that tenant and kind.
func (r *Registry) AddKeys(keys ...Key) []Key {
	r.mu.Lock()
	defer r.mu.Unlock()
	var added []Key
	for _, k := range keys {
		tenants := r.keys[k.Key]
		if tenants == nil {
			if len(r.keys) >= MaxKeys {
				r.refused.Add(1)
				continue
			}
			tenants = map[uint32]Kind{}
			r.keys[k.Key] = tenants
		}
		if tenants[k.Tenant]&k.Kind == k.Kind {
			continue
		}
		tenants[k.Tenant] |= k.Kind
		added = append(added, k)
	}
	if len(added) > 0 {
		r.publishLocked()
	}
	return added
}

// RemoveKey drops a tenant's registration of key for kind.
func (r *Registry) RemoveKey(tenant uint32, key solana.PublicKey, kind Kind) {
	r.mu.Lock()
	defer r.mu.Unlock()
	tenants := r.keys[key]
	if tenants == nil || tenants[tenant]&kind == 0 {
		return
	}
	tenants[tenant] &^= kind
	if tenants[tenant] == 0 {
		delete(tenants, tenant)
	}
	if len(tenants) == 0 {
		delete(r.keys, key)
	}
	r.publishLocked()
}

// AddSignature watches a signature this node sent until lastValidHeight.
func (r *Registry) AddSignature(sig solana.Signature, lastValidHeight uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if old, ok := r.sigs[sig]; ok && old >= lastValidHeight {
		return
	}
	r.sigs[sig] = lastValidHeight
	r.publishLocked()
}

// ExpireSignatures drops signatures that can no longer land above height.
func (r *Registry) ExpireSignatures(height uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	changed := false
	for sig, last := range r.sigs {
		if last < height {
			delete(r.sigs, sig)
			changed = true
		}
	}
	if changed {
		r.publishLocked()
	}
}

// Kinds returns the union of kinds key is watched for across tenants.
func (r *Registry) Kinds(key solana.PublicKey) Kind {
	r.mu.Lock()
	defer r.mu.Unlock()
	var k Kind
	for _, kind := range r.keys[key] {
		k |= kind
	}
	return k
}

// Tenants returns the tenants watching key, ascending.
func (r *Registry) Tenants(key solana.PublicKey) []uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]uint32, 0, len(r.keys[key]))
	for t := range r.keys[key] {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func (r *Registry) publishLocked() {
	match := make([]solana.PublicKey, 0, len(r.keys))
	var owners []solana.PublicKey
	for key, tenants := range r.keys {
		var kinds Kind
		for _, k := range tenants {
			kinds |= k
		}
		match = append(match, key)
		if kinds&Wallet != 0 {
			owners = append(owners, key)
		}
	}
	sigs := make([]solana.Signature, 0, len(r.sigs))
	for sig := range r.sigs {
		sigs = append(sigs, sig)
	}
	r.cur.Store(&Snapshot{Match: Build(match, sigs), Owners: Build(owners, nil)})
}

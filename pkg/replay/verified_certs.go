package replay

import (
	"crypto/sha256"
	"encoding/binary"
	"maps"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/rewardcerts"
)

// Certificates this node has already verified, remembered so that its own
// private replay of the same block does not repeat the BLS checks.
//
// An entry is keyed by everything the verification depends on: the kind of
// certificate, the block's slot, the epoch whose validator set checked it, the
// shred version, and a digest of the certificate bytes. A lookup therefore
// returns a result only for byte-identical input under the same verifier, and
// that result is what verifying again would return. Only the node's own replay
// stores entries; a private replay only reads them and verifies in full on a
// miss. Kept only while replay hints are on (MITHRIL_REPLAY_HINTS=1).
const verifiedCertSlots = 512

type verifiedCertKey struct {
	final        bool
	slot, epoch  uint64
	shredVersion uint16
	digest       [sha256.Size]byte
}

func rewardCertKey(slot, epoch uint64, shredVersion uint16, skipRaw, notarRaw []byte) verifiedCertKey {
	hash := sha256.New()
	var length [8]byte
	binary.LittleEndian.PutUint64(length[:], uint64(len(skipRaw)))
	hash.Write(length[:])
	hash.Write(skipRaw)
	hash.Write(notarRaw)
	key := verifiedCertKey{slot: slot, epoch: epoch, shredVersion: shredVersion}
	hash.Sum(key.digest[:0])
	return key
}

func finalCertKey(slot, epoch uint64, shredVersion uint16, raw []byte) verifiedCertKey {
	return verifiedCertKey{final: true, slot: slot, epoch: epoch, shredVersion: shredVersion, digest: sha256.Sum256(raw)}
}

type verifiedCertStore struct {
	mu      sync.Mutex
	entries map[verifiedCertKey]any
	order   []verifiedCertKey
}

var verifiedCerts = verifiedCertStore{entries: make(map[verifiedCertKey]any)}

func (store *verifiedCertStore) put(key verifiedCertKey, value any) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if _, replaced := store.entries[key]; !replaced {
		store.order = append(store.order, key)
		if len(store.order) > 2*verifiedCertSlots {
			delete(store.entries, store.order[0])
			store.order = store.order[1:]
		}
	}
	store.entries[key] = value
}

func (store *verifiedCertStore) get(key verifiedCertKey) any {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.entries[key]
}

// Stored and returned values are copies: callers own what they get.

func (store *verifiedCertStore) putReward(key verifiedCertKey, validated *rewardcerts.ValidatedRewardCert) {
	if validated != nil {
		store.put(key, &rewardcerts.ValidatedRewardCert{RewardSlot: validated.RewardSlot, Validators: maps.Clone(validated.Validators)})
	}
}

func (store *verifiedCertStore) reward(key verifiedCertKey) (*rewardcerts.ValidatedRewardCert, bool) {
	stored, _ := store.get(key).(*rewardcerts.ValidatedRewardCert)
	if stored == nil {
		return nil, false
	}
	return &rewardcerts.ValidatedRewardCert{RewardSlot: stored.RewardSlot, Validators: maps.Clone(stored.Validators)}, true
}

func (store *verifiedCertStore) putFinal(key verifiedCertKey, validated *rewardcerts.ValidatedFinalCert) {
	if validated != nil {
		store.put(key, &rewardcerts.ValidatedFinalCert{FinalSlot: validated.FinalSlot, Signers: maps.Clone(validated.Signers)})
	}
}

func (store *verifiedCertStore) final(key verifiedCertKey) (*rewardcerts.ValidatedFinalCert, bool) {
	stored, _ := store.get(key).(*rewardcerts.ValidatedFinalCert)
	if stored == nil {
		return nil, false
	}
	return &rewardcerts.ValidatedFinalCert{FinalSlot: stored.FinalSlot, Signers: maps.Clone(stored.Signers)}, true
}

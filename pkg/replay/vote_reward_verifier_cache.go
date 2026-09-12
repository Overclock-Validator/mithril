package replay

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/rewardcerts"
	"github.com/gagliardetto/solana-go"
)

const voteRewardVerifierCacheCapacity = 4

type voteRewardVerifierCacheKey struct {
	epoch        uint64
	generation   uint64
	shredVersion uint16
}

type voteRewardVerifierMaterial struct {
	verifier *alpenglow.CertificateVerifier
	snapshot epochstakes.Snapshot

	rewardValidationMu   sync.Mutex
	lastRewardValidation *voteRewardCertificateValidation
}

// Keep just the latest successful certificate pair. The immutable material
// fixes the validator epoch and shred version; these bytes and the containing
// slot fix everything else used by reward certificate validation.
type voteRewardCertificateValidation struct {
	slot              uint64
	skipRaw, notarRaw []byte
	validated         *rewardcerts.ValidatedRewardCert
}

func (material *voteRewardVerifierMaterial) validateRewardCertificates(
	currentSlot uint64,
	skipRaw, notarRaw []byte,
	measureTimings bool,
) (*rewardcerts.ValidatedRewardCert, rewardcerts.RewardCertificateValidationTimings, error) {
	material.rewardValidationMu.Lock()
	defer material.rewardValidationMu.Unlock()
	if cached := material.lastRewardValidation; cached != nil && cached.slot == currentSlot &&
		bytes.Equal(cached.skipRaw, skipRaw) && bytes.Equal(cached.notarRaw, notarRaw) {
		return cloneValidatedRewardCert(cached.validated), rewardcerts.RewardCertificateValidationTimings{}, nil
	}
	validated, timings, err := rewardcerts.ValidateRewardCertificatesWithVerifier(
		currentSlot, skipRaw, notarRaw, material.snapshot.Epoch, material.verifier, measureTimings,
	)
	if err != nil {
		return nil, timings, err
	}
	material.lastRewardValidation = &voteRewardCertificateValidation{
		slot:      currentSlot,
		skipRaw:   bytes.Clone(skipRaw),
		notarRaw:  bytes.Clone(notarRaw),
		validated: cloneValidatedRewardCert(validated),
	}
	return validated, timings, nil
}

func cloneValidatedRewardCert(validated *rewardcerts.ValidatedRewardCert) *rewardcerts.ValidatedRewardCert {
	if validated == nil {
		return nil
	}
	cloned := &rewardcerts.ValidatedRewardCert{
		RewardSlot: validated.RewardSlot,
		Validators: make(map[solana.PublicKey]struct{}, len(validated.Validators)),
	}
	for validator := range validated.Validators {
		cloned.Validators[validator] = struct{}{}
	}
	return cloned
}

type voteRewardVerifierCacheEntry struct {
	material *voteRewardVerifierMaterial
	lastUsed uint64
}

type voteRewardVerifierMaterialCache struct {
	mu            sync.Mutex
	entries       map[voteRewardVerifierCacheKey]voteRewardVerifierCacheEntry
	epochIdentity map[uint64][sha256.Size]byte
	useSeq        uint64
}

var cachedVoteRewardVerifiers voteRewardVerifierMaterialCache

func (cache *voteRewardVerifierMaterialCache) get(
	epoch uint64,
	shredVersion uint16,
) (*voteRewardVerifierMaterial, bool, error) {
	snapshot, ok := global.EpochStakesSnapshot(epoch)
	if !ok || len(snapshot.Stakes) == 0 {
		return nil, false, fmt.Errorf("missing epoch stakes for epoch %d", epoch)
	}
	key := voteRewardVerifierCacheKey{
		epoch:        epoch,
		generation:   snapshot.Generation,
		shredVersion: shredVersion,
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()

	if entry, ok := cache.entries[key]; ok {
		cache.useSeq++
		entry.lastUsed = cache.useSeq
		cache.entries[key] = entry
		return entry.material, true, nil
	}

	identity := voteRewardSnapshotIdentity(snapshot)
	if installed, ok := cache.epochIdentity[epoch]; ok && installed != identity {
		return nil, false, fmt.Errorf(
			"vote-reward material for epoch %d changed after installation; epoch material is immutable",
			epoch,
		)
	}

	// An identical same-epoch reload receives a new generation. Re-key the
	// existing verifier and bind it to the newly published immutable snapshot
	// rather than decompressing every validator BLS key again.
	for oldKey, entry := range cache.entries {
		if oldKey.epoch == epoch && oldKey.shredVersion == shredVersion {
			material := &voteRewardVerifierMaterial{
				verifier: entry.material.verifier,
				snapshot: snapshot,
			}
			cache.removeStaleEpochEntries(epoch, snapshot.Generation)
			cache.insert(key, material)
			return material, true, nil
		}
	}

	validatorSet, err := buildValidatorSetForSnapshot(snapshot)
	if err != nil {
		return nil, false, err
	}
	verifier := alpenglow.NewCertificateVerifier()
	if err := verifier.SetValidatorSet(validatorSet); err != nil {
		return nil, false, fmt.Errorf("configure validator set for epoch %d: %w", epoch, err)
	}
	verifier.SetShredVersion(shredVersion)

	if cache.epochIdentity == nil {
		cache.epochIdentity = make(map[uint64][sha256.Size]byte)
	}
	cache.epochIdentity[epoch] = identity
	cache.removeStaleEpochEntries(epoch, snapshot.Generation)
	material := &voteRewardVerifierMaterial{
		verifier: verifier,
		snapshot: snapshot,
	}
	cache.insert(key, material)
	return material, false, nil
}

func (cache *voteRewardVerifierMaterialCache) removeStaleEpochEntries(epoch, generation uint64) {
	for staleKey := range cache.entries {
		if staleKey.epoch == epoch && staleKey.generation != generation {
			delete(cache.entries, staleKey)
		}
	}
}

func (cache *voteRewardVerifierMaterialCache) insert(
	key voteRewardVerifierCacheKey,
	material *voteRewardVerifierMaterial,
) {
	if cache.entries == nil {
		cache.entries = make(map[voteRewardVerifierCacheKey]voteRewardVerifierCacheEntry)
	}
	if len(cache.entries) >= voteRewardVerifierCacheCapacity {
		var oldestKey voteRewardVerifierCacheKey
		var oldestUse uint64
		first := true
		for candidateKey, candidate := range cache.entries {
			if first || candidate.lastUsed < oldestUse {
				oldestKey = candidateKey
				oldestUse = candidate.lastUsed
				first = false
			}
		}
		delete(cache.entries, oldestKey)
	}
	cache.useSeq++
	cache.entries[key] = voteRewardVerifierCacheEntry{
		material: material,
		lastUsed: cache.useSeq,
	}
}

func loadVoteRewardVerifierMaterial(
	epoch uint64,
	shredVersion uint16,
	details *metrics.VoteRewardDetails,
) (*voteRewardVerifierMaterial, error) {
	var start time.Time
	if details != nil {
		start = time.Now()
	}
	material, hit, err := cachedVoteRewardVerifiers.get(epoch, shredVersion)
	if details != nil {
		details.ValidatorPreparation.AddTimingSince(start)
		if hit {
			atomic.AddUint64(&details.ValidatorCacheHits, 1)
		} else {
			atomic.AddUint64(&details.ValidatorCacheMisses, 1)
		}
	}
	return material, err
}

func clearVoteRewardVerifierCacheForTest() {
	cachedVoteRewardVerifiers.mu.Lock()
	cachedVoteRewardVerifiers.entries = nil
	cachedVoteRewardVerifiers.epochIdentity = nil
	cachedVoteRewardVerifiers.useSeq = 0
	cachedVoteRewardVerifiers.mu.Unlock()
}

func buildValidatorSetForSnapshot(snapshot epochstakes.Snapshot) (alpenglow.ValidatorSet, error) {
	return alpenglow.BuildValidatorSet(
		snapshot.Epoch,
		snapshot.Stakes,
		snapshot.VoteAccounts,
		snapshot.TotalStake,
	)
}

// voteRewardSnapshotIdentity covers both signature-verification material and
// reward/leader metadata. This makes a same-epoch reload either an exact,
// reusable copy or a fail-closed consensus error.
func voteRewardSnapshotIdentity(snapshot epochstakes.Snapshot) [sha256.Size]byte {
	hasher := sha256.New()
	var scratch [8]byte
	writeUint64 := func(value uint64) {
		binary.LittleEndian.PutUint64(scratch[:], value)
		_, _ = hasher.Write(scratch[:])
	}
	writePubkey := func(pubkey solana.PublicKey) {
		_, _ = hasher.Write(pubkey[:])
	}

	writeUint64(snapshot.Epoch)
	writeUint64(snapshot.TotalStake)

	stakeKeys := sortedEpochPublicKeys(snapshot.Stakes)
	writeUint64(uint64(len(stakeKeys)))
	for _, pubkey := range stakeKeys {
		writePubkey(pubkey)
		writeUint64(snapshot.Stakes[pubkey])
	}

	voteKeys := sortedEpochPublicKeys(snapshot.VoteAccounts)
	writeUint64(uint64(len(voteKeys)))
	for _, pubkey := range voteKeys {
		writePubkey(pubkey)
		voteAccount := snapshot.VoteAccounts[pubkey]
		if voteAccount == nil {
			_, _ = hasher.Write([]byte{0})
			continue
		}
		_, _ = hasher.Write([]byte{1})
		writeUint64(voteAccount.Lamports)
		writePubkey(voteAccount.NodePubkey)
		if voteAccount.BlsPubkeyCompressed == nil {
			_, _ = hasher.Write([]byte{0})
		} else {
			_, _ = hasher.Write([]byte{1})
			_, _ = hasher.Write(voteAccount.BlsPubkeyCompressed[:])
		}
		writeUint64(uint64(voteAccount.LastTimestampTs))
		writeUint64(voteAccount.LastTimestampSlot)
		writePubkey(voteAccount.Owner)
		_, _ = hasher.Write([]byte{voteAccount.Executable})
		writeUint64(voteAccount.RentEpoch)
	}

	var identity [sha256.Size]byte
	copy(identity[:], hasher.Sum(nil))
	return identity
}

func sortedEpochPublicKeys[V any](values map[solana.PublicKey]V) []solana.PublicKey {
	keys := make([]solana.PublicKey, 0, len(values))
	for pubkey := range values {
		keys = append(keys, pubkey)
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i][:], keys[j][:]) < 0
	})
	return keys
}

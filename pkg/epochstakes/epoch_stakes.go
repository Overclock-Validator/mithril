package epochstakes

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/gagliardetto/solana-go"
)

type EpochStakesCache struct {
	mu              sync.RWMutex
	stakeCache      map[uint64]map[solana.PublicKey]uint64
	voteAcctCache   map[uint64]map[solana.PublicKey]*VoteAccount
	totalStakeCache map[uint64]uint64
	generations     map[uint64]uint64
}

var nextEpochStakesGeneration uint64

type VoteAccount struct {
	Lamports            uint64
	NodePubkey          solana.PublicKey
	BlsPubkeyCompressed *[48]byte
	// Nil means an older persisted cache omitted this consensus-critical field.
	// Legacy vote states explicitly use the scheduled node identity.
	BlockRevenueCollector *solana.PublicKey
	LastTimestampTs       int64
	LastTimestampSlot     uint64
	Owner                 solana.PublicKey
	Executable            byte
	RentEpoch             uint64
}

// Snapshot is one immutable, atomically published view of an epoch's stake
// material. Its maps and vote-account records must be treated as read-only.
type Snapshot struct {
	Epoch        uint64
	Generation   uint64
	Stakes       map[solana.PublicKey]uint64
	VoteAccounts map[solana.PublicKey]*VoteAccount
	TotalStake   uint64
}

func NewEpochStakesCache() *EpochStakesCache {
	return &EpochStakesCache{
		stakeCache:      make(map[uint64]map[solana.PublicKey]uint64),
		voteAcctCache:   make(map[uint64]map[solana.PublicKey]*VoteAccount),
		totalStakeCache: make(map[uint64]uint64),
		generations:     make(map[uint64]uint64),
	}
}

// PutEpoch atomically replaces every piece of material for epoch. The inputs
// are cloned before publication so callers cannot mutate the installed view.
func (cache *EpochStakesCache) PutEpoch(
	epoch uint64,
	stakes map[solana.PublicKey]uint64,
	voteAccounts map[solana.PublicKey]*VoteAccount,
	totalStake uint64,
) uint64 {
	return cache.putEpochOwned(epoch, cloneStakes(stakes), cloneVoteAccounts(voteAccounts), totalStake)
}

// Snapshot returns an internally coherent, allocation-free view of epoch. Its
// fields are cache-owned and must be treated as immutable. PutEpoch clones its
// inputs and compatibility mutators use copy-on-write, so a published Snapshot
// remains unchanged across later cache updates.
func (cache *EpochStakesCache) Snapshot(epoch uint64) (Snapshot, bool) {
	cache.mu.RLock()
	defer cache.mu.RUnlock()

	stakes, exists := cache.stakeCache[epoch]
	if !exists {
		return Snapshot{Epoch: epoch, Generation: cache.generations[epoch]}, false
	}
	return Snapshot{
		Epoch:        epoch,
		Generation:   cache.generations[epoch],
		Stakes:       stakes,
		VoteAccounts: cache.voteAcctCache[epoch],
		TotalStake:   cache.totalStakeCache[epoch],
	}, true
}

// PutEntry is retained for compatibility. It uses copy-on-write so readers can
// never observe a map while it is being modified.
func (cache *EpochStakesCache) PutEntry(epoch uint64, pubkey solana.PublicKey, stake uint64, voteAcct *VoteAccount) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.ensureMapsLocked()

	stakes := cloneStakes(cache.stakeCache[epoch])
	if stakes == nil {
		stakes = make(map[solana.PublicKey]uint64)
	}
	voteAccounts := cloneVoteAccounts(cache.voteAcctCache[epoch])
	if voteAccounts == nil {
		voteAccounts = make(map[solana.PublicKey]*VoteAccount)
	}
	stakes[pubkey] = stake
	voteAccounts[pubkey] = cloneVoteAccount(voteAcct)
	cache.stakeCache[epoch] = stakes
	cache.voteAcctCache[epoch] = voteAccounts
	cache.bumpGenerationLocked(epoch)
}

func (cache *EpochStakesCache) PutTotalEpochStake(epoch uint64, totalStake uint64) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.ensureMapsLocked()
	cache.totalStakeCache[epoch] = totalStake
	cache.bumpGenerationLocked(epoch)
}

func (cache *EpochStakesCache) EpochStakes(epoch uint64) map[solana.PublicKey]uint64 {
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	return cloneStakes(cache.stakeCache[epoch])
}

func (cache *EpochStakesCache) HasEpochStakes(epoch uint64) bool {
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	_, exists := cache.stakeCache[epoch]
	return exists
}

func (cache *EpochStakesCache) EpochStakesAccts(epoch uint64) map[solana.PublicKey]*VoteAccount {
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	return cloneVoteAccounts(cache.voteAcctCache[epoch])
}

func (cache *EpochStakesCache) TotalStake(epoch uint64) uint64 {
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	return cache.totalStakeCache[epoch]
}

// Generation identifies the exact epoch-stakes material currently installed.
// It changes on every mutation so derived verifier caches cannot survive a
// same-epoch reload with stale validator keys or stakes.
func (cache *EpochStakesCache) Generation(epoch uint64) uint64 {
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	return cache.generations[epoch]
}

func (cache *EpochStakesCache) bumpGenerationLocked(epoch uint64) uint64 {
	generation := atomic.AddUint64(&nextEpochStakesGeneration, 1)
	if generation == 0 {
		generation = atomic.AddUint64(&nextEpochStakesGeneration, 1)
	}
	cache.generations[epoch] = generation
	return generation
}

// ClearEpochStakes removes all stakes for a specific epoch.
// Used on resume to force rebuild from AccountsDB.
func (cache *EpochStakesCache) ClearEpochStakes(epoch uint64) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.ensureMapsLocked()
	delete(cache.stakeCache, epoch)
	delete(cache.voteAcctCache, epoch)
	delete(cache.totalStakeCache, epoch)
	cache.bumpGenerationLocked(epoch)
}

// PersistedEpochStakes is the JSON-serializable format for epoch stakes.
type PersistedEpochStakes struct {
	Epoch      uint64                      `json:"epoch"`
	TotalStake uint64                      `json:"total_stake"`
	Stakes     map[string]uint64           `json:"stakes"`     // base58 pubkey → stake
	VoteAccts  map[string]*VoteAccountJSON `json:"vote_accts"` // base58 pubkey → metadata
}

// VoteAccountJSON is the JSON-serializable format for vote account metadata.
type VoteAccountJSON struct {
	Lamports              uint64 `json:"lamports"`
	NodePubkey            string `json:"node_pubkey"`
	BlsPubkeyCompressed   []byte `json:"bls_pubkey_compressed,omitempty"`
	BlockRevenueCollector string `json:"block_revenue_collector,omitempty"`
	LastTimestampTs       int64  `json:"last_ts"`
	LastTimestampSlot     uint64 `json:"last_ts_slot"`
	Owner                 string `json:"owner"`
	Executable            byte   `json:"executable"`
	RentEpoch             uint64 `json:"rent_epoch"`
}

// SerializeEpoch serializes the stakes for a single epoch to JSON.
func (cache *EpochStakesCache) SerializeEpoch(epoch uint64) ([]byte, error) {
	snapshot, exists := cache.Snapshot(epoch)
	if !exists {
		return nil, fmt.Errorf("no stakes for epoch %d", epoch)
	}

	persisted := PersistedEpochStakes{
		Epoch:      epoch,
		TotalStake: snapshot.TotalStake,
		Stakes:     make(map[string]uint64, len(snapshot.Stakes)),
		VoteAccts:  make(map[string]*VoteAccountJSON, len(snapshot.VoteAccounts)),
	}

	for pk, stake := range snapshot.Stakes {
		persisted.Stakes[pk.String()] = stake
	}

	for pk, va := range snapshot.VoteAccounts {
		if va != nil {
			var bls []byte
			if va.BlsPubkeyCompressed != nil {
				bls = append([]byte(nil), va.BlsPubkeyCompressed[:]...)
			}
			var collector string
			if va.BlockRevenueCollector != nil {
				collector = va.BlockRevenueCollector.String()
			}
			persisted.VoteAccts[pk.String()] = &VoteAccountJSON{
				BlockRevenueCollector: collector,
				Lamports:              va.Lamports,
				NodePubkey:            va.NodePubkey.String(),
				BlsPubkeyCompressed:   bls,
				LastTimestampTs:       va.LastTimestampTs,
				LastTimestampSlot:     va.LastTimestampSlot,
				Owner:                 va.Owner.String(),
				Executable:            va.Executable,
				RentEpoch:             va.RentEpoch,
			}
		}
	}

	return json.Marshal(persisted)
}

// DeserializeAndLoadEpoch deserializes epoch stakes from JSON and loads into cache.
// Returns the epoch number that was loaded.
func (cache *EpochStakesCache) DeserializeAndLoadEpoch(data []byte) (uint64, error) {
	var persisted PersistedEpochStakes
	if err := json.Unmarshal(data, &persisted); err != nil {
		return 0, fmt.Errorf("failed to unmarshal epoch stakes: %w", err)
	}

	epoch := persisted.Epoch

	// Decode privately and publish only after every key and account validates. A
	// failed reload must not mutate material behind an unchanged generation.
	stakes := make(map[solana.PublicKey]uint64, len(persisted.Stakes))
	voteAccounts := make(map[solana.PublicKey]*VoteAccount, len(persisted.VoteAccts))

	for pkStr, stake := range persisted.Stakes {
		pk, err := solana.PublicKeyFromBase58(pkStr)
		if err != nil {
			return 0, fmt.Errorf("invalid stake pubkey %q for epoch %d: %w", pkStr, epoch, err)
		}
		stakes[pk] = stake
	}

	for pkStr, vaJSON := range persisted.VoteAccts {
		if vaJSON == nil {
			return 0, fmt.Errorf("nil vote account metadata for vote acct %s epoch %d", pkStr, epoch)
		}
		pk, err := solana.PublicKeyFromBase58(pkStr)
		if err != nil {
			return 0, fmt.Errorf("invalid vote acct pubkey %q for epoch %d: %w", pkStr, epoch, err)
		}
		nodePubkey, err := solana.PublicKeyFromBase58(vaJSON.NodePubkey)
		if err != nil {
			return 0, fmt.Errorf("invalid node pubkey %q for vote acct %s epoch %d: %w", vaJSON.NodePubkey, pkStr, epoch, err)
		}
		owner, err := solana.PublicKeyFromBase58(vaJSON.Owner)
		if err != nil {
			return 0, fmt.Errorf("invalid owner pubkey %q for vote acct %s epoch %d: %w", vaJSON.Owner, pkStr, epoch, err)
		}
		var bls *[48]byte
		if len(vaJSON.BlsPubkeyCompressed) != 0 {
			if len(vaJSON.BlsPubkeyCompressed) != 48 {
				return 0, fmt.Errorf("invalid BLS pubkey length %d for vote acct %s epoch %d", len(vaJSON.BlsPubkeyCompressed), pkStr, epoch)
			}
			var b [48]byte
			copy(b[:], vaJSON.BlsPubkeyCompressed)
			bls = &b
		}
		var collector *solana.PublicKey
		if vaJSON.BlockRevenueCollector != "" {
			key, err := solana.PublicKeyFromBase58(vaJSON.BlockRevenueCollector)
			if err != nil {
				return 0, fmt.Errorf("invalid block revenue collector for vote acct %s epoch %d: %w", pkStr, epoch, err)
			}
			collector = &key
		}
		voteAccounts[pk] = &VoteAccount{
			BlockRevenueCollector: collector,
			Lamports:              vaJSON.Lamports,
			NodePubkey:            nodePubkey,
			BlsPubkeyCompressed:   bls,
			LastTimestampTs:       vaJSON.LastTimestampTs,
			LastTimestampSlot:     vaJSON.LastTimestampSlot,
			Owner:                 owner,
			Executable:            vaJSON.Executable,
			RentEpoch:             vaJSON.RentEpoch,
		}
	}

	cache.putEpochOwned(epoch, stakes, voteAccounts, persisted.TotalStake)
	return epoch, nil
}

// GetAllEpochs returns a list of all epochs in the cache.
func (cache *EpochStakesCache) GetAllEpochs() []uint64 {
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	epochs := make([]uint64, 0, len(cache.stakeCache))
	for epoch := range cache.stakeCache {
		epochs = append(epochs, epoch)
	}
	return epochs
}

// putEpochOwned publishes maps already owned by the cache in one critical
// section. Callers must not retain or mutate the maps after this call.
func (cache *EpochStakesCache) putEpochOwned(
	epoch uint64,
	stakes map[solana.PublicKey]uint64,
	voteAccounts map[solana.PublicKey]*VoteAccount,
	totalStake uint64,
) uint64 {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.ensureMapsLocked()
	cache.stakeCache[epoch] = stakes
	cache.voteAcctCache[epoch] = voteAccounts
	cache.totalStakeCache[epoch] = totalStake
	return cache.bumpGenerationLocked(epoch)
}

func (cache *EpochStakesCache) ensureMapsLocked() {
	if cache.stakeCache == nil {
		cache.stakeCache = make(map[uint64]map[solana.PublicKey]uint64)
	}
	if cache.voteAcctCache == nil {
		cache.voteAcctCache = make(map[uint64]map[solana.PublicKey]*VoteAccount)
	}
	if cache.totalStakeCache == nil {
		cache.totalStakeCache = make(map[uint64]uint64)
	}
	if cache.generations == nil {
		cache.generations = make(map[uint64]uint64)
	}
}

func cloneStakes(stakes map[solana.PublicKey]uint64) map[solana.PublicKey]uint64 {
	if stakes == nil {
		return nil
	}
	cloned := make(map[solana.PublicKey]uint64, len(stakes))
	for pubkey, stake := range stakes {
		cloned[pubkey] = stake
	}
	return cloned
}

func cloneVoteAccounts(voteAccounts map[solana.PublicKey]*VoteAccount) map[solana.PublicKey]*VoteAccount {
	if voteAccounts == nil {
		return nil
	}
	cloned := make(map[solana.PublicKey]*VoteAccount, len(voteAccounts))
	for pubkey, voteAccount := range voteAccounts {
		cloned[pubkey] = cloneVoteAccount(voteAccount)
	}
	return cloned
}

func cloneVoteAccount(voteAccount *VoteAccount) *VoteAccount {
	if voteAccount == nil {
		return nil
	}
	cloned := *voteAccount
	if voteAccount.BlockRevenueCollector != nil {
		collector := *voteAccount.BlockRevenueCollector
		cloned.BlockRevenueCollector = &collector
	}
	if voteAccount.BlsPubkeyCompressed != nil {
		bls := *voteAccount.BlsPubkeyCompressed
		cloned.BlsPubkeyCompressed = &bls
	}
	return &cloned
}

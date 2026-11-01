package accounts

import (
	"hash/maphash"
	"maps"
	"os"
	"strconv"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/spinlock"
	"github.com/gagliardetto/solana-go"
)

const (
	overlayMaxShardCount         = 128
	overlayTargetEntriesPerShard = 64
)

// Padding keeps adjacent shard locks off the same cache line. The exact mutex
// size is architecture-dependent, so a full line is deliberately conservative.
type overlayAccountShard struct {
	mu    sync.RWMutex
	delta map[[32]byte]*Account
	_     [64]byte
}

// OverlayAccounts is a branch-local MVCC overlay over a parent account set: writes
// go to an in-memory delta, reads fall back to the never-mutated parent.
type OverlayAccounts struct {
	shards        []overlayAccountShard
	shardMask     uint64
	shardCapacity int
	hashSeed      maphash.Seed
	parent        Accounts
}

func NewOverlayAccounts(parent Accounts) *OverlayAccounts {
	return NewOverlayAccountsWithLen(parent, 0)
}

func NewOverlayAccountsWithLen(parent Accounts, length int) *OverlayAccounts {
	return NewOverlayAccountsWithSizing(parent, length, length)
}

// NewOverlayAccountsWithSizing sizes lock sharding from the number of keys that
// may be accessed, while sizing lazy delta maps from the expected write set.
func NewOverlayAccountsWithSizing(parent Accounts, keyCount, writeCapacity int) *OverlayAccounts {
	shardCount := overlayShardCount(keyCount)
	shardCapacity := 0
	if writeCapacity > 0 {
		shardCapacity = (writeCapacity-1)/shardCount + 1
	}
	return &OverlayAccounts{
		shards:        make([]overlayAccountShard, shardCount),
		shardMask:     uint64(shardCount - 1),
		shardCapacity: shardCapacity,
		hashSeed:      maphash.MakeSeed(),
		parent:        parent,
	}
}

// overlayFixedShards is MITHRIL_OVERLAY_SHARDS (bench knob): when it is a
// power of two, every overlay uses exactly that many shards instead of one
// per overlayTargetEntriesPerShard keys. Commits lock one shard per touched
// key and reads hold a shard's read lock across the parent read, so with few
// shards, unrelated keys of concurrently executing transactions collide.
var overlayFixedShards = func() int {
	n, err := strconv.Atoi(os.Getenv("MITHRIL_OVERLAY_SHARDS"))
	if err != nil || n <= 0 || n&(n-1) != 0 || n > 1<<16 {
		return 0
	}
	return n
}()

func overlayShardCount(length int) int {
	if overlayFixedShards != 0 {
		return overlayFixedShards
	}
	requested := 0
	if length > 0 {
		requested = (length-1)/overlayTargetEntriesPerShard + 1
	}
	shardCount := 1
	for shardCount < requested && shardCount < overlayMaxShardCount {
		shardCount <<= 1
	}
	return shardCount
}

// ShardCount reports how many lock shards the overlay's delta is split over.
func (o *OverlayAccounts) ShardCount() int {
	return len(o.shards)
}

func (o *OverlayAccounts) shardForKey(pubkey [32]byte) *overlayAccountShard {
	if len(o.shards) == 1 {
		return &o.shards[0]
	}
	shardIdx := maphash.Comparable(o.hashSeed, pubkey) & o.shardMask
	return &o.shards[shardIdx]
}

// setAccountOnShard stores an account while the caller holds the shard write
// lock, or during quiescent construction through SetAccountWithoutLock.
func (o *OverlayAccounts) setAccountOnShard(shard *overlayAccountShard, pubkey [32]byte, acct *Account) {
	if shard.delta == nil {
		shard.delta = make(map[[32]byte]*Account, o.shardCapacity)
	}
	shard.delta[pubkey] = acct
}

func (o *OverlayAccounts) GetAccount(pubkey *[32]byte) (*Account, error) {
	shard := o.shardForKey(*pubkey)
	spinlock.RLock(&shard.mu)
	acct, ok := shard.delta[*pubkey]
	shard.mu.RUnlock()
	if ok {
		return acct, nil
	}
	// The shard lock guards only the delta map. Overlay writes never reach the
	// parent, so the parent value read after a delta miss is the value the key
	// had at that miss, whether or not a writer publishes it meanwhile; holding
	// the shard lock across the parent read would only make writers wait.
	return o.parent.GetAccount(pubkey)
}

func (o *OverlayAccounts) GetAccountWithoutLock(pubkey solana.PublicKey) (*Account, error) {
	shard := o.shardForKey(pubkey)
	if acct, ok := shard.delta[pubkey]; ok {
		return acct, nil
	}
	return o.parent.GetAccountWithoutLock(pubkey)
}

func (o *OverlayAccounts) SetAccount(pubkey *[32]byte, acct *Account) error {
	shard := o.shardForKey(*pubkey)
	spinlock.WLock(&shard.mu)
	o.setAccountOnShard(shard, *pubkey, acct)
	shard.mu.Unlock()
	return nil
}

func (o *OverlayAccounts) SetTransactionAccounts(accountStates []*Account, touched []bool) error {
	if err := validateTransactionAccountBatch(accountStates, touched); err != nil {
		return err
	}
	o.setTransactionAccounts(accountStates, touched)
	return nil
}

func (o *OverlayAccounts) setTransactionAccounts(accountStates []*Account, touched []bool) {
	if len(o.shards) == 1 {
		shard := &o.shards[0]
		spinlock.WLock(&shard.mu)
		defer shard.mu.Unlock()
		for idx, acct := range accountStates {
			if touched[idx] {
				o.setAccountOnShard(shard, acct.Key, transactionAccountForStorage(acct))
			}
		}
		return
	}

	// Publish one key at a time, matching the old visibility contract while
	// allowing scheduler-independent keys to proceed through separate shards.
	for idx, acct := range accountStates {
		if !touched[idx] {
			continue
		}
		shard := o.shardForKey(acct.Key)
		spinlock.WLock(&shard.mu)
		o.setAccountOnShard(shard, acct.Key, transactionAccountForStorage(acct))
		shard.mu.Unlock()
	}
}

// SetAccountWithoutLock is reserved for quiescent construction.
func (o *OverlayAccounts) SetAccountWithoutLock(pubkey solana.PublicKey, acct *Account) error {
	shard := o.shardForKey(pubkey)
	o.setAccountOnShard(shard, pubkey, acct)
	return nil
}

func (o *OverlayAccounts) lockAllShardsForRead() int {
	totalAccounts := 0
	for idx := range o.shards {
		o.shards[idx].mu.RLock()
		totalAccounts += len(o.shards[idx].delta)
	}
	return totalAccounts
}

func (o *OverlayAccounts) unlockAllShardsForRead() {
	for idx := len(o.shards) - 1; idx >= 0; idx-- {
		o.shards[idx].mu.RUnlock()
	}
}

func (o *OverlayAccounts) snapshotDelta() map[[32]byte]*Account {
	totalAccounts := o.lockAllShardsForRead()
	deltaCopy := make(map[[32]byte]*Account, totalAccounts)
	for idx := range o.shards {
		maps.Copy(deltaCopy, o.shards[idx].delta)
	}
	o.unlockAllShardsForRead()
	return deltaCopy
}

// AllAccounts returns the parent set with this branch's delta applied on top.
func (o *OverlayAccounts) AllAccounts() []*Account {
	deltaCopy := o.snapshotDelta()

	// Merge outside the overlay locks: parent set first, then delta shadows it.
	merged := make(map[[32]byte]*Account, len(deltaCopy))
	for _, acct := range o.parent.AllAccounts() {
		merged[[32]byte(acct.Key)] = acct
	}
	maps.Copy(merged, deltaCopy)
	out := make([]*Account, 0, len(merged))
	for _, acct := range merged {
		out = append(out, acct)
	}
	return out
}

// DeltaAccounts returns the accounts changed on this overlay (the branch diff).
// Multi-branch (#14) promote-winner path; not used by the current linear tip.
func (o *OverlayAccounts) DeltaAccounts() []*Account {
	totalAccounts := o.lockAllShardsForRead()
	out := make([]*Account, 0, totalAccounts)
	for idx := range o.shards {
		for _, acct := range o.shards[idx].delta {
			out = append(out, acct)
		}
	}
	o.unlockAllShardsForRead()
	return out
}

var _ Accounts = (*OverlayAccounts)(nil)

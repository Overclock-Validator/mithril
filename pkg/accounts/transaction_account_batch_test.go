package accounts

import (
	"encoding/binary"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func transactionBatchTestKey(n uint64) solana.PublicKey {
	var key solana.PublicKey
	binary.LittleEndian.PutUint64(key[:8], n)
	binary.LittleEndian.PutUint64(key[8:16], n*0x9e3779b97f4a7c15)
	return key
}

type fallbackTransactionAccountStore struct {
	MemAccounts
	setOrder []solana.PublicKey
	failKey  *solana.PublicKey
}

func (s *fallbackTransactionAccountStore) SetAccount(pubkey *[32]byte, acct *Account) error {
	key := solana.PublicKey(*pubkey)
	s.setOrder = append(s.setOrder, key)
	if s.failKey != nil && key == *s.failKey {
		return errors.New("fallback set failed")
	}
	return s.MemAccounts.SetAccount(pubkey, acct)
}

var _ Accounts = (*fallbackTransactionAccountStore)(nil)

func TestSetTransactionAccountsFallback(t *testing.T) {
	key1 := transactionBatchTestKey(1)
	key2 := transactionBatchTestKey(2)
	key3 := transactionBatchTestKey(3)
	store := &fallbackTransactionAccountStore{MemAccounts: NewMemAccounts()}

	require.NoError(t, SetTransactionAccounts(store, []*Account{
		{Key: key1, Lamports: 11},
		{Key: key2, Lamports: 22},
		{Key: key3, Lamports: 0, Data: []byte{9}},
	}, []bool{true, false, true}))
	assert.Equal(t, []solana.PublicKey{key1, key3}, store.setOrder)

	_, err := store.GetAccount((*[32]byte)(&key2))
	require.Error(t, err, "untouched fallback state must not be stored")
	tombstone, err := store.GetAccount((*[32]byte)(&key3))
	require.NoError(t, err)
	assert.Zero(t, tombstone.Lamports)
	assert.Equal(t, uint64(math.MaxUint64), tombstone.RentEpoch)
	assert.Empty(t, tombstone.Data)

	store.setOrder = nil
	store.failKey = &key2
	err = SetTransactionAccounts(store, []*Account{
		{Key: key1, Lamports: 12},
		{Key: key2, Lamports: 23},
	}, []bool{true, true})
	require.EqualError(t, err, "fallback set failed")
	assert.Equal(t, []solana.PublicKey{key1, key2}, store.setOrder)
}

func TestSetTransactionAccountsSemantics(t *testing.T) {
	for _, test := range []struct {
		name    string
		overlay bool
	}{
		{name: "memory"},
		{name: "overlay", overlay: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			key1 := transactionBatchTestKey(1)
			key2 := transactionBatchTestKey(2)
			key3 := transactionBatchTestKey(3)
			parent := NewMemAccountsWithLen(3)
			require.NoError(t, parent.SetAccountWithoutLock(key1, &Account{Key: key1, Lamports: 1}))
			require.NoError(t, parent.SetAccountWithoutLock(key2, &Account{Key: key2, Lamports: 2}))
			require.NoError(t, parent.SetAccountWithoutLock(key3, &Account{Key: key3, Lamports: 3}))

			var store Accounts = parent
			if test.overlay {
				store = NewOverlayAccountsWithLen(parent, overlayTargetEntriesPerShard+1)
			}

			states := []*Account{
				{Key: key1, Lamports: 11, Data: []byte{1, 2, 3}},
				{Key: key2, Lamports: 22},
				{Key: key3, Lamports: 0, Data: []byte{9}, Owner: [32]byte{7}},
			}
			require.NoError(t, SetTransactionAccounts(store, states, []bool{true, false, true}))

			got1, err := store.GetAccount((*[32]byte)(&key1))
			require.NoError(t, err)
			assert.Same(t, states[0], got1)
			assert.Equal(t, uint64(11), got1.Lamports)

			got2, err := store.GetAccount((*[32]byte)(&key2))
			require.NoError(t, err)
			assert.Equal(t, uint64(2), got2.Lamports, "untouched account must not be overwritten")

			got3, err := store.GetAccount((*[32]byte)(&key3))
			require.NoError(t, err)
			assert.Equal(t, key3, got3.Key)
			assert.Zero(t, got3.Lamports)
			assert.Equal(t, uint64(math.MaxUint64), got3.RentEpoch)
			assert.Empty(t, got3.Data)
			assert.Zero(t, got3.Owner)

			require.Error(t, SetTransactionAccounts(store, []*Account{{Key: key1, Lamports: 99}}, nil))
			still11, err := store.GetAccount((*[32]byte)(&key1))
			require.NoError(t, err)
			assert.Equal(t, uint64(11), still11.Lamports, "length mismatch must not partially mutate")

			require.Error(t, SetTransactionAccounts(store, []*Account{{Key: key1, Lamports: 99}, nil}, []bool{true, true}))
			still11, err = store.GetAccount((*[32]byte)(&key1))
			require.NoError(t, err)
			assert.Equal(t, uint64(11), still11.Lamports, "nil touched state must not partially mutate")

			require.NoError(t, SetTransactionAccounts(store,
				[]*Account{{Key: key1, Lamports: 12}, {Key: key1, Lamports: 13}},
				[]bool{true, true},
			))
			last, err := store.GetAccount((*[32]byte)(&key1))
			require.NoError(t, err)
			assert.Equal(t, uint64(13), last.Lamports, "duplicate key must retain last-write-wins ordering")

			if test.overlay {
				parentValue, err := parent.GetAccount((*[32]byte)(&key1))
				require.NoError(t, err)
				assert.Equal(t, uint64(1), parentValue.Lamports, "overlay publication must not mutate its parent")
			}
		})
	}
}

func TestOverlayShardCountScalesWithCapacity(t *testing.T) {
	parent := NewMemAccounts()
	assert.Len(t, NewOverlayAccountsWithLen(parent, 0).shards, 1)
	assert.Len(t, NewOverlayAccountsWithLen(parent, overlayTargetEntriesPerShard).shards, 1)
	assert.Len(t, NewOverlayAccountsWithLen(parent, overlayTargetEntriesPerShard+1).shards, 2)
	assert.Len(t, NewOverlayAccountsWithLen(parent, 1<<20).shards, overlayMaxShardCount)
	sized := NewOverlayAccountsWithSizing(parent, 1<<20, overlayMaxShardCount)
	assert.Len(t, sized.shards, overlayMaxShardCount)
	assert.Equal(t, 1, sized.shardCapacity)
}

func TestOverlayConcurrentTransactionPublicationAndSnapshots(t *testing.T) {
	const (
		writers          = 16
		batchesPerWriter = 128
		accountsPerBatch = 2
	)
	totalAccounts := writers * batchesPerWriter * accountsPerBatch
	parent := NewMemAccounts()
	overlay := NewOverlayAccountsWithLen(parent, totalAccounts)

	start := make(chan struct{})
	firstWritePublished := make(chan struct{})
	firstSnapshotTaken := make(chan struct{})
	stopSnapshots := make(chan struct{})
	var snapshotCount atomic.Uint64
	var snapshotWG sync.WaitGroup
	snapshotWG.Add(1)
	go func() {
		defer snapshotWG.Done()
		<-firstWritePublished
		_ = overlay.DeltaAccounts()
		_ = overlay.AllAccounts()
		snapshotCount.Add(1)
		close(firstSnapshotTaken)
		for {
			select {
			case <-stopSnapshots:
				return
			default:
				_ = overlay.DeltaAccounts()
				_ = overlay.AllAccounts()
				snapshotCount.Add(1)
			}
		}
	}()

	var writersWG sync.WaitGroup
	writersWG.Add(writers)
	for writer := range writers {
		go func(writer int) {
			defer writersWG.Done()
			<-start
			for batchIdx := range batchesPerWriter {
				first := uint64((writer*batchesPerWriter+batchIdx)*accountsPerBatch + 1)
				accountStates := []*Account{
					{Key: transactionBatchTestKey(first), Lamports: first},
					{Key: transactionBatchTestKey(first + 1), Lamports: first + 1},
				}
				if err := overlay.SetTransactionAccounts(accountStates, []bool{true, true}); err != nil {
					panic(err)
				}
				if writer == 0 && batchIdx == 0 {
					close(firstWritePublished)
					<-firstSnapshotTaken
				}
			}
		}(writer)
	}
	close(start)
	writersWG.Wait()
	close(stopSnapshots)
	snapshotWG.Wait()
	assert.NotZero(t, snapshotCount.Load(), "snapshot must complete before every writer returns")

	delta := overlay.DeltaAccounts()
	require.Len(t, delta, totalAccounts)
	for _, acct := range delta {
		assert.Equal(t, binary.LittleEndian.Uint64(acct.Key[:8]), acct.Lamports)
	}
}

type blockingOverlayParent struct {
	MemAccounts
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (p *blockingOverlayParent) GetAccount(_ *[32]byte) (*Account, error) {
	p.once.Do(func() { close(p.started) })
	<-p.release
	return nil, errors.New("missing account")
}

func TestOverlayParentMissDoesNotBlockOtherShard(t *testing.T) {
	parent := &blockingOverlayParent{
		MemAccounts: NewMemAccounts(),
		started:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	overlay := NewOverlayAccountsWithLen(parent, 1<<14)
	keyA := transactionBatchTestKey(1)
	shardA := overlay.shardForKey(keyA)

	var keyB solana.PublicKey
	for candidate := uint64(2); ; candidate++ {
		keyB = transactionBatchTestKey(candidate)
		if overlay.shardForKey(keyB) != shardA {
			break
		}
	}

	getDone := make(chan struct{})
	go func() {
		_, _ = overlay.GetAccount((*[32]byte)(&keyA))
		close(getDone)
	}()
	<-parent.started

	released := false
	defer func() {
		if !released {
			close(parent.release)
		}
	}()

	otherShardDone := make(chan struct{})
	go func() {
		if err := overlay.SetAccount((*[32]byte)(&keyB), &Account{Key: keyB, Lamports: 2}); err != nil {
			panic(err)
		}
		close(otherShardDone)
	}()
	select {
	case <-otherShardDone:
	case <-time.After(time.Second):
		t.Fatal("different-shard write blocked behind unrelated parent miss")
	}
	accountB, err := overlay.GetAccount((*[32]byte)(&keyB))
	require.NoError(t, err)
	assert.Equal(t, uint64(2), accountB.Lamports)

	close(parent.release)
	released = true
	select {
	case <-getDone:
	case <-time.After(time.Second):
		t.Fatal("parent fallback read did not finish")
	}
}

func TestOverlayParentMissDoesNotBlockSameKeyWrite(t *testing.T) {
	parent := &blockingOverlayParent{
		MemAccounts: NewMemAccounts(),
		started:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	overlay := NewOverlayAccountsWithLen(parent, 1<<14)
	key := transactionBatchTestKey(1)

	getDone := make(chan struct{})
	go func() {
		_, _ = overlay.GetAccount((*[32]byte)(&key))
		close(getDone)
	}()
	<-parent.started

	released := false
	defer func() {
		if !released {
			close(parent.release)
		}
	}()

	writeDone := make(chan struct{})
	go func() {
		overlay.setTransactionAccounts([]*Account{{Key: key, Lamports: 3}}, []bool{true})
		close(writeDone)
	}()
	select {
	case <-writeDone:
	case <-time.After(time.Second):
		t.Fatal("same-key write blocked behind a parent read")
	}
	acct, err := overlay.GetAccount((*[32]byte)(&key))
	require.NoError(t, err)
	assert.Equal(t, uint64(3), acct.Lamports)

	close(parent.release)
	released = true
	<-getDone
}

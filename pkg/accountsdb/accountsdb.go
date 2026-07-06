package accountsdb

import (
	"bufio"
	"bytes"
	"container/list"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime/trace"
	"sync"
	"sync/atomic"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/Overclock-Validator/mithril/pkg/sbpf/jit"
	"github.com/cockroachdb/pebble"
	"github.com/gagliardetto/solana-go"
	"github.com/maypok86/otter"
	"golang.org/x/sync/errgroup"
)

type AccountsDb struct {
	Index            *pebble.DB
	BankHashStore    *pebble.DB
	AcctsDir         string // primary shard's accounts dir; its parent is the metadata dir
	Shards           *Shards
	VoteAcctCache    otter.Cache[solana.PublicKey, *accounts.Account]
	CommonAcctsCache otter.Cache[solana.PublicKey, *accounts.Account]
	ProgramCache     otter.Cache[solana.PublicKey, *ProgramCacheEntry]

	// A list of store requests. They are added to the back as they arrive and
	// removed from the front as they are persisted.
	inProgressStoreRequestsMu sync.Mutex
	inProgressStoreRequests   *list.List
	storeRequestChan          chan *list.Element
	storeWorkerDone           chan struct{}
}

type storeRequest struct {
	accts []*accounts.Account
	slot  uint64
	m     map[solana.PublicKey]*accounts.Account
	cb    func()
}

func (accountsDb *AccountsDb) StoreQueueLen() int {
	if accountsDb.inProgressStoreRequests == nil {
		return 0
	}
	accountsDb.inProgressStoreRequestsMu.Lock()
	defer accountsDb.inProgressStoreRequestsMu.Unlock()
	return accountsDb.inProgressStoreRequests.Len()
}

// silentLogger implements pebble.Logger but discards all messages.
// This suppresses verbose WAL recovery messages on startup.
type silentLogger struct{}

func (silentLogger) Infof(format string, args ...any)  {}
func (silentLogger) Fatalf(format string, args ...any) { log.Fatalf(format, args...) }

var (
	ErrNoAccount = errors.New("ErrNoAccount")

	StoreAccountsWorkers = 128
	ProgramCacheMaxMB    = DefaultProgramCacheMaxMB
)

const (
	indexPebbleMemTableSize                = 64 << 20
	indexPebbleMemTableStopWritesThreshold = 4
	DefaultProgramCacheMaxMB               = 1024
	programCacheCostUnitBytes              = 1 << 20
)

func NewAccountsIndexPebbleOptions(logger pebble.Logger) *pebble.Options {
	return &pebble.Options{
		Logger:                      logger,
		MemTableSize:                indexPebbleMemTableSize,
		MemTableStopWritesThreshold: indexPebbleMemTableStopWritesThreshold,
	}
}

func OpenDb(accountsPaths []string) (*AccountsDb, error) {
	if len(accountsPaths) == 0 {
		return nil, fmt.Errorf("OpenDb: no accounts paths configured")
	}
	// The first path holds all metadata (index, manifest, state); every path
	// holds an "accounts" dir with that disk's shard data.
	accountsDbDir := accountsPaths[0]

	// num_shards records the shard count the DB was built with.
	b, err := os.ReadFile(filepath.Join(accountsDbDir, "num_shards"))
	if err != nil {
		return nil, fmt.Errorf("reading num_shards: %w", err)
	}
	if len(b) != 8 {
		return nil, fmt.Errorf("num_shards: expected 8 bytes, got %d", len(b))
	}
	numShards := int(binary.LittleEndian.Uint64(b))
	if numShards != len(accountsPaths) {
		return nil, fmt.Errorf("configured %d accounts dir(s) but AccountsDB was built with %d shard(s); rebuild required", len(accountsPaths), numShards)
	}

	shardDirs := make([]string, len(accountsPaths))
	for i, p := range accountsPaths {
		shardDirs[i] = filepath.Join(p, "accounts")
	}

	// check for existence of the primary 'accounts' directory, which holds the appendvecs
	appendVecsDir := shardDirs[0]
	if _, err := os.Stat(appendVecsDir); err != nil {
		return nil, err
	}

	indexDir := filepath.Join(accountsDbDir, "mithril_db")
	db, err := pebble.Open(indexDir, NewAccountsIndexPebbleOptions(silentLogger{}))
	if err != nil {
		return nil, fmt.Errorf("opening indexDir=%s: %w", indexDir, err)
	}

	bankhashDir := filepath.Join(accountsDbDir, "bankhash_db")
	bankhashDb, err := pebble.Open(bankhashDir, &pebble.Options{Logger: silentLogger{}})
	if err != nil {
		return nil, fmt.Errorf("opening bankhashDir=%s: %w", bankhashDir, err)
	}

	// The counter seeds runtime fileId minting; left at 0 so the first minted id
	// lands in segment 1, just past segment 0 (the coalesced "data" big file).
	shards := newShards(shardDirs)
	accountsDb := &AccountsDb{Index: db, BankHashStore: bankhashDb, AcctsDir: appendVecsDir, Shards: shards}

	accountsDb.inProgressStoreRequests = list.New()
	accountsDb.storeRequestChan = make(chan *list.Element)
	accountsDb.storeWorkerDone = make(chan struct{})
	go accountsDb.storeWorker()

	return accountsDb, nil
}

// Turns down the store worker. AccountsDb cannot accept writes after this.
// Should not be called concurrently.
func (accountsDb *AccountsDb) WaitForStoreWorker() {
	if accountsDb.storeWorkerDone == nil {
		mlog.Log.Infof("AccountsDb: async store worker already done.")
		return
	}
	mlog.Log.Infof("AccountsDb: waiting for async store worker...")
	close(accountsDb.storeRequestChan)
	<-accountsDb.storeWorkerDone
	accountsDb.storeWorkerDone = nil
}

func (accountsDb *AccountsDb) CloseDb() {
	accountsDb.WaitForStoreWorker()
	mlog.Log.Infof("CloseDb: syncing and closing Index...")
	if err := accountsDb.Index.Close(); err != nil {
		mlog.Log.Errorf("CloseDb: Index.Close() error: %v", err)
	}
	mlog.Log.Infof("CloseDb: syncing and closing BankHashStore...")
	if err := accountsDb.BankHashStore.Close(); err != nil {
		mlog.Log.Errorf("CloseDb: BankHashStore.Close() error: %v", err)
	}
	mlog.Log.Infof("CloseDb: done\n") // extra newline for spacing after close
}

func (accountsDb *AccountsDb) InitCaches() {
	var err error
	if accountsDb.inProgressStoreRequests == nil {
		accountsDb.inProgressStoreRequests = list.New()
	}
	accountsDb.VoteAcctCache, err = otter.MustBuilder[solana.PublicKey, *accounts.Account](2500).
		Cost(func(key solana.PublicKey, acct *accounts.Account) uint32 {
			return 1
		}).
		Build()
	if err != nil {
		panic(err)
	}

	accountsDb.ProgramCache, err = otter.MustBuilder[solana.PublicKey, *ProgramCacheEntry](programCacheCapacityUnits()).
		Cost(func(key solana.PublicKey, progEntry *ProgramCacheEntry) uint32 {
			return progEntry.CostUnits()
		}).
		Build()
	if err != nil {
		panic(err)
	}

	accountsDb.CommonAcctsCache, err = otter.MustBuilder[solana.PublicKey, *accounts.Account](5000).
		Cost(func(key solana.PublicKey, acct *accounts.Account) uint32 {
			return 1
		}).
		Build()
	if err != nil {
		panic(err)
	}
}

type ProgramCacheEntry struct {
	Program        *sbpf.Program
	DeploymentSlot uint64

	// JIT tier state, managed by pkg/sealevel: executions to date, the
	// compiled code once the program crosses the threshold, and a sticky
	// refusal for programs the compiler does not support. Evicting the
	// entry drops the compiled code with it.
	JitExecs  atomic.Uint64
	Jit       atomic.Pointer[jit.Compiled]
	JitFailed atomic.Bool
}

func programCacheCapacityUnits() int {
	if ProgramCacheMaxMB <= 0 {
		return DefaultProgramCacheMaxMB
	}
	return ProgramCacheMaxMB
}

func (entry *ProgramCacheEntry) CostUnits() uint32 {
	if entry == nil || entry.Program == nil {
		return 1
	}
	bytes := entry.Program.MemoryBytes()
	units := (bytes + programCacheCostUnitBytes - 1) / programCacheCostUnitBytes
	if units == 0 {
		return 1
	}
	max := uint64(^uint32(0))
	if units > max {
		return ^uint32(0)
	}
	return uint32(units)
}

func (accountsDb *AccountsDb) MaybeGetProgramFromCache(pubkey solana.PublicKey) (*ProgramCacheEntry, bool) {
	if accountsDb == nil {
		return nil, false
	}
	return accountsDb.ProgramCache.Get(pubkey)
}

func (accountsDb *AccountsDb) AddProgramToCache(pubkey solana.PublicKey, programEntry *ProgramCacheEntry) {
	accountsDb.ProgramCache.Set(pubkey, programEntry)
}

func (accountsDb *AccountsDb) RemoveProgramFromCache(pubkey solana.PublicKey) {
	accountsDb.ProgramCache.Delete(pubkey)
}

func (accountsDb *AccountsDb) GetAccount(slot uint64, pubkey solana.PublicKey) (*accounts.Account, error) {
	if accountsDb == nil {
		return nil, ErrNoAccount
	}
	accts := accountsDb.getStoreInProgressAccounts([]solana.PublicKey{pubkey})
	if accts[0] != nil {
		return accts[0], nil
	}
	return accountsDb.getStoredAccount(slot, pubkey)
}

func (accountsDb *AccountsDb) getStoredAccount(slot uint64, pubkey solana.PublicKey) (*accounts.Account, error) {
	if accountsDb.Index == nil {
		return nil, ErrNoAccount
	}
	r := trace.StartRegion(context.Background(), "GetStoredAccountCache")
	cachedAcct, hasAcct := accountsDb.VoteAcctCache.Get(pubkey)
	if hasAcct {
		r.End()
		return cachedAcct, nil
	}

	cachedAcct, hasAcct = accountsDb.CommonAcctsCache.Get(pubkey)
	if hasAcct {
		r.End()
		return cachedAcct, nil
	}
	r.End()

	defer trace.StartRegion(context.Background(), "GetStoredAccountDisk").End()
	acctIdxEntryBytes, c, err := accountsDb.Index.Get(pubkey[:])
	if err != nil {
		//mlog.Log.Debugf("no account found in accountsdb for pubkey %s: %s", pubkey, err)
		return nil, ErrNoAccount
	}

	acctIdxEntry, err := UnmarshalAcctIdxEntry(acctIdxEntryBytes)
	if err != nil {
		panic("failed to unmarshal AccountIndexEntry from index kv database")
	}
	c.Close()

	appendVecFileName := accountsDb.Shards.path(acctIdxEntry.Slot, acctIdxEntry.FileId)

	appendVecFile, err := os.Open(appendVecFileName)
	if err != nil {
		//mlog.Log.Debugf("failed to open appendvec file %s")
		return nil, err
	}
	defer appendVecFile.Close()

	offset, err := appendVecFile.Seek(int64(acctIdxEntry.Offset), 0)
	if err != nil {
		panic(fmt.Sprintf("file seek failed: %s\n", err))
	}
	if offset != int64(acctIdxEntry.Offset) {
		panic(fmt.Sprintf("file seek gave wrong idx (%d)\n", offset))
	}

	acct, err := unmarshalAcctFromAppendVecAcctHeader(appendVecFile)
	if err != nil {
		panic(fmt.Sprintf("failed to unmarshal account from appendvec file %s: %s", appendVecFileName, err))
	}

	if acct.Key != pubkey {
		panic(fmt.Sprintf("account unmarshaled from appendvec file %s has the wrong pubkey", appendVecFileName))
	}

	acct.Slot = acctIdxEntry.Slot

	owner := solana.PublicKeyFromBytes(acct.Owner[:])
	if owner == addresses.VoteProgramAddr {
		accountsDb.VoteAcctCache.Set(pubkey, acct)
	} else {
		accountsDb.CommonAcctsCache.Set(pubkey, acct)
	}

	return acct, err
}

// Returns a slice of the same length as the input with results matching indexes, nil if not found.
// Returns clones to avoid data races with the store worker.
func (accountsDb *AccountsDb) getStoreInProgressAccounts(pks []solana.PublicKey) []*accounts.Account {
	defer trace.StartRegion(context.Background(), "getStoreInProgressAccounts").End()
	out := make([]*accounts.Account, len(pks))
	accountsDb.inProgressStoreRequestsMu.Lock()
	defer accountsDb.inProgressStoreRequestsMu.Unlock()
	// Start with newest first.
	for e := accountsDb.inProgressStoreRequests.Back(); e != nil; e = e.Prev() {
		sr := e.Value.(storeRequest)
		for i := range len(pks) {
			if out[i] != nil {
				continue // Already found.
			}
			if acct := sr.m[pks[i]]; acct != nil {
				out[i] = acct.Clone()
			}
		}
	}
	return out
}

func (accountsDb *AccountsDb) StoreAccounts(
	accts []*accounts.Account,
	slot uint64,
	cb func(),
) error {
	for _, acct := range accts {
		if acct == nil {
			continue
		}
		acct.Slot = slot
	}

	m := make(map[solana.PublicKey]*accounts.Account, len(accts))
	for _, a := range accts {
		if a == nil {
			continue
		}
		m[a.Key] = a
	}
	// Must not hold lock during channel send to avoid deadlock with storeWorker.
	accountsDb.inProgressStoreRequestsMu.Lock()
	element := accountsDb.inProgressStoreRequests.PushBack(storeRequest{accts, slot, m, cb})
	accountsDb.inProgressStoreRequestsMu.Unlock()
	accountsDb.storeRequestChan <- element
	return nil
}

func (accountsDb *AccountsDb) storeAccountsSync(accts []*accounts.Account, slot uint64) {
	defer trace.StartRegion(context.Background(), "StoreAccounts").End()
	if StoreAccountsWorkers == 1 {
		accountsDb.storeAccountsInternal(accts, slot)
	} else {
		accountsDb.parallelStoreAccounts(StoreAccountsWorkers, accts, slot)
	}

	for _, acct := range accts {
		if acct == nil {
			continue
		}

		// If the account has been deleted (lamports == 0), remove the old version from
		// the cache, if it exists. This is essential for vote accounts because after
		// deletion (such as via a VoteWithdraw instruction) the account no longer has
		// the vote program as its owner, so this logic is necessary to prevent stale
		// versions of now deleted vote accounts from remaining in the vote cache and
		// thereafter being served up as invalid account state.
		if acct.Lamports == 0 {
			if accountsDb.CommonAcctsCache.Has(acct.Key) {
				accountsDb.CommonAcctsCache.Delete(acct.Key)
			} else if accountsDb.VoteAcctCache.Has(acct.Key) {
				accountsDb.VoteAcctCache.Delete(acct.Key)
			}
		} else {
			owner := solana.PublicKeyFromBytes(acct.Owner[:])
			if owner == addresses.VoteProgramAddr {
				accountsDb.VoteAcctCache.Set(acct.Key, acct)
			} else {
				accountsDb.CommonAcctsCache.Set(acct.Key, acct)
			}
		}
	}
}

func (accountsDb *AccountsDb) storeWorker() {
	defer close(accountsDb.storeWorkerDone)
	for elt := range accountsDb.storeRequestChan {
		sr := elt.Value.(storeRequest)
		accountsDb.storeAccountsSync(sr.accts, sr.slot)
		if sr.cb != nil {
			sr.cb()
		}
		// Remove after callback so DrainStoreQueue waits for callbacks (e.g. index flush) to complete
		accountsDb.inProgressStoreRequestsMu.Lock()
		accountsDb.inProgressStoreRequests.Remove(elt)
		accountsDb.inProgressStoreRequestsMu.Unlock()
	}
}

func (accountsDb *AccountsDb) storeAccountsInternal(accts []*accounts.Account, slot uint64) {
	fileId := accountsDb.Shards.mint(accountsDb.Shards.choose())
	appendVecFileName := accountsDb.Shards.path(slot, fileId)
	appendVecFile, err := os.OpenFile(appendVecFileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		//mlog.Log.Debugf("unable to open appendvec file %s for writing to accountsdb", appendVecFileName)
		panic(err)
	}
	defer appendVecFile.Close()

	appendVecAcctsBuf := new(bytes.Buffer)
	writer := new(bytes.Buffer)
	var acctIdxEntryBuf [24]byte

	for _, acct := range accts {
		if acct == nil {
			continue
		}

		// create index entry, encode it and write it to the index kv store
		// offset field is specified as the current num of bytes written to the appendvec buffer.
		writer.Reset()

		indexEntry := AccountIndexEntry{Slot: slot, FileId: fileId, Offset: uint64(appendVecAcctsBuf.Len())}
		indexEntry.Marshal(&acctIdxEntryBuf)

		// if an entry already existed in the index for this account, very often we can simply make the state update
		// in-place, i.e. into the account's existing appendvec blob.
		// we can make the account state update in-place iff the existing version's data length is the same as the
		// new version's data length, which is the case about 98% of the time.
		// if not, then we write out a new appendvec.
		existingacctIdxEntryBuf, c, err := accountsDb.Index.Get(acct.Key[:])
		if err == nil {
			acctIdxEntry, err := UnmarshalAcctIdxEntry(existingacctIdxEntryBuf)
			if err != nil {
				panic("failed to unmarshal AccountIndexEntry from index kv database")
			}
			c.Close()

			existingAppendVecFileName := accountsDb.Shards.path(acctIdxEntry.Slot, acctIdxEntry.FileId)
			existingAppendVecFile, err := os.OpenFile(existingAppendVecFileName, os.O_RDWR, 0666)
			if err != nil {
				panic(err)
			}

			_, err = existingAppendVecFile.Seek(int64(acctIdxEntry.Offset), 0)
			if err != nil {
				panic(err)
			}

			existingAcct, err := unmarshalAcctFromAppendVecAcctHeader(existingAppendVecFile)
			if err != nil {
				panic(fmt.Sprintf("failed to unmarshal account from appendvec file %s: %s", existingAppendVecFileName, err))
			}

			if len(acct.Data) == len(existingAcct.Data) {
				newAppendVecAcct := AppendVecAccount{DataLen: uint64(len(acct.Data)), Pubkey: acct.Key, Lamports: acct.Lamports,
					RentEpoch: acct.RentEpoch, Owner: acct.Owner, Executable: acct.Executable, Data: acct.Data}

				_, err = existingAppendVecFile.Seek(int64(acctIdxEntry.Offset), 0)
				if err != nil {
					panic(err)
				}

				err = newAppendVecAcct.Marshal(existingAppendVecFile)
				if err != nil {
					panic(fmt.Sprintf("error marshaling appendvec for storage: %s", err))
				}

				existingAppendVecFile.Close()
				continue
			}
		}

		err = accountsDb.Index.Set(acct.Key[:], acctIdxEntryBuf[:], &pebble.WriteOptions{})
		if err != nil {
			panic(fmt.Sprintf("unable to add acct for %s to acctsdb: %v", acct.Key, err))
		}

		// marshal up the account as an appendvec style account and write it to the buffer
		appendVecAcct := AppendVecAccount{DataLen: uint64(len(acct.Data)), Pubkey: acct.Key, Lamports: acct.Lamports,
			RentEpoch: acct.RentEpoch, Owner: acct.Owner, Executable: acct.Executable, Data: acct.Data}

		err = appendVecAcct.Marshal(appendVecAcctsBuf)
		if err != nil {
			panic(fmt.Sprintf("unable to add acct for %s to acctsdb: %v", acct.Key, err))
		}
	}

	// write the appendvecs data into the file
	_, err = appendVecFile.Write(appendVecAcctsBuf.Bytes())
	if err != nil {
		panic(err)
	}
}

// parallelStoreAccounts makes n workers which process a list of
// accounts in parallel. One worker receives accounts to add to a new
// appendvec file. The remaining workers do the following:
// for each account they receive:
// 1. Check the existing accounts length
// 2. If the length of the new account data is the same, overwrite the existing account
// 3. Otherwise, pass it on to be added to a new appendvec.
func (accountsDb *AccountsDb) parallelStoreAccounts(n int, accts []*accounts.Account, slot uint64) {
	if n < 2 {
		panic(fmt.Sprintf("AccountsDb.parallelStoreAccounts: n=%d must be >= 2", n))
	}

	acctsChan := make(chan *accounts.Account, len(accts))
	for i := range len(accts) {
		if accts[i] == nil {
			continue
		}
		acctsChan <- accts[i]
	}
	close(acctsChan)

	// Assumes that none of the accounts overlap in the same appendvec file.
	lengthChangedAccounts := make(chan *accounts.Account)
	overwriteOrPassGroup, ctx := errgroup.WithContext(context.Background())
	for range n - 1 {
		overwriteOrPassGroup.Go(func() error {
			for acct := range acctsChan {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				err := func(a *accounts.Account) error {
					existingacctIdxEntryBuf, c, err := accountsDb.Index.Get(a.Key[:])
					if errors.Is(err, pebble.ErrNotFound) {
						lengthChangedAccounts <- a
						return nil
					}
					if err != nil {
						return fmt.Errorf("reading from index: %w", err)
					}
					existingIdxEntry, err := UnmarshalAcctIdxEntry(existingacctIdxEntryBuf)
					c.Close()
					if err != nil {
						return fmt.Errorf("unmarshaling index entry: %w", err)
					}

					existingAppendVecFileName := accountsDb.Shards.path(existingIdxEntry.Slot, existingIdxEntry.FileId)
					existingAppendVecFile, err := os.OpenFile(existingAppendVecFileName, os.O_RDWR, 0666)
					if err != nil {
						return fmt.Errorf("open %s: %w", existingAppendVecFileName, err)
					}
					defer existingAppendVecFile.Close()

					existingDataLen, err := GetAppendVecDataLen(existingAppendVecFile, existingIdxEntry.Offset)
					if err != nil {
						return fmt.Errorf("GetAppendVecDataLen %s: %w", existingAppendVecFileName, err)
					}

					if uint64(len(a.Data)) != existingDataLen {
						lengthChangedAccounts <- a
						return nil
					}

					_, err = existingAppendVecFile.Seek(int64(existingIdxEntry.Offset), 0)
					if err != nil {
						return fmt.Errorf("seek %s %d: %w", existingAppendVecFileName, existingIdxEntry.Offset, err)
					}
					newAppendVecAcct := AppendVecAccount{
						DataLen:    uint64(len(a.Data)),
						Pubkey:     a.Key,
						Lamports:   a.Lamports,
						RentEpoch:  a.RentEpoch,
						Owner:      a.Owner,
						Executable: a.Executable,
						Data:       a.Data,
					}
					err = newAppendVecAcct.Marshal(existingAppendVecFile)
					if err != nil {
						return fmt.Errorf("marshaling appendvec: %w", err)
					}
					return nil
				}(acct)
				if err != nil {
					return fmt.Errorf("reading account key=%s: %w", acct.Key.String(), err)
				}
			}
			return nil
		})
	}
	newAppendVecGroup := errgroup.Group{}
	newAppendVecGroup.Go(func() error {
		fileId := accountsDb.Shards.mint(accountsDb.Shards.choose())
		appendVecFileName := accountsDb.Shards.path(slot, fileId)
		appendVecFile, err := os.OpenFile(appendVecFileName, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return err
		}
		defer appendVecFile.Close()
		appendVecWriter := bufio.NewWriter(appendVecFile)
		defer appendVecWriter.Flush()

		appendVecFileOffset := uint64(0)
		var acctIdxEntryBuf [24]byte

		for acct := range lengthChangedAccounts {
			indexEntry := AccountIndexEntry{Slot: slot, FileId: fileId, Offset: appendVecFileOffset}
			indexEntry.Marshal(&acctIdxEntryBuf)
			err = accountsDb.Index.Set(acct.Key[:], acctIdxEntryBuf[:], &pebble.WriteOptions{})
			if err != nil {
				return fmt.Errorf("unable to add acct for %s to acctsdb: %v", acct.Key, err)
			}

			appendVecAcct := AppendVecAccount{
				DataLen:    uint64(len(acct.Data)),
				Pubkey:     acct.Key,
				Lamports:   acct.Lamports,
				RentEpoch:  acct.RentEpoch,
				Owner:      acct.Owner,
				Executable: acct.Executable,
				Data:       acct.Data,
			}
			l, err := appendVecAcct.MarshalReturningLength(appendVecWriter)
			if err != nil {
				return fmt.Errorf("unable to add acct for %s to acctsdb: %v", acct.Key, err)
			}
			appendVecFileOffset += uint64(l)
		}

		return nil
	})

	e1 := overwriteOrPassGroup.Wait()
	close(lengthChangedAccounts)
	e2 := newAppendVecGroup.Wait()
	if err := errors.Join(e1, e2); err != nil {
		panic(err)
	}
}

func (accountsDb *AccountsDb) GetBankHashForSlot(slot uint64) ([]byte, error) {
	var slotBytes [8]byte
	binary.LittleEndian.PutUint64(slotBytes[:], slot)
	bh, c, err := accountsDb.BankHashStore.Get(slotBytes[:])
	if err != nil {
		return nil, fmt.Errorf("GetBankHashForSlot slot=%d: %w", slot, err)
	}
	out := make([]byte, len(bh))
	copy(out, bh)
	c.Close()
	return out, nil
}

func (accountsDb *AccountsDb) StoreBankHashForSlot(slot uint64, bankHash []byte) error {
	var slotBytes [8]byte
	binary.LittleEndian.PutUint64(slotBytes[:], slot)
	return accountsDb.BankHashStore.Set(slotBytes[:], bankHash, &pebble.WriteOptions{})
}

func (accountsDb *AccountsDb) KeysBetweenPrefixes(startPrefix uint64, endPrefix uint64) []solana.PublicKey {
	return nil
	/*keys := accountsDb.IndexDb.KeysBetweenPrefixes(startPrefix, endPrefix)

	keyObjs := make([]solana.PublicKey, 0)
	for _, key := range keys {
		keyObject := solana.PublicKeyFromBytes(key)
		keyObjs = append(keyObjs, keyObject)
	}

	return keyObjs*/
}

func (accountsDb *AccountsDb) AllKeys() [][]byte {
	return nil
}

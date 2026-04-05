package accountsdb

import (
	"bufio"
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
	"github.com/cockroachdb/pebble"
	"github.com/gagliardetto/solana-go"
	"github.com/maypok86/otter"
	"golang.org/x/sync/errgroup"
)

type AccountsDb struct {
	Index            *pebble.DB
	BankHashStore    *pebble.DB
	AcctsDir         string
	LargestFileId    atomic.Uint64
	AppendVecFiles   *appendVecFileCache
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

	beforeRelocatedIndexBatchCommitHook func()
)

const (
	voteAccountCacheCapacityBytes   = 128 << 20
	commonAccountCacheCapacityBytes = 512 << 20
	programCacheCapacityEntries     = 2000
	accountCacheEntryOverheadBytes  = 128
)

func OpenDb(accountsDbDir string) (*AccountsDb, error) {
	// check for existence of the 'accounts' directory, which holds the appendvecs
	appendVecsDir := fmt.Sprintf("%s/accounts", accountsDbDir)
	_, err := os.Stat(appendVecsDir)
	if err != nil {
		return nil, err
	}

	// attempt to open largest_file_id file
	largestFileIdFn := fmt.Sprintf("%s/largest_file_id", accountsDbDir)
	lfi, err := os.Open(largestFileIdFn)
	if err != nil {
		mlog.Log.Infof("failed to open %s\n", largestFileIdFn)
		return nil, err
	}

	largestFileIdBytes := make([]byte, 8)
	bytesRead, err := lfi.Read(largestFileIdBytes)
	if err != nil {
		mlog.Log.Infof("error reading %s: %s\n", largestFileIdFn, err)
		return nil, err
	} else if bytesRead != 8 {
		mlog.Log.Infof("error reading %s: expected 8 bytes, got %d\n", largestFileIdFn, bytesRead)
		return nil, fmt.Errorf("only got %d bytes", bytesRead)
	}

	largestFileId := binary.LittleEndian.Uint64(largestFileIdBytes)

	indexDir := filepath.Join(accountsDbDir, "mithril_db")
	db, err := pebble.Open(indexDir, &pebble.Options{Logger: silentLogger{}})
	if err != nil {
		return nil, fmt.Errorf("opening indexDir=%s: %w", indexDir, err)
	}

	bankhashDir := filepath.Join(accountsDbDir, "bankhash_db")
	bankhashDb, err := pebble.Open(bankhashDir, &pebble.Options{Logger: silentLogger{}})
	if err != nil {
		return nil, fmt.Errorf("opening bankhashDir=%s: %w", bankhashDir, err)
	}

	accountsDb := &AccountsDb{
		Index:          db,
		BankHashStore:  bankhashDb,
		AcctsDir:       appendVecsDir,
		AppendVecFiles: newAppendVecFileCache(defaultAppendVecFileCacheCapacity),
	}
	accountsDb.LargestFileId.Store(largestFileId)

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
	if accountsDb.AppendVecFiles != nil {
		if err := accountsDb.AppendVecFiles.Close(); err != nil {
			mlog.Log.Errorf("CloseDb: AppendVecFiles.Close() error: %v", err)
		}
	}
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
	accountsDb.VoteAcctCache, err = otter.MustBuilder[solana.PublicKey, *accounts.Account](voteAccountCacheCapacityBytes).
		Cost(accountCacheCost).
		Build()
	if err != nil {
		panic(err)
	}

	accountsDb.ProgramCache, err = otter.MustBuilder[solana.PublicKey, *ProgramCacheEntry](programCacheCapacityEntries).
		Cost(func(key solana.PublicKey, progEntry *ProgramCacheEntry) uint32 {
			return 1
		}).
		Build()
	if err != nil {
		panic(err)
	}

	accountsDb.CommonAcctsCache, err = otter.MustBuilder[solana.PublicKey, *accounts.Account](commonAccountCacheCapacityBytes).
		Cost(accountCacheCost).
		Build()
	if err != nil {
		panic(err)
	}
}

func accountCacheCost(_ solana.PublicKey, acct *accounts.Account) uint32 {
	if acct == nil {
		return 1
	}

	size := uint64(accountCacheEntryOverheadBytes) + uint64(len(acct.Data))
	if size == 0 {
		return 1
	}
	if size > uint64(^uint32(0)) {
		return ^uint32(0)
	}
	return uint32(size)
}

type ProgramCacheEntry struct {
	Program        *sbpf.Program
	DeploymentSlot uint64
}

func (accountsDb *AccountsDb) MaybeGetProgramFromCache(pubkey solana.PublicKey) (*ProgramCacheEntry, bool) {
	return accountsDb.ProgramCache.Get(pubkey)
}

func (accountsDb *AccountsDb) AddProgramToCache(pubkey solana.PublicKey, programEntry *ProgramCacheEntry) {
	accountsDb.ProgramCache.Set(pubkey, programEntry)
}

func (accountsDb *AccountsDb) RemoveProgramFromCache(pubkey solana.PublicKey) {
	accountsDb.ProgramCache.Delete(pubkey)
}

func (accountsDb *AccountsDb) GetAccount(slot uint64, pubkey solana.PublicKey) (*accounts.Account, error) {
	accts := accountsDb.getStoreInProgressAccounts([]solana.PublicKey{pubkey})
	if accts[0] != nil {
		return accts[0], nil
	}
	return accountsDb.getStoredAccount(slot, pubkey)
}

func (accountsDb *AccountsDb) getStoredAccount(slot uint64, pubkey solana.PublicKey) (*accounts.Account, error) {
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

	appendVecFileName := accountsDb.appendVecFileName(acctIdxEntry.Slot, acctIdxEntry.FileId)
	appendVecFile, release, err := accountsDb.AppendVecFiles.Acquire(appendVecFileName)
	if err != nil {
		return nil, err
	}
	defer release()

	acct, err := readAppendVecAccountAt(appendVecFile, acctIdxEntry.Offset)
	if err != nil {
		panic(fmt.Sprintf("failed to unmarshal account from appendvec file %s: %s", appendVecFileName, err))
	}

	if acct.Key != pubkey {
		panic(fmt.Sprintf("account unmarshaled from appendvec file %s has the wrong pubkey", appendVecFileName))
	}

	acct.Slot = acctIdxEntry.Slot
	acct.SetStorageInfo(acctIdxEntry.Slot, acctIdxEntry.FileId, acctIdxEntry.Offset, uint64(len(acct.Data)))

	owner := solana.PublicKeyFromBytes(acct.Owner[:])
	if owner == addresses.VoteProgramAddr {
		accountsDb.VoteAcctCache.Set(pubkey, acct)
	} else {
		accountsDb.CommonAcctsCache.Set(pubkey, acct)
	}

	return acct, err
}

func (accountsDb *AccountsDb) tryFastOverwriteStoredAccount(acct *accounts.Account) (bool, error) {
	storedSlot, storedFileId, storedOffset, storedDataLen, ok := acct.StorageInfo()
	if !ok || uint64(len(acct.Data)) != storedDataLen {
		return false, nil
	}

	appendVecFileName := accountsDb.appendVecFileName(storedSlot, storedFileId)
	appendVecFile, release, err := accountsDb.AppendVecFiles.Acquire(appendVecFileName)
	if err != nil {
		return false, err
	}
	defer release()

	err = writeAppendVecAccountAt(appendVecFile, storedOffset, appendVecAccountFromAccount(acct))
	if err != nil {
		return false, err
	}

	acct.SetStorageInfo(storedSlot, storedFileId, storedOffset, storedDataLen)
	return true, nil
}

func flushAndCloseAppendVecFile(f *os.File, w *bufio.Writer) error {
	if w != nil {
		if err := w.Flush(); err != nil {
			return err
		}
	}
	if f != nil {
		if err := f.Close(); err != nil {
			return err
		}
	}
	return nil
}

func queueRelocatedAccountIndexUpdate(
	indexBatch *pebble.Batch,
	acct *accounts.Account,
	entry AccountIndexEntry,
	acctIdxEntryBuf *[24]byte,
) error {
	entry.Marshal(acctIdxEntryBuf)
	err := indexBatch.Set(acct.Key[:], acctIdxEntryBuf[:], nil)
	if err != nil {
		return err
	}
	return nil
}

func commitRelocatedAccountIndexUpdates(indexBatch *pebble.Batch) error {
	if indexBatch == nil {
		return nil
	}
	if beforeRelocatedIndexBatchCommitHook != nil {
		beforeRelocatedIndexBatchCommitHook()
	}
	if err := indexBatch.Commit(nil); err != nil {
		return err
	}
	return nil
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
		owner := solana.PublicKeyFromBytes(acct.Owner[:])
		if owner == addresses.VoteProgramAddr {
			accountsDb.VoteAcctCache.Set(acct.Key, acct)
		} else {
			accountsDb.CommonAcctsCache.Set(acct.Key, acct)
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
	var (
		fileId              uint64
		appendVecFile       *os.File
		appendVecAcctsBuf   *bufio.Writer
		appendVecFileOffset uint64
		indexBatch          *pebble.Batch
	)
	defer func() {
		if indexBatch != nil {
			_ = indexBatch.Close()
		}
	}()
	var acctIdxEntryBuf [24]byte

	for _, acct := range accts {
		if acct == nil {
			continue
		}

		ok, err := accountsDb.tryFastOverwriteStoredAccount(acct)
		if err != nil {
			panic(err)
		}
		if ok {
			continue
		}

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

			existingAppendVecFileName := accountsDb.appendVecFileName(acctIdxEntry.Slot, acctIdxEntry.FileId)
			existingAppendVecFile, release, err := accountsDb.AppendVecFiles.Acquire(existingAppendVecFileName)
			if err != nil {
				panic(err)
			}
			existingDataLen, err := GetAppendVecDataLen(existingAppendVecFile, acctIdxEntry.Offset)
			if err != nil {
				release()
				panic(fmt.Sprintf("failed to read appendvec data len from %s: %s", existingAppendVecFileName, err))
			}

			if uint64(len(acct.Data)) == existingDataLen {
				err = writeAppendVecAccountAt(existingAppendVecFile, acctIdxEntry.Offset, appendVecAccountFromAccount(acct))
				release()
				if err != nil {
					panic(fmt.Sprintf("error marshaling appendvec for storage: %s", err))
				}
				acct.SetStorageInfo(acctIdxEntry.Slot, acctIdxEntry.FileId, acctIdxEntry.Offset, existingDataLen)
				continue
			}
			release()
		}

		if appendVecAcctsBuf == nil {
			fileId = accountsDb.LargestFileId.Add(1)
			appendVecFileName := fmt.Sprintf("%s/%d.%d", accountsDb.AcctsDir, slot, fileId)
			appendVecFile, err = os.OpenFile(appendVecFileName, os.O_RDWR|os.O_CREATE, 0666)
			if err != nil {
				panic(err)
			}
			appendVecAcctsBuf = bufio.NewWriter(appendVecFile)
		}

		// create index entry, encode it and write it to the index kv store
		// offset field is specified as the current num of bytes written to the appendvec buffer.
		indexEntry := AccountIndexEntry{Slot: slot, FileId: fileId, Offset: appendVecFileOffset}
		if indexBatch == nil {
			indexBatch = accountsDb.Index.NewBatch()
		}
		err = queueRelocatedAccountIndexUpdate(indexBatch, acct, indexEntry, &acctIdxEntryBuf)
		if err != nil {
			panic(fmt.Sprintf("unable to add acct for %s to acctsdb: %v", acct.Key, err))
		}

		// marshal up the account as an appendvec style account and write it to the buffer
		appendVecAcct := appendVecAccountFromAccount(acct)
		l, err := appendVecAcct.MarshalReturningLength(appendVecAcctsBuf)
		if err != nil {
			panic(fmt.Sprintf("unable to add acct for %s to acctsdb: %v", acct.Key, err))
		}
		acct.SetStorageInfo(slot, fileId, appendVecFileOffset, uint64(len(acct.Data)))
		appendVecFileOffset += uint64(l)
	}

	err := flushAndCloseAppendVecFile(appendVecFile, appendVecAcctsBuf)
	appendVecFile = nil
	appendVecAcctsBuf = nil
	if err != nil {
		panic(err)
	}
	err = commitRelocatedAccountIndexUpdates(indexBatch)
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
					ok, err := accountsDb.tryFastOverwriteStoredAccount(a)
					if err != nil {
						return err
					}
					if ok {
						return nil
					}

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

					existingAppendVecFileName := accountsDb.appendVecFileName(existingIdxEntry.Slot, existingIdxEntry.FileId)
					existingAppendVecFile, release, err := accountsDb.AppendVecFiles.Acquire(existingAppendVecFileName)
					if err != nil {
						return fmt.Errorf("open %s: %w", existingAppendVecFileName, err)
					}

					existingDataLen, err := GetAppendVecDataLen(existingAppendVecFile, existingIdxEntry.Offset)
					if err != nil {
						release()
						return fmt.Errorf("GetAppendVecDataLen %s: %w", existingAppendVecFileName, err)
					}

					if uint64(len(a.Data)) != existingDataLen {
						release()
						lengthChangedAccounts <- a
						return nil
					}

					err = writeAppendVecAccountAt(existingAppendVecFile, existingIdxEntry.Offset, appendVecAccountFromAccount(a))
					release()
					if err != nil {
						return fmt.Errorf("marshaling appendvec: %w", err)
					}
					a.SetStorageInfo(existingIdxEntry.Slot, existingIdxEntry.FileId, existingIdxEntry.Offset, existingDataLen)
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
		var (
			fileId              uint64
			appendVecFile       *os.File
			appendVecWriter     *bufio.Writer
			appendVecFileOffset uint64
			indexBatch          *pebble.Batch
		)
		defer func() {
			if indexBatch != nil {
				_ = indexBatch.Close()
			}
		}()
		var acctIdxEntryBuf [24]byte

		for acct := range lengthChangedAccounts {
			if appendVecWriter == nil {
				fileId = accountsDb.LargestFileId.Add(1)
				appendVecFileName := fmt.Sprintf("%s/%d.%d", accountsDb.AcctsDir, slot, fileId)
				var err error
				appendVecFile, err = os.OpenFile(appendVecFileName, os.O_RDWR|os.O_CREATE, 0666)
				if err != nil {
					return err
				}
				appendVecWriter = bufio.NewWriter(appendVecFile)
			}

			indexEntry := AccountIndexEntry{Slot: slot, FileId: fileId, Offset: appendVecFileOffset}
			if indexBatch == nil {
				indexBatch = accountsDb.Index.NewBatch()
			}
			err := queueRelocatedAccountIndexUpdate(indexBatch, acct, indexEntry, &acctIdxEntryBuf)
			if err != nil {
				return fmt.Errorf("unable to add acct for %s to acctsdb: %v", acct.Key, err)
			}

			appendVecAcct := appendVecAccountFromAccount(acct)
			l, err := appendVecAcct.MarshalReturningLength(appendVecWriter)
			if err != nil {
				return fmt.Errorf("unable to add acct for %s to acctsdb: %v", acct.Key, err)
			}
			acct.SetStorageInfo(slot, fileId, appendVecFileOffset, uint64(len(acct.Data)))
			appendVecFileOffset += uint64(l)
		}

		err := flushAndCloseAppendVecFile(appendVecFile, appendVecWriter)
		appendVecFile = nil
		appendVecWriter = nil
		if err != nil {
			return err
		}
		return commitRelocatedAccountIndexUpdates(indexBatch)
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

func (accountsDb *AccountsDb) appendVecFileName(slot uint64, fileId uint64) string {
	return fmt.Sprintf("%s/%d.%d", accountsDb.AcctsDir, slot, fileId)
}

func (accountsDb *AccountsDb) AllKeys() [][]byte {
	return nil
}

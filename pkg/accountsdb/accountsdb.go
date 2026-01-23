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

// silentLogger implements pebble.Logger but discards all messages.
// This suppresses verbose WAL recovery messages on startup.
type silentLogger struct{}

func (silentLogger) Infof(format string, args ...any)  {}
func (silentLogger) Fatalf(format string, args ...any) { log.Fatalf(format, args...) }

var (
	ErrNoAccount = errors.New("ErrNoAccount")

	StoreAccountsWorkers = 128
	StoreAsync           = false
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

	accountsDb := &AccountsDb{Index: db, BankHashStore: bankhashDb, AcctsDir: appendVecsDir}
	accountsDb.LargestFileId.Store(largestFileId)

	mlog.Log.Infof("StoreAsync=%t", StoreAsync)
	if StoreAsync {
		accountsDb.inProgressStoreRequests = list.New()
		accountsDb.storeRequestChan = make(chan *list.Element)
		accountsDb.storeWorkerDone = make(chan struct{})
		go accountsDb.storeWorker()
	}

	return accountsDb, nil
}

// Turns down the store worker. AccountsDb cannot accept writes after this if StoreAsync.
// Should not be called concurrently.
func (accountsDb *AccountsDb) WaitForStoreWorker() {
	if !StoreAsync {
		return
	}
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
	accountsDb.VoteAcctCache, err = otter.MustBuilder[solana.PublicKey, *accounts.Account](5000).
		Cost(func(key solana.PublicKey, acct *accounts.Account) uint32 {
			return 1
		}).
		Build()
	if err != nil {
		panic(err)
	}

	accountsDb.ProgramCache, err = otter.MustBuilder[solana.PublicKey, *ProgramCacheEntry](5000).
		Cost(func(key solana.PublicKey, progEntry *ProgramCacheEntry) uint32 {
			return 1
		}).
		Build()
	if err != nil {
		panic(err)
	}

	accountsDb.CommonAcctsCache, err = otter.MustBuilder[solana.PublicKey, *accounts.Account](10000).
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
	if StoreAsync {
		accts := accountsDb.getStoreInProgressAccounts([]solana.PublicKey{pubkey})
		if accts[0] != nil {
			return accts[0], nil
		}
	}
	return accountsDb.getStoredAccount(slot, pubkey)
}

func (accountsDb *AccountsDb) getStoredAccount(slot uint64, pubkey solana.PublicKey) (*accounts.Account, error) {
	cachedAcct, hasAcct := accountsDb.VoteAcctCache.Get(pubkey)
	if hasAcct {
		return cachedAcct, nil
	}

	cachedAcct, hasAcct = accountsDb.CommonAcctsCache.Get(pubkey)
	if hasAcct {
		return cachedAcct, nil
	}

	acctIdxEntryBytes, c, err := accountsDb.Index.Get(pubkey[:])
	if err != nil {
		//mlog.Log.Debugf("no account found in accountsdb for pubkey %s: %s", pubkey, err)
		return nil, ErrNoAccount
	}

	acctIdxEntry, err := unmarshalAcctIdxEntry(acctIdxEntryBytes)
	if err != nil {
		panic("failed to unmarshal AccountIndexEntry from index kv database")
	}
	c.Close()

	appendVecFileName := fmt.Sprintf("%s/%d.%d", accountsDb.AcctsDir, acctIdxEntry.Slot, acctIdxEntry.FileId)

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

	if solana.PublicKeyFromBytes(acct.Owner[:]) == addresses.VoteProgramAddr {
		accountsDb.VoteAcctCache.Set(pubkey, acct)
	} else {
		accountsDb.CommonAcctsCache.Set(pubkey, acct)
	}

	return acct, err
}

// Returns a slice of the same length as the input with results matching indexes, nil if not found.
// Returns clones to avoid data races with the store worker.
func (accountsDb *AccountsDb) getStoreInProgressAccounts(pks []solana.PublicKey) []*accounts.Account {
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

	if StoreAsync {
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
	} else {
		accountsDb.storeAccountsSync(accts, slot)
		if cb != nil {
			cb()
		}
		return nil
	}
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
		// if vote account, do not serialize up and write into accountsdb - just save it in cache.
		if solana.PublicKeyFromBytes(acct.Owner[:]) == addresses.VoteProgramAddr {
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
		accountsDb.inProgressStoreRequestsMu.Lock()
		accountsDb.inProgressStoreRequests.Remove(elt)
		accountsDb.inProgressStoreRequestsMu.Unlock()
		if sr.cb != nil {
			sr.cb()
		}
	}
}

func (accountsDb *AccountsDb) storeAccountsInternal(accts []*accounts.Account, slot uint64) {
	fileId := accountsDb.LargestFileId.Add(1)
	appendVecFileName := fmt.Sprintf("%s/%d.%d", accountsDb.AcctsDir, slot, fileId)
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
			acctIdxEntry, err := unmarshalAcctIdxEntry(existingacctIdxEntryBuf)
			if err != nil {
				panic("failed to unmarshal AccountIndexEntry from index kv database")
			}
			c.Close()

			existingAppendVecFileName := fmt.Sprintf("%s/%d.%d", accountsDb.AcctsDir, acctIdxEntry.Slot, acctIdxEntry.FileId)
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
					existingIdxEntry, err := unmarshalAcctIdxEntry(existingacctIdxEntryBuf)
					c.Close()
					if err != nil {
						return fmt.Errorf("unmarshaling index entry: %w", err)
					}

					existingAppendVecFileName := fmt.Sprintf("%s/%d.%d", accountsDb.AcctsDir, existingIdxEntry.Slot, existingIdxEntry.FileId)
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
		fileId := accountsDb.LargestFileId.Add(1)
		appendVecFileName := fmt.Sprintf("%s/%d.%d", accountsDb.AcctsDir, slot, fileId)
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

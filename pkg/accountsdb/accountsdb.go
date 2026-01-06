package accountsdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/cockroachdb/pebble"
	"github.com/gagliardetto/solana-go"
	"github.com/maypok86/otter"
)

type AccountsDb struct {
	Index            *pebble.DB
	BankHashStore    *pebble.DB
	AcctsDir         string
	LargestFileId    atomic.Uint64
	VoteAcctCache    otter.Cache[solana.PublicKey, *accounts.Account]
	CommonAcctsCache otter.Cache[solana.PublicKey, *accounts.Account]
	ProgramCache     otter.Cache[solana.PublicKey, *ProgramCacheEntry]
}

var (
	ErrNoAccount = errors.New("ErrNoAccount")
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
	mlog.Log.Infof("accountsdb.OpenDb: largestFileId=%d", largestFileId)

	indexDir := filepath.Join(accountsDbDir, "mithril_db")
	db, err := pebble.Open(indexDir, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("opening indexDir=%s: %w", indexDir, err)
	}

	bankhashDir := filepath.Join(accountsDbDir, "bankhash_db")
	bankhashDb, err := pebble.Open(bankhashDir, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("opening bankhashDir=%s: %w", bankhashDir, err)
	}

	accountsDb := &AccountsDb{Index: db, BankHashStore: bankhashDb, AcctsDir: appendVecsDir}
	accountsDb.LargestFileId.Store(largestFileId)

	return accountsDb, nil
}

func (accountsDb *AccountsDb) CloseDb() {
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

func (accountsDb *AccountsDb) StoreAccounts(accts []*accounts.Account, slot uint64) error {
	for _, acct := range accts {
		if acct == nil {
			continue
		}
		acct.Slot = slot
	}

	accountsDb.storeAccountsInternal(accts, slot)

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

	return nil
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

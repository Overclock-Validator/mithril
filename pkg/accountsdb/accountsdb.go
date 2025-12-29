package accountsdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync/atomic"

	"github.com/Overclock-Validator/fastcache"
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/gagliardetto/solana-go"
	"github.com/maypok86/otter/v2"
)

type AccountsDb struct {
	Index            fastcache.Cache
	BankHashStore    fastcache.Cache
	AcctsDir         string
	LargestFileId    atomic.Uint64
	BankHashBytes    [32]byte
	VoteAcctCache    *otter.Cache[solana.PublicKey, *accounts.Account]
	CommonAcctsCache *otter.Cache[solana.PublicKey, *accounts.Account]
	ProgramCache     *otter.Cache[solana.PublicKey, *ProgramCacheEntry]
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

	bankHashFn := fmt.Sprintf("%s/bank_hash", accountsDbDir)
	bhf, err := os.Open(bankHashFn)
	if err != nil {
		mlog.Log.Infof("failed to open %s\n", bankHashFn)
		return nil, err
	}

	bankHashBytes := make([]byte, 32)
	bytesRead, err = bhf.Read(bankHashBytes)
	if err != nil {
		mlog.Log.Infof("error reading %s: %s\n", bankHashFn, err)
		return nil, err
	} else if bytesRead != 32 {
		mlog.Log.Infof("error reading %s: expected 8 bytes, got %d\n", bankHashFn, bytesRead)
		return nil, fmt.Errorf("only got %d bytes", bytesRead)
	}
	mlog.Log.Infof("accountsdb.OpenDb: bankHashBytes=%x", bankHashBytes)

	// attempt to open the index kv store
	dbFn := fmt.Sprintf("%s/mithril_db", accountsDbDir)
	indexDb, err := fastcache.NewCache(fastcache.GB*256, &fastcache.Config{
		Shards: 256,
		//MaxElementLen: 2000000000,
		MemoryType: fastcache.MMAP,
		MemoryKey:  dbFn,
	})
	if err != nil {
		panic(err)
	}

	// attempt to open the index kv store
	bankHashDbFn := fmt.Sprintf("%s/bankhash_db", accountsDbDir)
	bankhashDb, err := fastcache.NewCache(fastcache.MB*128, &fastcache.Config{
		Shards: 256,
		//MaxElementLen: 2000000000,
		MemoryType: fastcache.MMAP,
		MemoryKey:  bankHashDbFn,
	})
	if err != nil {
		panic(err)
	}

	accountsDb := &AccountsDb{Index: indexDb, BankHashStore: bankhashDb, AcctsDir: appendVecsDir}
	accountsDb.LargestFileId.Store(largestFileId)
	copy(accountsDb.BankHashBytes[:], bankHashBytes)

	return accountsDb, nil
}

func (accountsDb *AccountsDb) CloseDb() {
	accountsDb.Index.Close()
}

// CacheConfig holds configuration for all in-memory caches
type CacheConfig struct {
	VoteCacheSize    int // Number of vote accounts to cache (default: 2000)
	ProgramCacheSize int // Number of compiled programs to cache (default: 5000)
	CommonCacheSize  int // Number of common accounts to cache (default: 10000)
}

// DefaultCacheConfig returns sensible defaults for cache sizes
func DefaultCacheConfig() CacheConfig {
	return CacheConfig{
		VoteCacheSize:    2000,
		ProgramCacheSize: 5000,
		CommonCacheSize:  10000,
	}
}

func (accountsDb *AccountsDb) InitCaches() {
	accountsDb.InitCachesWithConfig(DefaultCacheConfig())
}

func (accountsDb *AccountsDb) InitCachesWithConfig(cfg CacheConfig) {
	// Vote account cache - count-based eviction using otter v2 MaximumSize
	accountsDb.VoteAcctCache = otter.Must(&otter.Options[solana.PublicKey, *accounts.Account]{
		MaximumSize: cfg.VoteCacheSize,
	})

	// Program cache - count-based eviction for compiled SBPF programs
	accountsDb.ProgramCache = otter.Must(&otter.Options[solana.PublicKey, *ProgramCacheEntry]{
		MaximumSize: cfg.ProgramCacheSize,
	})

	// Common accounts cache - count-based eviction
	accountsDb.CommonAcctsCache = otter.Must(&otter.Options[solana.PublicKey, *accounts.Account]{
		MaximumSize: cfg.CommonCacheSize,
	})
}

type ProgramCacheEntry struct {
	Program        *sbpf.Program
	DeploymentSlot uint64
}

func (accountsDb *AccountsDb) MaybeGetProgramFromCache(pubkey solana.PublicKey) (*ProgramCacheEntry, bool) {
	return accountsDb.ProgramCache.GetIfPresent(pubkey)
}

func (accountsDb *AccountsDb) AddProgramToCache(pubkey solana.PublicKey, programEntry *ProgramCacheEntry) {
	accountsDb.ProgramCache.Set(pubkey, programEntry)
}

func (accountsDb *AccountsDb) RemoveProgramFromCache(pubkey solana.PublicKey) {
	accountsDb.ProgramCache.Invalidate(pubkey)
}

func (accountsDb *AccountsDb) GetAccount(slot uint64, pubkey solana.PublicKey) (*accounts.Account, error) {
	cachedAcct, hasAcct := accountsDb.VoteAcctCache.GetIfPresent(pubkey)
	if hasAcct {
		return cachedAcct, nil
	}

	cachedAcct, hasAcct = accountsDb.CommonAcctsCache.GetIfPresent(pubkey)
	if hasAcct {
		return cachedAcct, nil
	}

	acctIdxEntryBytes, err := accountsDb.Index.Get(pubkey[:])
	if err != nil {
		//mlog.Log.Debugf("no account found in accountsdb for pubkey %s: %s", pubkey, err)
		return nil, ErrNoAccount
	}

	acctIdxEntry, err := unmarshalAcctIdxEntry(acctIdxEntryBytes)
	if err != nil {
		panic("failed to unmarshal AccountIndexEntry from index kv database")
	}

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
		existingacctIdxEntryBuf, err := accountsDb.Index.Get(acct.Key[:])
		if err == nil {
			acctIdxEntry, err := unmarshalAcctIdxEntry(existingacctIdxEntryBuf)
			if err != nil {
				panic("failed to unmarshal AccountIndexEntry from index kv database")
			}

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

		err = accountsDb.Index.Set(acct.Key[:], acctIdxEntryBuf[:])
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
	return accountsDb.BankHashStore.Get(slotBytes[:])
}

func (accountsDb *AccountsDb) StoreBankHashForSlot(slot uint64, bankHash []byte) error {
	var slotBytes [8]byte
	binary.LittleEndian.PutUint64(slotBytes[:], slot)
	return accountsDb.BankHashStore.Set(slotBytes[:], bankHash)
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

func (accountsDb *AccountsDb) BankHash() [32]byte {
	return accountsDb.BankHashBytes
}

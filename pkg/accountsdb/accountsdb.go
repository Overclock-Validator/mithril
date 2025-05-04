package accountsdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync/atomic"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/Overclock-Validator/mithril/pkg/util"
	"github.com/Overclock-Validator/sniper"
	"github.com/Overclock-Validator/sniper/options"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/maypok86/otter"
)

type AccountsDb struct {
	IndexDb         *sniper.Store
	AcctsDir        string
	IndexDir        string
	LargestFileId   atomic.Uint64
	BankHashBytes   [32]byte
	VoteAcctCache   otter.Cache[solana.PublicKey, *accounts.Account]
	CommonAcctCache otter.Cache[solana.PublicKey, *accounts.Account]
	ProgramCache    otter.Cache[solana.PublicKey, *sbpf.Program]
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

	// attempt to open bank_hash file
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
		mlog.Log.Infof("error reading %s: expected 32 bytes, got %d\n", bankHashFn, bytesRead)
		return nil, fmt.Errorf("only got %d bytes", bytesRead)
	}

	// configure Sniper options to reduce heap usage
	indexDir := fmt.Sprintf("%s/index", accountsDbDir)
	opts := options.DefaultOptions
	opts.ReadOnlyMMap = false                // avoid loading all SSTs into Go heap
	opts.TableLoadingMode = options.LoadToDisk // read-on-demand from disk (or use LoadToRAM)
	opts.TableMaxOpenFiles = 4               // limit number of open SST files

	// open the index kv store with tuned options
	db, err := sniper.Open(
		sniper.Dir(indexDir),
		sniper.ChunksCollision(32),
		opts,
	)
	if err != nil {
		mlog.Log.Infof("failed to open sniper store: %s\n", err)
		return nil, err
	}

	accountsDb := &AccountsDb{IndexDb: db, AcctsDir: appendVecsDir, IndexDir: indexDir}
	accountsDb.LargestFileId.Store(largestFileId)
	copy(accountsDb.BankHashBytes[:], bankHashBytes)

	return accountsDb, nil
}

func (accountsDb *AccountsDb) CloseDb() {
	accountsDb.IndexDb.Close()
}

func (accountsDb *AccountsDb) InitCaches() {
	var err error
	accountsDb.VoteAcctCache, err = otter.MustBuilder[solana.PublicKey, *accounts.Account](10_000).
		Cost(func(key solana.PublicKey, acct *accounts.Account) uint32 { return 1 }).
		Build()
	if err != nil {
		panic(err)
	}

	accountsDb.ProgramCache, err = otter.MustBuilder[solana.PublicKey, *sbpf.Program](10_000).
		Cost(func(key solana.PublicKey, prog *sbpf.Program) uint32 { return 1 }).
		Build()
	if err != nil {
		panic(err)
	}

	accountsDb.CommonAcctCache, err = otter.MustBuilder[solana.PublicKey, *accounts.Account](250_000).
		Cost(func(key solana.PublicKey, acct *accounts.Account) uint32 { return 1 }).
		Build()
	if err != nil {
		panic(err)
	}
}

func (accountsDb *AccountsDb) MaybeGetProgramFromCache(pubkey solana.PublicKey) (*sbpf.Program, bool) {
	return accountsDb.ProgramCache.Get(pubkey)
}

func (accountsDb *AccountsDb) AddProgramToCache(pubkey solana.PublicKey, program *sbpf.Program) {
	accountsDb.ProgramCache.Set(pubkey, program)
}

func (accountsDb *AccountsDb) GetAccount(slot uint64, pubkey solana.PublicKey) (*accounts.Account, error) {
	if acct, ok := accountsDb.VoteAcctCache.Get(pubkey); ok {
		return acct, nil
	}
	if acct, ok := accountsDb.CommonAcctCache.Get(pubkey); ok {
		return acct, nil
	}

	entryBytes, err := accountsDb.IndexDb.Get(pubkey[:])
	if err != nil {
		mlog.Log.Debugf("no account in index for %s: %v", pubkey, err)
		return nil, ErrNoAccount
	}
	idxEntry, err := unmarshalAcctIdxEntry(entryBytes)
	if err != nil {
		panic("invalid account index entry")
	}

	fileName := fmt.Sprintf("%s/%d.%d", accountsDb.AcctsDir, idxEntry.Slot, idxEntry.FileId)
	f, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if _, err := f.Seek(int64(idxEntry.Offset), 0); err != nil {
		panic(err)
	}
	acct, err := unmarshalAcctFromAppendVecAcctHeader(f)
	if err != nil {
		panic(err)
	}
	acct.Slot = idxEntry.Slot

	if acct.Key != pubkey {
		panic("pubkey mismatch after unmarshal")
	}
	return acct, nil
}

var voteAcct = solana.MustPublicKeyFromBase58("Vote111111111111111111111111111111111111111")

func (accountsDb *AccountsDb) StoreAccounts(accts []*accounts.Account, slot uint64) error {
	fileId := accountsDb.LargestFileId.Add(1)
	buf := new(bytes.Buffer)
	for _, acct := range accts {
		acct.Slot = slot
		if solana.PublicKeyFromBytes(acct.Owner[:]) == voteAcct {
			accountsDb.VoteAcctCache.Set(acct.Key, acct)
			continue
		}
		accountsDb.CommonAcctCache.Set(acct.Key, acct)

		// index entry
		buf.Reset()
		enc := bin.NewBinEncoder(buf)
		entry := AccountIndexEntry{Slot: slot, FileId: fileId, Offset: uint64(buf.Len())}
		entry.MarshalWithEncoder(enc)
		accountsDb.IndexDb.SetIfSlotHigher(acct.Key[:], buf.Bytes(), 0)

		// appendvec
		raw := AppendVecAccount{
			DataLen:   uint64(len(acct.Data)),
			Pubkey:    acct.Key,
			Lamports:  acct.Lamports,
			RentEpoch: acct.RentEpoch,
			Owner:     acct.Owner,
			Executable: acct.Executable,
			Data:      acct.Data,
		}
		raw.Marshal(buf)
	}

	outFile := fmt.Sprintf("%s/%d.%d", accountsDb.AcctsDir, slot, fileId)
	f, err := os.OpenFile(outFile, os.O_CREATE|os.O_WRONLY, 0o666)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(buf.Bytes()); err != nil {
		return err
	}
	return nil
}

func (accountsDb *AccountsDb) KeysBetweenPrefixes(startPrefix uint64, endPrefix uint64) []solana.PublicKey {
	keys := accountsDb.IndexDb.KeysBetweenPrefixes(startPrefix, endPrefix)
	out := make([]solana.PublicKey, len(keys))
	for i, k := range keys {
		out[i] = solana.PublicKeyFromBytes(k)
	}
	return out
}

func (accountsDb *AccountsDb) AllKeys() [][]byte {
	keys := accountsDb.IndexDb.AllKeys()
	sort.Slice(keys, func(i, j int) bool {
		return util.PubkeyCmpByteSlice(keys[i], keys[j])
	})
	return keys
}

func (accountsDb *AccountsDb) BankHash() [32]byte {
	return accountsDb.BankHashBytes
}

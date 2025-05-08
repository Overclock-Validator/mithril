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
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/maypok86/otter"
	"runtime"
	"runtime/debug"
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

var ErrNoAccount = errors.New("ErrNoAccount")

// -----------------------------------------------------------------------------
// Open existing AccountsDB
// -----------------------------------------------------------------------------
func OpenDb(dir string) (*AccountsDb, error) {
	// ensure append‑vec folder exists
	avecs := fmt.Sprintf("%s/accounts", dir)
	if _, err := os.Stat(avecs); err != nil {
		return nil, err
	}

	// read largest_file_id
	lfiPath := fmt.Sprintf("%s/largest_file_id", dir)
	lfi, err := os.ReadFile(lfiPath)
	if err != nil || len(lfi) != 8 {
		return nil, fmt.Errorf("cannot read %s: %v", lfiPath, err)
	}
	largest := binary.LittleEndian.Uint64(lfi)

	// read bank_hash
	bhPath := fmt.Sprintf("%s/bank_hash", dir)
	bh, err := os.ReadFile(bhPath)
	if err != nil || len(bh) != 32 {
		return nil, fmt.Errorf("cannot read %s: %v", bhPath, err)
	}

	// open sniper index – default options
	indexDir := fmt.Sprintf("%s/index", dir)
	db, err := sniper.Open(
		sniper.Dir(indexDir),
		sniper.ChunksCollision(32),
	)
	if err != nil {
		return nil, fmt.Errorf("sniper.Open: %w", err)
	}

	// ---- GC away Sniper chunk structs --------------------------------------
	runtime.GC()         // sweep now‑unreferenced chunks
	debug.FreeOSMemory() // madvise; RSS drops immediately
	// ------------------------------------------------------------------------

	adb := &AccountsDb{
		IndexDb:  db,
		AcctsDir: avecs,
		IndexDir: indexDir,
	}
	adb.LargestFileId.Store(largest)
	copy(adb.BankHashBytes[:], bh)
	return adb, nil
}

func (a *AccountsDb) CloseDb() { a.IndexDb.Close() }

// -----------------------------------------------------------------------------
// Caches
// -----------------------------------------------------------------------------
func (a *AccountsDb) InitCaches() {
	buildAcct := func(cap int) otter.Cache[solana.PublicKey, *accounts.Account] {
		c, err := otter.MustBuilder[solana.PublicKey, *accounts.Account](cap).
			Cost(func(_ solana.PublicKey, _ *accounts.Account) uint32 { return 1 }).Build()
		if err != nil { panic(err) }
		return c
	}
	a.VoteAcctCache   = buildAcct(4_000)
	a.CommonAcctCache = buildAcct(250_000)

	p, err := otter.MustBuilder[solana.PublicKey, *sbpf.Program](10_000).
		Cost(func(_ solana.PublicKey, _ *sbpf.Program) uint32 { return 1 }).Build()
	if err != nil { panic(err) }
	a.ProgramCache = p
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

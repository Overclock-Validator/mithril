package bankview

import (
	"context"
	"errors"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/gagliardetto/solana-go"
)

// StoreAccounts reads a replay working set layered over AccountsDb.
type StoreAccounts struct {
	WorkingSet *accounts.WorkingSet
	DB         *accountsdb.AccountsDb
}

var _ Accounts = StoreAccounts{}

func (s StoreAccounts) Generation() uint64 { return s.WorkingSet.Generation() }

func (s StoreAccounts) LookupAt(pubkey [32]byte, maxSlot uint64) (*accounts.Account, bool) {
	return s.WorkingSet.LookupAt(pubkey, maxSlot)
}

func (s StoreAccounts) ReadPublication() (uint64, uint64, bool) { return durablePublication(s.DB) }

// durablePublication combines the store's read-cache epoch, which every fold
// publication advances, with its committed account version, which rewinds
// and recoveries advance and which is unstable while one is in progress.
func durablePublication(db *accountsdb.AccountsDb) (uint64, uint64, bool) {
	epoch, through := db.ReadPublication()
	version, stable := db.CommittedAccountVersion()
	return epoch ^ version<<32, through, stable
}

func (s StoreAccounts) GetDurable(pubkey solana.PublicKey) (*accounts.Account, error) {
	a, err := s.DB.GetAccount(0, pubkey)
	if errors.Is(err, accountsdb.ErrNoAccount) {
		return nil, nil
	}
	return a, err
}

func (s StoreAccounts) GetDurableBatch(ctx context.Context, pubkeys []solana.PublicKey, maxDataBytes uint64) ([]*accounts.Account, error) {
	if maxDataBytes == NoDataLimit {
		return s.DB.GetAccountsBatchShared(ctx, 0, pubkeys)
	}
	out, _, err := s.DB.GetAccountsBatchSharedWithDataLimit(ctx, 0, pubkeys, maxDataBytes)
	return out, err
}

// ScanAccounts visits every unrooted version of accounts owned by
// cfg.Programs, then every record in the account files (see
// accountsdb.AccountsDb.ScanRecords). The working set goes first: a version
// folded while the files are read is then still seen, because the files
// listed after it include its fold segment.
func (s StoreAccounts) ScanAccounts(ctx context.Context, cfg accountsdb.ScanRecordsConfig, fn func(pubkey, owner [32]byte, lamports uint64, data []byte)) error {
	programs := make(map[[32]byte]bool, len(cfg.Programs))
	for _, p := range cfg.Programs {
		programs[p] = true
	}
	s.WorkingSet.RangeVersions(func(_ uint64, a *accounts.Account) {
		if len(programs) == 0 || programs[a.Owner] {
			fn(a.Key, a.Owner, a.Lamports, a.Data)
		}
	})
	if !s.DB.KeySidecarsEnabled() {
		cfg.Keys = nil // read every file; the caller filters
	}
	_, err := s.DB.ScanRecords(ctx, cfg, fn)
	return err
}

// DurableAccounts reads only the durable store: the account state of a
// rooted bank when no replay working set is attached. With a nil DB every
// account is missing, which serves a bank's scalars alone.
type DurableAccounts struct{ DB *accountsdb.AccountsDb }

var _ Accounts = DurableAccounts{}

func (DurableAccounts) Generation() uint64 { return 0 }

func (DurableAccounts) LookupAt([32]byte, uint64) (*accounts.Account, bool) { return nil, false }

func (d DurableAccounts) ReadPublication() (uint64, uint64, bool) {
	if d.DB == nil {
		return 0, 0, true
	}
	return durablePublication(d.DB)
}

func (d DurableAccounts) GetDurable(pubkey solana.PublicKey) (*accounts.Account, error) {
	if d.DB == nil {
		return nil, nil
	}
	return StoreAccounts{DB: d.DB}.GetDurable(pubkey)
}

func (d DurableAccounts) GetDurableBatch(ctx context.Context, pubkeys []solana.PublicKey, maxDataBytes uint64) ([]*accounts.Account, error) {
	if d.DB == nil {
		return make([]*accounts.Account, len(pubkeys)), nil
	}
	return StoreAccounts{DB: d.DB}.GetDurableBatch(ctx, pubkeys, maxDataBytes)
}

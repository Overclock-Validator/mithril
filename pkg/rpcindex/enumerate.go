package rpcindex

import (
	"context"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/rpcdb"
	"github.com/Overclock-Validator/mithril/pkg/watch"
	"github.com/gagliardetto/solana-go"
)

// AccountScanner visits every stored version of accounts owned by the given
// programs: unrooted writes first, then the account files. Versions may be
// stale; the caller checks what it keeps against a bank. fn may run on
// several goroutines at once and must not retain data.
type AccountScanner interface {
	ScanAccounts(ctx context.Context, cfg accountsdb.ScanRecordsConfig, fn func(pubkey, owner [32]byte, lamports uint64, data []byte)) error
}

// Enumerator finds the token accounts a watched wallet owned before it was
// registered. Accounts it acquires later are discovered when replay writes
// them; accounts it already held and that stay idle are found only by
// scanning the account store.
//
// A wallet is armed in the watch set before its scan starts, so every write
// after that point is discovered. The scan then reads the unrooted writes
// and every account file, keeping every version that names the wallet as
// owner. The result is a superset: requests check each candidate against the
// bank they read. The wallet is ready once that superset is stored.
//
// Discovery depends on the indexer seeing every completed block. When the
// indexer drops a block, wallets that were ready are scanned again.
type Enumerator struct {
	ix     *Indexer
	source func() AccountScanner
	cfg    accountsdb.ScanRecordsConfig

	mu    sync.Mutex
	ready map[solana.PublicKey]uint64 // owner -> indexer drop count when it became ready
	want  map[solana.PublicKey]bool
	kick  chan struct{}
}

// TokenOwnerSidecar fingerprints token accounts by owner in the account
// store's key sidecars, so a wallet's accounts are found without reading
// every data file. Token-2022 mints padded to account length are included;
// callers check the account type.
var TokenOwnerSidecar = accountsdb.KeySidecarConfig{
	Programs:   [][32]byte{SPLTokenProgram, Token2022Program},
	KeyOffset:  32,
	MinDataLen: 165,
}

// EnumerationRetry is how long the enumerator waits before retrying a scan
// that could not run.
var EnumerationRetry = 10 * time.Second

// NewEnumerator restores the wallets enumerated before a restart. Markers are
// valid only if the history store recorded every block up to the account
// store's durable slot: replay resumes after that slot, so discoveries in a
// gap below it would never be made again.
func NewEnumerator(ix *Indexer, source func() AccountScanner, cfg accountsdb.ScanRecordsConfig, durableSlot uint64) (*Enumerator, error) {
	e := &Enumerator{
		ix:     ix,
		source: source,
		cfg:    cfg,
		ready:  map[solana.PublicKey]uint64{},
		want:   map[solana.PublicKey]bool{},
		kick:   make(chan struct{}, 1),
	}
	markers, err := ix.Store().Enumerated()
	if err != nil {
		return nil, err
	}
	watermark, err := ix.Store().Watermark()
	if err != nil {
		return nil, err
	}
	if watermark >= durableSlot {
		for owner := range markers {
			e.ready[owner] = 0
		}
	} else if len(markers) > 0 {
		mlog.Log.Infof("rpc history: store watermark %d is below the durable slot %d; rescanning %d wallets", watermark, durableSlot, len(markers))
		batch := ix.Store().NewBatch()
		for owner := range markers {
			if err := batch.DeleteEnumerated(owner); err != nil {
				return nil, err
			}
		}
		if err := batch.Commit(); err != nil {
			return nil, err
		}
	}
	return e, nil
}

// Want schedules owners that are not ready for a scan. Owners must already
// be watched as wallets.
func (e *Enumerator) Want(owners ...solana.PublicKey) {
	e.mu.Lock()
	for _, o := range owners {
		if _, ok := e.ready[o]; !ok {
			e.want[o] = true
		}
	}
	e.mu.Unlock()
	e.wake()
}

func (e *Enumerator) wake() {
	select {
	case e.kick <- struct{}{}:
	default:
	}
}

// Ready reports whether owner's token accounts are fully known. A wallet that
// was ready becomes not ready, and is scanned again, once the indexer drops a
// block.
func (e *Enumerator) Ready(owner solana.PublicKey) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	dropped, ok := e.ready[owner]
	if ok && e.ix.Dropped() != dropped {
		delete(e.ready, owner)
		e.want[owner] = true
		e.wake()
		return false
	}
	return ok
}

// Run scans for wanted owners until ctx ends. Owners requested while a scan
// runs wait for the next one, so a burst of registrations shares one scan.
func (e *Enumerator) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-e.kick:
		}
		e.mu.Lock()
		batch := make(map[solana.PublicKey]bool, len(e.want))
		for o := range e.want {
			batch[o] = true
		}
		e.mu.Unlock()
		if len(batch) == 0 {
			continue
		}
		if err := e.scan(ctx, batch); err != nil {
			if ctx.Err() != nil {
				return
			}
			mlog.Log.Warnf("rpc history: token account scan for %d wallets failed: %v", len(batch), err)
			time.AfterFunc(EnumerationRetry, e.wake)
		}
	}
}

func (e *Enumerator) scan(ctx context.Context, owners map[solana.PublicKey]bool) error {
	src := e.source()
	if src == nil {
		return errNoAccountStore
	}
	// Read before scanning: a block dropped during the scan may hold a
	// discovery the scan cannot see, so it must leave the owner not ready.
	dropped := e.ix.Dropped()
	watermark, err := e.ix.Store().Watermark()
	if err != nil {
		return err
	}
	started := time.Now()
	var mu sync.Mutex
	// Every version naming a wanted owner is kept: an account can have
	// versions owned by different watched wallets, and each owner needs the
	// candidate for the bank where it held the account.
	found := map[rpcdb.TokenAccount]struct{}{}
	cfg := e.cfg
	cfg.Programs = [][32]byte{SPLTokenProgram, Token2022Program}
	cfg.Keys = make([][32]byte, 0, len(owners))
	for o := range owners {
		cfg.Keys = append(cfg.Keys, o)
	}
	err = src.ScanAccounts(ctx, cfg, func(pubkey, program [32]byte, lamports uint64, data []byte) {
		mint, owner, _, ok := TokenAccountFields(program, lamports, data)
		if !ok || !owners[owner] {
			return
		}
		ta := rpcdb.TokenAccount{Owner: owner, Mint: mint, Account: pubkey}
		if solana.PublicKey(program) == Token2022Program {
			ta.Program = 1
		}
		mu.Lock()
		found[ta] = struct{}{}
		mu.Unlock()
	})
	if err != nil {
		return err
	}

	batch := e.ix.Store().NewBatch()
	keys := make([]watch.Key, 0, len(found))
	watched := make(map[solana.PublicKey]bool, len(found))
	for ta := range found {
		if err := batch.PutTokenAccountCandidate(ta); err != nil {
			return err
		}
		if watched[ta.Account] {
			continue
		}
		watched[ta.Account] = true
		// Persisted like discovered token accounts, so a restart that trusts
		// the ready marker below still captures transactions naming only
		// this account.
		if err := batch.PutWatch(0, ta.Account, uint8(watch.TokenAccount), watermark); err != nil {
			return err
		}
		keys = append(keys, watch.Key{Key: ta.Account, Kind: watch.TokenAccount})
	}
	for owner := range owners {
		if err := batch.PutEnumerated(owner, watermark); err != nil {
			return err
		}
	}
	if err := batch.Commit(); err != nil {
		return err
	}
	// Transactions that name only one of these accounts are captured from
	// here on.
	e.ix.Registry().AddKeys(keys...)

	e.mu.Lock()
	for owner := range owners {
		delete(e.want, owner)
		e.ready[owner] = dropped
	}
	e.mu.Unlock()
	mlog.Log.Infof("rpc history: enumerated %d token accounts for %d wallets in %s", len(found), len(owners), time.Since(started).Round(time.Millisecond))
	return nil
}

type enumerationError string

func (e enumerationError) Error() string { return string(e) }

const errNoAccountStore = enumerationError("account store is not open yet")

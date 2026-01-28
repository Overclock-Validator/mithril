package accountsdb

import (
	"context"
	"math"
	"runtime"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
)

var systemProgramAddr [32]byte

func (db *AccountsDb) GetAccountsBatch(ctx context.Context, pks []solana.PublicKey) ([]*accounts.Account, error) {
	n := len(pks)
	if n == 0 {
		return nil, nil
	}
	var out []*accounts.Account
	if StoreAsync {
		out = db.getStoreInProgressAccounts(pks)
	} else {
		out = make([]*accounts.Account, n)
	}

	// Use a bounded semaphore so we don’t spawn unbounded I/O.
	maxWorkers := runtime.NumCPU() * 2
	sem := make(chan struct{}, maxWorkers)

	// Fan‑out jobs.
	var wg sync.WaitGroup
	errCh := make(chan error, 1)

	for i, pk := range pks {
		if out[i] != nil {
			continue
		}
		wg.Add(1)
		go func(idx int, key solana.PublicKey) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			// Early exit if the context has been cancelled.
			select {
			case <-ctx.Done():
				return
			default:
			}

			acct, err := db.getStoredAccount(key)
			if err != nil && err != ErrNoAccount {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			if err == ErrNoAccount || (acct != nil && acct.Lamports == 0) {
				acct = &accounts.Account{Key: pk, Owner: systemProgramAddr, RentEpoch: math.MaxUint64}
			}
			out[idx] = acct
		}(i, pk)
	}

	// Wait and propagate first non‑nil error.
	go func() {
		wg.Wait()
		close(errCh)
	}()

	if err, ok := <-errCh; ok && err != nil {
		return nil, err
	}
	return out, nil
}

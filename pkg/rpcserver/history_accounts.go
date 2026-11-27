package rpcserver

import (
	"context"
	"errors"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/bankview"
	"github.com/Overclock-Validator/mithril/pkg/history"
	"github.com/gagliardetto/solana-go"
)

// accountsAtSlot reads accounts as they were at the end of a past slot, from
// the account history (a Mithril extension: the "slot" field of an account
// method's config). Accounts history records no change for after the slot are
// read from the durable store, which must not be ahead of what history checked.
func (rpcServer *RpcServer) accountsAtSlot(ctx context.Context, keys []solana.PublicKey, slot uint64) ([]*accounts.Account, error) {
	if rpcServer.acctsDb == nil {
		return nil, &InvalidParamsError{Message: "account history is not enabled on this node"}
	}
	hs := rpcServer.acctsDb.History()
	if hs == nil {
		return nil, &InvalidParamsError{Message: "account history is not enabled on this node (rpc.account_history)"}
	}
	for attempt := 0; attempt < 3; attempt++ {
		out := make([]*accounts.Account, len(keys))
		var unchanged []int
		var through uint64
		for i, k := range keys {
			v, th, err := hs.AccountAt([32]byte(k), slot)
			through = th
			switch {
			case err == nil:
				out[i] = historyAccount(k, v)
			case errors.Is(err, history.ErrUnchanged):
				unchanged = append(unchanged, i)
			case errors.Is(err, history.ErrBeforeHistory), errors.Is(err, history.ErrAfterHistory):
				first, last, _ := hs.Range()
				return nil, &InvalidParamsError{Message: fmt.Sprintf("slot %d is outside recorded account history (%d to %d)", slot, first, last)}
			default:
				return nil, err
			}
		}
		if len(unchanged) == 0 {
			return out, nil
		}
		pks := make([]solana.PublicKey, len(unchanged))
		for j, i := range unchanged {
			pks[j] = keys[i]
		}
		var stale bool
		err := rpcServer.readView(ctx, commitmentConfig{level: bankview.Durable}, func(r *bankview.Read) error {
			// The durable store must not hold folds history had not recorded
			// when it reported these accounts unchanged.
			if r.View.Slot > through {
				stale = true
				return nil
			}
			accts, err := r.Accounts(ctx, pks)
			if err != nil {
				return err
			}
			for j, i := range unchanged {
				out[i] = accts[j]
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		if !stale {
			return out, nil
		}
	}
	return nil, errors.New("account history moved during the read; retry")
}

func historyAccount(k solana.PublicKey, v *history.Version) *accounts.Account {
	if v == nil || v.Absent {
		return nil
	}
	return &accounts.Account{Key: k, Lamports: v.Lamports, Owner: v.Owner, Executable: v.Executable, RentEpoch: v.RentEpoch, Data: v.Data}
}

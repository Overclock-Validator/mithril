package rpcserver

import (
	"context"
	"errors"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/bankview"
	"github.com/Overclock-Validator/mithril/pkg/history"
	"github.com/gagliardetto/solana-go"
)

// accountsAtSlot reads accounts as they were at the end of a finalized slot
// (a Mithril extension: the "slot" field of an account method's config).
//
// Everything is read inside one validated finalized view, whose durable store
// reaches slot D:
//   - after D, the working set still holds every version, so the view reads
//     them as of the slot;
//   - at or before D, from the account history. History is recorded before
//     each fold commits, so it reaches at least D. An account history records
//     no change for after the slot through its last slot T is read from the
//     durable store: with slot <= D <= T that value is the one at the slot.
//
// maxDataBytes bounds the account data returned, duplicates included.
func (rpcServer *RpcServer) accountsAtSlot(ctx context.Context, keys []solana.PublicKey, slot uint64, maxDataBytes uint64) ([]*accounts.Account, error) {
	if rpcServer.acctsDb == nil {
		return nil, &InvalidParamsError{Message: "account history is not enabled on this node"}
	}
	var out []*accounts.Account
	err := rpcServer.readView(ctx, commitmentConfig{level: bankview.Finalized}, func(r *bankview.Read) error {
		out = nil
		if slot > r.View.Slot {
			return &InvalidParamsError{Message: fmt.Sprintf("slot %d is not finalized yet (finalized slot %d)", slot, r.View.Slot)}
		}
		durable := r.DurableSlot()
		if slot > durable {
			var err error
			out, err = r.AccountsAtSlot(ctx, keys, slot, maxDataBytes)
			return err
		}
		hs := rpcServer.acctsDb.History()
		if hs == nil {
			return &InvalidParamsError{Message: "account history is not enabled on this node (rpc.account_history)"}
		}
		keys32 := make([][32]byte, len(keys))
		for i, k := range keys {
			keys32[i] = [32]byte(k)
		}
		versions, errs, through, err := hs.AccountsAtLimit(keys32, slot, maxDataBytes)
		if errors.Is(err, history.ErrDataLimit) {
			return accountsdb.ErrAccountDataLimit
		}
		if err != nil {
			return err
		}
		if through < durable {
			return &InvalidParamsError{Message: fmt.Sprintf("account history reaches slot %d, behind the durable store at %d", through, durable)}
		}
		res := make([]*accounts.Account, len(keys))
		used := uint64(0)
		var unchanged []int
		for i, e := range errs {
			switch {
			case e == nil:
				res[i] = historyAccount(keys[i], versions[i])
				if res[i] != nil {
					used += uint64(len(res[i].Data))
				}
			case errors.Is(e, history.ErrUnchanged):
				unchanged = append(unchanged, i)
			case errors.Is(e, history.ErrBeforeHistory), errors.Is(e, history.ErrAfterHistory):
				first, last, _ := hs.Range()
				return &InvalidParamsError{Message: fmt.Sprintf("slot %d is outside recorded account history (%d to %d)", slot, first, last)}
			default:
				return e
			}
		}
		if len(unchanged) > 0 {
			pks := make([]solana.PublicKey, len(unchanged))
			for j, i := range unchanged {
				pks[j] = keys[i]
			}
			remaining := bankview.NoDataLimit
			if maxDataBytes != bankview.NoDataLimit {
				if used > maxDataBytes {
					return accountsdb.ErrAccountDataLimit
				}
				remaining = maxDataBytes - used
			}
			accts, err := r.AccountsAtSlot(ctx, pks, durable, remaining)
			if err != nil {
				return err
			}
			for j, i := range unchanged {
				res[i] = accts[j]
			}
		}
		out = res
		return nil
	})
	return out, err
}

func historyAccount(k solana.PublicKey, v *history.Version) *accounts.Account {
	if v == nil || v.Absent || v.Lamports == 0 {
		return nil
	}
	return &accounts.Account{Key: k, Lamports: v.Lamports, Owner: v.Owner, Executable: v.Executable, RentEpoch: v.RentEpoch, Data: v.Data}
}

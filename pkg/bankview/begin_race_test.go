package bankview

import (
	"context"
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"testing"
)

type generationHook struct {
	*testAccounts
	onGeneration func()
}

func (a *generationHook) Generation() uint64 {
	if f := a.onGeneration; f != nil {
		a.onGeneration = nil
		f()
	}
	return a.testAccounts.Generation()
}

// Schedule a complete publish/fold after begin loads its snapshot but before
// it samples the working-set generation and durable publication epoch.
func TestBeginCannotCombineOldViewWithNewFold(t *testing.T) {
	a := &generationHook{testAccounts: newTestAccounts()}
	pk := key(99)
	var p Publisher
	p.Reset(a)
	a.ws.Add(10, []*accounts.Account{acct(pk, 10)})
	p.PublishProcessed(view(10))
	p.MarkFinalized(10)
	a.onGeneration = func() {
		a.ws.Add(11, []*accounts.Account{acct(pk, 11)})
		p.PublishProcessed(view(11))
		p.MarkFinalized(11)
		a.durable[pk] = acct(pk, 11)
		a.epoch.Add(1)
		a.through.Store(11)
		a.ws.PromotePrefix(11)
	}
	var slot, balance uint64
	err := p.Do(context.Background(), Finalized, 0, func(r *Read) error {
		v, err := r.Account(pk)
		if err != nil {
			return err
		}
		slot, balance = r.View.Slot, v.Lamports
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if slot != balance {
		t.Fatalf("bank slot=%d but returned account from slot=%d", slot, balance)
	}
}

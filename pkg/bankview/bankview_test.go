package bankview

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
)

// testAccounts layers a real WorkingSet over an in-memory durable map with a
// controllable publication epoch.
type testAccounts struct {
	ws      *accounts.WorkingSet
	durable map[solana.PublicKey]*accounts.Account
	epoch   atomic.Uint64
	through atomic.Uint64
	// onDurable runs inside each durable read, to inject concurrent changes.
	onDurable func()
}

func newTestAccounts() *testAccounts {
	return &testAccounts{ws: accounts.NewWorkingSet(), durable: map[solana.PublicKey]*accounts.Account{}}
}

func (t *testAccounts) Generation() uint64 { return t.ws.Generation() }
func (t *testAccounts) LookupAt(pk [32]byte, maxSlot uint64) (*accounts.Account, bool) {
	return t.ws.LookupAt(pk, maxSlot)
}
func (t *testAccounts) ReadPublication() (uint64, uint64, bool) {
	return t.epoch.Load(), t.through.Load(), true
}
func (t *testAccounts) GetDurable(pk solana.PublicKey) (*accounts.Account, error) {
	if t.onDurable != nil {
		t.onDurable()
	}
	return t.durable[pk], nil
}
func (t *testAccounts) GetDurableBatch(_ context.Context, pks []solana.PublicKey, _ uint64) ([]*accounts.Account, error) {
	out := make([]*accounts.Account, len(pks))
	for i, pk := range pks {
		a, _ := t.GetDurable(pk)
		out[i] = a
	}
	return out, nil
}

func key(b byte) solana.PublicKey {
	var pk solana.PublicKey
	pk[0] = b
	return pk
}

func acct(pk solana.PublicKey, lamports uint64) *accounts.Account {
	return &accounts.Account{Key: pk, Lamports: lamports}
}

func view(slot uint64) *View {
	return &View{Slot: slot, BlockHeight: slot, BlockID: solana.Hash{byte(slot)}, HasBlockID: true}
}

func readLamports(t *testing.T, p *Publisher, level Level, pk solana.PublicKey) (uint64, uint64) {
	t.Helper()
	var lamports, slot uint64
	err := p.Do(context.Background(), level, 0, func(r *Read) error {
		a, err := r.Account(pk)
		if err != nil {
			return err
		}
		lamports, slot = 0, r.View.Slot
		if a != nil {
			lamports = a.Lamports
		}
		return nil
	})
	if err != nil {
		t.Fatalf("read %v: %v", level, err)
	}
	return lamports, slot
}

func TestParseCommitment(t *testing.T) {
	cases := map[string]Level{"": Finalized, "finalized": Finalized, "confirmed": Notarized, "notarized": Notarized, "processed": Processed, "durable": Durable}
	for in, want := range cases {
		got, err := ParseCommitment(in)
		if err != nil || got != want {
			t.Fatalf("ParseCommitment(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	if _, err := ParseCommitment("bogus"); err == nil {
		t.Fatal("expected an error for an unknown commitment")
	}
}

func TestNotReadyBeforePublication(t *testing.T) {
	var p Publisher
	if err := p.Do(context.Background(), Processed, 0, func(*Read) error { return nil }); !errors.Is(err, ErrNotReady) {
		t.Fatalf("got %v, want ErrNotReady", err)
	}
	p.Reset(newTestAccounts())
	if err := p.Do(context.Background(), Finalized, 0, func(*Read) error { return nil }); !errors.Is(err, ErrNotReady) {
		t.Fatalf("got %v, want ErrNotReady", err)
	}
}

func TestReadsAreBoundedByViewSlot(t *testing.T) {
	ta := newTestAccounts()
	pk := key(1)
	ta.durable[pk] = acct(pk, 5)
	var p Publisher
	p.Reset(ta)

	ta.ws.Add(10, []*accounts.Account{acct(pk, 10)})
	p.PublishProcessed(view(10))
	p.MarkFinalized(10)
	ta.ws.Add(11, []*accounts.Account{acct(pk, 11)})
	// Slot 11 is replayed but its view is not published yet: processed
	// readers must still see slot 10.
	if got, slot := readLamports(t, &p, Processed, pk); got != 10 || slot != 10 {
		t.Fatalf("processed read = %d at slot %d, want 10 at 10", got, slot)
	}
	p.PublishProcessed(view(11))
	if got, _ := readLamports(t, &p, Processed, pk); got != 11 {
		t.Fatalf("processed read = %d, want 11", got)
	}
	if got, slot := readLamports(t, &p, Finalized, pk); got != 10 || slot != 10 {
		t.Fatalf("finalized read = %d at slot %d, want 10 at 10", got, slot)
	}
}

func TestDeletedAccountReadsAsMissing(t *testing.T) {
	ta := newTestAccounts()
	pk := key(2)
	ta.durable[pk] = acct(pk, 7)
	var p Publisher
	p.Reset(ta)
	ta.ws.Add(5, []*accounts.Account{acct(pk, 0)})
	p.PublishProcessed(view(5))
	err := p.Do(context.Background(), Processed, 0, func(r *Read) error {
		a, err := r.Account(pk)
		if err != nil {
			return err
		}
		if a != nil {
			t.Fatalf("deleted account read as %+v", a)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestForkSwitchDuringReadRetries(t *testing.T) {
	ta := newTestAccounts()
	pk := key(3)
	var p Publisher
	p.Reset(ta)
	ta.ws.Add(20, []*accounts.Account{acct(pk, 20)})
	p.PublishProcessed(view(20))
	ta.ws.Add(21, []*accounts.Account{acct(pk, 21)})
	p.PublishProcessed(view(21))

	attempts := 0
	var got uint64
	err := p.Do(context.Background(), Processed, 0, func(r *Read) error {
		attempts++
		a, _ := r.Account(pk)
		got = a.Lamports
		if attempts == 1 {
			// A fork switch evicts slot 21 after the read; replay then
			// republishes the surviving parent.
			ta.ws.EvictFrom(21)
			p.Abandon(21)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 2 || got != 20 {
		t.Fatalf("attempts=%d lamports=%d, want a retry that reads slot 20's value", attempts, got)
	}
}

func TestStaleGenerationIsNotServed(t *testing.T) {
	ta := newTestAccounts()
	var p Publisher
	p.Reset(ta)
	ta.ws.Add(30, []*accounts.Account{acct(key(4), 1)})
	p.PublishProcessed(view(30))
	ta.ws.Add(31, []*accounts.Account{acct(key(4), 2)})
	p.PublishProcessed(view(31))
	// Evicted, but replay has not called Abandon yet: the published views
	// belong to the old generation and must not be served.
	ta.ws.EvictFrom(31)
	err := p.Do(context.Background(), Processed, 0, func(*Read) error { return nil })
	if !errors.Is(err, ErrInconsistent) {
		t.Fatalf("got %v, want ErrInconsistent", err)
	}
	p.Abandon(31)
	if got, slot := readLamports(t, &p, Processed, key(4)); got != 1 || slot != 30 {
		t.Fatalf("read %d at %d after Abandon, want 1 at 30", got, slot)
	}
}

func TestFoldDuringReadRetries(t *testing.T) {
	ta := newTestAccounts()
	pk := key(5)
	ta.durable[pk] = acct(pk, 1)
	var p Publisher
	p.Reset(ta)
	p.PublishProcessed(view(40))
	p.MarkFinalized(40)
	attempts := 0
	ta.onDurable = func() {
		attempts++
		if attempts == 1 {
			ta.epoch.Add(1) // a fold publishes while the first read is in flight
		}
	}
	if got, _ := readLamports(t, &p, Finalized, pk); got != 1 || attempts != 2 {
		t.Fatalf("lamports=%d attempts=%d, want 1 after one retry", got, attempts)
	}
}

func TestFoldPastViewBlocksOnlyThatView(t *testing.T) {
	ta := newTestAccounts()
	var p Publisher
	p.Reset(ta)
	p.PublishProcessed(view(50))
	p.MarkFinalized(50)
	p.PublishProcessed(view(53)) // slots 51 and 52 skipped
	// A fold through skipped slot 52 holds nothing newer than slot 50.
	ta.through.Store(52)
	if _, slot := readLamports(t, &p, Finalized, key(6)); slot != 50 {
		t.Fatalf("finalized slot %d, want 50", slot)
	}
	// A fold through 53 holds slot 53's writes: the slot-50 view is stale.
	ta.through.Store(53)
	if err := p.Do(context.Background(), Finalized, 0, func(*Read) error { return nil }); !errors.Is(err, ErrInconsistent) {
		t.Fatalf("got %v, want ErrInconsistent for a view behind the fold", err)
	}
	if _, slot := readLamports(t, &p, Processed, key(6)); slot != 53 {
		t.Fatalf("processed slot %d, want 53", slot)
	}
}

func TestNotarizedFallsBackToFinalized(t *testing.T) {
	ta := newTestAccounts()
	var p Publisher
	p.Reset(ta)
	p.PublishProcessed(view(60))
	p.MarkFinalized(60)
	p.PublishProcessed(view(61))
	v, served, err := p.Current(Notarized)
	if err != nil || v.Slot != 60 || served != Finalized {
		t.Fatalf("Current(Notarized) = %v %v %v, want slot 60 served as finalized", v, served, err)
	}
	p.MarkNotarized(61, solana.Hash{9}) // wrong block ID: ignored
	if v, _, _ := p.Current(Notarized); v.Slot != 60 {
		t.Fatalf("a mismatched block ID moved notarized to %d", v.Slot)
	}
	p.MarkNotarized(61, solana.Hash{61})
	v, served, _ = p.Current(Notarized)
	if v.Slot != 61 || served != Notarized {
		t.Fatalf("Current(Notarized) = %d %v, want 61 notarized", v.Slot, served)
	}
}

func TestAbandonClearsNotarizedOnDroppedFork(t *testing.T) {
	ta := newTestAccounts()
	var p Publisher
	p.Reset(ta)
	ta.ws.Add(70, nil)
	p.PublishProcessed(view(70))
	p.MarkFinalized(70)
	ta.ws.Add(71, []*accounts.Account{acct(key(7), 1)})
	p.PublishProcessed(view(71))
	p.MarkNotarized(71, solana.Hash{71})
	ta.ws.EvictFrom(71)
	p.Abandon(71)
	if v, _, _ := p.Current(Notarized); v.Slot != 70 {
		t.Fatalf("notarized slot %d after abandoning 71, want 70", v.Slot)
	}
	if v, _, _ := p.Current(Processed); v.Slot != 70 {
		t.Fatalf("processed slot %d after abandoning 71, want 70", v.Slot)
	}
}

func TestMinContextSlot(t *testing.T) {
	var p Publisher
	p.Reset(newTestAccounts())
	p.PublishProcessed(view(80))
	err := p.Do(context.Background(), Processed, 81, func(*Read) error { return nil })
	var mcs *MinContextSlotError
	if !errors.As(err, &mcs) || mcs.ContextSlot != 80 {
		t.Fatalf("got %v, want MinContextSlotError at 80", err)
	}
}

func TestBatchReadMixesLayers(t *testing.T) {
	ta := newTestAccounts()
	a, b, c := key(10), key(11), key(12)
	ta.durable[a] = acct(a, 1)
	ta.durable[b] = acct(b, 2)
	var p Publisher
	p.Reset(ta)
	ta.ws.Add(90, []*accounts.Account{acct(b, 20)})
	p.PublishProcessed(view(90))
	err := p.Do(context.Background(), Processed, 0, func(r *Read) error {
		out, err := r.Accounts(context.Background(), []solana.PublicKey{a, b, c})
		if err != nil {
			return err
		}
		if out[0].Lamports != 1 || out[1].Lamports != 20 || out[2] != nil {
			t.Fatalf("batch = %v %v %v", out[0], out[1], out[2])
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSlotStatuses(t *testing.T) {
	var p Publisher
	p.Reset(newTestAccounts())
	if _, _, _, err := p.SlotStatuses([]uint64{1}); !errors.Is(err, ErrNotReady) {
		t.Fatalf("got %v, want ErrNotReady", err)
	}
	p.PublishProcessed(view(100))
	p.MarkFinalized(100)
	p.PublishProcessed(view(102)) // 101 skipped on this chain
	p.PublishProcessed(view(103))

	processed, certified, got, err := p.SlotStatuses([]uint64{100, 101, 102, 103, 104, 50})
	if err != nil {
		t.Fatal(err)
	}
	if processed != 103 || certified {
		t.Fatalf("processed=%d certified=%v, want 103 without a certificate source", processed, certified)
	}
	want := []string{"finalized", "skipped", "processed", "processed", "unknown", "finalized"}
	for i, st := range got {
		if st.Status != want[i] {
			t.Fatalf("slot %d status %q, want %q", st.Slot, st.Status, want[i])
		}
	}
	if !got[5].FromHistory || got[0].FromHistory {
		t.Fatalf("only slot 50 predates the retained views: %+v", got)
	}

	p.MarkNotarized(103, solana.Hash{103})
	_, certified, got, _ = p.SlotStatuses([]uint64{102, 103})
	if !certified || got[0].Status != "notarized" || got[1].Status != "notarized" || !got[1].HasBlockID {
		t.Fatalf("after notarization: certified=%v statuses=%+v", certified, got)
	}
}

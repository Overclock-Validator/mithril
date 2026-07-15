package replay

import (
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

func TestSpeculativeStoreResolveWalksParentChain(t *testing.T) {
	store := newSpeculativeStore()
	store.SetFinalizedSlot(100)

	pkA := solana.PublicKey{1}
	pkB := solana.PublicKey{2}

	store.layers[102] = &SpeculativeLayer{
		Slot:       102,
		ParentSlot: 100,
		Deltas: map[solana.PublicKey]*accounts.Account{
			pkA: {Key: pkA, Lamports: 11},
		},
	}
	store.layers[104] = &SpeculativeLayer{
		Slot:       104,
		ParentSlot: 102,
		Deltas: map[solana.PublicKey]*accounts.Account{
			pkB: {Key: pkB, Lamports: 22},
		},
	}

	acctB, err := store.Resolve(104, pkB, nil)
	if err != nil {
		t.Fatalf("resolve pkB at 104: %v", err)
	}
	if acctB.Lamports != 22 {
		t.Fatalf("pkB lamports = %d, want 22", acctB.Lamports)
	}

	acctA, err := store.Resolve(104, pkA, nil)
	if err != nil {
		t.Fatalf("resolve pkA at 104 via parent chain: %v", err)
	}
	if acctA.Lamports != 11 {
		t.Fatalf("pkA lamports = %d, want 11", acctA.Lamports)
	}
}

func TestSpeculativeStoreResolveCurrentOrSkippedSlotUsesExecutedAncestor(t *testing.T) {
	store := newSpeculativeStore()
	store.SetFinalizedSlot(100)
	pk := solana.PublicKey{3}
	recordSpeculativeLayer(t, store, 104, 100, &accounts.Account{Key: pk, Lamports: 44})

	for _, requested := range []uint64{105, 108} {
		acct, err := store.Resolve(requested, pk, nil)
		if err != nil {
			t.Fatalf("resolve slot %d through executed ancestor: %v", requested, err)
		}
		if acct.Lamports != 44 {
			t.Fatalf("slot %d lamports = %d, want 44", requested, acct.Lamports)
		}
	}
}

func TestSpeculativeStoreRecordLayerCapturesBankHashSnapshot(t *testing.T) {
	store := newSpeculativeStore()
	store.SetFinalizedSlot(100)

	slotCtx := &sealevel.SlotCtx{
		Slot:       101,
		ParentSlot: 100,
		Accounts:   accounts.NewMemAccounts(),
		AcctMapsMu: new(sync.Mutex),
		ModifiedAccts: map[solana.PublicKey]bool{
			sealevel.SysvarClockAddr: true,
		},
	}
	clock := &accounts.Account{Key: sealevel.SysvarClockAddr, Lamports: 1, Data: []byte{9}}
	slotHashes := &accounts.Account{Key: sealevel.SysvarSlotHashesAddr, Lamports: 1, Data: []byte{7}}
	requireNoErr := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	requireNoErr(slotCtx.SetAccount(sealevel.SysvarClockAddr, clock))
	requireNoErr(slotCtx.SetAccount(sealevel.SysvarSlotHashesAddr, slotHashes))

	err := store.RecordLayer(101, 100, slotCtx, []*accounts.Account{slotHashes})
	if err != nil {
		t.Fatalf("RecordLayer: %v", err)
	}

	gotClock, err := store.Resolve(101, sealevel.SysvarClockAddr, nil)
	if err != nil {
		t.Fatalf("resolve clock: %v", err)
	}
	if gotClock.Data[0] != 9 {
		t.Fatalf("clock data = %v, want 9", gotClock.Data[0])
	}

	gotSlotHashes, err := store.Resolve(101, sealevel.SysvarSlotHashesAddr, nil)
	if err != nil {
		t.Fatalf("resolve slothashes: %v", err)
	}
	if gotSlotHashes.Data[0] != 7 {
		t.Fatalf("slothashes data = %v, want 7", gotSlotHashes.Data[0])
	}
}

func TestSpeculativeStorePruneLayersAbove(t *testing.T) {
	store := newSpeculativeStore()
	store.layers[101] = &SpeculativeLayer{Slot: 101, ParentSlot: 100}
	store.layers[102] = &SpeculativeLayer{Slot: 102, ParentSlot: 101}
	store.layers[103] = &SpeculativeLayer{Slot: 103, ParentSlot: 102}
	store.order = []uint64{101, 102, 103}

	store.PruneLayersAbove(101)
	if len(store.layers) != 1 || store.layers[101] == nil {
		t.Fatalf("expected only layer 101 to remain, got %d layers", len(store.layers))
	}
}

func TestSpeculativeStoreFlatLookupAndUndo(t *testing.T) {
	store := newSpeculativeStore()
	store.SetFinalizedSlot(100)
	pkA := solana.PublicKey{1}
	pkB := solana.PublicKey{2}
	recordSpeculativeLayer(t, store, 101, 100, &accounts.Account{Key: pkA, Lamports: 11})
	recordSpeculativeLayer(t, store, 102, 101,
		&accounts.Account{Key: pkA, Lamports: 22},
		&accounts.Account{Key: pkB, Lamports: 33},
	)

	acct, err := store.Resolve(102, pkA, nil)
	if err != nil || acct.Lamports != 22 {
		t.Fatalf("flat resolve = %+v, %v", acct, err)
	}
	store.PruneLayersAbove(101)
	acct, err = store.Resolve(101, pkA, nil)
	if err != nil || acct.Lamports != 11 {
		t.Fatalf("resolve after unwind = %+v, %v", acct, err)
	}
	if err := store.CheckInvariants(); err != nil {
		t.Fatal(err)
	}
}

func TestSpeculativeStorePromotionRebasesSurvivingUndo(t *testing.T) {
	store := newSpeculativeStore()
	store.SetFinalizedSlot(100)
	pk := solana.PublicKey{1}
	recordSpeculativeLayer(t, store, 101, 100, &accounts.Account{Key: pk, Lamports: 11})
	recordSpeculativeLayer(t, store, 102, 101, &accounts.Account{Key: pk, Lamports: 22})
	store.SetFinalizedSlot(101)
	store.PruneLayersThrough(101)
	if err := store.CheckInvariants(); err != nil {
		t.Fatal(err)
	}
	store.PruneLayersAbove(101)
	if len(store.flat) != 0 || len(store.layers) != 0 {
		t.Fatalf("unwind after promotion left flat=%d layers=%d", len(store.flat), len(store.layers))
	}
}

func recordSpeculativeLayer(t *testing.T, store *SpeculativeStore, slot, parent uint64, accts ...*accounts.Account) {
	t.Helper()
	ctx := &sealevel.SlotCtx{Slot: slot, ParentSlot: parent, AcctMapsMu: new(sync.Mutex)}
	if err := store.RecordLayer(slot, parent, ctx, accts); err != nil {
		t.Fatalf("record layer %d: %v", slot, err)
	}
}

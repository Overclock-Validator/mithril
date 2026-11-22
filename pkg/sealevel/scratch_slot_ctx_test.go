package sealevel

import (
	"reflect"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/gagliardetto/solana-go"
)

// How a context derived from a base (scratch or sealed) treats each SlotCtx
// field. A new field must be added here, and to newDerivedSlotCtx if it is
// shared, before this test passes: that keeps the decision deliberate.
//
//	replaced  the caller's accounts object
//	shared    constant for the block once transactions execute; same value or
//	          pointer as the base
//	private   bookkeeping an execution or its commit writes; the derived
//	          context has its own, so the base's is never touched
//	unset     written only when a bank is finalized or by commit-time
//	          accumulation; left zero in a derived context
var derivedSlotCtxFields = map[string]string{
	"Accounts":                  "replaced",
	"ParentAccts":               "shared",
	"AccountsDb":                "shared",
	"UnrootedRead":              "shared",
	"FeeRateGovernor":           "shared",
	"Slot":                      "shared",
	"ParentSlot":                "shared",
	"Epoch":                     "shared",
	"AcctMapsMu":                "private",
	"ModifiedAccts":             "private",
	"WritableAccts":             "private",
	"NumSignatures":             "shared",
	"Blockhash":                 "shared",
	"LastBlockhash":             "shared",
	"SlotBank":                  "shared",
	"Features":                  "shared",
	"VoteTimestampMu":           "private",
	"VoteTimestamps":            "private",
	"VoteAccts":                 "shared",
	"TotalEpochStake":           "shared",
	"FinalBankhash":             "unset",
	"AcctsLtHash":               "unset",
	"EpochsAcctHash":            "unset",
	"Replay":                    "shared",
	"LamportsBurnt":             "unset",
	"TotalComputeUnitsConsumed": "unset",
	"TransactionCounts":         "unset",
	"EahWorkaroundBankhash":     "unset",
	"HasEahWorkaround":          "unset",
	"LatestEvictedBlockhash":    "shared",
	"SerializedParameterArena":  "shared",
	"bankSysvars":               "shared",
	"TraceCtx":                  "shared",
	"DeferVoteCachePublication": "private",
	"PendingVoteCacheMu":        "private",
	"PendingVoteCache":          "private",
	"PendingVoteCacheDeletes":   "private",
	"PendingStakePubkeys":       "private",
	"VoteStakeDirty":            "private",
	"TrackProgramCacheAdds":     "private",
	"ParentReadHook":            "unset",
	"ProgramCacheAddsMu":        "private",
	"ProgramCacheAdds":          "private",
}

func TestDerivedSlotCtxFieldsClassified(t *testing.T) {
	typ := reflect.TypeOf(SlotCtx{})
	seen := map[string]bool{}
	for i := 0; i < typ.NumField(); i++ {
		name := typ.Field(i).Name
		seen[name] = true
		if _, ok := derivedSlotCtxFields[name]; !ok {
			t.Errorf("SlotCtx.%s is not classified for derived contexts: decide whether an execution on a scratch or sealed context shares it with the base, and record that in derivedSlotCtxFields (and in newDerivedSlotCtx if shared)", name)
		}
	}
	for name := range derivedSlotCtxFields {
		if !seen[name] {
			t.Errorf("derivedSlotCtxFields lists %s, which SlotCtx no longer has", name)
		}
	}
}

// A sealed context reads accounts only through the view it was given, shares
// the block constants with its base, and keeps its bookkeeping to itself.
func TestNewSealedSlotCtx(t *testing.T) {
	key := solana.PublicKey{7}
	parent := accounts.NewMemAccountsWithLen(1)
	if err := parent.SetAccountWithoutLock(key, &accounts.Account{Key: key, Lamports: 5}); err != nil {
		t.Fatal(err)
	}
	overlay := accounts.NewOverlayAccounts(parent)
	base := &SlotCtx{
		Accounts:        overlay,
		ParentAccts:     parent,
		Slot:            42,
		ParentSlot:      41,
		Epoch:           3,
		Blockhash:       [32]byte{1},
		LastBlockhash:   [32]byte{2},
		Features:        features.NewFeaturesDefault(),
		VoteAccts:       map[solana.PublicKey]uint64{key: 9},
		TotalEpochStake: 100,
		ModifiedAccts:   map[solana.PublicKey]bool{},
		WritableAccts:   map[solana.PublicKey]bool{},
	}
	view := accounts.NewRecordingReader(overlay)
	base.TransactionCounts = BankTransactionCounts{Total: 5}
	sealed := NewSealedSlotCtx(base, view)
	if sealed.TransactionCounts != (BankTransactionCounts{}) || NewScratchSlotCtx(base).TransactionCounts != (BankTransactionCounts{}) {
		t.Fatal("derived contexts must start without the committed bank transaction counts")
	}

	for field, kind := range derivedSlotCtxFields {
		if kind != "shared" || field == "bankSysvars" {
			continue
		}
		got := reflect.ValueOf(sealed).Elem().FieldByName(field)
		want := reflect.ValueOf(base).Elem().FieldByName(field)
		if !got.CanInterface() {
			continue
		}
		if !reflect.DeepEqual(got.Interface(), want.Interface()) {
			t.Errorf("shared field %s differs between the sealed context and its base", field)
		}
	}
	if sealed.BankSysvars() != base.BankSysvars() {
		t.Error("the bank sysvar snapshot must be the base's")
	}
	if sealed.Accounts != accounts.Accounts(view) {
		t.Fatal("a sealed context reads accounts through the view it was given")
	}

	acct, err := sealed.GetAccountShared(key)
	if err != nil || acct.Lamports != 5 {
		t.Fatalf("read through the sealed context: %v %v", acct, err)
	}
	if len(view.Reads()) != 1 || view.Reads()[0].Key != [32]byte(key) {
		t.Fatal("the read must be recorded by the view")
	}
	if err := sealed.SetAccount(key, &accounts.Account{Key: key, Lamports: 6}); err == nil || !view.Wrote() {
		t.Fatal("a write through the sealed context must be refused and remembered")
	}
	if got, _ := base.GetAccountShared(key); got.Lamports != 5 {
		t.Fatal("the base must not see the refused write")
	}
	sealed.RecordModifiedAcct(key)
	sealed.RecordWritableAcct(key)
	if len(base.ModifiedAccts) != 0 || len(base.WritableAccts) != 0 {
		t.Fatal("bookkeeping on the sealed context must not reach the base")
	}
}

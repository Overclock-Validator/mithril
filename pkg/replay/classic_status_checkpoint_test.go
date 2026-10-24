package replay

import (
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
	"testing"
)

func TestClassicStatusCheckpointRoundTrip(t *testing.T) {
	dir := t.TempDir()
	cache := NewTransactionStatusCache()
	st := &state.MithrilState{StateSchemaVersion: state.CurrentStateSchemaVersion}
	if err := saveClassicTransactionStatuses(dir, st, cache, 0); err != nil || st.LastTransactionStatusCheckpoint != nil {
		t.Fatal("empty replay should not create checkpoint", err)
	}
	cache = importedStatusCacheForTest(t)
	slot := cache.RootedThrough()
	if err := saveClassicTransactionStatuses(dir, st, cache, slot); err != nil {
		t.Fatal(err)
	}
	if err := st.Save(dir); err != nil {
		t.Fatal(err)
	}
	restored, err := state.LoadState(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := loadTransactionStatusCacheForReplay(dir, slot, 0, restored.LastTransactionStatusCheckpoint, solana.Hash{}, false); err != nil {
		t.Fatal(err)
	}
	if _, err := loadTransactionStatusCacheForReplay(dir, slot+1, 0, restored.LastTransactionStatusCheckpoint, solana.Hash{}, false); err == nil {
		t.Fatal("must reject checkpoint from a different slot")
	}
}

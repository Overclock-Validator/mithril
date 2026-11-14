package accountsdb

import (
	"bytes"
	"github.com/Overclock-Validator/mithril/pkg/progtrace"
	"github.com/gagliardetto/solana-go"
	"testing"
)

func TestProgramReadinessRefusedRefreshRecordsRemoval(t *testing.T) {
	var buf bytes.Buffer
	defer progtrace.Start(&buf, 64, 1000)()
	key := solana.PublicKey{102}
	db := &AccountsDb{ProgramCache: NewWeightedCache[solana.PublicKey, *ProgramCacheEntry](10, func(_ solana.PublicKey, e *ProgramCacheEntry) uint32 { return uint32(e.DeploymentSlot) })}
	admitted, at := db.AddProgramToCacheObserved(key, &ProgramCacheEntry{DeploymentSlot: 1})
	if !admitted || at == 0 {
		t.Fatal("missing initial admission")
	}
	_, ok, old := db.MaybeGetProgramFromCacheObserved(key)
	if !ok || !old.Known || old.PriorAdmit != at || old.InvalidatedAt != 0 || old.Ambiguous {
		t.Fatalf("initial history: %+v", old)
	}
	if db.AddProgramToCache(key, &ProgramCacheEntry{DeploymentSlot: 2}) {
		t.Fatal("synthetic refresh must be refused")
	}
	_, ok, after := db.MaybeGetProgramFromCacheObserved(key)
	if ok || !after.Known || after.InvalidatedAt == 0 || after.Ambiguous {
		t.Fatalf("refusal removal omitted: %+v", after)
	}
}

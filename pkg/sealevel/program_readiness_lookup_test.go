package sealevel

import (
	"bytes"
	"github.com/Overclock-Validator/mithril/fixtures"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/progtrace"
	"testing"
	"time"
)

func TestReadinessColdLookupKeepsLaterAdmissionSeparate(t *testing.T) {
	var buf bytes.Buffer
	defer progtrace.Start(&buf, 64, 1000)()
	key := benchPubkey(0xb1)
	db := programCacheWeighing(1)
	start := time.Now()
	_, ok, lookup := db.MaybeGetProgramFromCacheObserved(key)
	if ok {
		t.Fatal("fixture should start cold")
	}
	elf := fixtures.Load(t, "sbpf", "noop_aligned.so")
	entry := &accountsdb.ProgramCacheEntry{}
	entry.BindSource(elf, features.NewFeaturesDefault())
	admitted, at := db.AddProgramToCacheObserved(key, entry)
	if !admitted {
		t.Fatal("prefetch-style admission failed")
	}
	rec := compileRecord(key, elf, features.NewFeaturesDefault(), "absent", start, time.Now(), "ok", "skipped_speculative", compileDetails{lookup: lookup})
	if rec.PriorAdmit != 0 || rec.HistoryKnown || rec.LookupAt == 0 {
		t.Fatalf("future admission became prior: %+v", rec)
	}
	_, _, later := db.MaybeGetProgramFromCacheObserved(key)
	if !later.Known || later.PriorAdmit != at {
		t.Fatalf("later lookup failed to see admission: %+v", later)
	}
	rec = compileRecord(key, elf, features.NewFeaturesDefault(), "absent", start, time.Now(), "ok", "admitted", compileDetails{lookup: lookup, publishedAt: at})
	if rec.PublishedAt != at {
		t.Fatal("record construction moved publication timestamp")
	}
}

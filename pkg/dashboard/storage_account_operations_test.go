package dashboard

import (
	"encoding/json"
	"math"
	"testing"
	"time"
)

func TestStorageAccountOperationsCanonicalProjection(t *testing.T) {
	at := time.Now().UTC()
	d := maximumStorageDetail(at)
	d.Binding.NodeStartedAt = at.Add(-time.Hour)
	d.Binding.DBOpenedAt = at.Add(-time.Minute)
	d.AccountOperations = &AccountOperationsWindow{Scope: "runtime_db_requested_keys_and_committed_records", RecordingStartedAt: d.Binding.DBOpenedAt, StartedAt: at.Add(-time.Second), EndedAt: at, DurationNanoseconds: uint64(time.Second), RequestedKeys: math.MaxUint64 - 1, CommittedRecords: 0}
	b, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = decodeStorageDetail(b); err != nil {
		t.Fatal(err)
	}
	family, err := preparePublicStorageDetail(d, at, at.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var got PublicStorageDetail
	if json.Unmarshal([]byte(family.bodies[0]), &got) != nil || got.AccountOperations == nil || got.AccountOperations.RequestedKeys != math.MaxUint64-1 || got.AccountOperations.CommittedRecords != 0 {
		t.Fatal("public projection lost exact/zero values")
	}
	for _, mutate := range []func(*AccountOperationsWindow){func(w *AccountOperationsWindow) { w.DurationNanoseconds = 0 }, func(w *AccountOperationsWindow) { w.DurationNanoseconds = uint64(31 * time.Second) }, func(w *AccountOperationsWindow) { w.RequestedKeys = math.MaxUint64 }, func(w *AccountOperationsWindow) { w.EndedAt = at.Add(time.Second) }, func(w *AccountOperationsWindow) { w.Scope = "all_replay_operations" }} {
		bad := *d.AccountOperations
		mutate(&bad)
		if validAccountOperationsWindow(&bad, d.Binding.DBOpenedAt, at) {
			t.Fatalf("invalid window admitted %+v", bad)
		}
	}
	d.AccountOperations = nil
	b, err = json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = decodeStorageDetail(b); err != nil {
		t.Fatal("legacy absent optional field rejected", err)
	}
}

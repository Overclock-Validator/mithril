package dashboard

import (
	"bytes"
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func storageDetailFixture() StorageDetail {
	at := time.Date(2026, 11, 1, 12, 0, 0, 123456789, time.UTC)
	return StorageDetail{SchemaVersion: 1, Family: "storage", Scope: "runtime_pebble_latest_write_stall", State: "fresh",
		Binding: StorageDetailBinding{RunID: "storage-run", NodeStartedAt: at.Add(-time.Hour),
			GenesisHash: "JEKNVnkbo3jma5nREBBJCDoXFVeKkD56V3xKrvRmWxFG", DBGeneration: 1, DBOpenedAt: at.Add(-time.Minute)},
		CapturedAt: at, Index: StorageStoreDetail{State: "idle"}, BankHash: StorageStoreDetail{State: "idle"}}
}

func TestStorageDetailStatesAndOriginalDates(t *testing.T) {
	for _, state := range []string{"idle", "active", "ended"} {
		for _, reason := range []string{"memtable", "l0", "other"} {
			for _, duration := range []*uint64{nil, new(uint64), func() *uint64 { v := uint64(math.MaxInt64 - 1); return &v }()} {
				d := storageDetailFixture()
				s := StorageStoreDetail{State: state}
				if state != "idle" {
					start := d.Binding.NodeStartedAt.Add(-time.Hour)
					s.InitialReason, s.StartedAt = reason, &start
					if state == "ended" {
						end := start.Add(-time.Second) // Wall order does not define a monotonic duration.
						s.EndedAt, s.DurationNanoseconds = &end, duration
					}
				}
				d.Index, d.BankHash = s, s
				body, err := json.Marshal(d)
				if err != nil {
					t.Fatal(err)
				}
				got, err := decodeStorageDetail(body)
				if err != nil || !reflect.DeepEqual(got, d) {
					t.Fatalf("valid %s changed: %v", state, err)
				}
				clear(body)
				if got.Binding.RunID != d.Binding.RunID {
					t.Fatal("decoded record aliases input")
				}
			}
		}
	}
}

func TestStorageDetailRejectsInvalidShapes(t *testing.T) {
	for name, change := range map[string]func(*StorageDetail){
		"schema":          func(d *StorageDetail) { d.SchemaVersion = 2 },
		"family":          func(d *StorageDetail) { d.Family = "cache" },
		"scope":           func(d *StorageDetail) { d.Scope = "all_storage_health" },
		"state":           func(d *StorageDetail) { d.State = "stale" },
		"reason":          func(d *StorageDetail) { d.Reason = "waiting_source" },
		"run_empty":       func(d *StorageDetail) { d.Binding.RunID = "" },
		"run_long":        func(d *StorageDetail) { d.Binding.RunID = strings.Repeat("a", 129) },
		"run_utf8":        func(d *StorageDetail) { d.Binding.RunID = "\xff" },
		"generation_zero": func(d *StorageDetail) { d.Binding.DBGeneration = 0 },
		"generation_max":  func(d *StorageDetail) { d.Binding.DBGeneration = math.MaxUint64 },
		"genesis":         func(d *StorageDetail) { d.Binding.GenesisHash = strings.Repeat("1", 32) },
		"open_date":       func(d *StorageDetail) { d.Binding.DBOpenedAt = time.Time{} },
		"capture_date":    func(d *StorageDetail) { d.CapturedAt = time.Time{} },
		"idle_reason":     func(d *StorageDetail) { d.Index.InitialReason = "memtable" },
		"idle_start":      func(d *StorageDetail) { at := d.CapturedAt; d.Index.StartedAt = &at },
		"idle_duration":   func(d *StorageDetail) { v := uint64(0); d.Index.DurationNanoseconds = &v },
		"active_start":    func(d *StorageDetail) { d.Index = StorageStoreDetail{State: "active", InitialReason: "l0"} },
		"active_reason": func(d *StorageDetail) {
			at := d.CapturedAt
			d.Index = StorageStoreDetail{State: "active", InitialReason: "raw reason", StartedAt: &at}
		},
		"active_end": func(d *StorageDetail) {
			at := d.CapturedAt
			d.Index = StorageStoreDetail{State: "active", InitialReason: "l0", StartedAt: &at, EndedAt: &at}
		},
		"ended_end": func(d *StorageDetail) {
			at := d.CapturedAt
			d.Index = StorageStoreDetail{State: "ended", InitialReason: "l0", StartedAt: &at}
		},
		"ended_saturated": func(d *StorageDetail) {
			at := d.CapturedAt
			v := uint64(math.MaxInt64)
			d.Index = StorageStoreDetail{State: "ended", InitialReason: "l0", StartedAt: &at, EndedAt: &at, DurationNanoseconds: &v}
		},
		"store_unknown": func(d *StorageDetail) { d.BankHash.State = "unknown" },
	} {
		t.Run(name, func(t *testing.T) {
			d := storageDetailFixture()
			change(&d)
			if validateStorageDetail(d) == nil {
				t.Fatal("invalid detail accepted")
			}
		})
	}

	body, err := json.Marshal(storageDetailFixture())
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]byte{
		append([]byte(" "), body...), append(bytes.Clone(body), '\n'),
		bytes.Replace(body, []byte(`"db_generation":"1"`), []byte(`"db_generation":1`), 1),
		bytes.Replace(body, []byte(`"db_generation":"1"`), []byte(`"db_generation":"01"`), 1),
		bytes.Replace(body, []byte(`"index":`), []byte(`"path":"private","index":`), 1),
		bytes.Replace(body, []byte(`"bankhash":`), []byte(`"index":null,"bankhash":`), 1),
		bytes.Repeat([]byte(" "), MaxStorageDetailBytes+1),
	} {
		if _, err := decodeStorageDetail(bad); err == nil {
			t.Fatal("noncanonical detail accepted")
		}
	}
}

func TestStorageDetailCopyLeaseAndFutureDates(t *testing.T) {
	d := storageDetailFixture()
	at := d.CapturedAt
	valid := StorageDetailCapture{Detail: d, FreshUntil: at.Add(StorageDetailFreshFor - time.Second)}
	if storageDetailTimeInvalid(valid, at) {
		t.Fatal("valid conservative lease refused")
	}
	for _, change := range []func(*StorageDetailCapture){
		func(c *StorageDetailCapture) { c.FreshUntil = at },
		func(c *StorageDetailCapture) { c.FreshUntil = at.Add(StorageDetailFreshFor + time.Millisecond) },
		func(c *StorageDetailCapture) { c.Detail.CapturedAt = at.Add(-StorageDetailFreshFor) },
		func(c *StorageDetailCapture) { c.Detail.Binding.DBOpenedAt = at.Add(2 * time.Second) },
		func(c *StorageDetailCapture) {
			future := at.Add(2 * time.Second)
			c.Detail.Index = StorageStoreDetail{State: "active", InitialReason: "l0", StartedAt: &future}
		},
	} {
		bad := valid
		change(&bad)
		if !storageDetailTimeInvalid(bad, at) {
			t.Fatal("invalid source time/lease accepted")
		}
	}
}

func maximumStorageDetail(at time.Time) StorageDetail {
	d := storageDetailFixture()
	d.Binding.RunID, d.Binding.DBGeneration = strings.Repeat("<", 128), math.MaxUint64-1
	d.Binding.NodeStartedAt, d.Binding.DBOpenedAt, d.CapturedAt = at, at, at
	duration := uint64(math.MaxInt64 - 1)
	d.Index = StorageStoreDetail{State: "ended", InitialReason: "memtable", StartedAt: &at, EndedAt: &at, DurationNanoseconds: &duration}
	d.BankHash = d.Index
	allocated := uint64(math.MaxUint64)
	d.Inventory = &StorageInventory{Scope: "compaction_tracked_view_before_processing", SessionStartedAt: at,
		CycleStartedAt: at, CycleReturnedAt: at, CycleFailed: true, ObservedAt: at,
		DataFiles: math.MaxUint64, ManifestNames: math.MaxUint64, LogicalBytes: math.MaxUint64, AllocatedBytes: &allocated}
	d.Liveness = &StorageLiveness{Scope: "compaction_selected_candidates", SessionStartedAt: at, CycleStartedAt: at,
		CycleReturnedAt: at, CycleFailed: true, ScannedCandidates: math.MaxUint64, CompleteCandidates: math.MaxUint64 - 1,
		PartialCandidates: 1, RewriteCandidates: math.MaxUint64 - 1, SparseCandidates: 1, SourceBytes: math.MaxUint64,
		ExaminedBytes: math.MaxUint64, LiveBytes: 1, UndoPinnedBytes: 1, DeadBytes: math.MaxUint64 - 2, EligibleHoleBytes: math.MaxUint64 - 2}
	return d
}

func TestStorageLivenessCanonicalCoverage(t *testing.T) {
	d := maximumStorageDetail(sizingTime(time.Now()))
	body, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	if decoded, err := decodeStorageDetail(body); err != nil || !reflect.DeepEqual(decoded.Liveness, d.Liveness) {
		t.Fatalf("liveness lost in canonical response: %v", err)
	}
	for _, mutate := range []func(*StorageLiveness){
		func(v *StorageLiveness) { v.Scope = "whole_database" },
		func(v *StorageLiveness) { v.CompleteCandidates = math.MaxUint64 },
		func(v *StorageLiveness) { v.SparseCandidates = 0 },
		func(v *StorageLiveness) { v.SourceBytes-- },
		func(v *StorageLiveness) { v.LiveBytes = math.MaxUint64 },
		func(v *StorageLiveness) { v.DeadBytes-- },
		func(v *StorageLiveness) { v.EligibleHoleBytes = math.MaxUint64 },
	} {
		changed := *d.Liveness
		mutate(&changed)
		bad := d
		bad.Liveness = &changed
		if validateStorageDetail(bad) == nil {
			t.Fatal("invalid liveness coverage or arithmetic accepted")
		}
	}
}

func TestStorageInventoryCanonicalAndHistorical(t *testing.T) {
	d := storageDetailFixture()
	d.Inventory = maximumStorageDetail(d.CapturedAt).Inventory
	// Event age and wall-date order do not define the compactor's monotonic duration.
	d.Inventory.ObservedAt = d.CapturedAt.Add(-24 * time.Hour)
	d.Inventory.CycleStartedAt = d.CapturedAt.Add(-time.Hour)
	body, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := decodeStorageDetail(body); err != nil || !reflect.DeepEqual(got, d) {
		t.Fatalf("historical inventory changed: %v", err)
	}
	for _, mutate := range []func(*StorageInventory){
		func(i *StorageInventory) { i.Scope = "fragmentation" },
		func(i *StorageInventory) { i.ObservedAt = time.Time{} },
		func(i *StorageInventory) { i.CycleStartedAt = time.Time{} },
		func(i *StorageInventory) { i.SessionStartedAt = time.Time{} },
		func(i *StorageInventory) { i.CycleReturnedAt = time.Time{} },
		func(i *StorageInventory) { i.DataFiles = 0 },
	} {
		bad := *d.Inventory
		mutate(&bad)
		detail := d
		detail.Inventory = &bad
		if validateStorageDetail(detail) == nil {
			t.Fatal("invalid inventory accepted")
		}
	}
	for _, at := range []*time.Time{&d.Inventory.SessionStartedAt, &d.Inventory.CycleStartedAt, &d.Inventory.CycleReturnedAt, &d.Inventory.ObservedAt} {
		original := *at
		*at = d.CapturedAt.Add(2 * time.Second)
		if !storageDetailTimeInvalid(StorageDetailCapture{Detail: d, FreshUntil: d.CapturedAt.Add(time.Second)}, d.CapturedAt) {
			t.Fatal("future inventory accepted")
		}
		*at = original
	}
}

func TestStorageDetailTypedMaximum(t *testing.T) {
	d := maximumStorageDetail(sizingTime(time.Now()))
	assertSizingTimestamps(t, d)
	body, err := json.Marshal(d)
	if err != nil || len(body) > MaxStorageDetailBytes {
		t.Fatalf("maximum body: %d, %v", len(body), err)
	}
	got, err := decodeStorageDetail(body)
	if err != nil || got.Binding.DBGeneration != math.MaxUint64-1 || got.Index.DurationNanoseconds == nil || *got.Index.DurationNanoseconds != math.MaxInt64-1 || !reflect.DeepEqual(got, d) {
		t.Fatalf("maximum detail lost fields: %v", err)
	}
	t.Logf("storage private maximum: %d/%d bytes", len(body), MaxStorageDetailBytes)
}

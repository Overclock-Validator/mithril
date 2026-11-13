package dashboard

import (
	"encoding/json"
	"math"
	"reflect"
	"testing"
	"time"
)

func storageIOFixture(at time.Time) *StorageIODetail {
	process := ProcessIOCounts{ReadCharacters: 1, WriteCharacters: 2, ReadSyscalls: 3, WriteSyscalls: 4, ReadBytes: 5, WriteBytes: 6, CancelledWriteBytes: 7}
	block := BlockIOCounts{}
	v := reflect.ValueOf(&block).Elem()
	for i := 0; i < v.NumField(); i++ {
		v.Field(i).SetUint(uint64(i + 1))
	}
	b := BlockIODetail{Supported: true, StartedAt: at, CapturedAt: at,
		Device: &BlockIODevice{Major: 8, Minor: 1, HasDiscard: true, HasFlush: true}, Counters: &block}
	return &StorageIODetail{Scope: "linux_process_and_mounted_block_layer", StartedAt: at, CapturedAt: at,
		Process: ProcessIODetail{Supported: true, StartedAt: at, CapturedAt: at, Counters: &process,
			Window: &ProcessIOInterval{StartedAt: at.Add(-time.Second), EndedAt: at, DurationNanoseconds: uint64(time.Second), Counters: process}},
		Root: b, Accounts: b, Ledger: b}
}

func TestStorageIOContract(t *testing.T) {
	d := storageDetailFixture()
	d.IO = storageIOFixture(d.CapturedAt)
	body, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeStorageDetail(body)
	if err != nil || !reflect.DeepEqual(got, d) {
		t.Fatalf("I/O round trip: %v", err)
	}
	family, err := preparePublicStorageDetail(d, d.CapturedAt, d.CapturedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var public PublicStorageDetail
	if json.Unmarshal([]byte(family.bodies[0]), &public) != nil || !reflect.DeepEqual(public.IO, d.IO) {
		t.Fatal("public I/O fields changed")
	}
	for name, mutate := range map[string]func(*StorageIODetail){
		"scope":                 func(io *StorageIODetail) { io.Scope = "disk_health" },
		"duration":              func(io *StorageIODetail) { io.Process.Window.DurationNanoseconds = 0 },
		"window_without_counts": func(io *StorageIODetail) { io.Process.Counters = nil },
		"unsupported_counts":    func(io *StorageIODetail) { io.Process.Supported = false },
		"delta_above_total":     func(io *StorageIODetail) { io.Process.Window.Counters.ReadBytes = math.MaxUint64 },
		"device_without_counts": func(io *StorageIODetail) { io.Root.Counters = nil },
		"device_zero":           func(io *StorageIODetail) { io.Root.Device.Major = 0 },
		"unsupported_discard":   func(io *StorageIODetail) { io.Root.Device.HasDiscard = false },
		"unsupported_flush":     func(io *StorageIODetail) { io.Root.Device.HasFlush = false },
		"old_capture":           func(io *StorageIODetail) { io.StartedAt = io.StartedAt.Add(-StorageDetailFreshFor) },
	} {
		t.Run(name, func(t *testing.T) {
			x := storageIOFixture(d.CapturedAt)
			mutate(x)
			if validStorageIO(x, d.CapturedAt) {
				t.Fatal("invalid I/O accepted")
			}
		})
	}
	unsupported := &StorageIODetail{Scope: d.IO.Scope, StartedAt: d.CapturedAt, CapturedAt: d.CapturedAt}
	if !validStorageIO(unsupported, d.CapturedAt) {
		t.Fatal("unsupported sources confused with zeros")
	}
	unknown := storageIOFixture(d.CapturedAt)
	unknown.Process.Counters, unknown.Process.Window = nil, nil
	unknown.Root.Device, unknown.Root.Counters = nil, nil
	if !validStorageIO(unknown, d.CapturedAt) {
		t.Fatal("unreadable source refused instead of unknown")
	}
}

func maximumStorageIO(at time.Time) *StorageIODetail {
	d := storageIOFixture(at)
	d.Process.Window.StartedAt = at.Add(-30 * time.Second)
	d.Process.Window.DurationNanoseconds = uint64(30 * time.Second)
	for _, counts := range []*ProcessIOCounts{d.Process.Counters, &d.Process.Window.Counters} {
		v := reflect.ValueOf(counts).Elem()
		for i := 0; i < v.NumField(); i++ {
			v.Field(i).SetUint(math.MaxUint64)
		}
	}
	for _, block := range []*BlockIODetail{&d.Root, &d.Accounts, &d.Ledger} {
		v := reflect.ValueOf(block.Counters).Elem()
		for i := 0; i < v.NumField(); i++ {
			v.Field(i).SetUint(math.MaxUint64)
		}
		block.Device.Major, block.Device.Minor = math.MaxUint32, math.MaxUint32
	}
	return d
}

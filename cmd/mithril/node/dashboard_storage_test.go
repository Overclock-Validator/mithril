package node

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/dashboard"
)

func dashboardStorageDB(t *testing.T) (*accountsdb.AccountsDb, func()) {
	t.Helper()
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "accounts"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "largest_file_id"), make([]byte, 8), 0o600); err != nil {
		t.Fatal(err)
	}
	db, err := accountsdb.OpenDb(dir)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	close := func() {
		if !closed {
			db.CloseDb()
			closed = true
		}
	}
	t.Cleanup(close)
	return db, close
}

func storageNodeFixture() *nodeDashboard {
	genesis := "JEKNVnkbo3jma5nREBBJCDoXFVeKkD56V3xKrvRmWxFG"
	return &nodeDashboard{storageDetails: true, state: dashboard.Snapshot{RunID: "storage-node-run", StartedAt: time.Now().Add(-time.Hour).UTC(), GenesisHash: &genesis, Mode: "verifying"}}
}

func TestStorageDetailNodeOwnershipAndAllRoles(t *testing.T) {
	if validateDashboardStorageDetails(false, "") != nil || validateDashboardStorageDetails(true, "/private/node.sock") != nil {
		t.Fatal("valid option refused")
	}
	for _, socket := range []string{"", " \t"} {
		if validateDashboardStorageDetails(true, socket) == nil {
			t.Fatal("missing socket accepted")
		}
	}
	db, closeDB := dashboardStorageDB(t)
	d := storageNodeFixture()
	if !d.attachStorageOwner(db) || !d.attachStorageOwner(db) {
		t.Fatal("same owner attachment not idempotent")
	}
	for _, mode := range []string{"verifying", "validator"} {
		d.state.Mode, d.state.RPCConfigured = mode, true
		capture, err := d.sampleStorageDetail(time.Now().Add(-time.Hour))
		if err != nil || capture.Detail.Index.State != "idle" || capture.Detail.BankHash.State != "idle" || capture.Detail.Binding.DBGeneration != 1 ||
			capture.Detail.CapturedAt.Before(time.Now().Add(-time.Second)) || capture.FreshUntil.Sub(capture.Detail.CapturedAt) > dashboard.StorageDetailFreshFor {
			t.Fatalf("real DB copy: %+v, %v", capture, err)
		}
	}
	oldGenesis := *d.state.GenesisHash
	wrong := "11111111111111111111111111111111"
	d.state.GenesisHash = &wrong
	if _, err := d.sampleStorageDetail(time.Now()); err == nil || err.Error() != "binding_changed" {
		t.Fatal("changed chain accepted")
	}
	d.state.GenesisHash = &oldGenesis
	if _, err := d.sampleStorageDetail(time.Now()); err != nil {
		t.Fatal("transient binding loss destroyed original owner")
	}
	closeDB()
	if _, err := d.sampleStorageDetail(time.Now()); err == nil || err.Error() != "source_unavailable" {
		t.Fatal("closed owner published idle")
	}
	other, _ := dashboardStorageDB(t)
	if d.attachStorageOwner(other) || d.attachStorageOwner(db) {
		t.Fatal("unexpected replacement revived old generation")
	}
	if _, err := d.sampleStorageDetail(time.Now()); err == nil {
		t.Fatal("retired node monitor published")
	}
	newRun := storageNodeFixture()
	if !newRun.attachStorageOwner(other) {
		t.Fatal("independent run could not attach a valid DB")
	}
	newRun.stopStorageOwner()
	if _, err := newRun.sampleStorageDetail(time.Now()); err == nil {
		t.Fatal("stopping monitor published")
	}
	if _, ok := other.StorageObservation(); !ok {
		t.Fatal("stopping dashboard changed the database")
	}
}

func TestStorageDetailNodeUnknownAndDisabled(t *testing.T) {
	var absent *nodeDashboard
	if !absent.attachStorageOwner(nil) {
		t.Fatal("disabled owner attach failed")
	}
	if _, err := absent.sampleStorageDetail(time.Now()); err == nil {
		t.Fatal("absent dashboard fabricated data")
	}
	d := storageNodeFixture()
	if d.attachStorageOwner(nil) || d.attachStorageOwner(&accountsdb.AccountsDb{}) {
		t.Fatal("unopened owner accepted")
	}
	if _, err := d.sampleStorageDetail(time.Now()); err == nil || err.Error() != "waiting_source" {
		t.Fatal("unopened owner not unknown")
	}
	d.storageDetails = false
	if !d.attachStorageOwner(nil) {
		t.Fatal("disabled option touched owner")
	}
	if _, err := d.sampleStorageDetail(time.Now()); err == nil || err.Error() != "source_unavailable" {
		t.Fatal("disabled data exposed")
	}
}

func TestStorageDetailMeasurementPreservesDatesAndUnits(t *testing.T) {
	at := time.Now()
	b := dashboard.StorageDetailBinding{RunID: "storage-map", NodeStartedAt: at.Add(-time.Hour).UTC(), GenesisHash: strings.Repeat("a", 32), DBGeneration: 1, DBOpenedAt: at.Add(-time.Minute).UTC()}
	start, end := at.Add(-2*time.Hour), at.Add(-3*time.Hour)
	o := accountsdb.StorageObservation{OpenedAt: b.DBOpenedAt, Index: accountsdb.WriteStallObservation{HasEvent: true, InitialReason: "memtable", BeganAt: start, EndedAt: end, Duration: time.Millisecond, DurationKnown: true},
		BankHash: accountsdb.WriteStallObservation{HasEvent: true, Active: true, InitialReason: "l0", BeganAt: start}}
	completed := at.Add(time.Millisecond)
	capture, err := storageDetailMeasurement(b, o, at, completed)
	if err != nil || !capture.FreshUntil.Equal(at.Add(dashboard.StorageDetailFreshFor)) || !capture.Detail.CapturedAt.Equal(completed) ||
		capture.Detail.Index.State != "ended" || *capture.Detail.Index.DurationNanoseconds != 1000000 || *capture.Detail.Index.StartedAt != start.UTC() || *capture.Detail.Index.EndedAt != end.UTC() ||
		capture.Detail.BankHash.State != "active" || capture.Detail.BankHash.EndedAt != nil || capture.Detail.BankHash.DurationNanoseconds != nil {
		t.Fatalf("mapped values: %+v, %v", capture, err)
	}
	o.Index.InitialReason = "changed"
	o.Index.BeganAt = at
	if capture.Detail.Index.InitialReason != "memtable" || !capture.Detail.Index.StartedAt.Equal(start) {
		t.Fatal("caller mutated published values")
	}
	o.Index = accountsdb.WriteStallObservation{}
	for _, interval := range []struct{ start, end time.Time }{{time.Time{}, at}, {at, at.Add(-time.Nanosecond)}, {at, at.Add(time.Second + time.Nanosecond)}} {
		if _, err := storageDetailMeasurement(b, o, interval.start, interval.end); err == nil {
			t.Fatal("bad acquisition interval accepted")
		}
	}
	o.OpenedAt = at
	if _, err := storageDetailMeasurement(b, o, at, completed); err == nil || err.Error() != "binding_changed" {
		t.Fatal("another DB open accepted")
	}
}

func TestStorageStoreMeasurementRejectsContradictions(t *testing.T) {
	at := time.Now()
	for _, o := range []accountsdb.WriteStallObservation{
		{Active: true}, {InitialReason: "l0"}, {HasEvent: true, Active: true, InitialReason: "raw text", BeganAt: at},
		{HasEvent: true, Active: true, InitialReason: "l0", BeganAt: at, DurationKnown: true},
		{HasEvent: true, Active: true, InitialReason: "l0", BeganAt: at, EndedAt: at},
		{HasEvent: true, InitialReason: "l0", BeganAt: at},
		{HasEvent: true, InitialReason: "l0", BeganAt: at, EndedAt: at, Duration: time.Nanosecond},
		{HasEvent: true, InitialReason: "l0", BeganAt: at, EndedAt: at, Duration: -time.Nanosecond, DurationKnown: true},
	} {
		if _, err := storageStoreMeasurement(o); err == nil {
			t.Fatalf("contradictory source accepted: %+v", o)
		}
	}
	for _, known := range []bool{false, true} {
		o := accountsdb.WriteStallObservation{HasEvent: true, InitialReason: "other", BeganAt: at, EndedAt: at, DurationKnown: known}
		v, err := storageStoreMeasurement(o)
		if err != nil || (v.DurationNanoseconds != nil) != known || v.DurationNanoseconds != nil && *v.DurationNanoseconds != 0 {
			t.Fatal("unknown and known-zero duration merged")
		}
	}
}

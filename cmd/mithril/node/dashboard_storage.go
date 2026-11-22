package node

import (
	"errors"
	"math"
	"os"
	"strings"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/dashboard"
)

func physicalCensusEnabled() bool { return os.Getenv("MITHRIL_DASHBOARD_PHYSICAL_CENSUS") == "1" }

func validateDashboardStorageDetails(enabled bool, socket string) error {
	if enabled && strings.TrimSpace(socket) == "" {
		return errors.New("dashboard.storage_details requires dashboard.socket")
	}
	return nil
}

type storageDashboardMonitor struct {
	owner              *accountsdb.AccountsDb
	binding            dashboard.StorageDetailBinding
	operationsPrevious *accountOperationsPoint
	operationsRetired  bool
}

func storageMatchesNode(s dashboard.Snapshot, b dashboard.StorageDetailBinding) bool {
	return s.RunID == b.RunID && s.StartedAt.Equal(b.NodeStartedAt) && s.GenesisHash != nil && *s.GenesisHash == b.GenesisHash
}

// attachStorageOwner accepts one successful runtime DB per node run. An
// unexpected replacement permanently refuses this optional observation.
func (d *nodeDashboard) attachStorageOwner(owner *accountsdb.AccountsDb) bool {
	if d == nil {
		return true
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.storageDetails {
		return true
	}
	if d.storageRetired {
		return false
	}
	if m := d.storage; m != nil {
		if m.owner == owner && storageMatchesNode(d.state, m.binding) {
			return true
		}
		m.owner.StopPhysicalUsage()
		d.storage, d.storageRetired = nil, true
		return false
	}
	o, known := owner.StorageObservation()
	if !known || d.state.RunID == "" || d.state.StartedAt.IsZero() || d.state.GenesisHash == nil || !owner.EnableAccountOperations() {
		return false
	}
	d.storage = &storageDashboardMonitor{owner: owner, binding: dashboard.StorageDetailBinding{
		RunID: d.state.RunID, NodeStartedAt: d.state.StartedAt, GenesisHash: *d.state.GenesisHash,
		DBGeneration: 1, DBOpenedAt: o.OpenedAt.UTC(),
	}}
	if physicalCensusEnabled() {
		owner.EnablePhysicalUsage() // Optional census never gates existing storage details.
	}
	return true
}

func (d *nodeDashboard) stopStorageOwner() {
	if d != nil {
		d.mu.Lock()
		if d.storage != nil {
			d.storage.owner.StopPhysicalUsage()
		}
		d.storage, d.storageRetired = nil, true
		d.mu.Unlock()
	}
}

func (d *nodeDashboard) sampleStorageDetail(_ time.Time) (dashboard.StorageDetailCapture, error) {
	if d == nil {
		return dashboard.StorageDetailCapture{}, errors.New("source_unavailable")
	}
	startedAt := time.Now()
	d.mu.Lock()
	enabled, retired, m := d.storageDetails, d.storageRetired, d.storage
	compaction := d.compaction
	matched := m != nil && storageMatchesNode(d.state, m.binding)
	var operations *dashboard.AccountOperationsWindow
	if enabled && !retired && matched {
		// Serialize the actual counter reads with their baseline update.
		operationStarted := time.Now()
		observation, _ := m.owner.StorageObservation()
		operations = m.accountOperationsWindow(observation.AccountOperations, operationStarted, time.Now())
	}
	d.mu.Unlock()
	if !enabled || retired {
		return dashboard.StorageDetailCapture{}, errors.New("source_unavailable")
	}
	if m == nil {
		return dashboard.StorageDetailCapture{}, errors.New("waiting_source")
	}
	if !matched {
		return dashboard.StorageDetailCapture{}, errors.New("binding_changed")
	}
	o, known := m.owner.StorageObservation()
	cycle, cycleKnown := compaction.snapshot()
	physical, physicalUntil, physicalKnown := m.owner.PhysicalUsageObservation(time.Now())
	var ioObservation *dashboardIOObservation
	if d.io != nil {
		ioObservation = d.io.latest.Load()
	}
	completedAt := time.Now()
	if !known {
		return dashboard.StorageDetailCapture{}, errors.New("source_unavailable")
	}
	capture, err := storageDetailMeasurement(m.binding, o, startedAt, completedAt)
	if err != nil {
		return dashboard.StorageDetailCapture{}, err
	}
	if physicalKnown && admitStorageOptional(&capture, completedAt, physicalUntil) {
		value := &dashboard.StoragePhysicalUsage{Scope: "primary_db_tree_and_secondary_account_trees", StartedAt: physical.StartedAt.UTC(), CompletedAt: physical.CompletedAt.UTC(), DurationNanoseconds: physical.DurationNanoseconds, AllocatedBytes: physical.AllocatedBytes, LogicalFileBytes: physical.LogicalFileBytes, RegularFiles: physical.RegularFiles, Directories: physical.Directories}
		capture.Detail.PhysicalUsage = value
	}
	if cycleKnown {
		capture.Detail.Inventory = storageInventoryMeasurement(cycle, m.binding.DBOpenedAt)
		if capture.Detail.Inventory != nil {
			capture.Detail.Liveness = storageLivenessMeasurement(cycle, m.binding.DBOpenedAt)
		}
	}
	if ioObservation != nil && admitStorageOptional(&capture, completedAt, ioObservation.freshUntil) {
		value := cloneDashboardIO(ioObservation.value)
		capture.Detail.IO = &value
	}
	d.mu.Lock()
	matched = d.storage == m && d.compaction == compaction && d.storageDetails && !d.storageRetired && storageMatchesNode(d.state, m.binding)
	if matched {
		capture.Detail.AccountOperations = operations
	}
	d.mu.Unlock()
	if !matched {
		return dashboard.StorageDetailCapture{}, errors.New("binding_changed")
	}
	return capture, nil
}

// Admit optional observations only when their original lease can span one node
// sample interval, one reader poll interval, and two bounded private requests.
func admitStorageOptional(capture *dashboard.StorageDetailCapture, completedAt, until time.Time) bool {
	if until.Sub(completedAt) < dashboard.SampleInterval+dashboard.StorageDetailPollInterval+2*time.Second {
		return false
	}
	if until.Before(capture.FreshUntil) {
		capture.FreshUntil = until
	}
	return true
}

func storageLivenessMeasurement(c compactionObservation, openedAt time.Time) *dashboard.StorageLiveness {
	o := c.Stats.Liveness
	if !o.Known || !c.Stats.Inventory.Known || openedAt.IsZero() || !c.Stats.Inventory.DBOpenedAt.Equal(openedAt) ||
		c.SessionStartedAt.IsZero() || c.StartedAt.IsZero() || c.CompletedAt.IsZero() ||
		c.StartedAt.Before(c.SessionStartedAt) || c.CompletedAt.Before(c.StartedAt) {
		return nil
	}
	value := &dashboard.StorageLiveness{Scope: "compaction_selected_candidates", SessionStartedAt: c.SessionStartedAt.UTC(),
		CycleStartedAt: c.StartedAt.UTC(), CycleReturnedAt: c.CompletedAt.UTC(), CycleFailed: c.Failed,
		ScannedCandidates: o.ScannedCandidates, CompleteCandidates: o.CompleteCandidates, PartialCandidates: o.PartialCandidates,
		RewriteCandidates: o.RewriteCandidates, SparseCandidates: o.SparseCandidates, SourceBytes: o.SourceBytes,
		ExaminedBytes: o.ExaminedBytes, LiveBytes: o.LiveBytes, UndoPinnedBytes: o.UndoPinnedBytes,
		DeadBytes: o.DeadBytes, EligibleHoleBytes: o.EligibleHoleBytes}
	if !value.Valid() {
		return nil
	}
	return value
}

func storageInventoryMeasurement(c compactionObservation, openedAt time.Time) *dashboard.StorageInventory {
	o := c.Stats.Inventory
	if !o.Known || openedAt.IsZero() || !o.DBOpenedAt.Equal(openedAt) || o.ObservedAt.IsZero() ||
		c.SessionStartedAt.IsZero() || c.StartedAt.IsZero() || c.CompletedAt.IsZero() ||
		c.StartedAt.Before(c.SessionStartedAt) || c.CompletedAt.Before(c.StartedAt) ||
		o.ObservedAt.Before(c.StartedAt) || o.ObservedAt.After(c.CompletedAt) ||
		!o.AllocatedBytesKnown && o.AllocatedBytes != 0 ||
		o.DataFiles == 0 && (o.LogicalBytes != 0 || o.AllocatedBytes != 0) {
		return nil
	}
	var allocated *uint64
	if o.AllocatedBytesKnown {
		value := o.AllocatedBytes
		allocated = &value
	}
	return &dashboard.StorageInventory{Scope: "compaction_tracked_view_before_processing", SessionStartedAt: c.SessionStartedAt.UTC(),
		CycleStartedAt: c.StartedAt.UTC(), CycleReturnedAt: c.CompletedAt.UTC(), CycleFailed: c.Failed, ObservedAt: o.ObservedAt.UTC(),
		DataFiles: o.DataFiles, ManifestNames: o.ManifestNames, LogicalBytes: o.LogicalBytes, AllocatedBytes: allocated}
}

func storageDetailMeasurement(binding dashboard.StorageDetailBinding, o accountsdb.StorageObservation, startedAt, completedAt time.Time) (dashboard.StorageDetailCapture, error) {
	if startedAt.IsZero() || completedAt.Before(startedAt) || completedAt.Sub(startedAt) > time.Second {
		return dashboard.StorageDetailCapture{}, errors.New("source_time_invalid")
	}
	if o.OpenedAt.IsZero() || !o.OpenedAt.Equal(binding.DBOpenedAt) {
		return dashboard.StorageDetailCapture{}, errors.New("binding_changed")
	}
	index, err := storageStoreMeasurement(o.Index)
	if err != nil {
		return dashboard.StorageDetailCapture{}, err
	}
	bankHash, err := storageStoreMeasurement(o.BankHash)
	if err != nil {
		return dashboard.StorageDetailCapture{}, err
	}
	return dashboard.StorageDetailCapture{FreshUntil: startedAt.Add(dashboard.StorageDetailFreshFor), Detail: dashboard.StorageDetail{
		SchemaVersion: 1, Family: "storage", Scope: "runtime_pebble_latest_write_stall", State: "fresh",
		Binding: binding, CapturedAt: completedAt.UTC(), Index: index, BankHash: bankHash,
	}}, nil
}

func storageStoreMeasurement(o accountsdb.WriteStallObservation) (dashboard.StorageStoreDetail, error) {
	if !o.HasEvent {
		if o != (accountsdb.WriteStallObservation{}) {
			return dashboard.StorageStoreDetail{}, errors.New("source_invalid")
		}
		return dashboard.StorageStoreDetail{State: "idle"}, nil
	}
	if o.BeganAt.IsZero() || o.InitialReason != "memtable" && o.InitialReason != "l0" && o.InitialReason != "other" {
		return dashboard.StorageStoreDetail{}, errors.New("source_invalid")
	}
	start := o.BeganAt.UTC()
	d := dashboard.StorageStoreDetail{State: "active", InitialReason: o.InitialReason, StartedAt: &start}
	if o.Active {
		if !o.EndedAt.IsZero() || o.DurationKnown || o.Duration != 0 {
			return dashboard.StorageStoreDetail{}, errors.New("source_invalid")
		}
		return d, nil
	}
	if o.EndedAt.IsZero() || o.DurationKnown && (o.Duration < 0 || o.Duration == time.Duration(math.MaxInt64)) || !o.DurationKnown && o.Duration != 0 {
		return dashboard.StorageStoreDetail{}, errors.New("source_invalid")
	}
	end := o.EndedAt.UTC()
	d.State, d.EndedAt = "ended", &end
	if o.DurationKnown {
		value := uint64(o.Duration)
		d.DurationNanoseconds = &value
	}
	return d, nil
}

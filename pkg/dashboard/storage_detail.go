package dashboard

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"time"
	"unicode/utf8"
)

// MaxStorageDetailBytes bounds one runtime-store write-stall document.
const MaxStorageDetailBytes = 8192

// StorageDetailFreshFor bounds the owner copy, not its historical stall dates.
const StorageDetailFreshFor = 30 * time.Second

// StorageDetailBinding identifies one successful runtime DB open in a node run.
// Cache reinitialization and replay attempts do not change this lifetime.
type StorageDetailBinding struct {
	RunID         string    `json:"run_id"`
	NodeStartedAt time.Time `json:"node_started_at"`
	GenesisHash   string    `json:"genesis_hash"`
	DBGeneration  uint64    `json:"db_generation,string"`
	DBOpenedAt    time.Time `json:"db_opened_at"`
}

// StorageStoreDetail preserves one store's latest stall, not all DB latency.
// InitialReason names the first condition only. End means pressure was released,
// not that a write succeeded. Duration is measured locally, not from wall dates.
type StorageStoreDetail struct {
	State               string     `json:"state"`
	InitialReason       string     `json:"initial_reason"`
	StartedAt           *time.Time `json:"started_at"`
	EndedAt             *time.Time `json:"ended_at"`
	DurationNanoseconds *uint64    `json:"duration_nanoseconds,string"`
}

// StorageDetail contains independent fixed copies of the runtime index and
// bank-hash store. Earlier snapshot/format-upgrade handles are outside its scope.
type StorageDetail struct {
	SchemaVersion int                  `json:"schema_version"`
	Family        string               `json:"family"`
	Scope         string               `json:"scope"`
	State         string               `json:"state"`
	Reason        string               `json:"reason"`
	Binding       StorageDetailBinding `json:"binding"`
	CapturedAt    time.Time            `json:"captured_at"`
	Index         StorageStoreDetail   `json:"index"`
	BankHash      StorageStoreDetail   `json:"bankhash"`
	IO            *StorageIODetail     `json:"io,omitempty"`
	Inventory     *StorageInventory    `json:"inventory,omitempty"`
	Liveness      *StorageLiveness     `json:"liveness,omitempty"`
}

// StorageLiveness describes only completely scanned candidates in one cycle.
// Partial candidates contribute no classified bytes; eligible holes are not
// a guarantee that physical storage was reclaimed.
type StorageLiveness struct {
	Scope              string    `json:"scope"`
	SessionStartedAt   time.Time `json:"session_started_at"`
	CycleStartedAt     time.Time `json:"cycle_started_at"`
	CycleReturnedAt    time.Time `json:"cycle_returned_at"`
	CycleFailed        bool      `json:"cycle_failed"`
	ScannedCandidates  uint64    `json:"scanned_candidates,string"`
	CompleteCandidates uint64    `json:"complete_candidates,string"`
	PartialCandidates  uint64    `json:"partial_candidates,string"`
	RewriteCandidates  uint64    `json:"rewrite_candidates,string"`
	SparseCandidates   uint64    `json:"sparse_candidates,string"`
	SourceBytes        uint64    `json:"source_bytes,string"`
	ExaminedBytes      uint64    `json:"examined_bytes,string"`
	LiveBytes          uint64    `json:"live_bytes,string"`
	UndoPinnedBytes    uint64    `json:"undo_pinned_bytes,string"`
	DeadBytes          uint64    `json:"dead_bytes,string"`
	EligibleHoleBytes  uint64    `json:"eligible_hole_bytes,string"`
}

// Valid checks the coverage and arithmetic without treating absent data as zero.
func (l *StorageLiveness) Valid() bool {
	return l != nil && l.Scope == "compaction_selected_candidates" &&
		validReplayDetailTime(l.SessionStartedAt) && validReplayDetailTime(l.CycleStartedAt) && validReplayDetailTime(l.CycleReturnedAt) &&
		l.CompleteCandidates <= math.MaxUint64-l.PartialCandidates && l.CompleteCandidates+l.PartialCandidates == l.ScannedCandidates &&
		l.RewriteCandidates <= math.MaxUint64-l.SparseCandidates && l.RewriteCandidates+l.SparseCandidates == l.ScannedCandidates &&
		l.LiveBytes <= math.MaxUint64-l.UndoPinnedBytes && l.LiveBytes+l.UndoPinnedBytes <= math.MaxUint64-l.DeadBytes &&
		l.LiveBytes+l.UndoPinnedBytes+l.DeadBytes == l.ExaminedBytes && l.ExaminedBytes <= l.SourceBytes && l.EligibleHoleBytes <= l.DeadBytes &&
		(l.CompleteCandidates != 0 || l.ExaminedBytes == 0) && (l.ScannedCandidates != 0 || l.SourceBytes == 0) &&
		(l.SparseCandidates != 0 || l.EligibleHoleBytes == 0)
}

// StorageInventory preserves a historical tracked view from one returned
// compaction cycle. Its bytes exclude manifests and other store files.
type StorageInventory struct {
	Scope            string    `json:"scope"`
	SessionStartedAt time.Time `json:"session_started_at"`
	CycleStartedAt   time.Time `json:"cycle_started_at"`
	CycleReturnedAt  time.Time `json:"cycle_returned_at"`
	CycleFailed      bool      `json:"cycle_failed"`
	ObservedAt       time.Time `json:"observed_at"`
	DataFiles        uint64    `json:"data_files,string"`
	ManifestNames    uint64    `json:"manifest_names,string"`
	LogicalBytes     uint64    `json:"logical_bytes,string"`
	AllocatedBytes   *uint64   `json:"allocated_bytes,string"`
}

// StorageDetailCapture retains the original monotonic copy deadline.
type StorageDetailCapture struct {
	Detail     StorageDetail
	FreshUntil time.Time
}

// StorageDetailSampler copies bounded owner data on the ordinary Feed cycle.
// It performs no database calls or I/O and is never invoked by an HTTP request.
type StorageDetailSampler func(now time.Time) (StorageDetailCapture, error)

func validateStorageDetail(d StorageDetail) error {
	b := d.Binding
	if d.SchemaVersion != 1 || d.Family != "storage" || d.Scope != "runtime_pebble_latest_write_stall" ||
		d.State != "fresh" || d.Reason != "" || !utf8.ValidString(b.RunID) || b.RunID == "" || len(b.RunID) > 128 ||
		b.DBGeneration == 0 || b.DBGeneration == math.MaxUint64 || !validReplayDetailHash(b.GenesisHash, true) ||
		!validReplayDetailTime(b.NodeStartedAt) || !validReplayDetailTime(b.DBOpenedAt) || !validReplayDetailTime(d.CapturedAt) ||
		!validStorageStore(d.Index) || !validStorageStore(d.BankHash) || !validStorageIO(d.IO, d.CapturedAt) {
		return errors.New("source_invalid")
	}
	if i := d.Inventory; i != nil && (i.Scope != "compaction_tracked_view_before_processing" ||
		!validReplayDetailTime(i.SessionStartedAt) || !validReplayDetailTime(i.CycleStartedAt) ||
		!validReplayDetailTime(i.CycleReturnedAt) || !validReplayDetailTime(i.ObservedAt) ||
		i.DataFiles == 0 && (i.LogicalBytes != 0 || i.AllocatedBytes != nil && *i.AllocatedBytes != 0)) {
		return errors.New("source_invalid")
	}
	if l := d.Liveness; l != nil && (!l.Valid() || d.Inventory == nil ||
		!l.SessionStartedAt.Equal(d.Inventory.SessionStartedAt) || !l.CycleStartedAt.Equal(d.Inventory.CycleStartedAt) ||
		!l.CycleReturnedAt.Equal(d.Inventory.CycleReturnedAt) || l.CycleFailed != d.Inventory.CycleFailed) {
		return errors.New("source_invalid")
	}
	return nil
}

func validStorageStore(s StorageStoreDetail) bool {
	if s.State == "idle" {
		return s.InitialReason == "" && s.StartedAt == nil && s.EndedAt == nil && s.DurationNanoseconds == nil
	}
	if s.InitialReason != "memtable" && s.InitialReason != "l0" && s.InitialReason != "other" ||
		s.StartedAt == nil || !validReplayDetailTime(*s.StartedAt) {
		return false
	}
	switch s.State {
	case "active":
		return s.EndedAt == nil && s.DurationNanoseconds == nil
	case "ended":
		return s.EndedAt != nil && validReplayDetailTime(*s.EndedAt) &&
			(s.DurationNanoseconds == nil || *s.DurationNanoseconds < math.MaxInt64)
	default:
		return false
	}
}

// decodeStorageDetail accepts only the producer's canonical typed JSON.
func decodeStorageDetail(body []byte) (StorageDetail, error) {
	var d StorageDetail
	if len(body) > MaxStorageDetailBytes || json.Unmarshal(body, &d) != nil || validateStorageDetail(d) != nil {
		return StorageDetail{}, errors.New("source_invalid")
	}
	canonical, err := json.Marshal(d)
	if err != nil || !bytes.Equal(body, canonical) {
		return StorageDetail{}, errors.New("source_invalid")
	}
	return d, nil
}

func storageDetailTimeInvalid(c StorageDetailCapture, completedAt time.Time) bool {
	d, limit := c.Detail, completedAt.Add(time.Second)
	if c.FreshUntil.IsZero() || !c.FreshUntil.After(completedAt) || c.FreshUntil.After(completedAt.Add(StorageDetailFreshFor)) ||
		c.FreshUntil.Sub(d.CapturedAt) > StorageDetailFreshFor || completedAt.Sub(d.CapturedAt) >= StorageDetailFreshFor ||
		d.Binding.NodeStartedAt.After(limit) || d.Binding.DBOpenedAt.After(limit) || d.CapturedAt.After(limit) {
		return true
	}
	for _, s := range []StorageStoreDetail{d.Index, d.BankHash} {
		if s.StartedAt != nil && (s.StartedAt.After(limit) || s.StartedAt.After(d.CapturedAt.Add(time.Second))) ||
			s.EndedAt != nil && (s.EndedAt.After(limit) || s.EndedAt.After(d.CapturedAt.Add(time.Second))) {
			return true
		}
	}
	if i := d.Inventory; i != nil {
		for _, at := range []time.Time{i.SessionStartedAt, i.CycleStartedAt, i.CycleReturnedAt, i.ObservedAt} {
			if at.After(limit) || at.After(d.CapturedAt.Add(time.Second)) {
				return true
			}
		}
	}
	if l := d.Liveness; l != nil {
		for _, at := range []time.Time{l.SessionStartedAt, l.CycleStartedAt, l.CycleReturnedAt} {
			if at.After(limit) || at.After(d.CapturedAt.Add(time.Second)) {
				return true
			}
		}
	}
	return false
}

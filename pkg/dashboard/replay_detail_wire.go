package dashboard

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
)

// The private replay wire is the exact json.Marshal output of these concrete DTOs.
// Round-trip equality rejects omitted, duplicate and unknown members, and
// null/default substitutions at every depth. Keep nested fields typed, without
// maps, RawMessage or interfaces. The optional held schedule is a named typed
// child: omission is accepted, while explicit null remains noncanonical.
func decodeReplayDetailManifest(body []byte) (ReplayDetailManifest, error) {
	var m ReplayDetailManifest
	if len(body) == 0 || len(body) > MaxSnapshotBytes || json.Unmarshal(body, &m) != nil {
		return ReplayDetailManifest{}, errors.New("invalid replay detail manifest")
	}
	if err := validateReplayDetailManifest(m); err != nil {
		return ReplayDetailManifest{}, err
	}
	canonical, err := json.Marshal(m)
	if err != nil || !bytes.Equal(body, canonical) {
		return ReplayDetailManifest{}, errors.New("noncanonical replay detail manifest")
	}
	return m, nil
}

func validateReplayDetailManifest(m ReplayDetailManifest) error {
	if (m.SchemaVersion != 1 && m.SchemaVersion != 2) || m.Family != "replay" || m.Scope != "replay_row_history" ||
		!validReplayDetailTime(m.CollectorAt) || !validReplayDetailTime(m.RetainUntil) ||
		m.PageSize != replayDetailPageSize || m.PageCount < 0 || m.PageCount > ReplayDetailCapacity/replayDetailPageSize ||
		!validHeldSchedule(m.ScheduleWindow) || !validReplayFoldCapacity(m.FoldCapacity) || !validReplayNearReception(m.NearReception) || m.NearReception != nil && m.Count == 0 || !validReplaySelectedMetadata(m.LatestSelected, m.RecentSelected) {
		return errors.New("invalid replay detail manifest")
	}
	if m.State != "fresh" && m.State != "stale" || m.State == "fresh" && m.Reason != "" ||
		m.State == "stale" && m.Reason != "status_stale" && m.Reason != "source_unavailable" && m.Reason != "source_invalid" {
		return errors.New("invalid replay detail state")
	}
	if err := validateReplayDetailMetadata(m.Binding, m.CapturedAt, m.UpdatedAt, m.Capacity, m.Count, m.FirstSequence, m.LastSequence, m.Evicted); err != nil {
		return err
	}
	if m.PageCount != (m.Count+replayDetailPageSize-1)/replayDetailPageSize || len(m.Generation) != 64 {
		return errors.New("invalid replay detail manifest range")
	}
	digest, err := hex.DecodeString(m.Generation)
	if err != nil || hex.EncodeToString(digest) != m.Generation {
		return errors.New("invalid replay detail generation")
	}
	return nil
}

func decodeReplayDetailPage(body []byte, m ReplayDetailManifest, expectedPage int) (ReplayDetailPage, error) {
	if err := validateReplayDetailManifest(m); err != nil {
		return ReplayDetailPage{}, err
	}
	var p ReplayDetailPage
	if len(body) == 0 || len(body) > MaxSnapshotBytes || json.Unmarshal(body, &p) != nil {
		return ReplayDetailPage{}, errors.New("invalid replay detail page")
	}
	if expectedPage < 0 || expectedPage >= m.PageCount || p.SchemaVersion != m.SchemaVersion || p.Family != "replay" ||
		p.Generation != m.Generation || !sameReplayDetailBinding(p.Binding, m.Binding) ||
		p.Page != expectedPage || p.Offset != expectedPage*replayDetailPageSize || p.Records == nil ||
		len(p.Records) != min(replayDetailPageSize, m.Count-p.Offset) {
		return ReplayDetailPage{}, errors.New("invalid replay detail page context")
	}
	if expectedPage+1 < m.PageCount {
		if p.NextPage == nil || *p.NextPage != expectedPage+1 {
			return ReplayDetailPage{}, errors.New("invalid replay detail continuation")
		}
	} else if p.NextPage != nil {
		return ReplayDetailPage{}, errors.New("invalid replay detail continuation")
	}
	if err := validateReplayDetailRows(p.Records, m.Binding, m.FirstSequence+uint64(p.Offset), m.SchemaVersion); err != nil {
		return ReplayDetailPage{}, err
	}
	canonical, err := json.Marshal(p)
	if err != nil || !bytes.Equal(body, canonical) {
		return ReplayDetailPage{}, errors.New("noncanonical replay detail page")
	}
	return p, nil
}

// replayDetailCoreFromRecords verifies the complete ordered collection, including
// cross-page completion order and the final row, before returning a detached
// core. It does not establish a lease, freshness or continuity with an earlier
// generation; the reader collector owns those checks.
func replayDetailCoreFromRecords(m ReplayDetailManifest, records []ReplayDetailRecord) (ReplayDetailCore, error) {
	if err := validateReplayDetailManifest(m); err != nil {
		return ReplayDetailCore{}, err
	}
	if records == nil || len(records) != m.Count {
		return ReplayDetailCore{}, errors.New("incomplete replay detail collection")
	}
	binding := m.Binding
	if binding.GenesisHash != nil {
		genesis := *binding.GenesisHash
		binding.GenesisHash = &genesis
	}
	core := ReplayDetailCore{
		SchemaVersion: m.SchemaVersion, Family: m.Family, Scope: m.Scope,
		Binding: binding, CapturedAt: m.CapturedAt, UpdatedAt: m.UpdatedAt,
		Capacity: m.Capacity, Count: m.Count, FirstSequence: m.FirstSequence,
		LastSequence: m.LastSequence, Evicted: m.Evicted, Exhausted: m.Exhausted,
		Records:        cloneReplayDetailRecords(records),
		ScheduleWindow: cloneHeldSchedule(m.ScheduleWindow),
		FoldCapacity:   cloneReplayFoldCapacity(m.FoldCapacity),
		NearReception:  cloneReplayNearReception(m.NearReception),
		LatestSelected: cloneReplayLatestSelected(m.LatestSelected),
		RecentSelected: cloneReplayRecentSelected(m.RecentSelected),
	}
	generation, err := ReplayDetailGeneration(core)
	if err != nil {
		return ReplayDetailCore{}, err
	}
	if generation != m.Generation {
		return ReplayDetailCore{}, errors.New("replay detail generation mismatch")
	}
	return core, nil
}

package dashboard

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"time"
)

// HeldScheduleFreshFor bounds the latest owner revalidation, not dataset age.
const HeldScheduleFreshFor = 30 * time.Second

// HeldScheduleCapture revalidates an immutable dataset against the current
// node/replay/schedule owners. It does not recreate that dataset or redate its
// observation. FreshUntil must retain the original monotonic capture deadline.
type HeldScheduleCapture struct {
	Data                 *PreparedHeldScheduleData
	CapturedAt           time.Time
	FreshUntil           time.Time
	ReferenceSlot        uint64
	ReferenceBankhash    string
	ReferenceCompletedAt time.Time
}

func validateHeldScheduleManifest(m HeldScheduleManifest) error {
	if m.SchemaVersion != 1 || m.Family != "held_schedule" || m.Scope != "locally_held_epoch_assignments" ||
		m.State != "fresh" || m.Reason != "" || !validReplayDetailBinding(m.Binding) ||
		!validReplayDetailTime(m.CapturedAt) || !validReplayDetailTime(m.ObservedAt) || m.CaptureSequence == 0 ||
		!validHeldScheduleDigest(m.Digest) || m.Bytes < 1 || m.Bytes > MaxHeldScheduleDataBytes ||
		!validReplayDetailHash(m.ReferenceBankhash, true) || !validReplayDetailTime(m.ReferenceCompletedAt) ||
		!validHeldScheduleSummary(m.Current) || m.ReferenceSlot < m.Current.FirstSlot || m.ReferenceSlot-m.Current.FirstSlot >= m.Current.SlotCount {
		return errors.New("source_invalid")
	}
	if m.Next == nil {
		if m.NextReason != "waiting_source" && m.NextReason != "response_limit" && m.NextReason != "range_unavailable" {
			return errors.New("source_invalid")
		}
		return nil
	}
	if m.NextReason != "" || !validHeldScheduleSummary(*m.Next) || m.Current.Epoch == math.MaxUint64 ||
		m.Current.SlotCount > math.MaxUint64-m.Current.FirstSlot || m.Next.Epoch != m.Current.Epoch+1 ||
		m.Next.FirstSlot != m.Current.FirstSlot+m.Current.SlotCount {
		return errors.New("source_invalid")
	}
	return nil
}

func validHeldScheduleSummary(s HeldScheduleSummary) bool {
	return validHeldEpochGeometry(s.Epoch, s.FirstSlot, s.SlotCount) && s.KnownSlots <= s.SlotCount &&
		s.Leaders >= 0 && s.Leaders <= maxHeldEpochLeaders && uint64(s.Leaders) <= s.KnownSlots && (s.Leaders == 0) == (s.KnownSlots == 0)
}

func decodeHeldScheduleManifest(body []byte) (HeldScheduleManifest, error) {
	var m HeldScheduleManifest
	if len(body) > MaxSnapshotBytes || json.Unmarshal(body, &m) != nil || validateHeldScheduleManifest(m) != nil {
		return HeldScheduleManifest{}, errors.New("source_invalid")
	}
	canonical, err := json.Marshal(m)
	if err != nil || !bytes.Equal(body, canonical) {
		return HeldScheduleManifest{}, errors.New("source_invalid")
	}
	return m, nil
}

// HeldScheduleSampler runs on the existing joined detail worker, not HTTP or
// the one-second status loop. It must fence node/replay and held pointers before
// returning, cache assignments until those owners change, and perform no I/O.
type HeldScheduleSampler func(time.Time) (HeldScheduleCapture, error)

// HeldScheduleManifest is small enough to poll independently of assignment
// data. CaptureSequence belongs to the feed owner, not a wall-clock ordering.
// Its dataset digest changes only when an immutable observation is replaced.
type HeldScheduleManifest struct {
	SchemaVersion        int                  `json:"schema_version"`
	Family               string               `json:"family"`
	Scope                string               `json:"scope"`
	State                string               `json:"state"`
	Reason               string               `json:"reason"`
	Binding              ReplayDetailBinding  `json:"binding"`
	CapturedAt           time.Time            `json:"captured_at"`
	CaptureSequence      uint64               `json:"capture_sequence,string"`
	Digest               string               `json:"digest"`
	Bytes                int                  `json:"bytes"`
	ObservedAt           time.Time            `json:"observed_at"`
	ReferenceSlot        uint64               `json:"reference_slot,string"`
	ReferenceBankhash    string               `json:"reference_bankhash"`
	ReferenceCompletedAt time.Time            `json:"reference_completed_at"`
	Current              HeldScheduleSummary  `json:"current"`
	Next                 *HeldScheduleSummary `json:"next"`
	NextReason           string               `json:"next_reason"`
}

func prepareHeldScheduleManifest(c HeldScheduleCapture, sequence uint64, now time.Time) (HeldScheduleManifest, string, error) {
	p := c.Data
	if p == nil || sequence == 0 || !c.CapturedAt.Equal(now.UTC()) ||
		!validReplayDetailHash(c.ReferenceBankhash, true) || !validReplayDetailTime(c.ReferenceCompletedAt) ||
		c.ReferenceSlot < p.current.FirstSlot || c.ReferenceSlot-p.current.FirstSlot >= p.current.SlotCount {
		return HeldScheduleManifest{}, "", errors.New("source_invalid")
	}
	if c.FreshUntil == c.FreshUntil.Round(0) || !c.FreshUntil.After(time.Now()) || c.FreshUntil.After(now.Add(HeldScheduleFreshFor)) {
		return HeldScheduleManifest{}, "", errors.New("source_time_invalid")
	}
	m := HeldScheduleManifest{SchemaVersion: 1, Family: "held_schedule", Scope: "locally_held_epoch_assignments", State: "fresh",
		Binding: p.binding, CapturedAt: c.CapturedAt, CaptureSequence: sequence, Digest: p.digest, Bytes: len(p.body), ObservedAt: p.observedAt,
		ReferenceSlot: c.ReferenceSlot, ReferenceBankhash: c.ReferenceBankhash, ReferenceCompletedAt: c.ReferenceCompletedAt,
		Current: p.current, Next: p.next, NextReason: p.nextReason}
	if p.binding.GenesisHash != nil {
		value := *p.binding.GenesisHash
		m.Binding.GenesisHash = &value
	}
	if p.next != nil {
		next := *p.next
		m.Next = &next
	}
	body, err := json.Marshal(m)
	if err != nil || len(body) > MaxSnapshotBytes {
		return HeldScheduleManifest{}, "", errors.New("response_limit")
	}
	if !c.FreshUntil.After(time.Now()) {
		return HeldScheduleManifest{}, "", errors.New("source_time_invalid")
	}
	return m, string(body), nil
}

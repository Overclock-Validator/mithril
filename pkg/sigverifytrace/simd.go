package sigverifytrace

import (
	"encoding/json"
	"errors"
	"fmt"
)

var ErrBatchInferenceUnavailable = errors.New("sigverifytrace: exact SIMD groups require a scheduling_simulation trace")

type SlotState uint8

const (
	SlotInactive SlotState = iota
	SlotMissing
	SlotPresent
)

func (state SlotState) String() string {
	switch state {
	case SlotMissing:
		return "missing"
	case SlotPresent:
		return "present"
	default:
		return "inactive_tail"
	}
}

func (state SlotState) MarshalJSON() ([]byte, error) {
	return json.Marshal(state.String())
}

type SIMDSlot struct {
	State                SlotState `json:"state"`
	FlatLane             uint64    `json:"flat_lane"`
	JobIndex             uint32    `json:"job_index"`
	LaneIndex            uint32    `json:"lane_index"`
	VerificationSequence uint64    `json:"verification_sequence,omitempty"`
	Outcome              Outcome   `json:"outcome"`
	PublicKey            [32]byte  `json:"-"`
}

type SIMDGroup struct {
	DispatchID         uint64     `json:"dispatch_id"`
	Source             string     `json:"source"`
	GroupIndex         uint64     `json:"group_index"`
	StartLane          uint64     `json:"start_lane"`
	ActiveLanes        int        `json:"active_lanes"`
	Tail               bool       `json:"tail"`
	Complete           bool       `json:"complete"`
	ActiveMask         uint8      `json:"active_mask"`
	ObservedMask       uint8      `json:"observed_mask"`
	MissingMask        uint8      `json:"missing_mask"`
	ValidMask          uint8      `json:"valid_mask"`
	InvalidMask        uint8      `json:"invalid_mask"`
	UnknownMask        uint8      `json:"unknown_mask"`
	KeyHitMask         uint8      `json:"key_hit_mask"`
	DuplicateHitMask   uint8      `json:"duplicate_hit_mask"`
	JobStartMask       uint8      `json:"job_start_mask"`
	DistinctPublicKeys int        `json:"distinct_public_keys"`
	RepeatedSameKey    bool       `json:"repeated_same_key"`
	Slots              []SIMDSlot `json:"slots"`
}

type SIMDReport struct {
	Width             int         `json:"width"`
	Dispatches        uint64      `json:"dispatches"`
	Groups            []SIMDGroup `json:"groups,omitempty"`
	FullGroups        uint64      `json:"full_groups"`
	TailGroups        uint64      `json:"tail_groups"`
	CompleteGroups    uint64      `json:"complete_groups"`
	IncompleteGroups  uint64      `json:"incomplete_groups"`
	ObservedLanes     uint64      `json:"observed_lanes"`
	MissingLanes      uint64      `json:"missing_lanes"`
	InvalidLanes      uint64      `json:"invalid_lanes"`
	UngroupedAttempts uint64      `json:"ungrouped_attempts"`
	SameKeyGroups     uint64      `json:"same_key_groups"`
	MixedKeyGroups    uint64      `json:"mixed_key_groups"`
}

// InferSIMD reconstructs only groups actually claimed by the explicit
// scheduling simulation. A missing slot is retained as a hole: it can mean
// scalar early-stop after an invalid signature or independent trace-ring
// eviction, and is never fabricated as a verification or verdict.
func InferSIMD(trace *Trace, width int, policy *ReplayReport) (SIMDReport, error) {
	report := SIMDReport{Width: width}
	if width != 4 && width != 8 {
		return report, fmt.Errorf("sigverifytrace: SIMD width %d is not x4 or x8", width)
	}
	if err := trace.Validate(); err != nil {
		return report, err
	}
	if trace.Summary.Mode != ModeSchedulingSimulation {
		return report, ErrBatchInferenceUnavailable
	}
	dispatchByID := make(map[uint64]*Dispatch, len(trace.Dispatches))
	for index := range trace.Dispatches {
		dispatch := &trace.Dispatches[index]
		if dispatch.ReadyEventSequence == 0 {
			return report, fmt.Errorf("sigverifytrace: dispatch %d is not ready", dispatch.DispatchID)
		}
		dispatchByID[dispatch.DispatchID] = dispatch
	}
	verificationsByDispatch := make(map[uint64]map[uint64]*Verification)
	for index := range trace.Verifications {
		verification := &trace.Verifications[index]
		if verification.DispatchID == 0 {
			report.UngroupedAttempts++
			continue
		}
		dispatch := dispatchByID[verification.DispatchID]
		if dispatch == nil {
			report.UngroupedAttempts++
			continue
		}
		flat := flatLane(dispatch, verification.JobIndex, verification.LaneIndex)
		lanes := verificationsByDispatch[dispatch.DispatchID]
		if lanes == nil {
			lanes = make(map[uint64]*Verification)
			verificationsByDispatch[dispatch.DispatchID] = lanes
		}
		lanes[flat] = verification
	}

	for dispatchIndex := range trace.Dispatches {
		dispatch := &trace.Dispatches[dispatchIndex]
		report.Dispatches++
		boundaries := make(map[uint64]struct{}, len(dispatch.JobSignatures))
		var boundary uint64
		for _, count := range dispatch.JobSignatures {
			boundaries[boundary] = struct{}{}
			boundary += uint64(count)
		}
		for start, groupIndex := uint64(0), uint64(0); start < dispatch.SignatureLanes; start, groupIndex = start+uint64(width), groupIndex+1 {
			active := width
			if remaining := int(dispatch.SignatureLanes - start); remaining < active {
				active = remaining
			}
			group := SIMDGroup{
				DispatchID:  dispatch.DispatchID,
				Source:      dispatch.Source,
				GroupIndex:  groupIndex,
				StartLane:   start,
				ActiveLanes: active,
				Tail:        active != width,
				ActiveMask:  lowMask(active),
				Slots:       make([]SIMDSlot, width),
			}
			keys := make(map[[32]byte]struct{})
			for lane := 0; lane < width; lane++ {
				flat := start + uint64(lane)
				if lane >= active {
					group.Slots[lane] = SIMDSlot{State: SlotInactive, FlatLane: flat}
					continue
				}
				job, inJob := dispatchLane(dispatch, flat)
				slot := SIMDSlot{State: SlotMissing, FlatLane: flat, JobIndex: job, LaneIndex: inJob}
				bit := uint8(1 << lane)
				if _, ok := boundaries[flat]; ok {
					group.JobStartMask |= bit
				}
				verification := verificationsByDispatch[dispatch.DispatchID][flat]
				if verification == nil {
					group.MissingMask |= bit
					report.MissingLanes++
					group.Slots[lane] = slot
					continue
				}
				slot.State = SlotPresent
				slot.VerificationSequence = verification.Sequence
				slot.Outcome = verification.Outcome
				slot.PublicKey = verification.PublicKey
				group.ObservedMask |= bit
				report.ObservedLanes++
				keys[verification.PublicKey] = struct{}{}
				switch verification.Outcome {
				case OutcomeValid:
					group.ValidMask |= bit
				case OutcomeInvalid:
					group.InvalidMask |= bit
					report.InvalidLanes++
				default:
					group.UnknownMask |= bit
				}
				if policy != nil {
					decision, ok := policy.decisionBySequence[verification.Sequence]
					if !ok {
						return report, fmt.Errorf("sigverifytrace: policy report lacks verification %d", verification.Sequence)
					}
					if decision.KeyHit {
						group.KeyHitMask |= bit
					}
					if decision.DuplicateHit {
						group.DuplicateHitMask |= bit
					}
				}
				group.Slots[lane] = slot
			}
			group.Complete = group.MissingMask == 0
			group.DistinctPublicKeys = len(keys)
			group.RepeatedSameKey = len(keys) == 1 && bitCount(group.ObservedMask) > 1
			if group.Tail {
				report.TailGroups++
			} else {
				report.FullGroups++
			}
			if group.Complete {
				report.CompleteGroups++
			} else {
				report.IncompleteGroups++
			}
			if group.RepeatedSameKey {
				report.SameKeyGroups++
			} else if group.DistinctPublicKeys > 1 {
				report.MixedKeyGroups++
			}
			report.Groups = append(report.Groups, group)
		}
	}
	return report, nil
}

func flatLane(dispatch *Dispatch, jobIndex, laneIndex uint32) uint64 {
	var flat uint64
	for job := uint32(0); job < jobIndex; job++ {
		flat += uint64(dispatch.JobSignatures[job])
	}
	return flat + uint64(laneIndex)
}

func dispatchLane(dispatch *Dispatch, flat uint64) (jobIndex, laneIndex uint32) {
	for job, count := range dispatch.JobSignatures {
		if flat < uint64(count) {
			return uint32(job), uint32(flat)
		}
		flat -= uint64(count)
	}
	panic("sigverifytrace: validated flat lane outside dispatch")
}

func lowMask(lanes int) uint8 {
	return uint8((uint16(1) << lanes) - 1)
}

func bitCount(mask uint8) int {
	count := 0
	for mask != 0 {
		mask &= mask - 1
		count++
	}
	return count
}

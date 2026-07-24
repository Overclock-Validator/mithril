// Package sigverifytrace parses and validates Mithril's exact sigverify
// workload traces. It is an offline policy tool: no type in this package is
// used by the production verifier or telemetry hot path.
package sigverifytrace

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const SchemaV3 = "mithril-sigverify-v3"

type CollectionMode string

const (
	ModeDisabled             CollectionMode = "disabled"
	ModePassive              CollectionMode = "passive"
	ModeSchedulingSimulation CollectionMode = "scheduling_simulation"
)

type Outcome uint8

const (
	OutcomeUnknown Outcome = iota
	OutcomeValid
	OutcomeInvalid
)

func (outcome Outcome) String() string {
	switch outcome {
	case OutcomeValid:
		return "valid"
	case OutcomeInvalid:
		return "invalid"
	default:
		return "unknown"
	}
}

func (outcome Outcome) MarshalJSON() ([]byte, error) {
	return json.Marshal(outcome.String())
}

type Summary struct {
	Type                     string         `json:"type"`
	Schema                   string         `json:"schema"`
	Enabled                  bool           `json:"enabled"`
	Mode                     CollectionMode `json:"collection_mode"`
	Capacity                 uint64         `json:"capacity"`
	Transactions             uint64         `json:"transactions"`
	Signatures               uint64         `json:"signatures"`
	MessageBytes             uint64         `json:"message_bytes"`
	VerificationAttempts     uint64         `json:"verification_attempts"`
	ObservedEvents           uint64         `json:"observed_events"`
	RetainedVerifications    int            `json:"retained_verifications"`
	RetainedDispatches       int            `json:"retained_dispatches"`
	ExactDuplicateHits       uint64         `json:"exact_duplicate_hits"`
	PublicKeyReuseHits       uint64         `json:"public_key_reuse_hits"`
	QueueSamples             uint64         `json:"queue_samples"`
	QueueOccupancySum        uint64         `json:"queue_occupancy_sum"`
	QueueOccupancyMax        uint64         `json:"queue_occupancy_max"`
	BatchSamples             uint64         `json:"batch_samples"`
	NaturalBatchWidthSum     uint64         `json:"natural_batch_width_sum"`
	NaturalBatchWidthMax     uint64         `json:"natural_batch_width_max"`
	SignaturesPerTransaction [19]uint64     `json:"signatures_per_transaction"`
	MessageSize              [9]uint64      `json:"message_size"`
	ReuseDistance            [19]uint64     `json:"reuse_distance"`
	QueueOccupancy           [16]uint64     `json:"queue_occupancy"`
	NaturalBatchWidth        [20]uint64     `json:"natural_batch_width"`
}

type Verification struct {
	Sequence                uint64
	BeginEventSequence      uint64
	CompletionEventSequence uint64
	Source                  string
	PublicKey               [32]byte
	Signature               [64]byte
	Message                 []byte
	ExactDuplicate          bool
	PublicKeyReused         bool
	ReuseDistance           uint64
	Outcome                 Outcome
	DispatchID              uint64
	JobIndex                uint32
	LaneIndex               uint32
}

type Dispatch struct {
	DispatchID         uint64
	ClaimEventSequence uint64
	ReadyEventSequence uint64
	Mode               CollectionMode
	Source             string
	QueuedItemsBefore  uint64
	QueuedItemsAfter   uint64
	SignatureLanes     uint64
	JobSignatures      []uint16
}

type Trace struct {
	Summary       Summary
	Verifications []Verification
	Dispatches    []Dispatch
}

type verificationJSON struct {
	Type                    string `json:"type"`
	Sequence                uint64 `json:"sequence"`
	BeginEventSequence      uint64 `json:"begin_event_sequence"`
	CompletionEventSequence uint64 `json:"completion_event_sequence"`
	Source                  string `json:"source"`
	PublicKey               string `json:"public_key"`
	Signature               string `json:"signature"`
	Message                 string `json:"message_base64"`
	ExactDuplicate          bool   `json:"exact_duplicate"`
	PublicKeyReused         bool   `json:"public_key_reused"`
	ReuseDistance           uint64 `json:"reuse_distance"`
	Outcome                 string `json:"outcome"`
	DispatchID              uint64 `json:"dispatch_id"`
	JobIndex                uint32 `json:"job_index"`
	LaneIndex               uint32 `json:"lane_index"`
}

type dispatchJSON struct {
	Type               string         `json:"type"`
	DispatchID         uint64         `json:"dispatch_id"`
	ClaimEventSequence uint64         `json:"claim_event_sequence"`
	ReadyEventSequence uint64         `json:"ready_event_sequence"`
	Mode               CollectionMode `json:"dispatch_mode"`
	Source             string         `json:"source"`
	QueuedItemsBefore  uint64         `json:"queued_items_before"`
	QueuedItemsAfter   uint64         `json:"queued_items_after"`
	SignatureLanes     uint64         `json:"signature_lanes"`
	JobSignatures      []uint16       `json:"job_signatures"`
}

type recordEnvelope struct {
	Type string `json:"type"`
}

// Parse reads one v3 summary followed by its retained verification and
// dispatch records. The exporter emits bounded suffixes, so validation never
// assumes that sequence one or event one is retained.
func Parse(r io.Reader) (*Trace, error) {
	if r == nil {
		return nil, errors.New("sigverifytrace: nil reader")
	}
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	trace := new(Trace)
	line := 0
	seenSummary := false
	seenDispatch := false
	for scanner.Scan() {
		line++
		raw := bytes.TrimSpace(scanner.Bytes())
		if len(raw) == 0 {
			return nil, fmt.Errorf("sigverifytrace: line %d is empty", line)
		}
		var envelope recordEnvelope
		if err := json.Unmarshal(raw, &envelope); err != nil {
			return nil, fmt.Errorf("sigverifytrace: line %d envelope: %w", line, err)
		}
		switch envelope.Type {
		case "summary":
			if seenSummary || line != 1 {
				return nil, fmt.Errorf("sigverifytrace: line %d has duplicate or misplaced summary", line)
			}
			if err := decodeStrict(raw, &trace.Summary); err != nil {
				return nil, fmt.Errorf("sigverifytrace: line %d summary: %w", line, err)
			}
			seenSummary = true
		case "verification":
			if !seenSummary || seenDispatch {
				return nil, fmt.Errorf("sigverifytrace: line %d has misplaced verification", line)
			}
			var encoded verificationJSON
			if err := decodeStrict(raw, &encoded); err != nil {
				return nil, fmt.Errorf("sigverifytrace: line %d verification: %w", line, err)
			}
			verification, err := decodeVerification(encoded)
			if err != nil {
				return nil, fmt.Errorf("sigverifytrace: line %d verification: %w", line, err)
			}
			trace.Verifications = append(trace.Verifications, verification)
		case "dispatch":
			if !seenSummary {
				return nil, fmt.Errorf("sigverifytrace: line %d has dispatch before summary", line)
			}
			seenDispatch = true
			var encoded dispatchJSON
			if err := decodeStrict(raw, &encoded); err != nil {
				return nil, fmt.Errorf("sigverifytrace: line %d dispatch: %w", line, err)
			}
			trace.Dispatches = append(trace.Dispatches, Dispatch{
				DispatchID:         encoded.DispatchID,
				ClaimEventSequence: encoded.ClaimEventSequence,
				ReadyEventSequence: encoded.ReadyEventSequence,
				Mode:               encoded.Mode,
				Source:             encoded.Source,
				QueuedItemsBefore:  encoded.QueuedItemsBefore,
				QueuedItemsAfter:   encoded.QueuedItemsAfter,
				SignatureLanes:     encoded.SignatureLanes,
				JobSignatures:      append([]uint16(nil), encoded.JobSignatures...),
			})
		default:
			return nil, fmt.Errorf("sigverifytrace: line %d has unknown record type %q", line, envelope.Type)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("sigverifytrace: read trace: %w", err)
	}
	if !seenSummary {
		return nil, errors.New("sigverifytrace: missing summary")
	}
	if err := trace.Validate(); err != nil {
		return nil, err
	}
	return trace, nil
}

func decodeStrict(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func decodeVerification(encoded verificationJSON) (Verification, error) {
	var result Verification
	if err := decodeFixedHex(result.PublicKey[:], encoded.PublicKey); err != nil {
		return result, fmt.Errorf("public key: %w", err)
	}
	if err := decodeFixedHex(result.Signature[:], encoded.Signature); err != nil {
		return result, fmt.Errorf("signature: %w", err)
	}
	message, err := base64.StdEncoding.DecodeString(encoded.Message)
	if err != nil || base64.StdEncoding.EncodeToString(message) != encoded.Message {
		if err == nil {
			err = errors.New("noncanonical base64")
		}
		return result, fmt.Errorf("message: %w", err)
	}
	outcome, err := parseOutcome(encoded.Outcome)
	if err != nil {
		return result, err
	}
	result = Verification{
		Sequence:                encoded.Sequence,
		BeginEventSequence:      encoded.BeginEventSequence,
		CompletionEventSequence: encoded.CompletionEventSequence,
		Source:                  encoded.Source,
		PublicKey:               result.PublicKey,
		Signature:               result.Signature,
		Message:                 message,
		ExactDuplicate:          encoded.ExactDuplicate,
		PublicKeyReused:         encoded.PublicKeyReused,
		ReuseDistance:           encoded.ReuseDistance,
		Outcome:                 outcome,
		DispatchID:              encoded.DispatchID,
		JobIndex:                encoded.JobIndex,
		LaneIndex:               encoded.LaneIndex,
	}
	return result, nil
}

func decodeFixedHex(out []byte, encoded string) error {
	if len(encoded) != hex.EncodedLen(len(out)) {
		return fmt.Errorf("encoded length %d, want %d", len(encoded), hex.EncodedLen(len(out)))
	}
	decoded, err := hex.DecodeString(encoded)
	if err != nil {
		return err
	}
	if hex.EncodeToString(decoded) != encoded {
		return errors.New("noncanonical lowercase hex")
	}
	copy(out, decoded)
	return nil
}

func parseOutcome(label string) (Outcome, error) {
	switch label {
	case "unknown":
		return OutcomeUnknown, nil
	case "valid":
		return OutcomeValid, nil
	case "invalid":
		return OutcomeInvalid, nil
	default:
		return OutcomeUnknown, fmt.Errorf("unknown outcome %q", label)
	}
}

func validSource(source string) bool {
	switch source {
	case "replay", "tpu", "turbine", "unknown":
		return true
	default:
		return false
	}
}

// Validate checks all invariants available in a bounded v3 export. Missing
// cross-ring records are tolerated here because verification and dispatch
// histories are independently bounded; exact SIMD reconstruction rejects
// such incompleteness separately.
func (trace *Trace) Validate() error {
	if trace == nil {
		return errors.New("sigverifytrace: nil trace")
	}
	s := trace.Summary
	if s.Type != "summary" || s.Schema != SchemaV3 {
		return fmt.Errorf("sigverifytrace: summary type/schema = %q/%q", s.Type, s.Schema)
	}
	switch s.Mode {
	case ModeDisabled, ModePassive, ModeSchedulingSimulation:
	default:
		return fmt.Errorf("sigverifytrace: invalid collection mode %q", s.Mode)
	}
	if s.Enabled && s.Mode == ModeDisabled || !s.Enabled && s.Mode != ModeDisabled {
		return fmt.Errorf("sigverifytrace: enabled=%v conflicts with mode %q", s.Enabled, s.Mode)
	}
	if s.Enabled && s.Capacity == 0 {
		return errors.New("sigverifytrace: enabled trace has zero capacity")
	}
	if s.RetainedVerifications != len(trace.Verifications) || s.RetainedDispatches != len(trace.Dispatches) {
		return fmt.Errorf("sigverifytrace: retained counts summary=%d/%d records=%d/%d", s.RetainedVerifications, s.RetainedDispatches, len(trace.Verifications), len(trace.Dispatches))
	}
	if uint64(len(trace.Verifications)) > s.VerificationAttempts {
		return fmt.Errorf("sigverifytrace: retained verifications exceed attempts")
	}
	wantRetained := s.VerificationAttempts
	if wantRetained > s.Capacity {
		wantRetained = s.Capacity
	}
	if s.Enabled && uint64(len(trace.Verifications)) != wantRetained {
		return fmt.Errorf("sigverifytrace: retained verification suffix has %d records, want %d", len(trace.Verifications), wantRetained)
	}
	events := make(map[uint64]string, 2*len(trace.Verifications)+2*len(trace.Dispatches))
	claimByID := make(map[uint64]*Dispatch, len(trace.Dispatches))
	var previousSequence uint64
	var previousBegin uint64
	for index := range trace.Verifications {
		verification := &trace.Verifications[index]
		if verification.Sequence == 0 || verification.Sequence <= previousSequence || verification.Sequence > s.VerificationAttempts {
			return fmt.Errorf("sigverifytrace: verification sequence %d is not a retained increasing attempt", verification.Sequence)
		}
		previousSequence = verification.Sequence
		wantSequence := s.VerificationAttempts - uint64(len(trace.Verifications)) + uint64(index) + 1
		if verification.Sequence != wantSequence {
			return fmt.Errorf("sigverifytrace: verification sequence %d, want contiguous suffix value %d", verification.Sequence, wantSequence)
		}
		if verification.BeginEventSequence == 0 || verification.BeginEventSequence <= previousBegin {
			return fmt.Errorf("sigverifytrace: verification %d has non-increasing begin event %d", verification.Sequence, verification.BeginEventSequence)
		}
		previousBegin = verification.BeginEventSequence
		if !validSource(verification.Source) {
			return fmt.Errorf("sigverifytrace: verification %d has invalid source %q", verification.Sequence, verification.Source)
		}
		if verification.ExactDuplicate && !verification.PublicKeyReused {
			return fmt.Errorf("sigverifytrace: verification %d is duplicate without key reuse", verification.Sequence)
		}
		if verification.PublicKeyReused {
			if verification.ReuseDistance == 0 || verification.ReuseDistance > s.Capacity {
				return fmt.Errorf("sigverifytrace: verification %d has invalid reuse distance %d", verification.Sequence, verification.ReuseDistance)
			}
		} else if verification.ReuseDistance != 0 {
			return fmt.Errorf("sigverifytrace: verification %d has distance without reuse", verification.Sequence)
		}
		if err := addEvent(events, verification.BeginEventSequence, s.ObservedEvents, fmt.Sprintf("verification %d begin", verification.Sequence)); err != nil {
			return err
		}
		if verification.CompletionEventSequence == 0 {
			if verification.Outcome != OutcomeUnknown {
				return fmt.Errorf("sigverifytrace: verification %d has outcome without completion", verification.Sequence)
			}
		} else {
			if verification.CompletionEventSequence <= verification.BeginEventSequence || verification.Outcome == OutcomeUnknown {
				return fmt.Errorf("sigverifytrace: verification %d has invalid completion/outcome", verification.Sequence)
			}
			if err := addEvent(events, verification.CompletionEventSequence, s.ObservedEvents, fmt.Sprintf("verification %d completion", verification.Sequence)); err != nil {
				return err
			}
		}
		if verification.DispatchID == 0 && (verification.JobIndex != 0 || verification.LaneIndex != 0) {
			return fmt.Errorf("sigverifytrace: uncorrelated verification %d has job/lane", verification.Sequence)
		}
	}
	var previousDispatch uint64
	for index := range trace.Dispatches {
		dispatch := &trace.Dispatches[index]
		if dispatch.DispatchID == 0 || dispatch.DispatchID <= previousDispatch {
			return fmt.Errorf("sigverifytrace: dispatch ID %d is not increasing", dispatch.DispatchID)
		}
		previousDispatch = dispatch.DispatchID
		if index > 0 && dispatch.DispatchID != trace.Dispatches[index-1].DispatchID+1 {
			return fmt.Errorf("sigverifytrace: dispatch IDs %d and %d are not a contiguous retained suffix", trace.Dispatches[index-1].DispatchID, dispatch.DispatchID)
		}
		if dispatch.Mode != s.Mode || dispatch.Mode == ModeDisabled || !validSource(dispatch.Source) {
			return fmt.Errorf("sigverifytrace: dispatch %d has invalid mode/source", dispatch.DispatchID)
		}
		if dispatch.ClaimEventSequence == 0 {
			return fmt.Errorf("sigverifytrace: dispatch %d has zero claim event", dispatch.DispatchID)
		}
		if err := addEvent(events, dispatch.ClaimEventSequence, s.ObservedEvents, fmt.Sprintf("dispatch %d claim", dispatch.DispatchID)); err != nil {
			return err
		}
		if dispatch.ReadyEventSequence == 0 {
			if dispatch.SignatureLanes != 0 || len(dispatch.JobSignatures) != 0 {
				return fmt.Errorf("sigverifytrace: unready dispatch %d has lane metadata", dispatch.DispatchID)
			}
		} else {
			if dispatch.ReadyEventSequence <= dispatch.ClaimEventSequence {
				return fmt.Errorf("sigverifytrace: dispatch %d ready precedes claim", dispatch.DispatchID)
			}
			if err := addEvent(events, dispatch.ReadyEventSequence, s.ObservedEvents, fmt.Sprintf("dispatch %d ready", dispatch.DispatchID)); err != nil {
				return err
			}
			var lanes uint64
			for _, count := range dispatch.JobSignatures {
				lanes += uint64(count)
			}
			if lanes != dispatch.SignatureLanes {
				return fmt.Errorf("sigverifytrace: dispatch %d lane sum %d != %d", dispatch.DispatchID, lanes, dispatch.SignatureLanes)
			}
			if dispatch.Mode == ModePassive && len(dispatch.JobSignatures) != 1 {
				return fmt.Errorf("sigverifytrace: passive dispatch %d has %d jobs", dispatch.DispatchID, len(dispatch.JobSignatures))
			}
		}
		claimByID[dispatch.DispatchID] = dispatch
	}
	correlated := make(map[[3]uint64]uint64)
	invalidLane := make(map[[2]uint64]uint32)
	for index := range trace.Verifications {
		verification := &trace.Verifications[index]
		if verification.DispatchID == 0 {
			continue
		}
		dispatch := claimByID[verification.DispatchID]
		if dispatch == nil {
			continue
		}
		if dispatch.ReadyEventSequence == 0 || verification.BeginEventSequence <= dispatch.ReadyEventSequence {
			return fmt.Errorf("sigverifytrace: verification %d began before dispatch %d was ready", verification.Sequence, verification.DispatchID)
		}
		if verification.Source != dispatch.Source {
			return fmt.Errorf("sigverifytrace: verification %d source %q differs from dispatch %d source %q", verification.Sequence, verification.Source, dispatch.DispatchID, dispatch.Source)
		}
		if uint64(verification.JobIndex) >= uint64(len(dispatch.JobSignatures)) || verification.LaneIndex >= uint32(dispatch.JobSignatures[verification.JobIndex]) {
			return fmt.Errorf("sigverifytrace: verification %d job/lane is outside dispatch %d", verification.Sequence, verification.DispatchID)
		}
		key := [3]uint64{verification.DispatchID, uint64(verification.JobIndex), uint64(verification.LaneIndex)}
		if prior := correlated[key]; prior != 0 {
			return fmt.Errorf("sigverifytrace: verifications %d and %d share dispatch job/lane", prior, verification.Sequence)
		}
		correlated[key] = verification.Sequence
		jobKey := [2]uint64{verification.DispatchID, uint64(verification.JobIndex)}
		if stoppedAt, ok := invalidLane[jobKey]; ok && verification.LaneIndex > stoppedAt {
			return fmt.Errorf("sigverifytrace: verification %d appears after invalid lane %d in its job", verification.Sequence, stoppedAt)
		}
		if verification.Outcome == OutcomeInvalid {
			invalidLane[jobKey] = verification.LaneIndex
		}
	}
	return nil
}

func addEvent(events map[uint64]string, event, observed uint64, label string) error {
	if event == 0 || event > observed {
		return fmt.Errorf("sigverifytrace: %s event %d exceeds observed clock %d", label, event, observed)
	}
	if prior := events[event]; prior != "" {
		return fmt.Errorf("sigverifytrace: event %d is both %s and %s", event, prior, label)
	}
	events[event] = label
	return nil
}

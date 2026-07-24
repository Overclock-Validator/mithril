package sigverifytrace

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestParseValidV3TraceAndOwnsBytes(t *testing.T) {
	trace := simulationTraceWithHole()
	encoded := encodeTrace(t, trace)
	parsed, err := Parse(strings.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	if got := len(parsed.Verifications); got != 3 {
		t.Fatalf("verifications=%d", got)
	}
	if parsed.Verifications[0].Outcome != OutcomeInvalid || parsed.Dispatches[0].SignatureLanes != 5 {
		t.Fatalf("parsed trace=%+v %+v", parsed.Verifications[0], parsed.Dispatches[0])
	}
	parsed.Verifications[0].Message[0] = 'X'
	again, err := Parse(strings.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	if got := string(again.Verifications[0].Message); got != "job0-invalid" {
		t.Fatalf("message aliases input or another parse: %q", got)
	}
}

func TestParseRejectsMalformedV3Records(t *testing.T) {
	base := simulationTraceWithHole()
	tests := []struct {
		name   string
		mutate func(*Trace, *verificationJSON, *dispatchJSON, *Summary)
	}{
		{"schema", func(_ *Trace, _ *verificationJSON, _ *dispatchJSON, s *Summary) { s.Schema = "v2" }},
		{"uppercase-hex", func(_ *Trace, v *verificationJSON, _ *dispatchJSON, _ *Summary) { v.PublicKey = "AB" + v.PublicKey[2:] }},
		{"noncanonical-base64", func(_ *Trace, v *verificationJSON, _ *dispatchJSON, _ *Summary) {
			v.Message = base64.RawStdEncoding.EncodeToString([]byte("x"))
		}},
		{"completion-before-begin", func(_ *Trace, v *verificationJSON, _ *dispatchJSON, _ *Summary) {
			v.CompletionEventSequence = v.BeginEventSequence
		}},
		{"dispatch-lane-sum", func(_ *Trace, _ *verificationJSON, d *dispatchJSON, _ *Summary) { d.SignatureLanes++ }},
		{"source-mismatch", func(_ *Trace, v *verificationJSON, _ *dispatchJSON, _ *Summary) { v.Source = "tpu" }},
		{"lane-out-of-range", func(_ *Trace, v *verificationJSON, _ *dispatchJSON, _ *Summary) { v.LaneIndex = 9 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			summary := base.Summary
			verifications, dispatches := wireRecords(base)
			test.mutate(base, &verifications[0], &dispatches[0], &summary)
			_, err := Parse(strings.NewReader(encodeWire(t, summary, verifications, dispatches)))
			if err == nil {
				t.Fatal("malformed trace parsed")
			}
		})
	}

	encoded := encodeTrace(t, base)
	encoded = strings.Replace(encoded, `"type":"verification"`, `"type":"verification","extra":1`, 1)
	if _, err := Parse(strings.NewReader(encoded)); err == nil {
		t.Fatal("unknown field parsed")
	}
	if _, err := Parse(strings.NewReader("\n" + encoded)); err == nil {
		t.Fatal("summary after empty line parsed")
	}
}

func TestValidateAllowsIndependentBoundedCorrelationSuffixes(t *testing.T) {
	trace := passiveTrace([]Verification{
		verification(3, 1, 2, 1, OutcomeValid, 1),
		verification(4, 3, 4, 2, OutcomeValid, 2),
	})
	trace.Summary.Capacity = 2
	trace.Summary.VerificationAttempts = 4
	trace.Verifications[0].Sequence = 3
	trace.Verifications[1].Sequence = 4
	trace.Summary.ObservedEvents = 4
	if err := trace.Validate(); err != nil {
		t.Fatal(err)
	}
	trace.Verifications[1].Sequence = 5
	if err := trace.Validate(); err == nil {
		t.Fatal("noncontiguous attempt suffix validated")
	}
}

func TestReplayUsesCompletionOrderAndBoundsTables(t *testing.T) {
	var pubA, pubB [32]byte
	pubA[0], pubB[0] = 1, 2
	trace := passiveTrace([]Verification{
		verificationWithKey(1, 1, 4, pubA, OutcomeValid, "a"),
		verificationWithKey(2, 2, 3, pubB, OutcomeValid, "b"),
		verificationWithKey(3, 5, 6, pubB, OutcomeValid, "b2"),
	})
	report, err := Replay(trace, ReplayConfig{
		MaxKeyEntries:     1,
		TableBytesPerKey:  20480,
		AdmitAfterValid:   1,
		DuplicateCapacity: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Keys.Lookups != 3 || report.Keys.Hits != 0 || report.Keys.Misses != 3 || report.Keys.Admissions != 3 || report.Keys.RetainedBuilds != 3 || report.Keys.Evictions != 2 {
		t.Fatalf("key stats=%+v", report.Keys)
	}
	if report.Keys.ResidentEntries != 1 || report.Keys.ResidentTableBytes != 20480 || report.Keys.PeakTableBytes != 20480 {
		t.Fatalf("key footprint=%+v", report.Keys)
	}
	if !report.Attempts[0].Admitted || report.Attempts[0].Evictions != 1 {
		t.Fatalf("late A completion did not evict completion-first B: %+v", report.Attempts)
	}
	if report.Attempts[2].KeyHit {
		t.Fatalf("B survived A's later completion: %+v", report.Attempts[2])
	}
}

func TestReplayAdmitsOnlyAfterValidMissThreshold(t *testing.T) {
	var pub [32]byte
	pub[0] = 7
	trace := passiveTrace([]Verification{
		verificationWithKey(1, 1, 4, pub, OutcomeValid, "same"),
		verificationWithKey(2, 2, 3, pub, OutcomeInvalid, "bad"),
		verificationWithKey(3, 5, 6, pub, OutcomeValid, "same"),
		verificationWithKey(4, 7, 8, pub, OutcomeValid, "hit"),
	})
	trace.Verifications[2].Signature = trace.Verifications[0].Signature
	report, err := Replay(trace, ReplayConfig{
		MaxKeyEntries:     4,
		MaxTableBytes:     4 * 1024,
		TableBytesPerKey:  1024,
		AdmitAfterValid:   2,
		DuplicateCapacity: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Keys.ValidMissCompletions != 2 || report.Keys.InvalidCompletions != 1 || report.Keys.Admissions != 1 || report.Keys.Hits != 1 {
		t.Fatalf("key stats=%+v", report.Keys)
	}
	if report.Attempts[0].Admitted || report.Attempts[1].Admitted || !report.Attempts[2].Admitted || !report.Attempts[3].KeyHit {
		t.Fatalf("decisions=%+v", report.Attempts)
	}
	if report.Duplicates.Hits != 1 || !report.Attempts[2].DuplicateHit {
		t.Fatalf("duplicate stats=%+v decisions=%+v", report.Duplicates, report.Attempts)
	}
}

func TestReplayReportsRejectedOversizeAdmissionAndTruncation(t *testing.T) {
	trace := passiveTrace([]Verification{verification(2, 1, 2, 1, OutcomeValid, 1)})
	trace.Summary.VerificationAttempts = 2
	trace.Verifications[0].Sequence = 2
	report, err := Replay(trace, ReplayConfig{MaxTableBytes: 10, TableBytesPerKey: 11, AdmitAfterValid: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !report.TruncatedPrefix || report.Keys.RetainedBuilds != 1 || report.Keys.RejectedAdmissions != 1 || report.Keys.ResidentEntries != 0 {
		t.Fatalf("report=%+v", report)
	}
}

func TestInferSIMDReconstructsHolesTailsMasksAndKeys(t *testing.T) {
	trace := simulationTraceWithHole()
	policy, err := Replay(trace, ReplayConfig{MaxKeyEntries: 2, TableBytesPerKey: 1024, AdmitAfterValid: 1, DuplicateCapacity: 8})
	if err != nil {
		t.Fatal(err)
	}
	x4, err := InferSIMD(trace, 4, &policy)
	if err != nil {
		t.Fatal(err)
	}
	if len(x4.Groups) != 2 || x4.FullGroups != 1 || x4.TailGroups != 1 || x4.IncompleteGroups != 1 || x4.MissingLanes != 2 || x4.InvalidLanes != 1 {
		t.Fatalf("x4 report=%+v", x4)
	}
	first := x4.Groups[0]
	if first.ActiveMask != 0x0f || first.ObservedMask != 0x09 || first.MissingMask != 0x06 || first.InvalidMask != 0x01 || first.ValidMask != 0x08 || first.JobStartMask != 0x09 || first.Complete {
		t.Fatalf("first x4 group=%+v", first)
	}
	if first.Slots[1].State != SlotMissing || first.Slots[2].State != SlotMissing || first.Slots[3].JobIndex != 1 || first.Slots[3].LaneIndex != 0 {
		t.Fatalf("first slots=%+v", first.Slots)
	}
	second := x4.Groups[1]
	if !second.Tail || second.ActiveMask != 0x01 || second.ObservedMask != 0x01 || !second.Complete || second.Slots[1].State != SlotInactive {
		t.Fatalf("second x4 group=%+v", second)
	}
	x8, err := InferSIMD(trace, 8, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(x8.Groups) != 1 || !x8.Groups[0].Tail || x8.Groups[0].ActiveMask != 0x1f || x8.Groups[0].MissingMask != 0x06 {
		t.Fatalf("x8 report=%+v", x8)
	}
}

func TestInferSIMDRefusesPassiveAndUnreadyTraces(t *testing.T) {
	passive := passiveTrace([]Verification{verification(1, 1, 2, 1, OutcomeValid, 1)})
	if _, err := InferSIMD(passive, 4, nil); !errors.Is(err, ErrBatchInferenceUnavailable) {
		t.Fatalf("passive inference error=%v", err)
	}
	if _, err := InferSIMD(simulationTraceWithHole(), 5, nil); err == nil {
		t.Fatal("width 5 inferred")
	}
	unready := simulationTraceWithHole()
	unready.Dispatches[0].ReadyEventSequence = 0
	unready.Dispatches[0].SignatureLanes = 0
	unready.Dispatches[0].JobSignatures = nil
	for index := range unready.Verifications {
		unready.Verifications[index].DispatchID = 0
		unready.Verifications[index].JobIndex = 0
		unready.Verifications[index].LaneIndex = 0
	}
	if _, err := InferSIMD(unready, 4, nil); err == nil {
		t.Fatal("unready dispatch inferred")
	}
}

func FuzzParseV3(f *testing.F) {
	zeroPub := strings.Repeat("00", 32)
	zeroSig := strings.Repeat("00", 64)
	seed := fmt.Sprintf("%s\n%s\n",
		`{"type":"summary","schema":"mithril-sigverify-v3","enabled":true,"collection_mode":"passive","capacity":1,"verification_attempts":1,"observed_events":2,"retained_verifications":1,"retained_dispatches":0}`,
		fmt.Sprintf(`{"type":"verification","sequence":1,"begin_event_sequence":1,"completion_event_sequence":2,"source":"replay","public_key":"%s","signature":"%s","message_base64":"","exact_duplicate":false,"public_key_reused":false,"reuse_distance":0,"outcome":"valid","dispatch_id":0,"job_index":0,"lane_index":0}`, zeroPub, zeroSig))
	f.Add([]byte(seed))
	f.Add([]byte("not json"))
	f.Fuzz(func(t *testing.T, input []byte) {
		trace, err := Parse(bytes.NewReader(input))
		if err != nil {
			return
		}
		if err := trace.Validate(); err != nil {
			t.Fatalf("Parse returned invalid trace: %v", err)
		}
		if _, err := Replay(trace, ReplayConfig{}); err != nil && trace.Summary.Enabled {
			t.Fatalf("disabled-policy replay of parsed trace: %v", err)
		}
	})
}

func passiveTrace(verifications []Verification) *Trace {
	trace := &Trace{Verifications: verifications}
	finalizeTrace(trace, ModePassive)
	return trace
}

func simulationTraceWithHole() *Trace {
	var pubA, pubB [32]byte
	pubA[0], pubB[0] = 1, 2
	trace := &Trace{
		Verifications: []Verification{
			verificationWithKey(1, 3, 4, pubA, OutcomeInvalid, "job0-invalid"),
			verificationWithKey(2, 5, 6, pubA, OutcomeValid, "job1-a"),
			verificationWithKey(3, 7, 8, pubB, OutcomeValid, "job1-b"),
		},
		Dispatches: []Dispatch{{
			DispatchID:         1,
			ClaimEventSequence: 1,
			ReadyEventSequence: 2,
			Mode:               ModeSchedulingSimulation,
			Source:             "replay",
			SignatureLanes:     5,
			JobSignatures:      []uint16{3, 2},
		}},
	}
	trace.Verifications[0].DispatchID = 1
	trace.Verifications[0].JobIndex = 0
	trace.Verifications[0].LaneIndex = 0
	trace.Verifications[1].DispatchID = 1
	trace.Verifications[1].JobIndex = 1
	trace.Verifications[1].LaneIndex = 0
	trace.Verifications[2].DispatchID = 1
	trace.Verifications[2].JobIndex = 1
	trace.Verifications[2].LaneIndex = 1
	finalizeTrace(trace, ModeSchedulingSimulation)
	return trace
}

func verification(sequence, begin, completion uint64, marker byte, outcome Outcome, distance uint64) Verification {
	var pub [32]byte
	pub[0] = marker
	result := verificationWithKey(sequence, begin, completion, pub, outcome, fmt.Sprintf("message-%d", marker))
	if distance > 1 {
		result.PublicKeyReused = true
		result.ReuseDistance = distance
	}
	return result
}

func verificationWithKey(sequence, begin, completion uint64, pub [32]byte, outcome Outcome, message string) Verification {
	var sig [64]byte
	sig[0] = byte(sequence)
	return Verification{
		Sequence:                sequence,
		BeginEventSequence:      begin,
		CompletionEventSequence: completion,
		Source:                  "replay",
		PublicKey:               pub,
		Signature:               sig,
		Message:                 []byte(message),
		Outcome:                 outcome,
	}
}

func finalizeTrace(trace *Trace, mode CollectionMode) {
	var observed uint64
	for _, verification := range trace.Verifications {
		if verification.BeginEventSequence > observed {
			observed = verification.BeginEventSequence
		}
		if verification.CompletionEventSequence > observed {
			observed = verification.CompletionEventSequence
		}
	}
	for _, dispatch := range trace.Dispatches {
		if dispatch.ClaimEventSequence > observed {
			observed = dispatch.ClaimEventSequence
		}
		if dispatch.ReadyEventSequence > observed {
			observed = dispatch.ReadyEventSequence
		}
	}
	capacity := len(trace.Verifications)
	if len(trace.Dispatches) > capacity {
		capacity = len(trace.Dispatches)
	}
	if capacity == 0 {
		capacity = 1
	}
	trace.Summary = Summary{
		Type:                  "summary",
		Schema:                SchemaV3,
		Enabled:               true,
		Mode:                  mode,
		Capacity:              uint64(capacity),
		VerificationAttempts:  uint64(len(trace.Verifications)),
		ObservedEvents:        observed,
		RetainedVerifications: len(trace.Verifications),
		RetainedDispatches:    len(trace.Dispatches),
	}
}

func encodeTrace(t *testing.T, trace *Trace) string {
	t.Helper()
	verifications, dispatches := wireRecords(trace)
	return encodeWire(t, trace.Summary, verifications, dispatches)
}

func wireRecords(trace *Trace) ([]verificationJSON, []dispatchJSON) {
	verifications := make([]verificationJSON, len(trace.Verifications))
	for index, verification := range trace.Verifications {
		verifications[index] = verificationJSON{
			Type:                    "verification",
			Sequence:                verification.Sequence,
			BeginEventSequence:      verification.BeginEventSequence,
			CompletionEventSequence: verification.CompletionEventSequence,
			Source:                  verification.Source,
			PublicKey:               hex.EncodeToString(verification.PublicKey[:]),
			Signature:               hex.EncodeToString(verification.Signature[:]),
			Message:                 base64.StdEncoding.EncodeToString(verification.Message),
			ExactDuplicate:          verification.ExactDuplicate,
			PublicKeyReused:         verification.PublicKeyReused,
			ReuseDistance:           verification.ReuseDistance,
			Outcome:                 outcomeString(verification.Outcome),
			DispatchID:              verification.DispatchID,
			JobIndex:                verification.JobIndex,
			LaneIndex:               verification.LaneIndex,
		}
	}
	dispatches := make([]dispatchJSON, len(trace.Dispatches))
	for index, dispatch := range trace.Dispatches {
		dispatches[index] = dispatchJSON{
			Type:               "dispatch",
			DispatchID:         dispatch.DispatchID,
			ClaimEventSequence: dispatch.ClaimEventSequence,
			ReadyEventSequence: dispatch.ReadyEventSequence,
			Mode:               dispatch.Mode,
			Source:             dispatch.Source,
			QueuedItemsBefore:  dispatch.QueuedItemsBefore,
			QueuedItemsAfter:   dispatch.QueuedItemsAfter,
			SignatureLanes:     dispatch.SignatureLanes,
			JobSignatures:      append([]uint16(nil), dispatch.JobSignatures...),
		}
	}
	return verifications, dispatches
}

func encodeWire(t *testing.T, summary Summary, verifications []verificationJSON, dispatches []dispatchJSON) string {
	t.Helper()
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	if err := encoder.Encode(summary); err != nil {
		t.Fatal(err)
	}
	for _, verification := range verifications {
		if err := encoder.Encode(verification); err != nil {
			t.Fatal(err)
		}
	}
	for _, dispatch := range dispatches {
		if err := encoder.Encode(dispatch); err != nil {
			t.Fatal(err)
		}
	}
	return output.String()
}

func outcomeString(outcome Outcome) string {
	switch outcome {
	case OutcomeValid:
		return "valid"
	case OutcomeInvalid:
		return "invalid"
	default:
		return "unknown"
	}
}

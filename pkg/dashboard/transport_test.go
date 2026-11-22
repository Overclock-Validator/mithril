package dashboard

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestPublicTurbineDiagnosticsPreserveUnknownZeroAndSessionScope(t *testing.T) {
	at := time.Now().UTC()
	s := publicTestSnapshot(at)
	turbine := s.BlockSource.Turbine
	turbine.Diagnostics = nil
	if p := publicTurbineDiagnostics(turbine); p != nil {
		t.Fatal("old source acquired fabricated diagnostics")
	}
	d := &TurbineDiagnostics{BlockEmissionAttempts: 1<<53 + 1, RecoveredDataShreds: 2, AssemblyErrors: 3,
		ActiveSlots: 4, EvictedSlots: 5, IgnoredOldShreds: 6, SignatureVerifications: 7, SignatureCacheHits: 8,
		RepairOutstanding: 9, RepairPeers: 10, RepairRespondingPeers: 11}
	turbine.Diagnostics = d
	turbine.ParseErrors, turbine.UsefulRepairShreds, turbine.SpooledRepairShreds = 12, 13, 14
	read := func(state string, now time.Time) PublicSnapshot {
		t.Helper()
		b, err := encodePublicSnapshot(&s, state, now, nil, nil, nil, nil)
		if err != nil || len(b) > MaxSnapshotBytes {
			t.Fatalf("projection: bytes=%d err=%v", len(b), err)
		}
		if !strings.Contains(string(b), `"block_emission_attempts":"9007199254740993"`) && state != "unavailable" && s.BlockSource.LiveConnected {
			t.Fatal("transport counter lost precision")
		}
		var p PublicSnapshot
		if err := json.Unmarshal(b, &p); err != nil {
			t.Fatal(err)
		}
		return p
	}
	p := read("fresh", at).Input.Turbine.Diagnostics
	if p.ObservedHeaderSlot != nil || p.ReconstructedSlot != nil || p.ParseErrors != 12 || p.UsefulRepairShreds != 13 || p.SpooledRepairShreds != 14 || p.Repair == nil || p.Repair.Outstanding != 9 || p.Repair.RespondingPeers != 11 {
		t.Fatalf("unknown/frontier or counter mapping: %+v", p)
	}
	zero, max := uint64(0), uint64(math.MaxUint64)
	d.ObservedHeaderSlot, d.ReconstructedSlot = &zero, &zero
	p = read("fresh", at).Input.Turbine.Diagnostics
	if p.ObservedHeaderSlot == nil || *p.ObservedHeaderSlot != 0 || p.ReconstructedSlot == nil || *p.ReconstructedSlot != 0 {
		t.Fatal("known zero was erased")
	}
	d.ObservedHeaderSlot = &max
	copy := publicTurbineDiagnostics(turbine)
	max = 10
	if *copy.ObservedHeaderSlot != math.MaxUint64 {
		t.Fatal("projection aliases the producer's frontier")
	}
	turbine.Repair = nil
	if p := read("fresh", at).Input.Turbine.Diagnostics; p.Repair != nil {
		t.Fatal("disabled repair acquired active counters")
	}
	if p := read("stale", at.Add(time.Minute)); p.Input.Turbine.State != "stale" || p.Input.Turbine.Diagnostics == nil {
		t.Fatal("historical diagnostics were relabelled current or erased")
	}
	s.BlockSource.LiveConnected = false
	if p := read("fresh", at); p.Input.Turbine != nil {
		t.Fatal("disconnected receiver retained an available sample")
	}
	if p := read("unavailable", at); p.Input != nil {
		t.Fatal("unavailable source retained diagnostics")
	}
}

func TestPeerPayloadBytesOmitUnknownAndDetachExactValues(t *testing.T) {
	private := &Turbine{Diagnostics: &TurbineDiagnostics{}}
	zero, max := uint64(0), uint64(math.MaxUint64)
	for _, value := range []*uint64{nil, &zero, &max} {
		private.Diagnostics.PeerPayloadBytes = value
		public := publicTurbineDiagnostics(private)
		for _, document := range []any{private.Diagnostics, public} {
			body, err := json.Marshal(document)
			if err != nil {
				t.Fatal(err)
			}
			if value == nil {
				if strings.Contains(string(body), `"peer_payload_bytes"`) {
					t.Fatal("unknown relay traffic was fabricated")
				}
			} else if !strings.Contains(string(body), `"peer_payload_bytes":"`+strconv.FormatUint(*value, 10)+`"`) {
				t.Fatalf("peer bytes lost precision: %s", body)
			}
		}
		if value != nil {
			want := *value
			*value ^= 1
			if *public.PeerPayloadBytes != want {
				t.Fatal("public peer bytes alias their private source")
			}
		}
	}
}

func TestTurbineAdmissionOmitUnknownAndDetachExactValues(t *testing.T) {
	private := &Turbine{Diagnostics: &TurbineDiagnostics{}}
	if public := publicTurbineDiagnostics(private); public.Admission != nil {
		t.Fatal("older source acquired admission counts")
	}
	private.Diagnostics.Admission = &TurbineAdmission{MissingLeaders: 0, ShredVersionMismatch: math.MaxUint64}
	public := publicTurbineDiagnostics(private)
	body, err := json.Marshal(public)
	if err != nil || !strings.Contains(string(body), `"missing_leaders":"0"`) || !strings.Contains(string(body), `"shred_version_mismatch":"18446744073709551615"`) {
		t.Fatalf("incorrect admission encoding: %s, %v", body, err)
	}
	private.Diagnostics.Admission.ShredVersionMismatch = 1
	if public.Admission.ShredVersionMismatch != math.MaxUint64 {
		t.Fatal("public admission counters alias their private source")
	}
}

func TestPublicTurbineDiagnosticsRejectInvalidGaugesAndFrontiers(t *testing.T) {
	zero, one := uint64(0), uint64(1)
	for _, d := range []TurbineDiagnostics{
		{ActiveSlots: -1}, {ReconstructedSlot: &zero}, {ObservedHeaderSlot: &zero, ReconstructedSlot: &one},
	} {
		if publicTurbineDiagnostics(&Turbine{Diagnostics: &d}) != nil {
			t.Fatalf("invalid diagnostic survived: %+v", d)
		}
	}
	for _, d := range []TurbineDiagnostics{{RepairOutstanding: -1}, {RepairPeers: -1}, {RepairRespondingPeers: -1}} {
		p := publicTurbineDiagnostics(&Turbine{Diagnostics: &d, Repair: &Repair{Requests: 3}})
		if p == nil || p.Repair != nil {
			t.Fatalf("invalid repair gauge or unrelated data handling: %+v", p)
		}
	}
}

func TestPublicTurbineObservationRejectsInvalidLifecycle(t *testing.T) {
	at := time.Now().UTC()
	for _, mutate := range []func(*Snapshot){
		func(s *Snapshot) { s.BlockSource.Turbine.Session = 0 },
		func(s *Snapshot) { s.BlockSource.Turbine.StartedAt = at.Add(time.Second) },
		func(s *Snapshot) { s.BlockSource.Turbine.StartedAt = s.StartedAt.Add(-time.Second) },
		func(s *Snapshot) { s.BlockSource.StartedAt = time.Time{} },
		func(s *Snapshot) { s.BlockSource.StartedAt = s.StartedAt.Add(-time.Second) },
		func(s *Snapshot) { s.BlockSource.SampledAt = s.StartedAt.Add(-time.Second) },
		func(s *Snapshot) { s.BlockSource.SampledAt = at.Add(2 * time.Second) },
		func(s *Snapshot) { s.BlockSource.Turbine.SampledAt = s.StartedAt.Add(-time.Second) },
	} {
		s := publicTestSnapshot(at)
		mutate(&s)
		p, _ := publicValue(t, &s, "fresh", at)
		if p.Input != nil && p.Input.Turbine != nil {
			t.Fatalf("invalid receiver lifecycle survived: %+v", s.BlockSource)
		}
	}
	// A valid older sample in the same source/session is retained as historical,
	// rather than being forced to match the new feed heartbeat.
	s := publicTestSnapshot(at)
	s.BlockSource.StartedAt = at.Add(-time.Minute)
	s.BlockSource.Turbine.StartedAt = at.Add(-time.Minute)
	s.BlockSource.Turbine.SampledAt = at.Add(-40 * time.Second)
	p, _ := publicValue(t, &s, "fresh", at)
	if p.Input.Turbine == nil || p.Input.Turbine.State != "stale" {
		t.Fatal("valid old receiver observation was erased or relabelled fresh")
	}
}

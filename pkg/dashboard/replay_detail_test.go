package dashboard

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func replayDetailTestCore(count int) ReplayDetailCore {
	at := time.Date(2026, 10, 31, 12, 0, 0, 999999999, time.UTC)
	c := ReplayDetailCore{
		SchemaVersion: 1, Family: "replay", Scope: "replay_row_history",
		Binding:    ReplayDetailBinding{RunID: "detail-test", NodeStartedAt: at, SourceGeneration: 1, Attempt: 2, Rewinds: 3, AttemptStartedAt: at, ResetAt: at},
		CapturedAt: at, UpdatedAt: at, Capacity: ReplayDetailCapacity, Count: count,
		Records: make([]ReplayDetailRecord, count),
	}
	for i := range c.Records {
		c.Records[i] = ReplayDetailRecord{
			Sequence: uint64(i + 1), CompletedIndex: uint64(10 + i*2), Attempt: 2, Rewinds: 3,
			Bankhash:    "CktRuQ2mttgRGkXJtyksdKHjUdc2C4TgDzyB98oEzy8",
			CompletedAt: at, ObservedAt: at, Scope: "replay_row",
			SharedExecutionTimingsUnavailable: true,
			Streaming:                         ReplayDetailStreaming{LastDiscardReason: "none", AttemptBreakdownUnavailable: true},
		}
		c.Records[i].AccountLoader.InProgressLookup = &ReplayDetailTiming{}
		c.Records[i].AccountLoader.SysvarInProgressLookup = &ReplayDetailTiming{}
		c.Records[i].AccountLoader.InProgressHits = new(uint64)
		c.Records[i].AccountLoader.SysvarInProgressHits = new(uint64)
	}
	if count > 0 {
		c.FirstSequence, c.LastSequence = 1, uint64(count)
	}
	return c
}

func TestReplayDetailCoreValidation(t *testing.T) {
	for _, count := range []int{0, 1, ReplayDetailCapacity} {
		if err := ValidateReplayDetailCore(replayDetailTestCore(count)); err != nil {
			t.Fatal(err)
		}
	}
	tests := []struct {
		name   string
		change func(*ReplayDetailCore)
	}{
		{"schema", func(c *ReplayDetailCore) { c.SchemaVersion = 3 }},
		{"family", func(c *ReplayDetailCore) { c.Family = "other" }},
		{"scope", func(c *ReplayDetailCore) { c.Scope = "bank" }},
		{"empty run", func(c *ReplayDetailCore) { c.Binding.RunID = "" }},
		{"long run", func(c *ReplayDetailCore) { c.Binding.RunID = strings.Repeat("a", 129) }},
		{"invalid UTF8", func(c *ReplayDetailCore) { c.Binding.RunID = string([]byte{255}) }},
		{"generation", func(c *ReplayDetailCore) { c.Binding.SourceGeneration = 0 }},
		{"attempt", func(c *ReplayDetailCore) { c.Binding.Attempt = 0 }},
		{"missing node date", func(c *ReplayDetailCore) { c.Binding.NodeStartedAt = time.Time{} }},
		{"missing attempt date", func(c *ReplayDetailCore) { c.Binding.AttemptStartedAt = time.Time{} }},
		{"missing reset date", func(c *ReplayDetailCore) { c.Binding.ResetAt = time.Time{} }},
		{"missing capture date", func(c *ReplayDetailCore) { c.CapturedAt = time.Time{} }},
		{"missing update date", func(c *ReplayDetailCore) { c.UpdatedAt = time.Time{} }},
		{"out of range date", func(c *ReplayDetailCore) { c.UpdatedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC) }},
		{"non UTC date", func(c *ReplayDetailCore) { c.CapturedAt = c.CapturedAt.In(time.FixedZone("offset", 3600)) }},
		{"bad genesis", func(c *ReplayDetailCore) { s := "x"; c.Binding.GenesisHash = &s }},
		{"capacity", func(c *ReplayDetailCore) { c.Capacity = 257 }},
		{"excess count", func(c *ReplayDetailCore) { c.Count = 257 }},
		{"missing records", func(c *ReplayDetailCore) { c.Records = nil }},
		{"wrong count", func(c *ReplayDetailCore) { c.Count = 1 }},
		{"first sequence", func(c *ReplayDetailCore) { c.FirstSequence = 0 }},
		{"last sequence", func(c *ReplayDetailCore) { c.LastSequence = 0 }},
		{"eviction range", func(c *ReplayDetailCore) { c.Evicted = 1 }},
		{"early eviction", func(c *ReplayDetailCore) {
			c.FirstSequence, c.LastSequence, c.Evicted = 4, 5, 3
			c.Records[0].Sequence, c.Records[1].Sequence = 4, 5
		}},
		{"record sequence", func(c *ReplayDetailCore) { c.Records[1].Sequence = 1 }},
		{"completion duplicate", func(c *ReplayDetailCore) { c.Records[1].CompletedIndex = c.Records[0].CompletedIndex }},
		{"zero completion", func(c *ReplayDetailCore) { c.Records[0].CompletedIndex = 0 }},
		{"record attempt", func(c *ReplayDetailCore) { c.Records[1].Attempt++ }},
		{"record rewind", func(c *ReplayDetailCore) { c.Records[1].Rewinds++ }},
		{"bad bank hash", func(c *ReplayDetailCore) { c.Records[1].Bankhash = "not-a-hash" }},
		{"zero bank hash", func(c *ReplayDetailCore) { c.Records[1].Bankhash = strings.Repeat("1", 32) }},
		{"noncanonical hash", func(c *ReplayDetailCore) { c.Records[1].Bankhash = strings.Repeat("1", 33) }},
		{"completed date", func(c *ReplayDetailCore) { c.Records[1].CompletedAt = time.Time{} }},
		{"observed date", func(c *ReplayDetailCore) { c.Records[1].ObservedAt = time.Time{} }},
		{"row scope", func(c *ReplayDetailCore) { c.Records[1].Scope = "bank" }},
		{"shared coverage", func(c *ReplayDetailCore) { c.Records[1].SharedExecutionTimingsUnavailable = false }},
		{"attempt coverage", func(c *ReplayDetailCore) { c.Records[1].Streaming.AttemptBreakdownUnavailable = false }},
		{"unknown reason", func(c *ReplayDetailCore) { c.Records[1].Streaming.LastDiscardReason = "private error" }},
		{"missing discard", func(c *ReplayDetailCore) { c.Records[1].Streaming.LastDiscardReason = "timeout" }},
		{"missing reason", func(c *ReplayDetailCore) { c.Records[1].Streaming.DiscardObserved = true }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := replayDetailTestCore(2)
			tt.change(&c)
			if _, err := ReplayDetailGeneration(c); err == nil {
				t.Fatal("invalid core accepted")
			}
		})
	}
	empty := replayDetailTestCore(0)
	body, err := json.Marshal(empty)
	if err != nil || !strings.Contains(string(body), `"records":[]`) {
		t.Fatal("initialized empty history lost")
	}
	for _, field := range []*uint64{&empty.FirstSequence, &empty.LastSequence, &empty.Evicted} {
		*field = 1
		if err := ValidateReplayDetailCore(empty); err == nil {
			t.Fatal("invalid empty range accepted")
		}
		*field = 0
	}
}

func TestReplayDetailRetentionLimitsOmissionAndKnownDisabledChecks(t *testing.T) {
	c := replayDetailTestCore(1)
	old, err := json.Marshal(c.Records[0])
	if err != nil || strings.Contains(string(old), "retention_limits") {
		t.Fatal("absent owner changed legacy row bytes")
	}
	for _, n := range []uint64{0, math.MaxUint64} {
		c.Records[0].RetentionLimits = ReplayDetailRetentionLimits{Observed: true, RetainedBytesPauseAt: n, HeldSlotsHaltAbove: n}
		if ValidateReplayDetailCore(c) != nil {
			t.Fatal("known zero-disabled or full-width limits rejected")
		}
		body, err := json.Marshal(c.Records[0])
		var decoded ReplayDetailRecord
		if err != nil || !strings.Contains(string(body), `"retention_limits":{"observed":true`) || json.Unmarshal(body, &decoded) != nil || !replayDetailRecordsEqual(decoded, c.Records[0]) {
			t.Fatal("value-only retention limits changed width or comparability")
		}
		copy := decoded
		copy.RetentionLimits.HeldSlotsHaltAbove = 7
		if !replayDetailRecordsEqual(decoded, c.Records[0]) {
			t.Fatal("copied retention configuration aliases another record")
		}
	}
	c.Records[0].RetentionLimits.Observed = false
	if ValidateReplayDetailCore(c) == nil {
		t.Fatal("unobserved nonzero budgets accepted")
	}
}

func TestReplayDetailRetentionLimitsStrictPrivateWire(t *testing.T) {
	c := replayDetailTestCore(1)
	c.Records[0].RetentionLimits = ReplayDetailRetentionLimits{Observed: true, RetainedBytesPauseAt: 0, HeldSlotsHaltAbove: math.MaxUint64}
	p := replayDetailTestPrepared(t, c)
	m, err := decodeReplayDetailManifest([]byte(p.manifests[0]))
	if err != nil {
		t.Fatal(err)
	}
	page, err := decodeReplayDetailPage([]byte(p.pages[0]), m, 0)
	if err != nil || !replayDetailRecordsEqual(page.Records[0], c.Records[0]) {
		t.Fatal("actual optional value child failed canonical transport")
	}
	child := `"retention_limits":{"observed":true,"retained_bytes_pause_at":"0","held_slots_halt_above":"18446744073709551615"}`
	for _, replacement := range []string{
		`"retention_limits":null`, `"retention_limits":{}`, `"retention_limits":{"observed":true}`,
		strings.Replace(child, `"observed":true`, `"observed":false`, 1),
		strings.Replace(child, `"observed":true,`, "", 1),
		strings.Replace(child, `"observed":true`, `"observed":true,"observed":true`, 1),
		strings.Replace(child, `"observed":true`, `"observed":true,"path":"private"`, 1),
		strings.Replace(child, `"retained_bytes_pause_at":"0"`, `"retained_bytes_pause_at":0`, 1),
	} {
		body := strings.Replace(p.pages[0], child, replacement, 1)
		if body == p.pages[0] {
			t.Fatal("negative wire control did not modify the actual child")
		}
		if _, err := decodeReplayDetailPage([]byte(body), m, 0); err == nil {
			t.Fatal("noncanonical optional retention child accepted")
		}
	}
}

func TestReplayDetailGenerationBindsCaptureAndEveryRow(t *testing.T) {
	c := replayDetailTestCore(256)
	original, err := ReplayDetailGeneration(c)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	expected := sha256.Sum256(body)
	if original != hex.EncodeToString(expected[:]) {
		t.Fatal("digest differs from exact typed core")
	}
	var received ReplayDetailCore
	if err := json.Unmarshal(body, &received); err != nil {
		t.Fatal(err)
	}
	copied, err := ReplayDetailGeneration(received)
	if err != nil || copied != original {
		t.Fatal("reader reconstruction changed digest")
	}
	c.CapturedAt = c.CapturedAt.Add(10 * time.Second)
	later, err := ReplayDetailGeneration(c)
	if err != nil || later == original {
		t.Fatal("idle capture reused generation")
	}
	stable, err := ReplayDetailGeneration(c)
	if err != nil || stable != later {
		t.Fatal("identical core changed generation")
	}
	received.Records[255].AccountLoader.SysvarCachePublication.SumNanoseconds++
	changed, err := ReplayDetailGeneration(received)
	if err != nil || changed == original {
		t.Fatal("last-row mutation retained old token")
	}
}

func TestReplayDetailSourceBoundariesAndWallDates(t *testing.T) {
	c := replayDetailTestCore(256)
	c.Binding.RunID = strings.Repeat("<", 128)
	c.FirstSequence, c.LastSequence, c.Evicted = math.MaxUint64-255, math.MaxUint64, math.MaxUint64-256
	for i := range c.Records {
		c.Records[i].Sequence = c.FirstSequence + uint64(i)
		c.Records[i].CompletedIndex = c.Records[i].Sequence
	}
	// Historical wall dates can reverse; this layer must not manufacture durations.
	c.Binding.NodeStartedAt = c.CapturedAt.Add(time.Hour)
	c.Binding.AttemptStartedAt = c.CapturedAt.Add(-time.Hour)
	c.Binding.ResetAt = c.CapturedAt.Add(2 * time.Hour)
	c.UpdatedAt = c.CapturedAt.Add(-2 * time.Hour)
	c.Records[1].ObservedAt = c.Records[0].ObservedAt.Add(-time.Hour)
	c.Records[1].Slot = 0
	c.Exhausted = true
	if _, err := ReplayDetailGeneration(c); err != nil {
		t.Fatal(err)
	}
	for _, reason := range []string{"other_block", "parent_changed", "fork_switch", "timeout", "cancelled", "generation_gone", "input_rejected", "finalization", "shutdown", "other"} {
		c.Records[0].Streaming.DiscardObserved = true
		c.Records[0].Streaming.LastDiscardReason = reason
		if err := ValidateReplayDetailCore(c); err != nil {
			t.Fatal(reason, err)
		}
	}
	c.Records[0].Timings.SlotReplay = ReplayDetailTiming{Count: 0, SumNanoseconds: math.MaxUint64}
	// Raw row counters are not ratios, balanced phases or accepted transaction counts.
	c.Records[0].Streaming.RecordedTransactions = 13
	c.Records[0].Transactions = 8
	if err := ValidateReplayDetailCore(c); err != nil {
		t.Fatal(err)
	}
}

func TestReplayDetailFullWidthCoreBudget(t *testing.T) {
	c := replayDetailTestCore(256)
	maximumReplayOptionalMetadata(&c)
	c.Binding.RunID = strings.Repeat("<", 128)
	c.Binding.SourceGeneration, c.Binding.Attempt, c.Binding.Rewinds = math.MaxUint64, math.MaxUint64, math.MaxUint64
	hash := "JEKNVnkbo3jma5nREBBJCDoXFVeKkD56V3xKrvRmWxFG"
	c.Binding.GenesisHash = &hash
	c.FirstSequence, c.LastSequence, c.Evicted = math.MaxUint64-255, math.MaxUint64, math.MaxUint64-256
	var set func(reflect.Value)
	set = func(v reflect.Value) {
		if v.Type() == reflect.TypeFor[time.Time]() {
			return
		}
		switch v.Kind() {
		case reflect.Pointer:
			if !v.IsNil() {
				set(v.Elem())
			}
		case reflect.Struct:
			for i := 0; i < v.NumField(); i++ {
				set(v.Field(i))
			}
		case reflect.Uint64:
			v.SetUint(math.MaxUint64)
		case reflect.Bool:
			v.SetBool(true)
		}
	}
	for i := range c.Records {
		set(reflect.ValueOf(&c.Records[i]).Elem())
		c.Records[i].Sequence = c.FirstSequence + uint64(i)
		c.Records[i].CompletedIndex = c.Records[i].Sequence
		c.Records[i].Bankhash = hash
		c.Records[i].Streaming.LastDiscardReason = "generation_gone"
		c.Records[i].WorkCountersSaturated = false
		c.Records[i].UnknownStages = false
		c.Records[i].Streaming.OpenedObserved = false
	}
	if _, err := ReplayDetailGeneration(c); err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if len(body) > 4<<20 || len(c.Records) != 256 {
		t.Fatal("full history exceeded family budget or lost records")
	}
	row, err := json.Marshal(c.Records[0])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(row), `"count":"18446744073709551615"`) ||
		!strings.Contains(string(row), `"sum_nanoseconds":"18446744073709551615"`) ||
		len(c.CapturedAt.Format(time.RFC3339Nano)) != 30 {
		t.Fatal("width fixture lost u64 or timestamp precision")
	}
	t.Logf("full-width rows=%d row_bytes=%d core_bytes=%d", len(c.Records), len(row), len(body))
}

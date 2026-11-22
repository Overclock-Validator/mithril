package dashboard

import (
	"bytes"
	"encoding/json"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestReplayDetailWireMetadataExtractionDigest(t *testing.T) {
	// Canonical v1 fixtures, including the two replay-row planner markers.
	for _, test := range []struct {
		count int
		want  string
	}{
		{0, "248f70cd88374e90e37c7f810ab381025effdca56ac053f1874247217e377975"},
		{1, "0707fe82954d2812ef4bc3cf1f5e39ced71590776a50264373b204150fa37f11"},
		{3, "8f180247df07940b2476eb298f945a5db31f431b00f5a5c30632663473960d58"},
		{256, "1466daa4aec6fad065e1558ea0a134ecd281fae4d7f286a7eef4018fb08f680d"},
	} {
		got, err := ReplayDetailGeneration(replayDetailTestCore(test.count))
		if err != nil || got != test.want {
			t.Fatalf("count=%d generation changed: %s %v", test.count, got, err)
		}
	}
}

func replayDetailWireFixture(t *testing.T, count int) (*replayDetailPrepared, ReplayDetailManifest) {
	t.Helper()
	p := replayDetailTestPrepared(t, replayDetailTestCore(count))
	m, err := decodeReplayDetailManifest([]byte(p.manifests[0]))
	if err != nil {
		t.Fatal(err)
	}
	return p, m
}

func replayDetailWireJSON(t *testing.T, value any) []byte {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func TestReplayDetailWireCompleteCollections(t *testing.T) {
	for _, count := range []int{0, 1, 3, 256} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			p, m := replayDetailWireFixture(t, count)
			for _, body := range p.manifests {
				if _, err := decodeReplayDetailManifest([]byte(body)); err != nil {
					t.Fatal(err)
				}
			}
			records := make([]ReplayDetailRecord, 0, count)
			for i, body := range p.pages {
				page, err := decodeReplayDetailPage([]byte(body), m, i)
				if err != nil {
					t.Fatal(err)
				}
				records = append(records, page.Records...)
			}
			core, err := replayDetailCoreFromRecords(m, records)
			if err != nil || !reflect.DeepEqual(core, replayDetailTestCore(count)) {
				t.Fatalf("complete collection changed: count=%d error=%v", count, err)
			}
			if count == 0 {
				if _, err := decodeReplayDetailPage([]byte(`{}`), m, 0); err == nil {
					t.Fatal("initialized empty generation accepted a page")
				}
				if _, err := replayDetailCoreFromRecords(m, nil); err == nil {
					t.Fatal("nil rows became a known empty collection")
				}
			}
		})
	}
}

func TestReplayDetailWireCanonicalMembers(t *testing.T) {
	p, m := replayDetailWireFixture(t, 1)
	page, err := decodeReplayDetailPage([]byte(p.pages[0]), m, 0)
	if err != nil {
		t.Fatal(err)
	}
	retentionCore := replayDetailTestCore(1)
	retentionCore.Records[0].RetentionLimits = ReplayDetailRetentionLimits{Observed: true, RetainedBytesPauseAt: 65536}
	retentionPrepared := replayDetailTestPrepared(t, retentionCore)
	retentionManifest, err := decodeReplayDetailManifest([]byte(retentionPrepared.manifests[0]))
	if err != nil {
		t.Fatal(err)
	}
	retentionPage, err := decodeReplayDetailPage([]byte(retentionPrepared.pages[0]), retentionManifest, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		value  any
		body   []byte
		decode func([]byte) error
	}{
		{"manifest", m, []byte(p.manifests[0]), func(b []byte) error { _, err := decodeReplayDetailManifest(b); return err }},
		{"page", page, []byte(p.pages[0]), func(b []byte) error { _, err := decodeReplayDetailPage(b, m, 0); return err }},
		{"page_retention", retentionPage, []byte(retentionPrepared.pages[0]), func(b []byte) error { _, err := decodeReplayDetailPage(b, retentionManifest, 0); return err }},
	} {
		t.Run(test.name, func(t *testing.T) {
			checks := 0
			refuse := func(name string, offset, length int, replacement []byte) {
				t.Helper()
				changed := append(slices.Clone(test.body[:offset]), replacement...)
				changed = append(changed, test.body[offset+length:]...)
				if bytes.Equal(changed, test.body) || !json.Valid(changed) {
					t.Fatalf("mutation %s is not distinct valid JSON", name)
				}
				if err := test.decode(changed); err == nil {
					t.Fatalf("accepted %s", name)
				}
				checks++
			}
			// Walk the concrete producer values, preserving field order and all
			// unrelated bytes. Every nested member gets independent omission,
			// duplicate and null controls, not only top-level sample mutations.
			var walk func(reflect.Value, []byte, int, string)
			walk = func(v reflect.Value, body []byte, offset int, path string) {
				if v.Type() == reflect.TypeFor[time.Time]() {
					return
				}
				switch v.Kind() {
				case reflect.Struct:
					refuse(path+" unknown", offset+1, 0, []byte(`"unreviewed":0,`))
					cursor := 1
					var emitted []int
					for i := range v.NumField() {
						tag := v.Type().Field(i).Tag.Get("json")
						if (strings.Contains(tag, ",omitempty") || strings.Contains(tag, ",omitzero")) && v.Field(i).IsZero() {
							continue
						}
						emitted = append(emitted, i)
					}
					for position, i := range emitted {
						field := v.Type().Field(i)
						name := strings.Split(field.Tag.Get("json"), ",")[0]
						key := replayDetailWireJSON(t, name)
						encoded := replayDetailWireJSON(t, v.Field(i).Interface())
						if strings.Contains(field.Tag.Get("json"), ",string") {
							encoded = replayDetailWireJSON(t, string(encoded))
						}
						pair := append(append(slices.Clone(key), ':'), encoded...)
						if !bytes.HasPrefix(body[cursor:], pair) {
							t.Fatalf("cannot locate exact typed field %s.%s", path, name)
						}
						start, length := cursor, len(pair)
						if position+1 < len(emitted) {
							length++
						} else if position > 0 {
							start--
							length++
						}
						if !strings.Contains(field.Tag.Get("json"), ",omitempty") && !strings.Contains(field.Tag.Get("json"), ",omitzero") {
							refuse(path+"."+name+" omitted", offset+start, length, nil)
						}
						duplicate := append(append(slices.Clone(pair), ','), pair...)
						refuse(path+"."+name+" duplicate", offset+cursor, len(pair), duplicate)
						if !bytes.Equal(encoded, []byte("null")) {
							refuse(path+"."+name+" null", offset+cursor+len(key)+1, len(encoded), []byte("null"))
						}
						walk(v.Field(i), encoded, offset+cursor+len(key)+1, path+"."+name)
						cursor += len(pair) + 1
					}
				case reflect.Slice:
					cursor := 1
					for i := range v.Len() {
						encoded := replayDetailWireJSON(t, v.Index(i).Interface())
						walk(v.Index(i), encoded, offset+cursor, path+"[]")
						cursor += len(encoded) + 1
					}
				case reflect.Pointer:
					if !v.IsNil() {
						walk(v.Elem(), body, offset, path)
					}
				}
			}
			walk(reflect.ValueOf(test.value), test.body, 0, test.name)
			if checks < 50 {
				t.Fatalf("insufficient nested member checks: %d", checks)
			}
			t.Logf("rejected %d exact member mutations", checks)
		})
	}
}

func TestReplayDetailWireNoncanonicalBodies(t *testing.T) {
	p, m := replayDetailWireFixture(t, 1)
	for _, test := range []struct {
		name   string
		body   []byte
		decode func([]byte) error
	}{
		{"manifest", []byte(p.manifests[0]), func(b []byte) error { _, err := decodeReplayDetailManifest(b); return err }},
		{"page", []byte(p.pages[0]), func(b []byte) error { _, err := decodeReplayDetailPage(b, m, 0); return err }},
	} {
		t.Run(test.name, func(t *testing.T) {
			mutations := [][]byte{
				nil, []byte("null"), []byte("[]"), []byte("{}"),
				append(slices.Clone(test.body), '\n'), append([]byte(" "), test.body...),
				append(slices.Clone(test.body), []byte("{}")...), bytes.Repeat([]byte(" "), MaxSnapshotBytes+1),
				bytes.Replace(test.body, []byte(`"schema_version":1,"family":"replay"`), []byte(`"family":"replay","schema_version":1`), 1),
				bytes.Replace(test.body, []byte(`"run_id"`), []byte(`"run_\u0069d"`), 1),
				bytes.Replace(test.body, []byte(`"detail-test"`), []byte(`"detail\u002dtest"`), 1),
				bytes.Replace(test.body, []byte(`"attempt":"2"`), []byte(`"attempt":"02"`), 1),
				bytes.Replace(test.body, []byte(`"attempt":"2"`), []byte(`"attempt":"+2"`), 1),
				bytes.Replace(test.body, []byte(`"attempt":"2"`), []byte(`"attempt":2`), 1),
				bytes.Replace(test.body, []byte(`"attempt":"2"`), []byte(`"attempt":"-1"`), 1),
				bytes.Replace(test.body, []byte(`"attempt":"2"`), []byte(`"attempt":"18446744073709551616"`), 1),
				bytes.Replace(test.body, []byte(`"schema_version":1`), []byte(`"schema_version":1.0`), 1),
				bytes.Replace(test.body, []byte(`999999999Z"`), []byte(`999999999+00:00"`), 1),
				bytes.Replace(test.body, []byte(`999999999Z"`), []byte(`999999999+01:00"`), 1),
				bytes.Replace(test.body, []byte(`"detail-test"`), []byte{'"', 255, '"'}, 1),
			}
			for i, body := range mutations {
				if bytes.Equal(body, test.body) || test.decode(body) == nil {
					t.Fatalf("noncanonical mutation %d not rejected", i)
				}
			}
		})
	}
}

func TestReplayDetailWireManifestSemantics(t *testing.T) {
	_, original := replayDetailWireFixture(t, 3)
	for _, test := range []struct {
		name   string
		change func(*ReplayDetailManifest)
	}{
		{"schema", func(m *ReplayDetailManifest) { m.SchemaVersion = 3 }},
		{"family", func(m *ReplayDetailManifest) { m.Family = "other" }},
		{"scope", func(m *ReplayDetailManifest) { m.Scope = "bank" }},
		{"state", func(m *ReplayDetailManifest) { m.State = "unavailable" }},
		{"fresh reason", func(m *ReplayDetailManifest) { m.Reason = "status_stale" }},
		{"stale no reason", func(m *ReplayDetailManifest) { m.State = "stale" }},
		{"arbitrary reason", func(m *ReplayDetailManifest) { m.State, m.Reason = "stale", "private reason" }},
		{"collector date", func(m *ReplayDetailManifest) { m.CollectorAt = time.Time{} }},
		{"retention date", func(m *ReplayDetailManifest) { m.RetainUntil = time.Time{} }},
		{"run", func(m *ReplayDetailManifest) { m.Binding.RunID = "" }},
		{"long run", func(m *ReplayDetailManifest) { m.Binding.RunID = strings.Repeat("x", 129) }},
		{"source generation", func(m *ReplayDetailManifest) { m.Binding.SourceGeneration = 0 }},
		{"attempt", func(m *ReplayDetailManifest) { m.Binding.Attempt = 0 }},
		{"start date", func(m *ReplayDetailManifest) { m.Binding.NodeStartedAt = time.Time{} }},
		{"attempt date", func(m *ReplayDetailManifest) { m.Binding.AttemptStartedAt = time.Time{} }},
		{"reset date", func(m *ReplayDetailManifest) { m.Binding.ResetAt = time.Time{} }},
		{"capture date", func(m *ReplayDetailManifest) { m.CapturedAt = time.Time{} }},
		{"update date", func(m *ReplayDetailManifest) { m.UpdatedAt = time.Time{} }},
		{"genesis", func(m *ReplayDetailManifest) { bad := "bad"; m.Binding.GenesisHash = &bad }},
		{"capacity", func(m *ReplayDetailManifest) { m.Capacity-- }},
		{"count", func(m *ReplayDetailManifest) { m.Count = 257 }},
		{"negative count", func(m *ReplayDetailManifest) { m.Count = -1 }},
		{"page size", func(m *ReplayDetailManifest) { m.PageSize++ }},
		{"page count", func(m *ReplayDetailManifest) { m.PageCount++ }},
		{"negative pages", func(m *ReplayDetailManifest) { m.PageCount = -1 }},
		{"many pages", func(m *ReplayDetailManifest) { m.PageCount = 129 }},
		{"first", func(m *ReplayDetailManifest) { m.FirstSequence++ }},
		{"last", func(m *ReplayDetailManifest) { m.LastSequence++ }},
		{"evicted", func(m *ReplayDetailManifest) { m.Evicted++ }},
		{"short generation", func(m *ReplayDetailManifest) { m.Generation = m.Generation[1:] }},
		{"upper generation", func(m *ReplayDetailManifest) { m.Generation = strings.ToUpper(m.Generation) }},
		{"nonhex generation", func(m *ReplayDetailManifest) { m.Generation = strings.Repeat("z", 64) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			m := original
			test.change(&m)
			if _, err := decodeReplayDetailManifest(replayDetailWireJSON(t, m)); err == nil {
				t.Fatal("invalid manifest accepted")
			}
		})
	}
}

func TestReplayDetailWirePageSemantics(t *testing.T) {
	prepared, m := replayDetailWireFixture(t, 3)
	original, err := decodeReplayDetailPage([]byte(prepared.pages[0]), m, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		change func(*ReplayDetailPage)
	}{
		{"schema", func(p *ReplayDetailPage) { p.SchemaVersion++ }},
		{"family", func(p *ReplayDetailPage) { p.Family = "other" }},
		{"generation", func(p *ReplayDetailPage) { p.Generation = strings.Repeat("0", 64) }},
		{"run", func(p *ReplayDetailPage) { p.Binding.RunID += "x" }},
		{"node date", func(p *ReplayDetailPage) { p.Binding.NodeStartedAt = p.Binding.NodeStartedAt.Add(time.Nanosecond) }},
		{"genesis", func(p *ReplayDetailPage) { h := p.Records[0].Bankhash; p.Binding.GenesisHash = &h }},
		{"source generation", func(p *ReplayDetailPage) { p.Binding.SourceGeneration++ }},
		{"attempt", func(p *ReplayDetailPage) { p.Binding.Attempt++ }},
		{"rewinds", func(p *ReplayDetailPage) { p.Binding.Rewinds++ }},
		{"attempt date", func(p *ReplayDetailPage) {
			p.Binding.AttemptStartedAt = p.Binding.AttemptStartedAt.Add(time.Nanosecond)
		}},
		{"reset date", func(p *ReplayDetailPage) { p.Binding.ResetAt = p.Binding.ResetAt.Add(time.Nanosecond) }},
		{"page", func(p *ReplayDetailPage) { p.Page++ }},
		{"offset", func(p *ReplayDetailPage) { p.Offset++ }},
		{"next", func(p *ReplayDetailPage) { n := 2; p.NextPage = &n }},
		{"missing next", func(p *ReplayDetailPage) { p.NextPage = nil }},
		{"nil rows", func(p *ReplayDetailPage) { p.Records = nil }},
		{"no rows", func(p *ReplayDetailPage) { p.Records = []ReplayDetailRecord{} }},
		{"missing row", func(p *ReplayDetailPage) { p.Records = p.Records[:1] }},
		{"extra row", func(p *ReplayDetailPage) { p.Records = append(p.Records, p.Records[0]) }},
		{"sequence", func(p *ReplayDetailPage) { p.Records[1].Sequence++ }},
		{"row attempt", func(p *ReplayDetailPage) { p.Records[0].Attempt++ }},
		{"row rewind", func(p *ReplayDetailPage) { p.Records[0].Rewinds++ }},
		{"completion order", func(p *ReplayDetailPage) { p.Records[1].CompletedIndex = p.Records[0].CompletedIndex }},
		{"zero completion", func(p *ReplayDetailPage) { p.Records[0].CompletedIndex = 0 }},
		{"hash", func(p *ReplayDetailPage) { p.Records[0].Bankhash = "bad" }},
		{"zero hash", func(p *ReplayDetailPage) { p.Records[0].Bankhash = strings.Repeat("1", 32) }},
		{"row date", func(p *ReplayDetailPage) { p.Records[0].CompletedAt = time.Time{} }},
		{"observed date", func(p *ReplayDetailPage) { p.Records[0].ObservedAt = time.Time{} }},
		{"row scope", func(p *ReplayDetailPage) { p.Records[0].Scope = "bank" }},
		{"shared coverage", func(p *ReplayDetailPage) { p.Records[0].SharedExecutionTimingsUnavailable = false }},
		{"attempt coverage", func(p *ReplayDetailPage) { p.Records[0].Streaming.AttemptBreakdownUnavailable = false }},
		{"discard reason", func(p *ReplayDetailPage) { p.Records[0].Streaming.LastDiscardReason = "private error" }},
		{"discard flag", func(p *ReplayDetailPage) { p.Records[0].Streaming.DiscardObserved = true }},
	} {
		t.Run(test.name, func(t *testing.T) {
			p := original
			p.Records = slices.Clone(p.Records)
			test.change(&p)
			if _, err := decodeReplayDetailPage(replayDetailWireJSON(t, p), m, 0); err == nil {
				t.Fatal("invalid page accepted")
			}
		})
	}
	for _, index := range []int{-1, 1, 128, math.MaxInt} {
		if _, err := decodeReplayDetailPage([]byte(prepared.pages[0]), m, index); err == nil {
			t.Fatal("unexpected page accepted")
		}
	}
	last, err := decodeReplayDetailPage([]byte(prepared.pages[1]), m, 1)
	if err != nil {
		t.Fatal(err)
	}
	next := 2
	last.NextPage = &next
	if _, err := decodeReplayDetailPage(replayDetailWireJSON(t, last), m, 1); err == nil {
		t.Fatal("final page accepted continuation")
	}
}

func TestReplayDetailWireWholeDigestAndOwnership(t *testing.T) {
	c := replayDetailTestCore(3)
	genesis := c.Records[0].Bankhash
	c.Binding.GenesisHash = &genesis
	p := replayDetailTestPrepared(t, c)
	m, err := decodeReplayDetailManifest([]byte(p.manifests[0]))
	if err != nil {
		t.Fatal(err)
	}
	records := make([]ReplayDetailRecord, 0, m.Count)
	for i, body := range p.pages {
		page, err := decodeReplayDetailPage([]byte(body), m, i)
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, page.Records...)
	}
	for _, test := range []struct {
		name   string
		change func(*ReplayDetailManifest, *[]ReplayDetailRecord)
	}{
		{"missing", func(_ *ReplayDetailManifest, r *[]ReplayDetailRecord) { *r = (*r)[:2] }},
		{"extra", func(_ *ReplayDetailManifest, r *[]ReplayDetailRecord) { *r = append(*r, (*r)[0]) }},
		{"duplicate", func(_ *ReplayDetailManifest, r *[]ReplayDetailRecord) { (*r)[2] = (*r)[1] }},
		{"reordered", func(_ *ReplayDetailManifest, r *[]ReplayDetailRecord) { (*r)[0], (*r)[1] = (*r)[1], (*r)[0] }},
		{"final row changed", func(_ *ReplayDetailManifest, r *[]ReplayDetailRecord) { (*r)[2].Timings.SlotReplay.SumNanoseconds++ }},
		{"cross page completion", func(_ *ReplayDetailManifest, r *[]ReplayDetailRecord) {
			(*r)[2].CompletedIndex = (*r)[1].CompletedIndex
		}},
		{"captured date", func(m *ReplayDetailManifest, _ *[]ReplayDetailRecord) {
			m.CapturedAt = m.CapturedAt.Add(time.Nanosecond)
		}},
		{"updated date", func(m *ReplayDetailManifest, _ *[]ReplayDetailRecord) { m.UpdatedAt = m.UpdatedAt.Add(time.Nanosecond) }},
		{"exhausted", func(m *ReplayDetailManifest, _ *[]ReplayDetailRecord) { m.Exhausted = !m.Exhausted }},
		{"valid token changed", func(m *ReplayDetailManifest, _ *[]ReplayDetailRecord) { m.Generation = strings.Repeat("0", 64) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			manifest, copy := m, slices.Clone(records)
			test.change(&manifest, &copy)
			if _, err := replayDetailCoreFromRecords(manifest, copy); err == nil {
				t.Fatal("incomplete or changed core accepted")
			}
		})
	}
	// A semantically valid changed final page is not a verified whole family.
	last, err := decodeReplayDetailPage([]byte(p.pages[1]), m, 1)
	if err != nil {
		t.Fatal(err)
	}
	last.Records[0].Transactions++
	changed, err := decodeReplayDetailPage(replayDetailWireJSON(t, last), m, 1)
	if err != nil {
		t.Fatal(err)
	}
	mutated := slices.Clone(records)
	mutated[2] = changed.Records[0]
	if _, err := replayDetailCoreFromRecords(m, mutated); err == nil {
		t.Fatal("last-page mutation passed whole-family digest")
	}
	core, err := replayDetailCoreFromRecords(m, records)
	if err != nil {
		t.Fatal(err)
	}
	records[0].Transactions++
	*m.Binding.GenesisHash = "changed"
	if !reflect.DeepEqual(core, c) {
		t.Fatal("caller mutation changed retained core")
	}
}

func TestReplayDetailWireDTOsRemainConcrete(t *testing.T) {
	var check func(reflect.Type)
	check = func(typ reflect.Type) {
		if typ == reflect.TypeFor[time.Time]() || typ == reflect.TypeFor[*time.Time]() {
			return // Canonical dates and nullable dates use the same reviewed codec.
		}
		if typ.Implements(reflect.TypeFor[json.Marshaler]()) || reflect.PointerTo(typ).Implements(reflect.TypeFor[json.Marshaler]()) ||
			typ.Implements(reflect.TypeFor[json.Unmarshaler]()) || reflect.PointerTo(typ).Implements(reflect.TypeFor[json.Unmarshaler]()) {
			t.Fatalf("custom JSON codec invalidates canonical member checks: %s", typ)
		}
		switch typ.Kind() {
		case reflect.Struct:
			for i := range typ.NumField() {
				field := typ.Field(i)
				tag := strings.Split(field.Tag.Get("json"), ",")
				schedule := typ == reflect.TypeFor[ReplayDetailManifest]() && field.Name == "ScheduleWindow" &&
					field.Type == reflect.TypeFor[*HeldScheduleWindow]() && field.Tag.Get("json") == "schedule_window,omitempty"
				retention := typ == reflect.TypeFor[ReplayDetailRecord]() && field.Name == "RetentionLimits" &&
					field.Type == reflect.TypeFor[ReplayDetailRetentionLimits]() && field.Tag.Get("json") == "retention_limits,omitzero"
				fold := typ == reflect.TypeFor[ReplayDetailManifest]() && field.Name == "FoldCapacity" &&
					field.Type == reflect.TypeFor[*ReplayFoldCapacity]() && field.Tag.Get("json") == "fold_capacity,omitempty"
				own := typ == reflect.TypeFor[HeldScheduleWindow]() && field.Name == "OwnAssignments" &&
					field.Type == reflect.TypeFor[*HeldOwnAssignments]() && field.Tag.Get("json") == "own_assignments,omitempty"
				near := typ == reflect.TypeFor[ReplayDetailManifest]() && field.Name == "NearReception" &&
					field.Type == reflect.TypeFor[*ReplayNearReception]() && field.Tag.Get("json") == "near_reception,omitempty"
				selected := typ == reflect.TypeFor[ReplayDetailManifest]() && field.Name == "LatestSelected" &&
					field.Type == reflect.TypeFor[*ReplayLatestSelected]() && field.Tag.Get("json") == "latest_selected,omitempty"
				recent := typ == reflect.TypeFor[ReplayDetailManifest]() && field.Name == "RecentSelected" &&
					field.Type == reflect.TypeFor[*ReplayRecentSelected]() && field.Tag.Get("json") == "recent_selected,omitempty"
				compute := typ == reflect.TypeFor[ReplaySelectedCounts]() && field.Name == "ComputeUnits" &&
					field.Type == reflect.TypeFor[*uint64]() && field.Tag.Get("json") == "compute_units,string,omitempty"
				burned := typ == reflect.TypeFor[ReplaySelectedFees]() && field.Name == "BurnedLamports" &&
					field.Type == reflect.TypeFor[*uint64]() && field.Tag.Get("json") == "burned_lamports,string,omitempty"
				if field.Anonymous || !field.IsExported() || tag[0] == "" || tag[0] == "-" || !compute && !burned && (len(tag) > 2 || len(tag) == 2 && tag[1] != "string" && !schedule && !retention && !fold && !own && !near && !selected && !recent) {
					t.Fatalf("non-concrete JSON field: %s.%s", typ, field.Name)
				}
				check(field.Type)
			}
		case reflect.Pointer, reflect.Slice:
			check(typ.Elem())
		case reflect.String, reflect.Int, reflect.Uint64, reflect.Bool:
		default:
			t.Fatalf("unreviewed private DTO type: %s", typ)
		}
	}
	check(reflect.TypeFor[ReplayDetailManifest]())
	check(reflect.TypeFor[ReplayDetailPage]())
}

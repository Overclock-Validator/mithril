package node

import (
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/gagliardetto/solana-go"
)

var replayDetailAt = time.Date(2026, 10, 31, 12, 0, 0, 999999999, time.UTC)

func replayDetailSource(t *testing.T, count int) replay.ReplayDiagnosticSnapshot {
	t.Helper()
	s := replay.ReplayDiagnosticSnapshot{RunID: "source-test", Generation: 7, Attempt: 4, Rewinds: 2, StartedAt: replayDetailAt, ResetAt: replayDetailAt, UpdatedAt: replayDetailAt, Records: make([]replay.ReplayDiagnostic, count)}
	next := uint64(100)
	var fill func(reflect.Value)
	fill = func(v reflect.Value) {
		switch v.Type() {
		case reflect.TypeFor[time.Time]():
			v.Set(reflect.ValueOf(replayDetailAt))
			return
		case reflect.TypeFor[solana.Hash]():
			v.Set(reflect.ValueOf(solana.Hash{1, 2, 3}))
			return
		case reflect.TypeFor[replay.ReplayDiagnosticScope]():
			v.SetUint(uint64(replay.ReplayDiagnosticRowScope))
			return
		case reflect.TypeFor[replay.ReplayDiagnosticDiscardReason]():
			v.SetUint(uint64(replay.ReplayDiscardGenerationGone))
			return
		}
		switch v.Kind() {
		case reflect.Struct:
			for i := 0; i < v.NumField(); i++ {
				fill(v.Field(i))
			}
		case reflect.Array:
			for i := 0; i < v.Len(); i++ {
				fill(v.Index(i))
			}
		case reflect.Uint64:
			v.SetUint(next)
			next++
		case reflect.Bool:
			v.SetBool(true)
		default:
			t.Fatalf("unreviewed source type %s", v.Type())
		}
	}
	for i := range s.Records {
		fill(reflect.ValueOf(&s.Records[i]).Elem())
		s.Records[i].Sequence = uint64(i + 1)
		s.Records[i].CompletedIndex = uint64(10 + i*2)
		s.Records[i].Attempt, s.Records[i].Rewinds = s.Attempt, s.Rewinds
	}
	if count > 0 {
		s.FirstSequence, s.LastSequence = 1, uint64(count)
	}
	return s
}

func replayDetailSnake(s string) string {
	var b strings.Builder
	chars := []rune(s)
	for i, r := range chars {
		if unicode.IsUpper(r) && i > 0 && (unicode.IsLower(chars[i-1]) || unicode.IsDigit(chars[i-1]) || i+1 < len(chars) && unicode.IsLower(chars[i+1])) {
			b.WriteByte('_')
		}
		b.WriteRune(unicode.ToLower(r))
	}
	return b.String()
}

// Reflection is deliberately test-only: every source member must appear in the
// explicit DTO with the same value, so additions cannot silently disappear.
func replayDetailExpected(t *testing.T, v reflect.Value) any {
	t.Helper()
	switch v.Type() {
	case reflect.TypeFor[time.Time]():
		return v.Interface().(time.Time).UTC().Format(time.RFC3339Nano)
	case reflect.TypeFor[solana.Hash]():
		return v.Interface().(solana.Hash).String()
	case reflect.TypeFor[replay.ReplayDiagnosticScope]():
		return v.Interface().(replay.ReplayDiagnosticScope).String()
	case reflect.TypeFor[replay.ReplayDiagnosticDiscardReason]():
		return v.Interface().(replay.ReplayDiagnosticDiscardReason).String()
	}
	switch v.Kind() {
	case reflect.Struct:
		m := map[string]any{}
		for i := 0; i < v.NumField(); i++ {
			m[replayDetailSnake(v.Type().Field(i).Name)] = replayDetailExpected(t, v.Field(i))
		}
		return m
	case reflect.Array:
		if v.Type() != reflect.TypeFor[[replay.ReplayDiagnosticStageCount]metrics.Timing]() {
			t.Fatal("unreviewed source array")
		}
		m := map[string]any{}
		for i := 0; i < v.Len(); i++ {
			m[replay.ReplayDiagnosticStage(i).String()] = replayDetailExpected(t, v.Index(i))
		}
		return m
	case reflect.Uint64:
		return strconv.FormatUint(v.Uint(), 10)
	case reflect.Bool:
		return v.Bool()
	default:
		t.Fatalf("unreviewed source value %s", v.Type())
		return nil
	}
}

func TestReplayDetailMappingDetached(t *testing.T) {
	s := replayDetailSource(t, 1)
	genesis := (solana.Hash{9}).String()
	c, err := replayDetailCore(s, replayDetailAt, replayDetailAt, &genesis)
	if err != nil {
		t.Fatal(err)
	}
	before, err := dashboard.ReplayDetailGeneration(c)
	if err != nil {
		t.Fatal(err)
	}
	originalGenesis := genesis
	s.Records[0].Timings.SlotReplay.Count++
	s.Records[0].AccountLoader.RequestedKeys++
	s.Records[0].Stages[0].SumNanoseconds++
	genesis = (solana.Hash{10}).String()
	after, err := dashboard.ReplayDetailGeneration(c)
	if err != nil || before != after || *c.Binding.GenesisHash != originalGenesis {
		t.Fatal("source mutations changed detached output")
	}
	sourceCount := s.Records[0].AccountLoader.RequestedKeys
	c.Records[0].AccountLoader.RequestedKeys++
	if s.Records[0].AccountLoader.RequestedKeys != sourceCount {
		t.Fatal("output mutation changed source")
	}
}

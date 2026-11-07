package dashboard

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func maximumProductionFinalization() *ProductionFinalizationDetail {
	f := &ProductionFinalizationDetail{BankPhases: &ProductionBankFinalizationDetail{}}
	for _, group := range []any{f, f.BankPhases} {
		v := reflect.ValueOf(group).Elem()
		for i := range v.NumField() {
			if v.Field(i).Kind() == reflect.Uint64 {
				v.Field(i).SetUint(math.MaxInt64 - 1)
			}
		}
	}
	return f
}

func maximumProductionWindowCompletion(slot uint64, at time.Time) *ProductionWindowCompletionDetail {
	start, duration, elapsed := slot-slot%4, uint64(1000000000000000000), uint64(math.MaxInt64-1)
	return &ProductionWindowCompletionDetail{WindowStartSlot: start, SlotDurationNanoseconds: duration, ReadyAt: at, NetworkCompletedAt: at,
		ReadyToCompletionNanoseconds: elapsed, FinalizationToCompletionNanoseconds: elapsed,
		DeadlineMarginNanoseconds: int64((slot-start+1)*duration) - int64(elapsed)}
}

func TestProductionWindowCompletionRejectsUnalignedStart(t *testing.T) {
	d := productionDetailFixture()
	d.LastTerminal.Finalization = maximumProductionFinalization()
	w := maximumProductionWindowCompletion(d.LastTerminal.Slot, d.LastTerminal.ObservedAt)
	w.WindowStartSlot = 41
	w.DeadlineMarginNanoseconds = int64(2*w.SlotDurationNanoseconds) - int64(w.ReadyToCompletionNanoseconds)
	d.LastTerminal.Finalization.WindowCompletion = w
	if validateProductionDetail(d) == nil {
		t.Fatal("non-aligned production window accepted with self-consistent wrong deadline")
	}
}

func TestProductionWindowCompletionContract(t *testing.T) {
	d := productionDetailFixture()
	d.LastTerminal.Finalization = maximumProductionFinalization()
	d.LastTerminal.Finalization.WindowCompletion = maximumProductionWindowCompletion(d.LastTerminal.Slot, d.LastTerminal.ObservedAt)
	body, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := decodeProductionDetail(body); err != nil || !reflect.DeepEqual(got, d) {
		t.Fatalf("maximum window completion changed: %v", err)
	}
	for _, mutate := range []func(*ProductionWindowCompletionDetail){
		func(w *ProductionWindowCompletionDetail) { w.WindowStartSlot = d.LastTerminal.Slot + 1 },
		func(w *ProductionWindowCompletionDetail) { w.WindowStartSlot = d.LastTerminal.Slot - 4 },
		func(w *ProductionWindowCompletionDetail) { w.SlotDurationNanoseconds = 0 },
		func(w *ProductionWindowCompletionDetail) { w.SlotDurationNanoseconds = math.MaxInt64/4 + 1 },
		func(w *ProductionWindowCompletionDetail) { w.ReadyAt = time.Time{} },
		func(w *ProductionWindowCompletionDetail) { w.NetworkCompletedAt = time.Time{} },
		func(w *ProductionWindowCompletionDetail) { w.ReadyToCompletionNanoseconds = math.MaxInt64 },
		func(w *ProductionWindowCompletionDetail) { w.FinalizationToCompletionNanoseconds = math.MaxInt64 },
		func(w *ProductionWindowCompletionDetail) { w.DeadlineMarginNanoseconds++ },
	} {
		bad, err := decodeProductionDetail(body)
		if err != nil {
			t.Fatal(err)
		}
		mutate(bad.LastTerminal.Finalization.WindowCompletion)
		if validateProductionDetail(bad) == nil {
			t.Fatal("invalid window completion accepted")
		}
	}
	w := d.LastTerminal.Finalization.WindowCompletion
	w.WindowStartSlot, w.SlotDurationNanoseconds = 40, 200000000
	w.ReadyToCompletionNanoseconds, w.FinalizationToCompletionNanoseconds, w.DeadlineMarginNanoseconds = 0, 0, 600000000
	// Original wall dates may reverse; they do not define monotonic durations.
	w.ReadyAt = w.NetworkCompletedAt.Add(time.Second)
	if validateProductionDetail(d) != nil {
		t.Fatal("known zero or reversed wall dates rejected")
	}
	w.ReadyToCompletionNanoseconds, w.FinalizationToCompletionNanoseconds, w.DeadlineMarginNanoseconds = 650000000, 100000000, -50000000
	if validateProductionDetail(d) != nil {
		t.Fatal("late local submission rejected")
	}
	for _, field := range []*time.Time{&w.ReadyAt, &w.NetworkCompletedAt} {
		original := *field
		*field = d.CapturedAt.Add(2 * time.Second)
		if !productionDetailTimeInvalid(ProductionDetailCapture{Detail: d, FreshUntil: d.CapturedAt.Add(time.Second)}, d.CapturedAt) {
			t.Fatal("future completion date accepted")
		}
		*field = original
	}
}

func TestProductionFinalizationContractAndCanonicalWire(t *testing.T) {
	d := productionDetailFixture()
	legacy, _ := json.Marshal(d)
	if strings.Contains(string(legacy), "finalization") {
		t.Fatal("ordinary old terminal gained timing fields")
	}
	for _, f := range []*ProductionFinalizationDetail{{}, maximumProductionFinalization()} {
		d.LastTerminal.Finalization = f
		body, _ := json.Marshal(d)
		got, err := decodeProductionDetail(body)
		if err != nil || !reflect.DeepEqual(got, d) {
			t.Fatal("known zero or full-width timing failed round trip")
		}
		for _, change := range []func(*ProductionDetail){
			func(d *ProductionDetail) { d.LastTerminal.Bank = nil },
			func(d *ProductionDetail) { d.LastTerminal.Costs = nil },
			func(d *ProductionDetail) {
				d.LastTerminal.Outcome, d.LastTerminal.Reason, d.LastTerminal.Terminal, d.LastTerminal.Cause = "missed", "freeze_failed", "freeze_failed", "freeze_failed"
				d.LastTerminal.Costs = nil
			},
		} {
			bad, _ := decodeProductionDetail(body)
			change(&bad)
			if validateProductionDetail(bad) == nil {
				t.Fatal("failed or unattributed terminal acquired finalization timings")
			}
		}
	}
	d.LastTerminal.Finalization = &ProductionFinalizationDetail{BankPhases: &ProductionBankFinalizationDetail{}}
	body, _ := json.Marshal(d)
	for _, replacement := range []string{`null`, `0`, `"-1"`, `"01"`, `"9223372036854775807"`, `"18446744073709551615"`} {
		changed := strings.Replace(string(body), `"freeze_nanoseconds":"0"`, `"freeze_nanoseconds":`+replacement, 1)
		if changed == string(body) {
			t.Fatal("negative control did not change the wire")
		}
		if _, err := decodeProductionDetail([]byte(changed)); err == nil {
			t.Fatal("invalid duration accepted")
		}
	}
	for _, group := range []any{d.LastTerminal.Finalization, d.LastTerminal.Finalization.BankPhases} {
		v := reflect.ValueOf(group).Elem()
		for i := range v.NumField() {
			if v.Field(i).Kind() != reflect.Uint64 {
				continue
			}
			v.Field(i).SetUint(math.MaxInt64)
			if validateProductionDetail(d) == nil {
				t.Fatalf("saturated %s accepted", v.Type().Field(i).Name)
			}
			v.Field(i).SetUint(0)
		}
	}
}

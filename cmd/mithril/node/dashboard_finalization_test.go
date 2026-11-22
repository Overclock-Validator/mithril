package node

import (
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/blockprod"
)

func TestDashboardFinalizationMappingAndOwnerLifetime(t *testing.T) {
	d := productionNodeFixture()
	m := d.attachProduction()
	m.running()
	o := productionNodeEvent(12)
	o.FinalizationKnown, o.Finalization.BankPhasesKnown = true, true
	amount := int64(9007199254740993)
	var fill func(reflect.Value)
	fill = func(v reflect.Value) {
		for i := range v.NumField() {
			f := v.Field(i)
			if f.Kind() == reflect.Struct && f.Type() != reflect.TypeFor[time.Time]() {
				fill(f)
			} else if f.Kind() == reflect.Int64 {
				f.SetInt(amount)
			}
		}
	}
	fill(reflect.ValueOf(&o.Finalization).Elem())
	m.observe(o)
	c, err := d.sampleProductionDetail(time.Now())
	if err != nil || c.Detail.LastTerminal.Finalization == nil || c.Detail.LastTerminal.Finalization.BankPhases == nil || !c.Detail.LastTerminal.ObservedAt.Equal(o.SampledAt) {
		t.Fatal("mapped phase timing missing or terminal event redated")
	}
	f := c.Detail.LastTerminal.Finalization
	checked := 0
	for _, group := range []any{f, f.BankPhases} {
		v := reflect.ValueOf(group).Elem()
		for i := range v.NumField() {
			if v.Field(i).Kind() == reflect.Uint64 {
				if v.Field(i).Uint() != uint64(amount) {
					t.Fatal("phase duration lost precision or mapping")
				}
				checked++
			}
		}
	}
	if checked != 18 {
		t.Fatal("phase mapping lost an existing source field")
	}
	f.FreezeNanoseconds, f.BankPhases.BankHashNanoseconds = 0, 0
	again, err := d.sampleProductionDetail(time.Now())
	if err != nil || again.Detail.LastTerminal.Finalization.FreezeNanoseconds != uint64(amount) || again.Detail.LastTerminal.Finalization.BankPhases.BankHashNanoseconds != uint64(amount) {
		t.Fatal("projected phase pointers alias retained owner state")
	}
	o.Finalization.BankPhasesKnown = false
	o.Finalization.Bank.BankHash = -1
	m.observe(o)
	c, err = d.sampleProductionDetail(time.Now())
	if err != nil || c.Detail.LastTerminal.Finalization == nil || c.Detail.LastTerminal.Finalization.BankPhases != nil {
		t.Fatal("unknown inner phases became measured zero or inspected")
	}
	for _, bad := range []time.Duration{-1, math.MaxInt64} {
		o.Finalization.Freeze = bad
		m.observe(o)
		requireProductionSourceError(t, d, "source_invalid")
	}
	o.FinalizationKnown = false
	m.observe(o)
	c, err = d.sampleProductionDetail(time.Now())
	if err != nil || c.Detail.LastTerminal.Finalization != nil {
		t.Fatal("old/unobserved timing values acquired availability")
	}
	d.attachProduction()
	m.observe(productionNodeEvent(13))
	requireProductionSourceError(t, d, "waiting_source")
}

func TestDashboardFinalizationRejectsUnattributedObservations(t *testing.T) {
	for _, modify := range []func(*blockprod.LeaderTerminalObservation){
		func(o *blockprod.LeaderTerminalObservation) { o.BankOpened = false },
		func(o *blockprod.LeaderTerminalObservation) { o.CostsKnown = false },
		func(o *blockprod.LeaderTerminalObservation) {
			o.Outcome, o.Reason, o.Terminal, o.Cause = "missed", "freeze_failed", "freeze_failed", "freeze_failed"
			o.CostsKnown = false
		},
		func(o *blockprod.LeaderTerminalObservation) {
			o.Finalization.BankPhasesKnown = true
			o.Finalization.Bank.BankHash = math.MaxInt64
		},
	} {
		d := productionNodeFixture()
		m := d.attachProduction()
		m.running()
		o := productionNodeEvent(12)
		o.FinalizationKnown = true
		modify(&o)
		m.observe(o)
		requireProductionSourceError(t, d, "source_invalid")
	}
}

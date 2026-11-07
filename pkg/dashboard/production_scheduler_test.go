package dashboard

import (
	"testing"
	"time"
)

func TestProductionSchedulerOriginalLease(t *testing.T) {
	d := productionDetailFixture()
	d.SchedulerBudget = &ProductionSchedulerDetail{Scope: "attached_scheduler_snapshot", SampledAt: d.CapturedAt.Add(-20 * time.Second)}
	c := ProductionDetailCapture{Detail: d, FreshUntil: d.CapturedAt.Add(10 * time.Second)}
	if validateProductionDetail(d) != nil || productionDetailTimeInvalid(c, d.CapturedAt) {
		t.Fatal("known zero scheduler snapshot rejected")
	}
	c.FreshUntil = c.FreshUntil.Add(time.Nanosecond)
	if !productionDetailTimeInvalid(c, d.CapturedAt) {
		t.Fatal("production refresh renewed scheduler lease")
	}
	for _, change := range []func(*ProductionSchedulerDetail){
		func(s *ProductionSchedulerDetail) { s.Scope = "producer_lifetime" },
		func(s *ProductionSchedulerDetail) { s.SampledAt = time.Time{} },
		func(s *ProductionSchedulerDetail) { s.SampledAt = d.CapturedAt.Add(time.Second + time.Nanosecond) },
		func(s *ProductionSchedulerDetail) { s.SampledAt = d.CapturedAt.Add(-ProductionDetailFreshFor) },
	} {
		s := *d.SchedulerBudget
		change(&s)
		if validProductionScheduler(&s, d.CapturedAt) {
			t.Fatal("invalid scheduler snapshot admitted")
		}
	}
}

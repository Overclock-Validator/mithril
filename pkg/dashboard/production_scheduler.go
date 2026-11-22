package dashboard

import "time"

// ProductionSchedulerDetail copies the attached scheduler's cumulative attempt
// counts. Replacement may lower them; they are not per-producer or unique losses.
type ProductionSchedulerDetail struct {
	Scope         string    `json:"scope"`
	SampledAt     time.Time `json:"sampled_at"`
	BlockCost     uint64    `json:"block_cost,string"`
	AccountCost   uint64    `json:"account_cost,string"`
	AllocatedData uint64    `json:"allocated_data,string"`
	EntryBytes    uint64    `json:"entry_bytes,string"`
}

func validProductionScheduler(s *ProductionSchedulerDetail, capturedAt time.Time) bool {
	return s == nil || s.Scope == "attached_scheduler_snapshot" && validReplayDetailTime(s.SampledAt) &&
		!s.SampledAt.After(capturedAt.Add(time.Second)) && capturedAt.Sub(s.SampledAt) < ProductionDetailFreshFor
}

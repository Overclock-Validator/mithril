package dashboard

import (
	"math"
	"time"
)

// ProductionEngineDetail contains the configured service's local counters.
// Its generation is independent of producer/slot lifetimes. Observations are
// a bounded copy across owners, not an atomic transaction or delivery receipt.
type ProductionEngineDetail struct {
	Scope             string               `json:"scope"`
	ServiceGeneration uint64               `json:"service_generation,string"`
	StartedAt         time.Time            `json:"started_at"`
	SampledAt         time.Time            `json:"sampled_at"`
	Strategy          string               `json:"strategy"`
	BlockEngine       EngineSessionDetail  `json:"block_engine"`
	Relayer           *EngineRelayerDetail `json:"relayer"`
	BundlePipeline    EngineBundleDetail   `json:"bundle_pipeline"`
	LastSlot          *EngineSlotDetail    `json:"last_slot"`
}

type EngineSessionDetail struct {
	Sessions       uint64 `json:"sessions,string"`
	Resets         uint64 `json:"resets,string"`
	Packets        uint64 `json:"packets,string"`
	DroppedSize    uint64 `json:"dropped_size,string"`
	DroppedFull    uint64 `json:"dropped_full,string"`
	Blocks         uint64 `json:"blocks,string"`
	NoticesSent    uint64 `json:"notices_sent,string"`
	NoticesExpired uint64 `json:"notices_expired,string"`
	NoticeErrors   uint64 `json:"notice_errors,string"`
	StreamsLost    uint64 `json:"streams_lost,string"`
	StrategyErrors uint64 `json:"strategy_errors,string"`
	InvalidFeeInfo uint64 `json:"invalid_fee_info,string"`
}

type EngineRelayerDetail struct {
	Sessions          uint64 `json:"sessions,string"`
	Resets            uint64 `json:"resets,string"`
	Heartbeats        uint64 `json:"heartbeats,string"`
	Batches           uint64 `json:"batches,string"`
	Packets           uint64 `json:"packets,string"`
	DroppedSize       uint64 `json:"dropped_size,string"`
	DroppedFull       uint64 `json:"dropped_full,string"`
	InvalidTPUConfigs uint64 `json:"invalid_tpu_configs,string"`
}

type EngineBundleDetail struct {
	Messages       uint64 `json:"messages,string"`
	WrongSlot      uint64 `json:"wrong_slot,string"`
	LostSlot       uint64 `json:"lost_slot,string"`
	Bundles        uint64 `json:"bundles,string"`
	InvalidBundles uint64 `json:"invalid_bundles,string"`
	Transactions   uint64 `json:"transactions,string"`
}

// EngineSlotDetail is an executor report. Bundle outcomes and native crank
// classifications do not establish chain inclusion, finality or tip revenue.
type EngineSlotDetail struct {
	ObservedAt              time.Time `json:"observed_at"`
	Slot                    uint64    `json:"slot,string"`
	Mode                    string    `json:"mode"`
	Timeout                 bool      `json:"timeout"`
	Failed                  bool      `json:"failed"`
	Reverted                bool      `json:"reverted"`
	Unavailable             bool      `json:"unavailable"`
	Aborted                 bool      `json:"aborted"`
	ExecutedBundles         uint64    `json:"executed_bundles,string"`
	RevertedBundles         uint64    `json:"reverted_bundles,string"`
	RejectedBundles         uint64    `json:"rejected_bundles,string"`
	MaxParallel             uint32    `json:"max_parallel"`
	Crank                   string    `json:"crank"`
	FirstArrivalNanoseconds *uint64   `json:"first_arrival_nanoseconds,string"`
}

func validProductionEngine(e *ProductionEngineDetail, capturedAt time.Time) bool {
	if e == nil {
		return true
	}
	if e.Scope != "harmonic_service_instance" || e.ServiceGeneration == 0 || e.ServiceGeneration == math.MaxUint64 ||
		!validReplayDetailTime(e.StartedAt) || !validReplayDetailTime(e.SampledAt) ||
		e.StartedAt.After(capturedAt.Add(time.Second)) ||
		e.SampledAt.After(capturedAt.Add(time.Second)) || capturedAt.Sub(e.SampledAt) >= ProductionDetailFreshFor ||
		e.Strategy != "fifo" && e.Strategy != "fba" && e.Strategy != "mrev" {
		return false
	}
	if s := e.LastSlot; s != nil {
		if !validReplayDetailTime(s.ObservedAt) || s.ObservedAt.After(capturedAt.Add(time.Second)) ||
			s.Mode != "undecided" && s.Mode != "harmonic" && s.Mode != "fallback" ||
			s.Crank != "not_run" && s.Crank != "not_needed" && s.Crank != "landed" && s.Crank != "failed" ||
			s.FirstArrivalNanoseconds != nil && *s.FirstArrivalNanoseconds >= math.MaxInt64 {
			return false
		}
	}
	return true
}

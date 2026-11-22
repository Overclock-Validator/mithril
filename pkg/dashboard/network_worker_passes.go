package dashboard

import (
	"encoding/json"
	"math"
	"strings"
	"time"
)

// TPUSigverifyWorkerPass is one goroutine's cumulative batch-pass wall time
// since TPUQueuesDetail.StartedAt. It includes drain, verification, output
// blockage and descheduling; it is not CPU usage or a pinned-core measurement.
type TPUSigverifyWorkerPass struct {
	Index              uint64    `json:"index,string"`
	SampledAt          time.Time `json:"sampled_at"`
	ActiveNanoseconds  uint64    `json:"active_nanoseconds,string"`
	ElapsedNanoseconds uint64    `json:"elapsed_nanoseconds,string"`
}

func (p TPUSigverifyWorkerPass) Valid(started, sampled time.Time) bool {
	if p.Index >= 64 || !validReplayDetailTime(started) || !validReplayDetailTime(sampled) || !validReplayDetailTime(p.SampledAt) || !p.SampledAt.After(started) || p.SampledAt.After(sampled) || sampled.Sub(p.SampledAt) > time.Second || p.ElapsedNanoseconds == 0 || p.ElapsedNanoseconds >= math.MaxInt64 || p.ActiveNanoseconds > p.ElapsedNanoseconds {
		return false
	}
	wall, elapsed := p.SampledAt.Sub(started), time.Duration(p.ElapsedNanoseconds)
	return wall-elapsed <= 250*time.Millisecond && elapsed-wall <= 250*time.Millisecond
}

func validTPUSigverifyWorkerPasses(w *TPUSigverifyWorkers, started, sampled, captured time.Time) bool {
	if len(w.Passes) == 0 {
		return true
	}
	if w.Configured == 0 || w.Configured > 64 || uint64(len(w.Passes)) != w.Configured {
		return false
	}
	for i, p := range w.Passes {
		if p.Index != uint64(i) || !p.Valid(started, sampled) || captured.Sub(p.SampledAt) >= NetworkDetailFreshFor {
			return false
		}
	}
	return true
}

// The returned document owns its changed pointers; the captured original is immutable.
func withoutTPUWorkerPasses(d NodeNetworkDetail) (NodeNetworkDetail, bool) {
	if d.TPUQueues == nil || d.TPUQueues.SigverifyWorkers == nil || len(d.TPUQueues.SigverifyWorkers.Passes) == 0 {
		return d, false
	}
	queues := *d.TPUQueues
	workers := *queues.SigverifyWorkers
	workers.Passes = nil
	queues.SigverifyWorkers = &workers
	d.TPUQueues = &queues
	return d, true
}

// Compare locally encoded variants after removing the exact member generated
// by the same JSON encoder. No untrusted JSON is parsed or textually rewritten.
func workerPassMember(d NodeNetworkDetail) string {
	if d.TPUQueues == nil || d.TPUQueues.SigverifyWorkers == nil {
		return ""
	}
	body, err := json.Marshal(d.TPUQueues.SigverifyWorkers.Passes)
	if err != nil {
		return ""
	}
	return `,"passes":` + string(body)
}
func preservesWorkerBaseline(candidate, baseline, member string) bool {
	return baseline == "" || member != "" && strings.Replace(candidate, member, "", 1) == baseline
}

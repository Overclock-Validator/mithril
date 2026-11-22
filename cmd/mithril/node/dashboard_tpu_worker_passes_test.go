package node

import (
	"math"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/tpu/pipeline"
)

func TestTPUWorkerPassProjectionPreservesQueues(t *testing.T) {
	now := time.Now()
	raw := pipeline.QueueObservation{StartedAt: now.Add(-10 * time.Second), SampledAt: now, Ingress: pipeline.QueueGauge{Capacity: 2}, DedupOutput: pipeline.QueueGauge{Capacity: 2}, Verified: pipeline.QueueGauge{Capacity: 2}, SigverifyWorkers: 2, WorkerPassCount: 2}
	for i := range 2 {
		raw.WorkerPasses[i] = pipeline.WorkerPassObservation{Index: i, SampledAt: now, Active: time.Duration(i) * time.Second, Elapsed: 10 * time.Second}
	}
	binding := dashboard.NodeNetworkBinding{RunID: "run", NodeStartedAt: now.Add(-time.Hour).UTC()}
	measure := func(r pipeline.QueueObservation) *tpuQueuesCapture {
		return tpuQueuesMeasurement(r, true, binding, 1, now.Add(-time.Millisecond), now)
	}
	capture := measure(raw)
	if capture == nil || len(capture.detail.SigverifyWorkers.Passes) != 2 || capture.detail.SigverifyWorkers.Passes[0].ActiveNanoseconds != 0 {
		t.Fatal("valid or idle worker lost")
	}
	for name, change := range map[string]func(*pipeline.QueueObservation){
		"default_off":      func(r *pipeline.QueueObservation) { r.WorkerPassCount = 0 },
		"bad_count":        func(r *pipeline.QueueObservation) { r.WorkerPassCount = 65 },
		"negative_count":   func(r *pipeline.QueueObservation) { r.WorkerPassCount = -1 },
		"partial":          func(r *pipeline.QueueObservation) { r.WorkerPassCount = 1 },
		"bad_index":        func(r *pipeline.QueueObservation) { r.WorkerPasses[1].Index = 0 },
		"negative_active":  func(r *pipeline.QueueObservation) { r.WorkerPasses[0].Active = -1 },
		"elapsed_sentinel": func(r *pipeline.QueueObservation) { r.WorkerPasses[0].Elapsed = time.Duration(math.MaxInt64) },
		"wall_only":        func(r *pipeline.QueueObservation) { r.WorkerPasses[0].SampledAt = r.WorkerPasses[0].SampledAt.Round(0) },
		"stale_child": func(r *pipeline.QueueObservation) {
			r.WorkerPasses[0].SampledAt = now.Add(-2 * time.Second)
			r.WorkerPasses[0].Elapsed = 8 * time.Second
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := raw
			change(&r)
			c := measure(r)
			if c == nil || c.detail.SigverifyWorkers == nil || c.detail.SigverifyWorkers.Configured != 2 || len(c.detail.SigverifyWorkers.Passes) != 0 {
				t.Fatal("bad optional timing hid old queue facts or escaped")
			}
		})
	}
}

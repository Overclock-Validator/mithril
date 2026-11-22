package node

import (
	"math"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/tpu/pipeline"
)

// Invalid optional timing never hides existing queue or in-flight observations.
func tpuWorkerPassesMeasurement(raw pipeline.QueueObservation) []dashboard.TPUSigverifyWorkerPass {
	if raw.WorkerPassCount == 0 {
		return nil
	}
	if raw.WorkerPassCount < 0 || raw.WorkerPassCount > len(raw.WorkerPasses) || raw.WorkerPassCount != raw.SigverifyWorkers || raw.StartedAt == raw.StartedAt.Round(0) {
		return nil
	}
	passes := make([]dashboard.TPUSigverifyWorkerPass, raw.WorkerPassCount)
	for i, item := range raw.WorkerPasses[:raw.WorkerPassCount] {
		if item.Index != i || item.SampledAt == item.SampledAt.Round(0) || item.SampledAt.Sub(raw.StartedAt) != item.Elapsed || item.Active < 0 || item.Elapsed <= 0 || item.Elapsed == time.Duration(math.MaxInt64) {
			return nil
		}
		value := dashboard.TPUSigverifyWorkerPass{Index: uint64(item.Index), SampledAt: item.SampledAt.UTC(), ActiveNanoseconds: uint64(item.Active), ElapsedNanoseconds: uint64(item.Elapsed)}
		if !value.Valid(raw.StartedAt.UTC(), raw.SampledAt.UTC()) {
			return nil
		}
		passes[i] = value
	}
	return passes
}

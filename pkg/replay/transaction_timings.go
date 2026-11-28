package replay

import (
	"os"

	"github.com/Overclock-Validator/mithril/pkg/metrics"
)

// Collect transaction timings locally by default; =0 restores per-call atomics.
var replayLocalTimingsEnabled = os.Getenv("MITHRIL_REPLAY_LOCAL_TIMINGS") != "0"

type workerTransactionTimings []metrics.TransactionTimings

func newWorkerTransactionTimings(workers int) workerTransactionTimings {
	if !replayLocalTimingsEnabled || workers <= 0 {
		return nil
	}
	return make(workerTransactionTimings, workers)
}

func (w workerTransactionTimings) at(worker int) *metrics.TransactionTimings {
	if worker < 0 || worker >= len(w) {
		return nil
	}
	return &w[worker]
}

func (w workerTransactionTimings) merge() {
	for i := range w {
		w[i].Merge()
	}
}

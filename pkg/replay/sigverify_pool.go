package replay

import (
	"runtime"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/sigverifytelemetry"
)

// Transaction signature verification runs CONCURRENT with execution: the
// snapshot is enqueued before the transaction executes, and each block's
// WaitGroup joins at ProcessBlock exit, so a block is never reported
// executed with unverified signatures. This used to spawn one goroutine per
// transaction — a 30k-tx block launched 30k goroutines mid-execution,
// paying spawn cost and scheduler churn against the execution workers. A
// fixed pool does the same work with zero per-transaction spawns.
//
// Sizing: half the machine. The heaviest observed blocks (~30k txs in a
// ~600ms execution window) demand roughly three cores of ed25519; half of
// GOMAXPROCS clears that with headroom on typical hardware while leaving
// the other half to the execution lanes. The bounded queue turns a
// verification backlog into gentle backpressure on the enqueuers instead of
// an unbounded goroutine pileup; the block-end WaitGroup drain is where any
// residual lag surfaces (inside the exec time, same as before).
const sigverifyQueueDepth = 8192

const (
	telemetryDispatchMaxJobs        = 64
	telemetryDispatchTargetSigLanes = 64
)

type sigverifyJob struct {
	snapshot *sigverifySnapshot
	wg       *sync.WaitGroup
}

var (
	sigverifyOnce  sync.Once
	sigverifyQueue chan sigverifyJob
)

// enqueueSigverify hands a snapshot to the verification pool. The caller
// must have added to wg; a worker calls verifySignatures, which Done()s it.
// An invalid signature panics in the worker — a deliberate halt, identical
// to the per-goroutine behavior it replaces.
func enqueueSigverify(snapshot *sigverifySnapshot, wg *sync.WaitGroup) {
	sigverifyOnce.Do(func() {
		sigverifyQueue = make(chan sigverifyJob, sigverifyQueueDepth)
		workers := max(2, runtime.GOMAXPROCS(0)/2)
		for i := 0; i < workers; i++ {
			go runSigverifyJobs(sigverifyQueue)
		}
	})
	sigverifyQueue <- sigverifyJob{snapshot: snapshot, wg: wg}
}

func runSigverifyJobs(in <-chan sigverifyJob) {
	for job := range in {
		mode := sigverifytelemetry.CurrentMode()
		if mode == sigverifytelemetry.CollectionDisabled {
			verifySignatures(job.snapshot, job.wg)
			continue
		}
		if mode == sigverifytelemetry.CollectionPassive {
			queued := len(in)
			dispatch := sigverifytelemetry.ReserveSignatureDispatch(sigverifytelemetry.SourceReplay, queued, queued, 1)
			counts := [1]uint16{boundedSignatureCount(len(job.snapshot.signatures))}
			dispatch.Ready(counts[:])
			verifySignaturesInDispatch(job.snapshot, job.wg, dispatch.Job(0))
			continue
		}

		jobs, signatureCounts, queuedBefore, queuedAfter := collectSigverifyTelemetryBatch(job, in)
		dispatch := sigverifytelemetry.ReserveSignatureDispatch(
			sigverifytelemetry.SourceReplay,
			queuedBefore,
			queuedAfter,
			len(jobs),
		)
		dispatch.Ready(signatureCounts)
		for i, claimed := range jobs {
			verifySignaturesInDispatch(claimed.snapshot, claimed.wg, dispatch.Job(i))
		}
	}
}

// collectSigverifyTelemetryBatch is used only by explicit scheduling
// simulation. It claims work already visible after the first receive and
// never waits for a fuller group.
func collectSigverifyTelemetryBatch(first sigverifyJob, in <-chan sigverifyJob) ([]sigverifyJob, []uint16, int, int) {
	jobs := make([]sigverifyJob, 0, telemetryDispatchMaxJobs)
	signatureCounts := make([]uint16, 0, telemetryDispatchMaxJobs)
	jobs = append(jobs, first)
	firstCount := boundedSignatureCount(len(first.snapshot.signatures))
	signatureCounts = append(signatureCounts, firstCount)
	signatureLanes := int(firstCount)
	queuedBefore := len(in)

collect:
	for len(jobs) < telemetryDispatchMaxJobs && signatureLanes < telemetryDispatchTargetSigLanes {
		select {
		case job, ok := <-in:
			if !ok {
				break collect
			}
			count := boundedSignatureCount(len(job.snapshot.signatures))
			jobs = append(jobs, job)
			signatureCounts = append(signatureCounts, count)
			signatureLanes += int(count)
		default:
			break collect
		}
	}
	return jobs, signatureCounts, queuedBefore, len(in)
}

func boundedSignatureCount(count int) uint16 {
	if count <= 0 {
		return 0
	}
	if count > int(^uint16(0)) {
		return ^uint16(0)
	}
	return uint16(count)
}

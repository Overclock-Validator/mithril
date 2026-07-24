package sigverifytelemetry

import (
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestObserverExactDuplicatesAndKeyReuse(t *testing.T) {
	o := newObserver(3)
	var pubA, pubB [32]byte
	pubA[0] = 1
	pubB[0] = 2
	var sigA, sigB [64]byte
	sigA[0] = 3
	sigB[0] = 4
	msgA := []byte("a")
	msgB := []byte("b")

	if duplicate, _, reused := o.recordVerification(pubA, sigA, msgA); duplicate || reused {
		t.Fatal("first observation classified as a hit")
	}
	if duplicate, distance, reused := o.recordVerification(pubA, sigB, msgB); duplicate || !reused || distance != 1 {
		t.Fatalf("key reuse = duplicate %v, reused %v, distance %d", duplicate, reused, distance)
	}
	if duplicate, distance, reused := o.recordVerification(pubB, sigB, msgB); duplicate || reused || distance != 0 {
		t.Fatalf("new key = duplicate %v, reused %v, distance %d", duplicate, reused, distance)
	}
	if duplicate, distance, reused := o.recordVerification(pubA, sigA, msgA); !duplicate || !reused || distance != 2 {
		t.Fatalf("exact hit = duplicate %v, reused %v, distance %d", duplicate, reused, distance)
	}

	s := o.snapshot()
	if s.ExactDuplicateHits != 1 || s.PublicKeyReuseHits != 2 {
		t.Fatalf("snapshot hits = duplicate %d, key %d", s.ExactDuplicateHits, s.PublicKeyReuseHits)
	}
}

func TestObserverHistoryIsBoundedAndExact(t *testing.T) {
	o := newObserver(2)
	var pub [32]byte
	var sig [64]byte

	o.recordVerification(pub, sig, []byte("one"))
	o.recordVerification(pub, sig, []byte("two"))
	if duplicate, _, _ := o.recordVerification(pub, sig, []byte("one")); !duplicate {
		t.Fatal("entry should still be present at capacity")
	}
	// The update above evicts the oldest generation. A different message must
	// never alias merely because key and signature match.
	if duplicate, _, _ := o.recordVerification(pub, sig, []byte("three")); duplicate {
		t.Fatal("different message classified as an exact duplicate")
	}
	if len(o.exact) > 2 || len(o.keys) > 2 {
		t.Fatalf("history exceeded capacity: exact=%d keys=%d", len(o.exact), len(o.keys))
	}
}

func TestObserverExactWindowEvictionBoundary(t *testing.T) {
	o := newObserver(2)
	var pubA, pubB, pubC [32]byte
	pubA[0], pubB[0], pubC[0] = 1, 2, 3
	var sig [64]byte
	msg := []byte("same")

	o.recordVerification(pubA, sig, msg) // sequence 1
	o.recordVerification(pubB, sig, msg) // sequence 2
	if duplicate, distance, reused := o.recordVerification(pubA, sig, msg); !duplicate || !reused || distance != 2 {
		t.Fatalf("capacity boundary: duplicate=%v reused=%v distance=%d", duplicate, reused, distance)
	}

	// The live window is now B,A. Inserting C evicts B exactly; neither its
	// exact tuple nor its key may survive through a stale ring generation.
	o.recordVerification(pubC, sig, msg)
	if duplicate, _, reused := o.recordVerification(pubB, sig, msg); duplicate || reused {
		t.Fatalf("evicted B survived: duplicate=%v reused=%v", duplicate, reused)
	}
	if len(o.exact) > 2 || len(o.keys) > 2 {
		t.Fatalf("history exceeded capacity: exact=%d keys=%d", len(o.exact), len(o.keys))
	}
}

func TestObserverRepeatedGenerationSurvivesOlderSlotEviction(t *testing.T) {
	o := newObserver(2)
	var pubA, pubB [32]byte
	pubA[0], pubB[0] = 1, 2
	var sig [64]byte
	msg := []byte("message")

	o.recordVerification(pubA, sig, msg) // A generation 1
	o.recordVerification(pubA, sig, msg) // A generation 2
	o.recordVerification(pubB, sig, msg) // overwrites generation 1's slot
	if duplicate, distance, reused := o.recordVerification(pubA, sig, msg); !duplicate || !reused || distance != 2 {
		t.Fatalf("newer A generation was deleted with older slot: duplicate=%v reused=%v distance=%d", duplicate, reused, distance)
	}
}

func TestObserverCopiesMessageBytes(t *testing.T) {
	o := newObserver(2)
	var pub [32]byte
	var sig [64]byte
	msg := []byte("a")
	o.recordVerification(pub, sig, msg)
	msg[0] = 'b'
	if duplicate, _, _ := o.recordVerification(pub, sig, []byte("a")); !duplicate {
		t.Fatal("mutating the caller's message changed retained exact identity")
	}
}

func TestVerificationTraceIsChronologicalBoundedAndOwned(t *testing.T) {
	o := newObserver(3)
	var pubA, pubB [32]byte
	pubA[0], pubB[0] = 1, 2
	var sigA, sigB [64]byte
	sigA[0], sigB[0] = 3, 4

	o.recordVerificationFrom(SourceTPU, pubA, sigA, []byte("evicted"))
	o.recordVerificationFrom(SourceTurbine, pubA, sigB, []byte("key-reuse"))
	o.recordVerificationFrom(SourceReplay, pubB, sigB, []byte("other"))
	o.recordVerificationFrom(SourceReplay, pubA, sigB, []byte("key-reuse"))

	s := o.snapshot()
	if len(s.VerificationTrace) != 3 {
		t.Fatalf("trace length = %d, want 3", len(s.VerificationTrace))
	}
	wantSequences := []uint64{2, 3, 4}
	wantSources := []Source{SourceTurbine, SourceReplay, SourceReplay}
	wantMessages := []string{"key-reuse", "other", "key-reuse"}
	for i, entry := range s.VerificationTrace {
		if entry.Sequence != wantSequences[i] || entry.Source != wantSources[i] || string(entry.Message) != wantMessages[i] {
			t.Fatalf("trace[%d] = %+v", i, entry)
		}
	}
	last := s.VerificationTrace[2]
	if !last.ExactDuplicate || !last.PublicKeyReused || last.ReuseDistance != 2 {
		t.Fatalf("last trace classification = %+v", last)
	}

	// Snapshot messages must not alias either the observer history or later
	// snapshots returned to offline policy experiments.
	s.VerificationTrace[0].Message[0] = 'X'
	again := o.snapshot()
	if got := string(again.VerificationTrace[0].Message); got != "key-reuse" {
		t.Fatalf("snapshot mutation changed observer history: %q", got)
	}
}

func TestVerificationAttemptRecordsOutcomeOnOriginalObserver(t *testing.T) {
	Enable(2)
	t.Cleanup(Disable)
	var pub [32]byte
	var sig [64]byte
	validAttempt, _, _, _ := BeginVerification(SourceTurbine, pub, sig, []byte("valid"))
	invalidAttempt, _, _, _ := BeginVerification(SourceReplay, pub, sig, []byte("invalid"))

	// Replacing the process-wide observer must not redirect outstanding result
	// handles into the new generation.
	Enable(2)
	validAttempt.RecordResult(true)
	invalidAttempt.RecordResult(false)
	if s := Current(); len(s.VerificationTrace) != 0 {
		t.Fatalf("old results appeared in new observer: %+v", s.VerificationTrace)
	}
	Disable()

	o := newObserver(2)
	_, _, _, validSequence := o.recordVerificationFrom(SourceTurbine, pub, sig, []byte("valid"))
	_, _, _, invalidSequence := o.recordVerificationFrom(SourceReplay, pub, sig, []byte("invalid"))
	VerificationAttempt{observer: o, sequence: validSequence}.RecordResult(true)
	VerificationAttempt{observer: o, sequence: invalidSequence}.RecordResult(false)
	// Outcomes are write-once, guarding accidental duplicate finalization.
	VerificationAttempt{observer: o, sequence: validSequence}.RecordResult(false)

	trace := o.snapshot().VerificationTrace
	if trace[0].Outcome != VerificationOutcomeValid || trace[1].Outcome != VerificationOutcomeInvalid {
		t.Fatalf("outcomes = %v, %v", trace[0].Outcome, trace[1].Outcome)
	}
	// Once evicted, a late result cannot update the new occupant of that slot.
	_, _, _, replacementSequence := o.recordVerificationFrom(SourceTPU, pub, sig, []byte("replacement"))
	VerificationAttempt{observer: o, sequence: validSequence}.RecordResult(false)
	VerificationAttempt{observer: o, sequence: replacementSequence}.RecordResult(true)
	trace = o.snapshot().VerificationTrace
	if trace[1].Outcome != VerificationOutcomeValid {
		t.Fatalf("replacement outcome = %v", trace[1].Outcome)
	}
}

func TestVerificationCompletionAndDispatchShareExactEventOrder(t *testing.T) {
	o := newObserver(8)
	var pub [32]byte
	var sig [64]byte
	_, _, _, firstSequence := o.recordVerificationFrom(SourceReplay, pub, sig, []byte("first"))
	_, _, _, secondSequence := o.recordVerificationFrom(SourceTPU, pub, sig, []byte("second"))

	// Complete out of begin order, then place a dispatch between completions.
	VerificationAttempt{observer: o, sequence: secondSequence}.RecordResult(true)
	o.recordSignatureDispatch(SourceReplay, 3, 1, []uint16{2, 1})
	VerificationAttempt{observer: o, sequence: firstSequence}.RecordResult(false)

	s := o.snapshot()
	if s.ObservedEvents != 6 {
		t.Fatalf("observed events = %d, want 6", s.ObservedEvents)
	}
	first, second := s.VerificationTrace[0], s.VerificationTrace[1]
	if first.BeginEventSequence != 1 || first.CompletionEventSequence != 6 {
		t.Fatalf("first event order = %+v", first)
	}
	if second.BeginEventSequence != 2 || second.CompletionEventSequence != 3 {
		t.Fatalf("second event order = %+v", second)
	}
	if len(s.DispatchTrace) != 1 || s.DispatchTrace[0].ClaimEventSequence != 4 || s.DispatchTrace[0].ReadyEventSequence != 5 {
		t.Fatalf("dispatch event order = %+v", s.DispatchTrace)
	}
}

func TestDispatchCorrelationMapsJobsAndLanes(t *testing.T) {
	EnableSchedulingSimulation(8)
	t.Cleanup(Disable)
	dispatch := ReserveSignatureDispatch(SourceReplay, 3, 1, 2)
	dispatch.Ready([]uint16{2, 1})
	var pub [32]byte
	var sig [64]byte
	first, _, _, _ := BeginVerificationInDispatch(SourceReplay, dispatch.Job(0), 1, pub, sig, []byte("first"))
	second, _, _, _ := BeginVerificationInDispatch(SourceReplay, dispatch.Job(1), 0, pub, sig, []byte("second"))
	second.RecordResult(true)
	first.RecordResult(false)

	s := Current()
	if s.Mode != CollectionSchedulingSimulation || len(s.DispatchTrace) != 1 {
		t.Fatalf("snapshot mode/dispatch = %+v", s)
	}
	d := s.DispatchTrace[0]
	if d.DispatchID == 0 || d.Mode != CollectionSchedulingSimulation || d.ClaimEventSequence == 0 || d.ReadyEventSequence <= d.ClaimEventSequence {
		t.Fatalf("dispatch = %+v", d)
	}
	if got := s.VerificationTrace[0]; got.DispatchID != d.DispatchID || got.JobIndex != 0 || got.LaneIndex != 1 || got.Outcome != VerificationOutcomeInvalid {
		t.Fatalf("first correlation = %+v", got)
	}
	if got := s.VerificationTrace[1]; got.DispatchID != d.DispatchID || got.JobIndex != 1 || got.LaneIndex != 0 || got.Outcome != VerificationOutcomeValid {
		t.Fatalf("second correlation = %+v", got)
	}
}

func TestPassiveDispatchDoesNotPopulateSimulationWidth(t *testing.T) {
	Enable(4)
	t.Cleanup(Disable)
	ReserveSignatureDispatch(SourceTPU, 7, 5, 2).Ready([]uint16{1, 1})
	if got := len(Current().DispatchTrace); got != 0 {
		t.Fatalf("passive mode accepted a multi-job claim: %d", got)
	}
	dispatch := ReserveSignatureDispatch(SourceTPU, 7, 7, 1)
	dispatch.Ready([]uint16{3})
	s := Current()
	if s.Mode != CollectionPassive || s.BatchSamples != 0 || s.NaturalBatchWidthSum != 0 {
		t.Fatalf("passive snapshot = %+v", s)
	}
	if got := s.DispatchTrace[0]; got.Mode != CollectionPassive || got.SignatureLanes != 3 {
		t.Fatalf("passive dispatch = %+v", got)
	}
}

func TestDispatchTraceIsBoundedExactAndOwned(t *testing.T) {
	o := newObserver(2)
	firstJobs := []uint16{1, 7}
	o.recordSignatureDispatch(SourceReplay, 5, 3, firstJobs)
	firstJobs[0] = 99
	o.recordSignatureDispatch(SourceTPU, 2, 0, []uint16{4, 4})
	o.recordSignatureDispatch(SourceReplay, 1, 0, []uint16{3})

	s := o.snapshot()
	if len(s.DispatchTrace) != 2 || s.DispatchTrace[0].Sequence != 2 || s.DispatchTrace[1].Sequence != 3 {
		t.Fatalf("bounded dispatch trace = %+v", s.DispatchTrace)
	}
	if got := s.DispatchTrace[0]; got.Source != SourceTPU || got.SignatureLanes != 8 || got.QueuedItemsBefore != 2 || got.QueuedItemsAfter != 0 {
		t.Fatalf("dispatch details = %+v", got)
	}
	s.DispatchTrace[0].JobSignatures[0] = 55
	again := o.snapshot()
	if got := again.DispatchTrace[0].JobSignatures[0]; got != 4 {
		t.Fatalf("snapshot mutation changed dispatch history: %d", got)
	}
}

func TestObserverShapeAndDispatchBuckets(t *testing.T) {
	o := newObserver(4)
	o.recordTransaction(2, 200)
	o.recordTransaction(19, 1233)
	o.recordQueue(0)
	o.recordQueue(9)
	o.recordBatch(8)
	o.recordBatch(33)

	s := o.snapshot()
	if s.Transactions != 2 || s.Signatures != 21 || s.MessageBytes != 1433 {
		t.Fatalf("shape totals = %+v", s)
	}
	if s.SignaturesPerTransaction[2] != 1 || s.SignaturesPerTransaction[18] != 1 {
		t.Fatalf("signature buckets = %v", s.SignaturesPerTransaction)
	}
	if s.MessageSize[3] != 1 || s.MessageSize[8] != 1 {
		t.Fatalf("message buckets = %v", s.MessageSize)
	}
	if s.QueueSamples != 2 || s.QueueOccupancyMax != 9 || s.BatchSamples != 2 || s.NaturalBatchWidthMax != 33 {
		t.Fatalf("dispatch totals = %+v", s)
	}
}

func TestObserverDispatchOpportunityIsAtomicAndUncapped(t *testing.T) {
	o := newObserver(4)
	o.recordDispatch(70, 71)
	s := o.snapshot()
	if s.QueueSamples != 1 || s.BatchSamples != 1 || s.QueueOccupancyMax != 70 || s.NaturalBatchWidthMax != 71 {
		t.Fatalf("dispatch snapshot = %+v", s)
	}
	if s.NaturalBatchWidth[19] != 1 {
		t.Fatalf("width >64 did not reach overflow bucket: %v", s.NaturalBatchWidth)
	}
}

func TestRecordTransactionClampsBeforePrometheusCounter(t *testing.T) {
	Enable(4)
	t.Cleanup(Disable)
	RecordTransaction(SourceReplay, -1, -1)
	s := Current()
	if s.Transactions != 1 || s.Signatures != 0 || s.MessageBytes != 0 || s.SignaturesPerTransaction[0] != 1 || s.MessageSize[0] != 1 {
		t.Fatalf("negative values were not consistently clamped: %+v", s)
	}
}

func TestProcessWideDuplicateHistoryAcrossSources(t *testing.T) {
	Enable(4)
	t.Cleanup(Disable)
	var pub [32]byte
	var sig [64]byte
	msg := []byte("cross-source")
	tpuAttemptsBefore := counterValue(t, metricVerificationAttempts.WithLabelValues("tpu"))
	replayAttemptsBefore := counterValue(t, metricVerificationAttempts.WithLabelValues("replay"))
	tpuDuplicatesBefore := counterValue(t, metricExactDuplicateHits.WithLabelValues("tpu"))
	replayDuplicatesBefore := counterValue(t, metricExactDuplicateHits.WithLabelValues("replay"))
	if duplicate, _, _ := RecordVerification(SourceTPU, pub, sig, msg); duplicate {
		t.Fatal("first source classified as duplicate")
	}
	if duplicate, distance, reused := RecordVerification(SourceReplay, pub, sig, msg); !duplicate || !reused || distance != 1 {
		t.Fatalf("second source: duplicate=%v reused=%v distance=%d", duplicate, reused, distance)
	}
	if s := Current(); s.VerificationAttempts != 2 || s.ExactDuplicateHits != 1 || s.PublicKeyReuseHits != 1 {
		t.Fatalf("cross-source snapshot = %+v", s)
	}
	if got := counterValue(t, metricVerificationAttempts.WithLabelValues("tpu")) - tpuAttemptsBefore; got != 1 {
		t.Fatalf("TPU attempt delta = %v", got)
	}
	if got := counterValue(t, metricVerificationAttempts.WithLabelValues("replay")) - replayAttemptsBefore; got != 1 {
		t.Fatalf("replay attempt delta = %v", got)
	}
	if got := counterValue(t, metricExactDuplicateHits.WithLabelValues("tpu")) - tpuDuplicatesBefore; got != 0 {
		t.Fatalf("TPU duplicate delta = %v, want 0", got)
	}
	if got := counterValue(t, metricExactDuplicateHits.WithLabelValues("replay")) - replayDuplicatesBefore; got != 1 {
		t.Fatalf("replay duplicate delta = %v, want 1", got)
	}
}

func counterValue(t *testing.T, counter prometheus.Counter) float64 {
	t.Helper()
	metric := &dto.Metric{}
	if err := counter.Write(metric); err != nil {
		t.Fatal(err)
	}
	return metric.GetCounter().GetValue()
}

func TestDisabledGlobalPath(t *testing.T) {
	Disable()
	if Enabled() {
		t.Fatal("telemetry remained enabled")
	}
	RecordTransaction(SourceReplay, 1, 64)
	RecordVerification(SourceReplay, [32]byte{}, [64]byte{}, []byte("message"))
	RecordQueueOccupancy(SourceReplay, 2)
	RecordNaturalBatchWidth(SourceReplay, 3)
	RecordDispatchOpportunity(SourceReplay, 4)
	RecordSignatureDispatch(SourceReplay, 4, 1, []uint16{2, 3})
	ReserveSignatureDispatch(SourceReplay, 4, 1, 2).Ready([]uint16{2, 3})
	if mode := CurrentMode(); mode != CollectionDisabled {
		t.Fatalf("disabled mode = %q", mode)
	}
	if got := Current(); got.Enabled || got.Mode != CollectionDisabled {
		t.Fatalf("disabled snapshot = %+v", got)
	}
}

func TestDisabledGlobalPathAllocations(t *testing.T) {
	Disable()
	var pub [32]byte
	var sig [64]byte
	msg := make([]byte, 200)
	jobs := []uint16{2, 3}
	allocs := testing.AllocsPerRun(1000, func() {
		RecordTransaction(SourceReplay, 1, len(msg))
		RecordVerification(SourceReplay, pub, sig, msg)
		BeginVerificationInDispatch(SourceReplay, DispatchJob{}, 0, pub, sig, msg)
		RecordQueueOccupancy(SourceReplay, 2)
		RecordNaturalBatchWidth(SourceReplay, 3)
		RecordDispatchOpportunity(SourceReplay, 4)
		RecordSignatureDispatch(SourceReplay, 4, 1, jobs)
		ReserveSignatureDispatch(SourceReplay, 4, 1, len(jobs)).Ready(jobs)
	})
	if allocs != 0 {
		t.Fatalf("disabled telemetry allocated %.2f objects per run", allocs)
	}
}

func TestPrometheusCollectorsRegistered(t *testing.T) {
	Enable(2)
	RecordVerification(SourceReplay, [32]byte{}, [64]byte{}, nil)
	Disable()

	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{
		"mithril_sigverify_telemetry_capacity":                   false,
		"mithril_sigverify_workload_verification_attempts_total": false,
	}
	for _, family := range families {
		if _, ok := want[family.GetName()]; ok {
			want[family.GetName()] = true
		}
	}
	for name, found := range want {
		if !found {
			t.Errorf("Prometheus collector %q is not registered", name)
		}
	}
}

func TestConcurrentLifecycleAndRecording(t *testing.T) {
	Disable()
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(id byte) {
			defer wg.Done()
			var pub [32]byte
			pub[0] = id
			for i := 0; i < 250; i++ {
				RecordVerification(SourceTPU, pub, [64]byte{}, []byte("race"))
				RecordDispatchOpportunity(SourceTPU, i&127)
				RecordSignatureDispatch(SourceTPU, i&127, i&63, []uint16{1})
			}
		}(byte(worker))
	}
	for i := 0; i < 50; i++ {
		Enable(8 + (i & 7))
		Disable()
	}
	wg.Wait()
	Disable()
	if Enabled() {
		t.Fatal("telemetry remained enabled")
	}
}

func TestSourceLabelsAreBounded(t *testing.T) {
	if got := sourceLabel(SourceTurbine); got != "turbine" {
		t.Fatalf("turbine source label = %q", got)
	}
	if got := sourceLabel(Source("future-or-untrusted")); got != "unknown" {
		t.Fatalf("unknown source label = %q", got)
	}
}

func BenchmarkRecordVerification(b *testing.B) {
	var pub [32]byte
	var sig [64]byte
	msg := make([]byte, 200)

	b.Run("disabled", func(b *testing.B) {
		Disable()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			RecordVerification(SourceReplay, pub, sig, msg)
		}
	})
	b.Run("enabled", func(b *testing.B) {
		Enable(1024)
		defer Disable()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			RecordVerification(SourceReplay, pub, sig, msg)
		}
	})
}

func BenchmarkRecordDispatchOpportunity(b *testing.B) {
	b.Run("disabled", func(b *testing.B) {
		Disable()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			RecordDispatchOpportunity(SourceTPU, i&8191)
		}
	})
	b.Run("enabled", func(b *testing.B) {
		Enable(1024)
		defer Disable()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			RecordDispatchOpportunity(SourceTPU, i&8191)
		}
	})
}

func BenchmarkRecordSignatureDispatch(b *testing.B) {
	jobs := []uint16{1, 1, 2, 4}
	b.Run("disabled", func(b *testing.B) {
		Disable()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			RecordSignatureDispatch(SourceTPU, i&8191, i&4095, jobs)
		}
	})
	b.Run("enabled", func(b *testing.B) {
		EnableSchedulingSimulation(1024)
		defer Disable()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			RecordSignatureDispatch(SourceTPU, i&8191, i&4095, jobs)
		}
	})
}

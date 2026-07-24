// Package sigverifytelemetry provides opt-in workload measurements for
// choosing sigverify batching and cache policies from real traffic.
//
// Each disabled recording hook returns after loading the process-wide observer
// pointer and allocates nothing. When enabled, the observer retains an exact,
// bounded window of verification inputs; this is intentionally a measurement
// mode rather than an always-on production cache. Capacity-only collection is
// passive. Scheduling simulation must be enabled separately and is allowed to
// change which worker claims work that is already queued, but never waits for
// a fuller group.
package sigverifytelemetry

import (
	"os"
	"strconv"
	"sync"
	"sync/atomic"
)

type Source string

const (
	SourceReplay  Source = "replay"
	SourceTPU     Source = "tpu"
	SourceTurbine Source = "turbine"
	SourceUnknown Source = "unknown"
)

type VerificationOutcome uint8

const (
	VerificationOutcomeUnknown VerificationOutcome = iota
	VerificationOutcomeValid
	VerificationOutcomeInvalid
)

// CollectionMode distinguishes passive observation from the explicitly
// intrusive nonblocking scheduling experiment. Its string values are stable
// trace-format values.
type CollectionMode string

const (
	CollectionDisabled             CollectionMode = "disabled"
	CollectionPassive              CollectionMode = "passive"
	CollectionSchedulingSimulation CollectionMode = "scheduling_simulation"
)

// VerificationAttempt identifies one retained attempt in the Observer that
// was active when BeginVerification ran. RecordResult is safe after Disable or
// a later Enable; it updates only that observer generation and is ignored if
// the bounded ring has already evicted the attempt.
type VerificationAttempt struct {
	observer *Observer
	sequence uint64
}

// SignatureDispatch is a worker claim reserved before input parsing. Ready
// attaches exact signature counts after parsing. Job returns the immutable
// correlation token passed into the verifier for a claimed job.
type SignatureDispatch struct {
	observer *Observer
	id       uint64
	jobs     uint32
	source   Source
}

// DispatchJob identifies one zero-based job inside a SignatureDispatch.
// A zero value means that the verification was not claimed by an instrumented
// replay/TPU worker and therefore has no dispatch correlation.
type DispatchJob struct {
	observer   *Observer
	dispatchID uint64
	jobIndex   uint32
	source     Source
}

// Job returns the correlation token for one claimed job. Out-of-range indexes
// deliberately return the zero value rather than inventing a correlation.
func (d SignatureDispatch) Job(index int) DispatchJob {
	if d.observer == nil || index < 0 || uint32(index) >= d.jobs {
		return DispatchJob{}
	}
	return DispatchJob{observer: d.observer, dispatchID: d.id, jobIndex: uint32(index), source: d.source}
}

// Ready records exact job boundaries after any parsing needed to discover
// them. It is write-once; a late call is ignored if the bounded dispatch ring
// has already evicted this dispatch.
func (d SignatureDispatch) Ready(jobSignatures []uint16) {
	if d.observer == nil {
		return
	}
	source, mode, lanes, ready := d.observer.readySignatureDispatch(d.id, jobSignatures)
	if !ready || mode != CollectionSchedulingSimulation {
		return
	}
	metricNaturalBatchWidth.WithLabelValues(sourceLabel(source), string(mode)).Observe(float64(lanes))
}

// RecordResult attaches the cryptographic outcome to a prior attempt.
func (a VerificationAttempt) RecordResult(valid bool) {
	if a.observer == nil {
		return
	}
	outcome := VerificationOutcomeInvalid
	if valid {
		outcome = VerificationOutcomeValid
	}
	a.observer.recordVerificationOutcome(a.sequence, outcome)
}

const (
	envCapacity             = "MITHRIL_SIGVERIFY_TELEMETRY_CAPACITY"
	envSchedulingSimulation = "MITHRIL_SIGVERIFY_TELEMETRY_SCHEDULING_SIMULATION"
)

var active atomic.Pointer[Observer]
var lifecycleMu sync.Mutex

func init() {
	capacity, err := strconv.Atoi(os.Getenv(envCapacity))
	if err == nil && capacity > 0 {
		simulate, _ := strconv.ParseBool(os.Getenv(envSchedulingSimulation))
		if simulate {
			EnableSchedulingSimulation(capacity)
		} else {
			Enable(capacity)
		}
	}
}

// Enable starts a fresh passive process-wide observation window. Capacity
// independently bounds exact-verification, key-recurrence, and dispatch rings.
func Enable(capacity int) {
	enable(capacity, CollectionPassive)
}

// EnableSchedulingSimulation starts a fresh observation window whose replay
// and TPU workers may nonblockingly claim already-visible work. It adds no
// timer or deliberate latency, but it is not a passive traffic trace.
func EnableSchedulingSimulation(capacity int) {
	enable(capacity, CollectionSchedulingSimulation)
}

func enable(capacity int, mode CollectionMode) {
	lifecycleMu.Lock()
	defer lifecycleMu.Unlock()
	if capacity <= 0 {
		disableLocked()
		return
	}
	o := newObserverWithMode(capacity, mode)
	metricCapacity.Set(float64(capacity))
	active.Store(o)
}

// Disable turns off observation and releases the process-wide history after
// in-flight observers release their references.
func Disable() {
	lifecycleMu.Lock()
	defer lifecycleMu.Unlock()
	disableLocked()
}

func disableLocked() {
	active.Store(nil)
	metricCapacity.Set(0)
}

// Enabled reports whether the opt-in observer is active.
func Enabled() bool { return active.Load() != nil }

// CurrentMode loads the active observer once and returns its immutable mode.
func CurrentMode() CollectionMode {
	o := active.Load()
	if o == nil {
		return CollectionDisabled
	}
	return o.mode
}

// RecordTransaction records the shape of a transaction presented to a
// signature verifier. It does not imply that every signature was valid.
func RecordTransaction(source Source, signatures, messageBytes int) {
	o := active.Load()
	if o == nil {
		return
	}
	if signatures < 0 {
		signatures = 0
	}
	if messageBytes < 0 {
		messageBytes = 0
	}
	o.recordTransaction(signatures, messageBytes)
	label := sourceLabel(source)
	metricTransactions.WithLabelValues(label).Inc()
	metricSignatures.WithLabelValues(label).Add(float64(signatures))
	metricSignaturesPerTransaction.WithLabelValues(label).Observe(float64(signatures))
	metricMessageBytes.WithLabelValues(label).Observe(float64(messageBytes))
}

// RecordVerification records one exact (public key, signature, message)
// verification attempt. Exact duplicate classification uses byte equality,
// not a hash, within the configured bounded history. The return values are
// exposed for focused tests and optional diagnostics.
func RecordVerification(source Source, pub [32]byte, sig [64]byte, message []byte) (duplicate bool, reuseDistance uint64, reused bool) {
	_, duplicate, reuseDistance, reused = BeginVerification(source, pub, sig, message)
	return duplicate, reuseDistance, reused
}

// BeginVerification records an exact attempt before cryptographic evaluation
// and returns a handle for attaching its eventual outcome. Call RecordResult
// exactly once after verification when policy replay needs valid-only cache
// admission. RecordVerification remains available for observations whose
// outcome is intentionally unknown.
func BeginVerification(source Source, pub [32]byte, sig [64]byte, message []byte) (attempt VerificationAttempt, duplicate bool, reuseDistance uint64, reused bool) {
	return beginVerification(source, pub, sig, message, DispatchJob{}, 0)
}

// BeginVerificationInDispatch is BeginVerification plus an exact zero-based
// lane correlation to a replay/TPU worker claim. If job is zero or belongs to
// an older observer generation, the attempt keeps the explicit source but is
// deliberately left uncorrelated.
func BeginVerificationInDispatch(source Source, job DispatchJob, lane int, pub [32]byte, sig [64]byte, message []byte) (attempt VerificationAttempt, duplicate bool, reuseDistance uint64, reused bool) {
	if job.observer != nil {
		source = job.source
	}
	return beginVerification(source, pub, sig, message, job, lane)
}

func beginVerification(source Source, pub [32]byte, sig [64]byte, message []byte, job DispatchJob, lane int) (attempt VerificationAttempt, duplicate bool, reuseDistance uint64, reused bool) {
	o := active.Load()
	if o == nil {
		return VerificationAttempt{}, false, 0, false
	}
	dispatchID := uint64(0)
	jobIndex := uint32(0)
	if job.observer == o && job.dispatchID != 0 {
		dispatchID = job.dispatchID
		jobIndex = job.jobIndex
	}
	if lane < 0 {
		lane = 0
	}
	source = normalizeSource(source)
	duplicate, reuseDistance, reused, sequence := o.recordVerificationFromDispatch(source, pub, sig, message, dispatchID, jobIndex, uint32(lane))
	label := string(source)
	metricVerificationAttempts.WithLabelValues(label).Inc()
	if duplicate {
		metricExactDuplicateHits.WithLabelValues(label).Inc()
	}
	if reused {
		metricKeyReuseHits.WithLabelValues(label).Inc()
		metricKeyReuseDistance.WithLabelValues(label).Observe(float64(reuseDistance))
	}
	return VerificationAttempt{observer: o, sequence: sequence}, duplicate, reuseDistance, reused
}

// RecordQueueOccupancy records the queue depth visible at an enqueue or
// dequeue boundary. The source label keeps replay and TPU pressure separate.
func RecordQueueOccupancy(source Source, depth int) {
	o := active.Load()
	if o == nil {
		return
	}
	if depth < 0 {
		depth = 0
	}
	o.recordQueue(depth)
	metricQueueOccupancy.WithLabelValues(sourceLabel(source), string(o.mode)).Observe(float64(depth))
}

// RecordNaturalBatchWidth records an explicit scheduling-simulation width. It
// is ignored in passive mode, which cannot know queued jobs' signature counts
// without claiming them.
func RecordNaturalBatchWidth(source Source, width int) {
	o := active.Load()
	if o == nil || o.mode != CollectionSchedulingSimulation {
		return
	}
	if width < 1 {
		width = 1
	}
	o.recordBatch(width)
	metricNaturalBatchWidth.WithLabelValues(sourceLabel(source), string(o.mode)).Observe(float64(width))
}

// RecordDispatchOpportunity records queue occupancy when a worker takes an
// item. In scheduling-simulation mode it also records queued+current as a
// candidate item width; passive mode does not call that an exact lane width.
// When disabled, this hook returns after its observer pointer load and
// allocates nothing.
func RecordDispatchOpportunity(source Source, queued int) {
	o := active.Load()
	if o == nil {
		return
	}
	if queued < 0 {
		queued = 0
	}
	width := queued + 1
	label := sourceLabel(source)
	metricQueueOccupancy.WithLabelValues(label, string(o.mode)).Observe(float64(queued))
	if o.mode == CollectionSchedulingSimulation {
		o.recordDispatch(queued, width)
		metricNaturalBatchWidth.WithLabelValues(label, string(o.mode)).Observe(float64(width))
	} else {
		o.recordQueue(queued)
	}
}

// ReserveSignatureDispatch records a worker claim before parsing and returns
// a handle that later supplies exact job boundaries. queuedBefore is sampled
// immediately after the first receive; queuedAfter is sampled after the full
// claim. Capacity-only passive mode always reserves a one-job claim. Only
// scheduling-simulation mode may reserve a larger group.
func ReserveSignatureDispatch(source Source, queuedBefore, queuedAfter, jobs int) SignatureDispatch {
	o := active.Load()
	if o == nil || jobs <= 0 || uint64(jobs) > uint64(^uint32(0)) {
		return SignatureDispatch{}
	}
	if o.mode == CollectionPassive && jobs != 1 {
		return SignatureDispatch{}
	}
	if queuedBefore < 0 {
		queuedBefore = 0
	}
	if queuedAfter < 0 {
		queuedAfter = 0
	}
	source = normalizeSource(source)
	id := o.reserveSignatureDispatch(source, queuedBefore, queuedAfter, jobs)
	metricQueueOccupancy.WithLabelValues(string(source), string(o.mode)).Observe(float64(queuedBefore))
	return SignatureDispatch{observer: o, id: id, jobs: uint32(jobs), source: source}
}

// RecordSignatureDispatch is a compatibility helper for callers that already
// know all job boundaries. New worker hooks should reserve before parsing and
// call Ready afterward so claim and ready events remain distinct.
func RecordSignatureDispatch(source Source, queuedBefore, queuedAfter int, jobSignatures []uint16) {
	dispatch := ReserveSignatureDispatch(source, queuedBefore, queuedAfter, len(jobSignatures))
	if dispatch.observer == nil {
		return
	}
	dispatch.Ready(jobSignatures)
}

// Current returns a consistent aggregate snapshot. Its histories are process
// wide; Prometheus metrics retain source-specific labels.
func Current() Snapshot {
	o := active.Load()
	if o == nil {
		return Snapshot{Mode: CollectionDisabled}
	}
	return o.snapshot()
}

type verificationKey struct {
	pub     [32]byte
	sig     [64]byte
	message string
}

type exactSlot struct {
	key           verificationKey
	seq           uint64
	beginEvent    uint64
	completeEvent uint64
	source        Source
	duplicate     bool
	reuseDistance uint64
	reused        bool
	outcome       VerificationOutcome
	dispatchID    uint64
	jobIndex      uint32
	laneIndex     uint32
}

type dispatchSlot struct {
	id             uint64
	claimEvent     uint64
	readyEvent     uint64
	source         Source
	mode           CollectionMode
	queuedBefore   uint64
	queuedAfter    uint64
	signatureLanes uint64
	jobSignatures  []uint16
}

type keySlot struct {
	key [32]byte
	seq uint64
}

type Observer struct {
	mu          sync.Mutex
	capacity    uint64
	mode        CollectionMode
	seq         uint64
	eventSeq    uint64
	dispatchSeq uint64

	exact        map[verificationKey]uint64
	exactRing    []exactSlot
	keys         map[[32]byte]uint64
	keyRing      []keySlot
	dispatchRing []dispatchSlot

	transactions uint64
	signatures   uint64
	messageBytes uint64
	duplicates   uint64
	keyReuses    uint64
	queueSamples uint64
	queueSum     uint64
	queueMax     uint64
	batchSamples uint64
	batchSum     uint64
	batchMax     uint64

	signaturesPerTransaction [19]uint64 // exact 0..17, then 18+
	messageSize              [9]uint64  // <= 0/64/128/200/256/512/1024/1232, then larger
	reuseDistance            [19]uint64 // <= 1,2,4,...,2^17, then larger
	queueOccupancy           [16]uint64 // <= 0,1,2,4,...,8192, then larger
	naturalBatchWidth        [20]uint64 // <=1, exact 2..17, <=32, <=64, then larger
}

func newObserver(capacity int) *Observer {
	return newObserverWithMode(capacity, CollectionPassive)
}

func newObserverWithMode(capacity int, mode CollectionMode) *Observer {
	if mode != CollectionSchedulingSimulation {
		mode = CollectionPassive
	}
	return &Observer{
		capacity:     uint64(capacity),
		mode:         mode,
		exact:        make(map[verificationKey]uint64, capacity),
		exactRing:    make([]exactSlot, capacity),
		keys:         make(map[[32]byte]uint64, capacity),
		keyRing:      make([]keySlot, capacity),
		dispatchRing: make([]dispatchSlot, capacity),
	}
}

func (o *Observer) recordTransaction(signatures, messageBytes int) {
	if signatures < 0 {
		signatures = 0
	}
	if messageBytes < 0 {
		messageBytes = 0
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.transactions++
	o.signatures += uint64(signatures)
	o.messageBytes += uint64(messageBytes)
	sigBucket := signatures
	if sigBucket > 18 {
		sigBucket = 18
	}
	o.signaturesPerTransaction[sigBucket]++
	o.messageSize[messageBucket(messageBytes)]++
}

func (o *Observer) recordVerification(pub [32]byte, sig [64]byte, message []byte) (bool, uint64, bool) {
	duplicate, reuseDistance, reused, _ := o.recordVerificationFrom(SourceUnknown, pub, sig, message)
	return duplicate, reuseDistance, reused
}

func (o *Observer) recordVerificationFrom(source Source, pub [32]byte, sig [64]byte, message []byte) (bool, uint64, bool, uint64) {
	return o.recordVerificationFromDispatch(source, pub, sig, message, 0, 0, 0)
}

func (o *Observer) recordVerificationFromDispatch(source Source, pub [32]byte, sig [64]byte, message []byte, dispatchID uint64, jobIndex, laneIndex uint32) (bool, uint64, bool, uint64) {
	// The string conversion takes an immutable copy of message. Together with
	// the fixed-size arrays this is exact byte identity without a digest.
	// This allocation is why the observer is explicitly opt-in.
	exactKey := verificationKey{pub: pub, sig: sig, message: string(message)}

	o.mu.Lock()
	defer o.mu.Unlock()

	o.seq++
	seq := o.seq
	o.eventSeq++
	beginEvent := o.eventSeq
	duplicate := false
	if _, ok := o.exact[exactKey]; ok {
		duplicate = true
		o.duplicates++
	}

	reuseDistance := uint64(0)
	reused := false
	if previous, ok := o.keys[pub]; ok {
		reused = true
		reuseDistance = seq - previous
		o.keyReuses++
		o.reuseDistance[powerOfTwoBucket(reuseDistance, len(o.reuseDistance))]++
	}

	idx := (seq - 1) % o.capacity
	oldExact := o.exactRing[idx]
	if oldExact.seq != 0 && o.exact[oldExact.key] == oldExact.seq {
		delete(o.exact, oldExact.key)
	}
	oldKey := o.keyRing[idx]
	if oldKey.seq != 0 && o.keys[oldKey.key] == oldKey.seq {
		delete(o.keys, oldKey.key)
	}

	o.exact[exactKey] = seq
	o.exactRing[idx] = exactSlot{
		key:           exactKey,
		seq:           seq,
		beginEvent:    beginEvent,
		source:        normalizeSource(source),
		duplicate:     duplicate,
		reuseDistance: reuseDistance,
		reused:        reused,
		dispatchID:    dispatchID,
		jobIndex:      jobIndex,
		laneIndex:     laneIndex,
	}
	o.keys[pub] = seq
	o.keyRing[idx] = keySlot{key: pub, seq: seq}
	return duplicate, reuseDistance, reused, seq
}

func (o *Observer) recordVerificationOutcome(sequence uint64, outcome VerificationOutcome) {
	if sequence == 0 {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	idx := (sequence - 1) % o.capacity
	if o.exactRing[idx].seq == sequence && o.exactRing[idx].outcome == VerificationOutcomeUnknown {
		o.eventSeq++
		o.exactRing[idx].outcome = outcome
		o.exactRing[idx].completeEvent = o.eventSeq
	}
}

func (o *Observer) reserveSignatureDispatch(source Source, queuedBefore, queuedAfter, jobs int) uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.eventSeq++
	o.dispatchSeq++
	idx := (o.dispatchSeq - 1) % o.capacity
	o.dispatchRing[idx] = dispatchSlot{
		id:            o.dispatchSeq,
		claimEvent:    o.eventSeq,
		source:        normalizeSource(source),
		mode:          o.mode,
		queuedBefore:  uint64(queuedBefore),
		queuedAfter:   uint64(queuedAfter),
		jobSignatures: make([]uint16, jobs),
	}
	o.recordQueueLocked(queuedBefore)
	return o.dispatchSeq
}

func (o *Observer) readySignatureDispatch(id uint64, jobSignatures []uint16) (Source, CollectionMode, uint64, bool) {
	if id == 0 {
		return SourceUnknown, CollectionDisabled, 0, false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	idx := (id - 1) % o.capacity
	slot := &o.dispatchRing[idx]
	if slot.id != id || slot.readyEvent != 0 || len(jobSignatures) != len(slot.jobSignatures) {
		return SourceUnknown, CollectionDisabled, 0, false
	}
	copy(slot.jobSignatures, jobSignatures)
	for _, signatures := range slot.jobSignatures {
		slot.signatureLanes += uint64(signatures)
	}
	o.eventSeq++
	slot.readyEvent = o.eventSeq
	if slot.mode == CollectionSchedulingSimulation {
		o.recordBatchLocked(int(slot.signatureLanes))
	}
	return slot.source, slot.mode, slot.signatureLanes, true
}

func (o *Observer) recordSignatureDispatch(source Source, queuedBefore, queuedAfter int, jobSignatures []uint16) uint64 {
	id := o.reserveSignatureDispatch(source, queuedBefore, queuedAfter, len(jobSignatures))
	_, _, signatureLanes, _ := o.readySignatureDispatch(id, jobSignatures)
	return signatureLanes
}

func (o *Observer) recordQueue(depth int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.recordQueueLocked(depth)
}

func (o *Observer) recordBatch(width int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.recordBatchLocked(width)
}

func (o *Observer) recordDispatch(depth, width int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.recordQueueLocked(depth)
	o.recordBatchLocked(width)
}

func (o *Observer) recordQueueLocked(depth int) {
	o.queueSamples++
	o.queueSum += uint64(depth)
	if uint64(depth) > o.queueMax {
		o.queueMax = uint64(depth)
	}
	o.queueOccupancy[queueBucket(depth)]++
}

func (o *Observer) recordBatchLocked(width int) {
	o.batchSamples++
	o.batchSum += uint64(width)
	if uint64(width) > o.batchMax {
		o.batchMax = uint64(width)
	}
	o.naturalBatchWidth[batchBucket(width)]++
}

func (o *Observer) snapshot() Snapshot {
	o.mu.Lock()
	snapshot := Snapshot{
		Enabled:                  true,
		Mode:                     o.mode,
		Capacity:                 o.capacity,
		Transactions:             o.transactions,
		Signatures:               o.signatures,
		MessageBytes:             o.messageBytes,
		VerificationAttempts:     o.seq,
		ObservedEvents:           o.eventSeq,
		ExactDuplicateHits:       o.duplicates,
		PublicKeyReuseHits:       o.keyReuses,
		QueueSamples:             o.queueSamples,
		QueueOccupancySum:        o.queueSum,
		QueueOccupancyMax:        o.queueMax,
		BatchSamples:             o.batchSamples,
		NaturalBatchWidthSum:     o.batchSum,
		NaturalBatchWidthMax:     o.batchMax,
		SignaturesPerTransaction: o.signaturesPerTransaction,
		MessageSize:              o.messageSize,
		ReuseDistance:            o.reuseDistance,
		QueueOccupancy:           o.queueOccupancy,
		NaturalBatchWidth:        o.naturalBatchWidth,
	}

	// Take shallow copies of immutable trace slots while holding the lock,
	// then duplicate message bytes after releasing it. Exporting a large
	// profiling window therefore does not block new observations while the
	// snapshot takes ownership of retained messages.
	traceCount := o.seq
	if traceCount > o.capacity {
		traceCount = o.capacity
	}
	traceSlots := make([]exactSlot, traceCount)
	firstSequence := o.seq - traceCount + 1
	for i := range traceSlots {
		sequence := firstSequence + uint64(i)
		traceSlots[i] = o.exactRing[(sequence-1)%o.capacity]
	}
	dispatchCount := o.dispatchSeq
	if dispatchCount > o.capacity {
		dispatchCount = o.capacity
	}
	dispatchSlots := make([]dispatchSlot, dispatchCount)
	firstDispatch := o.dispatchSeq - dispatchCount + 1
	for i := range dispatchSlots {
		sequence := firstDispatch + uint64(i)
		dispatchSlots[i] = o.dispatchRing[(sequence-1)%o.capacity]
		dispatchSlots[i].jobSignatures = append([]uint16(nil), dispatchSlots[i].jobSignatures...)
	}
	o.mu.Unlock()

	snapshot.VerificationTrace = make([]VerificationTraceEntry, len(traceSlots))
	for i, slot := range traceSlots {
		snapshot.VerificationTrace[i] = VerificationTraceEntry{
			Sequence:                slot.seq,
			BeginEventSequence:      slot.beginEvent,
			CompletionEventSequence: slot.completeEvent,
			Source:                  slot.source,
			PublicKey:               slot.key.pub,
			Signature:               slot.key.sig,
			Message:                 []byte(slot.key.message),
			ExactDuplicate:          slot.duplicate,
			PublicKeyReused:         slot.reused,
			ReuseDistance:           slot.reuseDistance,
			Outcome:                 slot.outcome,
			DispatchID:              slot.dispatchID,
			JobIndex:                slot.jobIndex,
			LaneIndex:               slot.laneIndex,
		}
	}
	snapshot.DispatchTrace = make([]DispatchTraceEntry, len(dispatchSlots))
	for i, slot := range dispatchSlots {
		snapshot.DispatchTrace[i] = DispatchTraceEntry{
			DispatchID:         slot.id,
			Sequence:           slot.id,
			ClaimEventSequence: slot.claimEvent,
			ReadyEventSequence: slot.readyEvent,
			Mode:               slot.mode,
			Source:             slot.source,
			QueuedItemsBefore:  slot.queuedBefore,
			QueuedItemsAfter:   slot.queuedAfter,
			SignatureLanes:     slot.signatureLanes,
			JobSignatures:      slot.jobSignatures,
		}
	}
	return snapshot
}

// VerificationTraceEntry is one retained signature-verification attempt.
// Sequence is attempt-observation order. BeginEventSequence and
// CompletionEventSequence share a separate event clock with dispatch records;
// a zero completion event means the outcome is still unknown. Message is an
// exact snapshot-owned copy, so callers may retain or mutate it without
// changing duplicate classification or future snapshots.
type VerificationTraceEntry struct {
	Sequence                uint64
	BeginEventSequence      uint64
	CompletionEventSequence uint64
	Source                  Source
	PublicKey               [32]byte
	Signature               [64]byte
	Message                 []byte
	ExactDuplicate          bool
	PublicKeyReused         bool
	ReuseDistance           uint64
	Outcome                 VerificationOutcome
	DispatchID              uint64
	JobIndex                uint32
	LaneIndex               uint32
}

// DispatchTraceEntry is one worker claim. Passive claims contain exactly one
// job; scheduling-simulation claims may contain a larger nonblocking group.
// ClaimEventSequence is reserved before TPU parsing. ReadyEventSequence is set
// when all job boundaries are known; zero means the claim is not ready yet.
type DispatchTraceEntry struct {
	DispatchID uint64
	// Sequence is retained as an alias for DispatchID for in-process users of
	// the initial telemetry prototype.
	Sequence           uint64
	ClaimEventSequence uint64
	ReadyEventSequence uint64
	Mode               CollectionMode
	Source             Source
	QueuedItemsBefore  uint64
	QueuedItemsAfter   uint64
	SignatureLanes     uint64
	JobSignatures      []uint16
}

// Snapshot is the bounded observer's aggregate state. Bucket definitions are
// documented on the corresponding fields and in docs/SIGVERIFY_TELEMETRY.md.
type Snapshot struct {
	Enabled              bool
	Mode                 CollectionMode
	Capacity             uint64
	Transactions         uint64
	Signatures           uint64
	MessageBytes         uint64
	VerificationAttempts uint64
	ObservedEvents       uint64
	ExactDuplicateHits   uint64
	PublicKeyReuseHits   uint64
	QueueSamples         uint64
	QueueOccupancySum    uint64
	QueueOccupancyMax    uint64
	BatchSamples         uint64
	NaturalBatchWidthSum uint64
	NaturalBatchWidthMax uint64

	// VerificationTrace is the chronological suffix of exact verification
	// inputs retained by Capacity. DispatchTrace independently retains the
	// chronological suffix of exact worker dispatch groups. Verification data
	// is suitable for recurrence/cache replay. Exact multi-job SIMD grouping is
	// present only in scheduling-simulation mode; passive records preserve the
	// one job actually claimed. Identity never relies on a digest.
	VerificationTrace []VerificationTraceEntry
	DispatchTrace     []DispatchTraceEntry

	SignaturesPerTransaction [19]uint64
	MessageSize              [9]uint64
	ReuseDistance            [19]uint64
	QueueOccupancy           [16]uint64
	NaturalBatchWidth        [20]uint64
}

func sourceLabel(source Source) string {
	return string(normalizeSource(source))
}

func normalizeSource(source Source) Source {
	switch source {
	case SourceReplay, SourceTPU, SourceTurbine:
		return source
	default:
		return SourceUnknown
	}
}

func messageBucket(n int) int {
	limits := [...]int{0, 64, 128, 200, 256, 512, 1024, 1232}
	for i, limit := range limits {
		if n <= limit {
			return i
		}
	}
	return len(limits)
}

func powerOfTwoBucket(n uint64, buckets int) int {
	limit := uint64(1)
	for i := 0; i < buckets-1; i++ {
		if n <= limit {
			return i
		}
		limit <<= 1
	}
	return buckets - 1
}

func queueBucket(depth int) int {
	if depth <= 0 {
		return 0
	}
	return 1 + powerOfTwoBucket(uint64(depth), 15)
}

func batchBucket(width int) int {
	if width <= 1 {
		return 0
	}
	if width <= 17 {
		return width - 1
	}
	if width <= 32 {
		return 17
	}
	if width <= 64 {
		return 18
	}
	return 19
}

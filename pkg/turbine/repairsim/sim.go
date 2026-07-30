package repairsim

import (
	"container/heap"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
)

type Scenario string

const (
	ScenarioNearTip     Scenario = "near-tip"
	ScenarioDeepCatchup Scenario = "deep-catchup"
)

type Availability string

const (
	AvailabilityComplete Availability = "complete"
	AvailabilityNearLoss Availability = "near-loss"
	AvailabilitySparse   Availability = "sparse"
	AvailabilityMixed    Availability = "mixed"
)

// Config controls the deterministic network and local starting state.
type Config struct {
	Scenario             Scenario      `json:"scenario"`
	Availability         Availability  `json:"availability"`
	RepairEnabled        bool          `json:"repair_enabled"`
	RepairLatency        time.Duration `json:"repair_latency_ns"`
	RepairJitter         time.Duration `json:"repair_jitter_ns"`
	PacketLoss           float64       `json:"packet_loss"`
	DuplicateProbability float64       `json:"duplicate_probability"`
	BandwidthBytesPerSec int64         `json:"bandwidth_bytes_per_sec"`
	MaxConcurrent        int           `json:"max_concurrent_requests"`
	MaxRequestSlots      int           `json:"max_request_slots"`
	MaxMissingPerSlot    int           `json:"max_missing_per_slot"`
	CorruptResponses     int           `json:"corrupt_responses"`
	NaturalLateShreds    bool          `json:"natural_late_shreds"`
	CollectTrace         bool          `json:"collect_trace"`
	Seed                 int64         `json:"seed"`
	SpoolDir             string        `json:"spool_dir,omitempty"`
	SpoolMaxBytes        int64         `json:"spool_max_bytes"`
}

// DefaultConfig returns a deterministic starting point for a scenario.
func DefaultConfig(scenario Scenario) Config {
	cfg := Config{
		Scenario:             scenario,
		RepairEnabled:        true,
		RepairLatency:        20 * time.Millisecond,
		RepairJitter:         2 * time.Millisecond,
		DuplicateProbability: 0.02,
		BandwidthBytesPerSec: 100 * 1024 * 1024,
		MaxConcurrent:        256,
		MaxRequestSlots:      64,
		MaxMissingPerSlot:    256,
		NaturalLateShreds:    scenario == ScenarioNearTip,
		CollectTrace:         true,
		Seed:                 1,
		SpoolMaxBytes:        1 << 30,
	}
	if scenario == ScenarioDeepCatchup {
		cfg.Availability = AvailabilityMixed
	} else {
		cfg.Availability = AvailabilityNearLoss
	}
	return cfg
}

// TraceEvent is a deterministic logical-time event. CPU durations are kept in
// Result.StageCPU so trace equality does not depend on scheduler noise.
type TraceEvent struct {
	Sequence    int               `json:"sequence"`
	AtNanos     int64             `json:"at_ns"`
	Stage       string            `json:"stage"`
	Slot        uint64            `json:"slot,omitempty"`
	FECSetIndex uint32            `json:"fec_set_index,omitempty"`
	ShredIndex  uint32            `json:"shred_index,omitempty"`
	ShredType   turbine.ShredType `json:"shred_type,omitempty"`
	Bytes       int               `json:"bytes,omitempty"`
	Detail      string            `json:"detail,omitempty"`
}

type LatencySummary struct {
	P50 time.Duration `json:"p50_ns"`
	P95 time.Duration `json:"p95_ns"`
	P99 time.Duration `json:"p99_ns"`
}

// Result separates simulated-network time from actual local execution time.
type Result struct {
	Scenario                   Scenario                 `json:"scenario"`
	Availability               Availability             `json:"availability"`
	Slots                      int                      `json:"slots"`
	CompletedSlots             int                      `json:"completed_slots"`
	LogicalElapsed             time.Duration            `json:"logical_elapsed_ns"`
	WallElapsed                time.Duration            `json:"wall_elapsed_ns"`
	TimeToFirstReplayable      time.Duration            `json:"time_to_first_replayable_ns"`
	TimeToFirstRecoveredData   time.Duration            `json:"time_to_first_recovered_data_ns"`
	RepairEligibleToRecovery   time.Duration            `json:"repair_eligible_to_first_recovery_ns"`
	CompletionLatency          LatencySummary           `json:"completion_latency"`
	SlotsPerLogicalSecond      float64                  `json:"slots_per_logical_second"`
	SlotsPerCPUSecond          float64                  `json:"slots_per_cpu_second"`
	RepairRequests             uint64                   `json:"repair_requests"`
	RepairResponses            uint64                   `json:"repair_responses"`
	RepairBytesRequested       uint64                   `json:"repair_bytes_requested"`
	RepairBytesReceived        uint64                   `json:"repair_bytes_received"`
	UsefulNetworkDataShreds    uint64                   `json:"useful_network_data_shreds"`
	LocallyRecoveredDataShreds uint64                   `json:"locally_recovered_data_shreds"`
	LocallyRecoveredDataBytes  uint64                   `json:"locally_recovered_data_bytes"`
	InitialMissingDataShreds   uint64                   `json:"initial_missing_data_shreds"`
	FractionRecoveredLocally   float64                  `json:"fraction_missing_recovered_locally"`
	FECDecodes                 uint64                   `json:"fec_decodes"`
	DuplicateResponses         uint64                   `json:"duplicate_responses"`
	CanceledOrLateResponses    uint64                   `json:"canceled_or_late_responses"`
	LostResponses              uint64                   `json:"lost_responses"`
	RejectedCorruptResponses   uint64                   `json:"rejected_corrupt_responses"`
	ShredSignatureCacheHits    uint64                   `json:"shred_signature_cache_hits"`
	ShredEd25519Verifications  uint64                   `json:"shred_ed25519_verifications"`
	QueueHighWater             int                      `json:"queue_high_water"`
	SpoolBytes                 int64                    `json:"spool_bytes"`
	SpoolCompleteSlots         int                      `json:"spool_complete_slots"`
	DataShredBytesReplayable   uint64                   `json:"data_shred_bytes_replayable"`
	Allocations                uint64                   `json:"allocations"`
	StageCPU                   map[string]time.Duration `json:"stage_cpu_ns"`
	Trace                      []TraceEvent             `json:"trace"`
	Limitations                []string                 `json:"limitations"`
}

// Run executes the virtual network around production parsing, validation,
// repair selection, reconstruction, spool insertion, and block completion.
func Run(ledger *Ledger, cfg Config) (Result, error) {
	if ledger == nil || len(ledger.Slots) == 0 {
		return Result{}, errors.New("empty ledger")
	}
	if cfg.Scenario != ScenarioNearTip && cfg.Scenario != ScenarioDeepCatchup {
		return Result{}, fmt.Errorf("unsupported scenario %q", cfg.Scenario)
	}
	if cfg.Availability == "" {
		cfg.Availability = DefaultConfig(cfg.Scenario).Availability
	}
	if cfg.MaxConcurrent <= 0 {
		cfg.MaxConcurrent = 1
	}
	if cfg.MaxRequestSlots <= 0 {
		cfg.MaxRequestSlots = 64
	}
	if cfg.MaxMissingPerSlot <= 0 {
		cfg.MaxMissingPerSlot = 256
	}
	if cfg.SpoolMaxBytes <= 0 {
		cfg.SpoolMaxBytes = 1 << 30
	}

	spoolDir := cfg.SpoolDir
	if spoolDir == "" {
		var err error
		spoolDir, err = os.MkdirTemp("", "mithril-repair-sim-")
		if err != nil {
			return Result{}, err
		}
		defer os.RemoveAll(spoolDir)
	}
	spool, err := turbine.OpenShredSpool(spoolDir, cfg.SpoolMaxBytes)
	if err != nil {
		return Result{}, err
	}
	defer spool.Close()

	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)
	wallStarted := time.Now()
	s := &simulation{
		ledger:       ledger,
		cfg:          cfg,
		assembler:    turbine.NewSlotAssembler(),
		spool:        spool,
		rng:          rand.New(rand.NewSource(cfg.Seed)),
		firstShred:   make(map[uint64]time.Duration),
		completed:    make(map[uint64]*block.Block),
		replayableAt: make(map[uint64]time.Duration),
		pending:      make(map[repairKey]struct{}),
		stageCPU:     make(map[string]time.Duration),
	}
	s.assembler.SetRetentionFloor(ledger.Slots[0].Number)
	s.assembler.SetOnComplete(spool.MarkComplete)
	s.nextReplaySlot = ledger.Slots[0].Number

	if err := s.seedLocalState(); err != nil {
		return Result{}, err
	}
	if cfg.RepairEnabled {
		if err := s.repairUntilDone(); err != nil {
			return Result{}, err
		}
	}

	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)
	_, spoolBytes := spool.Stats()
	result := s.result
	result.Scenario = cfg.Scenario
	result.Availability = cfg.Availability
	result.Slots = len(ledger.Slots)
	result.CompletedSlots = len(s.completed)
	result.LogicalElapsed = s.now
	result.WallElapsed = time.Since(wallStarted)
	result.LocallyRecoveredDataShreds = s.assembler.RecoveredDataShreds()
	result.InitialMissingDataShreds = initialMissingDataShreds(ledger, cfg.Availability)
	if result.InitialMissingDataShreds > 0 {
		result.FractionRecoveredLocally = float64(result.LocallyRecoveredDataShreds) / float64(result.InitialMissingDataShreds)
	}
	if len(ledger.Slots[0].FECs) > 0 && len(ledger.Slots[0].FECs[0].Data) > 0 {
		result.LocallyRecoveredDataBytes = result.LocallyRecoveredDataShreds * uint64(len(ledger.Slots[0].FECs[0].Data[0].Bytes))
	}
	if s.haveRecoverAt {
		result.TimeToFirstRecoveredData = s.firstRecoverAt
		if s.haveRepairAt && s.firstRecoverAt >= s.firstRepairAt {
			result.RepairEligibleToRecovery = s.firstRecoverAt - s.firstRepairAt
		}
	}
	result.SpoolBytes = spoolBytes
	result.SpoolCompleteSlots = spool.CompleteSlots()
	result.Allocations = memAfter.Mallocs - memBefore.Mallocs
	result.StageCPU = s.stageCPU
	result.Trace = s.trace
	result.ShredSignatureCacheHits, result.ShredEd25519Verifications = s.shredVerifier.Stats()
	result.Limitations = []string{
		"remote peers and latency are simulated in process; no UDP/IP stack is measured",
		"synthetic entries contain no transactions, so transaction execution is not measured",
		"slot offered to replay means SlotAssembler emitted a verified block; replay execution is not run",
		"FEC decode start is observed at the AddShredFrom call boundary, not inside the Reed-Solomon library",
	}
	latencies := make([]time.Duration, 0, len(s.replayableAt))
	for slot, completedAt := range s.replayableAt {
		if first, ok := s.firstShred[slot]; ok {
			latencies = append(latencies, completedAt-first)
		}
	}
	result.CompletionLatency = summarizeLatencies(latencies)
	if len(s.replayableAt) > 0 {
		first := ledger.Slots[0].Number
		result.TimeToFirstReplayable = s.replayableAt[first]
	}
	if result.LogicalElapsed > 0 {
		result.SlotsPerLogicalSecond = float64(result.CompletedSlots) / result.LogicalElapsed.Seconds()
	}
	if result.WallElapsed > 0 {
		result.SlotsPerCPUSecond = float64(result.CompletedSlots) / result.WallElapsed.Seconds()
	}
	return result, nil
}

type simulation struct {
	ledger         *Ledger
	cfg            Config
	assembler      *turbine.SlotAssembler
	shredVerifier  turbine.ShredSignatureVerifier
	spool          *turbine.ShredSpool
	rng            *rand.Rand
	now            time.Duration
	nextWireAt     time.Duration
	sequence       int
	trace          []TraceEvent
	queue          deliveryHeap
	pending        map[repairKey]struct{}
	firstShred     map[uint64]time.Duration
	completed      map[uint64]*block.Block
	replayableAt   map[uint64]time.Duration
	nextReplaySlot uint64
	stageCPU       map[string]time.Duration
	result         Result
	corruptLeft    int
	firstRepairAt  time.Duration
	haveRepairAt   bool
	firstRecoverAt time.Duration
	haveRecoverAt  bool
}

func (s *simulation) seedLocalState() error {
	s.corruptLeft = s.cfg.CorruptResponses
	for ordinal := 0; ; ordinal++ {
		added := false
		for slotIdx := range s.ledger.Slots {
			packets := initialPackets(&s.ledger.Slots[slotIdx], s.cfg.Availability)
			if ordinal >= len(packets) {
				continue
			}
			added = true
			if err := s.ingest(packets[ordinal], false, false); err != nil {
				return fmt.Errorf("seed slot %d: %w", s.ledger.Slots[slotIdx].Number, err)
			}
			s.now++
		}
		if !added {
			break
		}
	}
	if s.cfg.NaturalLateShreds && s.cfg.Availability == AvailabilityNearLoss {
		for i := range s.ledger.Slots {
			slot := &s.ledger.Slots[i]
			if len(slot.FECs) == 0 || len(slot.FECs[0].Data) < 31 {
				continue
			}
			at := s.now + s.cfg.RepairLatency/2 + time.Duration(i)*time.Microsecond
			heap.Push(&s.queue, delivery{at: at, sequence: s.sequence, packet: slot.FECs[0].Data[30]})
			s.sequence++
		}
	}
	return nil
}

func initialPackets(slot *Slot, availability Availability) []Packet {
	var out []Packet
	for i := range slot.FECs {
		fec := &slot.FECs[i]
		switch availability {
		case AvailabilityComplete:
			out = append(out, fec.Data...)
		case AvailabilityNearLoss:
			if i%2 == 0 {
				out = append(out, fec.Data[:30]...)
				out = append(out, fec.Coding[0])
			} else {
				out = append(out, fec.Data[:31]...)
			}
		case AvailabilitySparse:
			out = append(out, fec.Data[:2]...)
		case AvailabilityMixed:
			out = append(out, fec.Data[:16]...)
			out = append(out, fec.Coding[:15]...)
		}
	}
	return out
}

func (s *simulation) repairUntilDone() error {
	const maxIterations = 10_000_000
	for iterations := 0; len(s.completed) < len(s.ledger.Slots); iterations++ {
		if iterations >= maxIterations {
			return errors.New("repair simulation exceeded iteration limit")
		}
		s.prioritizeHeadWindow()
		s.scheduleRequests()
		if len(s.queue) == 0 {
			return fmt.Errorf("repair stalled with %d/%d completed", len(s.completed), len(s.ledger.Slots))
		}
		event := heap.Pop(&s.queue).(delivery)
		if event.at > s.now {
			s.now = event.at
		}
		if event.primary {
			delete(s.pending, event.key)
		}
		if event.drop {
			s.result.LostResponses++
			s.record("repair_response_lost", event.packet, "")
			continue
		}
		if event.duplicate {
			s.result.DuplicateResponses++
		}
		if event.fromRepair {
			s.result.RepairResponses++
			s.result.RepairBytesReceived += uint64(len(event.packet.Bytes))
		}
		beforeUseful := s.assembler.UsefulRepairShreds()
		if err := s.ingest(event.packet, event.fromRepair, event.corrupt); err != nil {
			if event.corrupt && errors.Is(err, turbine.ErrInvalidSignature) {
				s.result.RejectedCorruptResponses++
				s.record("repair_response_rejected", event.packet, "invalid signature or Merkle proof")
				continue
			}
			return err
		}
		if event.fromRepair {
			afterUseful := s.assembler.UsefulRepairShreds()
			if afterUseful == beforeUseful {
				s.result.CanceledOrLateResponses++
			} else {
				s.result.UsefulNetworkDataShreds += afterUseful - beforeUseful
			}
		}
	}
	return nil
}

func (s *simulation) prioritizeHeadWindow() {
	var head uint64
	found := false
	for i := range s.ledger.Slots {
		slot := s.ledger.Slots[i].Number
		if _, complete := s.completed[slot]; !complete {
			head, found = slot, true
			break
		}
	}
	if !found {
		return
	}
	end := head + 63
	last := s.ledger.Slots[len(s.ledger.Slots)-1].Number
	if end > last {
		end = last
	}
	s.assembler.PrioritizeRepairRange(head, end)
}

func (s *simulation) scheduleRequests() {
	capacity := s.cfg.MaxConcurrent - len(s.pending)
	if capacity <= 0 {
		return
	}
	requests := s.assembler.RepairRequests(s.cfg.MaxRequestSlots, s.cfg.MaxMissingPerSlot)
	for _, req := range requests {
		if !s.haveRepairAt {
			s.firstRepairAt = s.now
			s.haveRepairAt = true
		}
		s.recordAt("repair_needed_decision", req.Slot, 0, 0, 0, 0,
			fmt.Sprintf("missing=%d need_highest=%t", len(req.MissingDataShreds), req.NeedHighestDataShred))
		slot, ok := s.ledger.Slot(req.Slot)
		if !ok {
			continue
		}
		indexes := append([]uint32(nil), req.MissingDataShreds...)
		if req.NeedHighestDataShred {
			indexes = append(indexes, slot.Highest)
		}
		seen := make(map[uint32]struct{}, len(indexes))
		for _, index := range indexes {
			if capacity == 0 {
				return
			}
			if _, duplicate := seen[index]; duplicate {
				continue
			}
			seen[index] = struct{}{}
			packet, ok := slot.Data[index]
			if !ok {
				continue
			}
			key := repairKey{slot: req.Slot, index: index}
			if _, outstanding := s.pending[key]; outstanding {
				continue
			}
			s.pending[key] = struct{}{}
			capacity--
			s.result.RepairRequests++
			s.result.RepairBytesRequested += uint64(len(packet.Bytes))
			s.record("repair_request_enqueue", packet, "data shred")
			s.record("repair_request_send", packet, "data shred")
			s.scheduleResponse(key, packet)
		}
	}
}

func (s *simulation) scheduleResponse(key repairKey, packet Packet) {
	jitter := time.Duration(0)
	if s.cfg.RepairJitter > 0 {
		span := int64(s.cfg.RepairJitter)*2 + 1
		jitter = time.Duration(s.rng.Int63n(span)) - s.cfg.RepairJitter
	}
	at := s.now + s.cfg.RepairLatency + jitter
	if at < s.now {
		at = s.now
	}
	if at < s.nextWireAt {
		at = s.nextWireAt
	}
	if s.cfg.BandwidthBytesPerSec > 0 {
		wire := time.Duration(float64(len(packet.Bytes)) / float64(s.cfg.BandwidthBytesPerSec) * float64(time.Second))
		if wire < time.Nanosecond {
			wire = time.Nanosecond
		}
		at += wire
		s.nextWireAt = at
	}
	d := delivery{at: at, sequence: s.sequence, packet: packet, key: key, primary: true, fromRepair: true}
	s.sequence++
	if s.cfg.PacketLoss > 0 && s.rng.Float64() < s.cfg.PacketLoss {
		d.drop = true
	}
	if s.corruptLeft > 0 {
		d.corrupt = true
		s.corruptLeft--
	}
	heap.Push(&s.queue, d)
	if !d.drop && s.cfg.DuplicateProbability > 0 && s.rng.Float64() < s.cfg.DuplicateProbability {
		dup := d
		dup.at++
		dup.sequence = s.sequence
		dup.primary = false
		dup.duplicate = true
		dup.corrupt = false
		s.sequence++
		heap.Push(&s.queue, dup)
	}
	if len(s.queue) > s.result.QueueHighWater {
		s.result.QueueHighWater = len(s.queue)
	}
}

func (s *simulation) ingest(packet Packet, fromRepair, corrupt bool) error {
	raw := packet.Bytes
	if corrupt {
		raw = append([]byte(nil), raw...)
		if len(raw) > 200 {
			raw[200] ^= 0x80
		} else if len(raw) > 0 {
			raw[len(raw)-1] ^= 0x80
		}
	}
	started := time.Now()
	shred, err := turbine.ParseShred(raw)
	s.stageCPU["shred_parse"] += time.Since(started)
	if err != nil {
		return err
	}
	started = time.Now()
	err = s.shredVerifier.Verify(shred, s.ledger.LeaderPub)
	s.stageCPU["shred_validation"] += time.Since(started)
	if err != nil {
		return err
	}
	s.record("shred_validation", packet, "Merkle proof and leader signature valid")
	if _, ok := s.firstShred[shred.Slot]; !ok {
		s.firstShred[shred.Slot] = s.now
		s.record("first_shred", packet, "")
	}
	started = time.Now()
	spooled := s.spool.AppendShred(shred, raw)
	s.stageCPU["blockstore_insert"] += time.Since(started)
	if spooled {
		s.record("blockstore_insert", packet, "verified shred spool")
	}
	beforeRecovered := s.assembler.RecoveredDataShreds()
	started = time.Now()
	blk, err := s.assembler.AddShredFrom(shred, fromRepair)
	s.stageCPU["assembler_ingest_and_recovery"] += time.Since(started)
	if err != nil {
		return err
	}
	afterRecovered := s.assembler.RecoveredDataShreds()
	if afterRecovered > beforeRecovered {
		s.result.FECDecodes++
		if !s.haveRecoverAt {
			s.firstRecoverAt = s.now
			s.haveRecoverAt = true
		}
		s.record("fec_threshold_reached", packet, "observed at AddShredFrom call boundary")
		s.record("fec_decode_complete", packet, fmt.Sprintf("recovered_data=%d", afterRecovered-beforeRecovered))
	}
	if fromRepair {
		s.record("repair_response_receive", packet, "")
	} else {
		s.record("live_shred_receive", packet, "")
	}
	if blk != nil {
		canonical, ok := s.ledger.Slot(blk.Slot)
		if !ok {
			return fmt.Errorf("completed unknown slot %d", blk.Slot)
		}
		if err := compareBlock(canonical, blk); err != nil {
			return err
		}
		if _, ok := s.spool.IsComplete(blk.Slot); !ok {
			return fmt.Errorf("slot %d completed without spool completion record", blk.Slot)
		}
		s.completed[blk.Slot] = blk
		for _, data := range canonical.Data {
			s.result.DataShredBytesReplayable += uint64(len(data.Bytes))
		}
		s.record("slot_offered_to_replay", packet, "verified block emitted")
		s.advanceReplayable()
	}
	return nil
}

func (s *simulation) advanceReplayable() {
	for {
		if _, ok := s.completed[s.nextReplaySlot]; !ok {
			return
		}
		s.replayableAt[s.nextReplaySlot] = s.now
		s.recordAt("slot_replayable", s.nextReplaySlot, 0, 0, 0, 0, "contiguous parent chain available")
		s.nextReplaySlot++
	}
}

func compareBlock(canonical *Slot, got *block.Block) error {
	if got.Slot != canonical.Number || got.SourceParentSlot != canonical.ParentSlot {
		return fmt.Errorf("block identity got slot=%d parent=%d, want slot=%d parent=%d", got.Slot, got.SourceParentSlot, canonical.Number, canonical.ParentSlot)
	}
	if len(got.Entries) != len(canonical.Entries) {
		return fmt.Errorf("slot %d entries=%d, want %d", got.Slot, len(got.Entries), len(canonical.Entries))
	}
	for i := range canonical.Entries {
		want := canonical.Entries[i]
		entry := got.Entries[i]
		if entry.NumHashes != want.NumHashes || string(entry.Hash) != string(want.Hash[:]) || len(entry.Indices) != len(want.Txns) {
			return fmt.Errorf("slot %d entry %d differs from canonical ledger", got.Slot, i)
		}
	}
	if !got.TransactionSignaturesVerified() {
		return fmt.Errorf("slot %d block was not signature-verified", got.Slot)
	}
	return nil
}

func (s *simulation) record(stage string, packet Packet, detail string) {
	s.recordAt(stage, packet.Slot, packet.FECSetIndex, packet.Index, packet.Type, len(packet.Bytes), detail)
}

func (s *simulation) recordAt(stage string, slot uint64, fec, index uint32, typ turbine.ShredType, bytes int, detail string) {
	if s.cfg.CollectTrace {
		s.trace = append(s.trace, TraceEvent{
			Sequence: s.sequence, AtNanos: int64(s.now), Stage: stage, Slot: slot,
			FECSetIndex: fec, ShredIndex: index, ShredType: typ, Bytes: bytes, Detail: detail,
		})
	}
	s.sequence++
}

func summarizeLatencies(values []time.Duration) LatencySummary {
	if len(values) == 0 {
		return LatencySummary{}
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	percentile := func(p float64) time.Duration {
		index := int(float64(len(values)-1)*p + 0.5)
		return values[index]
	}
	return LatencySummary{P50: percentile(.50), P95: percentile(.95), P99: percentile(.99)}
}

func initialMissingDataShreds(ledger *Ledger, availability Availability) uint64 {
	var heldPerFEC int
	switch availability {
	case AvailabilityComplete:
		heldPerFEC = 32
	case AvailabilityNearLoss:
		var missing uint64
		for i := range ledger.Slots {
			for fec := range ledger.Slots[i].FECs {
				if fec%2 == 0 {
					missing += 2
				} else {
					missing++
				}
			}
		}
		return missing
	case AvailabilitySparse:
		heldPerFEC = 2
	case AvailabilityMixed:
		heldPerFEC = 16
	}
	return uint64(len(ledger.Slots) * ledger.Config.FECsPerSlot * (dataShredsPerFEC - heldPerFEC))
}

type repairKey struct {
	slot  uint64
	index uint32
}

type delivery struct {
	at         time.Duration
	sequence   int
	packet     Packet
	key        repairKey
	primary    bool
	fromRepair bool
	drop       bool
	duplicate  bool
	corrupt    bool
}

type deliveryHeap []delivery

func (h deliveryHeap) Len() int { return len(h) }
func (h deliveryHeap) Less(i, j int) bool {
	if h[i].at != h[j].at {
		return h[i].at < h[j].at
	}
	return h[i].sequence < h[j].sequence
}
func (h deliveryHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *deliveryHeap) Push(x any)   { *h = append(*h, x.(delivery)) }
func (h *deliveryHeap) Pop() any {
	old := *h
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	return last
}

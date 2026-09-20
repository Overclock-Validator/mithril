package turbine

import (
	"reflect"
	"testing"
)

// Helpers building slotState directly: repair selection is a pure function of
// held-shred/FEC-set state, so tests assert on the policy without
// constructing signed merkle shreds.

func newRepairSelectionSlot(slot uint64) *slotState {
	return &slotState{
		slot:    slot,
		shreds:  make(map[uint32]*Shred),
		fecSets: make(map[uint32]*fecState),
	}
}

// addCodedSet installs an FEC set with a KNOWN layout holding the given
// absolute data indices and codingHeld coding shreds.
func addCodedSet(s *slotState, start uint32, dataShreds, codingShreds uint16, dataHeld []uint32, codingHeld int) {
	fec := &fecState{
		slot:        s.slot,
		fecSetIndex: start,
		data:        make(map[uint32]*Shred),
		coding:      make(map[uint16]*Shred),
		layout:      fecLayout{dataShreds: dataShreds, codingShreds: codingShreds, shardSize: 1},
		haveLayout:  true,
	}
	for _, idx := range dataHeld {
		sh := &Shred{Slot: s.slot, Type: ShredTypeData, Index: idx, FECSetIndex: start}
		s.shreds[idx] = sh
		fec.data[idx-start] = sh
	}
	for pos := 0; pos < codingHeld; pos++ {
		fec.coding[uint16(pos)] = &Shred{Slot: s.slot, Type: ShredTypeCode, FECSetIndex: start, Position: uint16(pos)}
	}
	s.fecSets[start] = fec
}

// addUncodedData holds data shreds with NO layout knowledge (no coding heard
// for their sets) — the cold pre-join regime.
func addUncodedData(s *slotState, indices ...uint32) {
	for _, idx := range indices {
		s.shreds[idx] = &Shred{Slot: s.slot, Type: ShredTypeData, Index: idx}
	}
}

func seq(from, through uint32) []uint32 {
	out := make([]uint32, 0, through-from+1)
	for i := from; i <= through; i++ {
		out = append(out, i)
	}
	return out
}

// A set missing 12 data shreds while holding 8 coding shreds needs only 4
// repairs before Reed-Solomon recovers the rest — requesting all 12 would
// waste two thirds of the budget.
func TestRepairSelectionDeficitCapsCodedSet(t *testing.T) {
	s := newRepairSelectionSlot(9)
	addCodedSet(s, 0, 32, 32, seq(0, 19), 8)
	s.haveLast = true
	s.lastIndex = 31

	req, ok := s.repairRequest(256)
	if !ok {
		t.Fatal("expected a repair request")
	}
	want := []uint32{20, 21, 22, 23} // deficit = 32 - 20 data - 8 coding
	if !reflect.DeepEqual(req.MissingDataShreds, want) {
		t.Fatalf("missing = %v, want %v", req.MissingDataShreds, want)
	}
}

// Zero coding held means zero recovery leverage: every missing data shred
// must be fetched individually, ascending — the old linear behavior.
func TestRepairSelectionColdSlotRequestsAllMissing(t *testing.T) {
	s := newRepairSelectionSlot(9)
	addUncodedData(s, seq(0, 9)...)
	s.haveLast = true
	s.lastIndex = 31

	req, ok := s.repairRequest(256)
	if !ok {
		t.Fatal("expected a repair request")
	}
	if !reflect.DeepEqual(req.MissingDataShreds, seq(10, 31)) {
		t.Fatalf("missing = %v, want 10..31", req.MissingDataShreds)
	}
}

// Scarce budget should cross recovery thresholds, not spread beneath them:
// the set one shred short of recovering goes first.
func TestRepairSelectionCheapestUnlockFirst(t *testing.T) {
	s := newRepairSelectionSlot(9)
	addCodedSet(s, 0, 32, 32, seq(0, 9), 12)   // deficit 32-10-12 = 10
	addCodedSet(s, 32, 32, 32, seq(32, 56), 6) // deficit 32-25-6 = 1
	s.haveLast = true
	s.lastIndex = 63

	req, ok := s.repairRequest(256)
	if !ok {
		t.Fatal("expected a repair request")
	}
	want := append([]uint32{57}, seq(10, 19)...) // set B's single unlock, then set A's 10
	if !reflect.DeepEqual(req.MissingDataShreds, want) {
		t.Fatalf("missing = %v, want %v", req.MissingDataShreds, want)
	}
}

// A set that held enough to recover but errored anyway must fall back to
// fetching its missing data directly — deficit-capping to zero would starve
// the slot forever on poisoned coding state.
func TestRepairSelectionRecoveryFailureRequestsSpanDirectly(t *testing.T) {
	s := newRepairSelectionSlot(9)
	addCodedSet(s, 0, 32, 32, seq(0, 29), 4) // held 34 >= 32: recovery should have fired
	s.haveLast = true
	s.lastIndex = 31

	req, ok := s.repairRequest(256)
	if !ok {
		t.Fatal("expected a repair request")
	}
	if !reflect.DeepEqual(req.MissingDataShreds, []uint32{30, 31}) {
		t.Fatalf("missing = %v, want [30 31]", req.MissingDataShreds)
	}
}

// A known layout proves data extends through the span end, so the scan
// reaches past the highest RECEIVED index instead of waiting for a
// HighestWindowIndex round trip to discover the tail.
func TestRepairSelectionExtendsThroughCodedSpan(t *testing.T) {
	s := newRepairSelectionSlot(9)
	addCodedSet(s, 0, 32, 32, seq(0, 5), 2) // maxObserved 5, deficit 32-6-2 = 24

	req, ok := s.repairRequest(256)
	if !ok {
		t.Fatal("expected a repair request")
	}
	if !req.NeedHighestDataShred {
		t.Fatal("still no last shred: NeedHighestDataShred should hold")
	}
	if req.HighestDataShredIndex != 6 {
		t.Fatalf("highest probe hint = %d, want 6", req.HighestDataShredIndex)
	}
	if !reflect.DeepEqual(req.MissingDataShreds, seq(6, 29)) {
		t.Fatalf("missing = %v, want 6..29 (24 = deficit, from a span-extended scan)", req.MissingDataShreds)
	}
}

func TestRepairSelectionRespectsMaxMissing(t *testing.T) {
	s := newRepairSelectionSlot(9)
	addUncodedData(s, seq(0, 9)...)
	s.haveLast = true
	s.lastIndex = 31

	req, ok := s.repairRequest(3)
	if !ok {
		t.Fatal("expected a repair request")
	}
	if !reflect.DeepEqual(req.MissingDataShreds, []uint32{10, 11, 12}) {
		t.Fatalf("missing = %v, want [10 11 12]", req.MissingDataShreds)
	}
}

// Head monopoly: the emission-gating head may list enough missing indices
// to occupy the whole in-flight admission window by itself; the slots
// behind it stay at the per-slot cap and only ever receive the sends the
// head cannot turn over.
func TestRepairRequestsTieredHeadMonopoly(t *testing.T) {
	a := NewSlotAssembler()
	a.maxObservedSlot = 5000
	a.retentionFloor = 10

	mkGappy := func(slot uint64, lastIndex uint32) {
		s := newRepairSelectionSlot(slot)
		s.haveLast = true
		s.lastIndex = lastIndex
		s.shreds[0] = &Shred{Slot: slot, Type: ShredTypeData}
		a.slots[slot] = s
	}
	mkGappy(10, 20000) // head: 20000 missing, above the head cap
	mkGappy(11, 999)   // next window slot: 999 missing
	a.PrioritizeRepairRange(10, 11)

	priority, _ := a.RepairRequestsTiered(32, 256)
	if len(priority) < 2 || priority[0].Slot != 10 || priority[1].Slot != 11 {
		t.Fatalf("priority = %+v, want slots 10 then 11", priority)
	}
	if got := len(priority[0].MissingDataShreds); got != repairHeadMaxMissing {
		t.Fatalf("head listed %d missing, want the head cap %d", got, repairHeadMaxMissing)
	}
	if got := len(priority[1].MissingDataShreds); got != 256 {
		t.Fatalf("window slot listed %d missing, want the per-slot cap 256", got)
	}
}

func TestSplitRepairBudget(t *testing.T) {
	cases := []struct {
		budget, edgeDemand, wantHead int
	}{
		{100, 3, 97},   // reserve bounded by tiny edge demand
		{100, 500, 80}, // full fifth reserved when the edge can use it
		{100, 0, 100},  // idle edge: the head is never capped
		{4, 100, 4},    // sub-5 budgets round the reserve to zero
	}
	for _, tc := range cases {
		if got := splitRepairBudget(tc.budget, tc.edgeDemand); got != tc.wantHead {
			t.Fatalf("splitRepairBudget(%d, %d) = %d, want %d", tc.budget, tc.edgeDemand, got, tc.wantHead)
		}
	}
}

func TestTierSendDemand(t *testing.T) {
	requests := []SlotRepairRequest{
		{Slot: 1, MissingDataShreds: []uint32{4, 7}, NeedHighestDataShred: true},
		{Slot: 2, MissingDataShreds: []uint32{0}},
	}
	// headInitial multiplies only the first request's demand (its initial
	// fanout); the rest count once. This is a generous token-draw estimate.
	if got := tierSendDemand(requests, 2); got != 7 {
		t.Fatalf("head demand = %d, want 7 (first request's 3 doubled, plus 1)", got)
	}
	if got := tierSendDemand(requests, 1); got != 4 {
		t.Fatalf("single-flight demand = %d, want 4", got)
	}
}

// headPolicy scales retry aggression by the head's distance from completion:
// endgame fans a shred across peers, a large head is fetched steadily.
func TestHeadPolicy(t *testing.T) {
	endgame := headPolicy(repairHeadFanoutMaxMissing)
	if endgame.initialConcurrent != 2 || endgame.maxConcurrent != repairMaxAttemptsHeadEndgame {
		t.Fatalf("endgame policy = %+v, want initial 2 / max %d", endgame, repairMaxAttemptsHeadEndgame)
	}
	if endgame.retryInterval != repairRetryHeadEndgame {
		t.Fatalf("endgame retry = %v, want %v", endgame.retryInterval, repairRetryHeadEndgame)
	}
	near := headPolicy(repairRetryHeadNearMissing)
	if near.initialConcurrent != 1 || near.maxConcurrent != 1 || near.retryInterval != repairRetryHeadNear {
		t.Fatalf("near policy = %+v, want single attempt / retry %v", near, repairRetryHeadNear)
	}
	far := headPolicy(repairRetryHeadNearMissing + 1)
	if far.maxConcurrent != 1 || far.retryInterval != repairRetryHeadFar {
		t.Fatalf("far policy = %+v, want single attempt / retry %v", far, repairRetryHeadFar)
	}
	bulk := bulkPolicy()
	if bulk.maxConcurrent != 1 {
		t.Fatalf("bulk policy = %+v, want no concurrent duplicate attempts", bulk)
	}
}

func TestRepairSelectionPrefixBeforeCheapest(t *testing.T) {
	s := newRepairSelectionSlot(9)
	addCodedSet(s, 0, 32, 32, seq(0, 9), 12)
	addCodedSet(s, 32, 32, 32, seq(32, 56), 6)
	s.haveLast = true
	s.lastIndex = 63
	got := s.missingDataForRepairWithPrefix(63, 4, true)
	if !reflect.DeepEqual(got, seq(10, 13)) {
		t.Fatalf("prefix %v", got)
	}
	all := s.missingDataForRepairWithPrefix(63, 256, true)
	if !reflect.DeepEqual(all, append(seq(10, 19), 57)) {
		t.Fatalf("remaining order %v", all)
	}
}

func TestRepairSelectionUncodedPrefixBeforeCoded(t *testing.T) {
	s := newRepairSelectionSlot(9)
	addUncodedData(s, seq(1, 31)...)
	addCodedSet(s, 32, 32, 32, seq(32, 56), 6)
	s.haveLast = true
	s.lastIndex = 63
	got := s.missingDataForRepairWithPrefix(63, 256, true)
	if !reflect.DeepEqual(got, []uint32{0, 57}) {
		t.Fatalf("uncoded prefix %v", got)
	}
	if got := s.missingDataForRepairWithPrefix(63, 0, true); len(got) != 0 {
		t.Fatalf("zero budget: %v", got)
	}
}

func TestRepairSelectionPrefixOnlyForStreamingPriorityHead(t *testing.T) {
	a := NewSlotAssembler()
	for _, slot := range []uint64{9, 10} {
		s := newRepairSelectionSlot(slot)
		addCodedSet(s, 0, 32, 32, seq(0, 9), 12)
		addCodedSet(s, 32, 32, 32, seq(32, 56), 6)
		s.haveLast = true
		s.lastIndex = 63
		a.slots[slot] = s
	}
	a.maxObservedSlot = 11
	a.PrioritizeRepairRange(9, 10)
	p, _ := a.RepairRequestsTiered(2, 256)
	if len(p) != 2 || p[0].MissingDataShreds[0] != 57 {
		t.Fatalf("nonstreaming order %v", p)
	}
	a.SubscribeStream(make(chan StreamEvent, 1))
	p, _ = a.RepairRequestsTiered(2, 256)
	if len(p) != 2 || p[0].MissingDataShreds[0] != 10 || p[1].MissingDataShreds[0] != 57 {
		t.Fatalf("streaming head scope %v", p)
	}
	a.SubscribeStream(nil)
	p, _ = a.RepairRequestsTiered(2, 256)
	if p[0].MissingDataShreds[0] != 57 {
		t.Fatal("disabled streaming retained prefix policy")
	}
}

func TestRepairPriorityParentPinnedAfterChild(t *testing.T) {
	a := NewSlotAssembler()
	a.maxObservedSlot = 101
	a.retentionFloor = 100
	a.PrioritizeRepairRange(101, 101)
	a.PrioritizeRepairRange(100, 100)
	priority, _ := a.RepairRequestsTiered(2, 16)
	if len(priority) != 2 || priority[0].Slot != 100 || priority[1].Slot != 101 {
		t.Fatalf("parent must precede earlier-pinned child: %+v", priority)
	}
	if a.priorityRepairOrder[0] != 101 {
		t.Fatal("selection changed pin retention order")
	}
}

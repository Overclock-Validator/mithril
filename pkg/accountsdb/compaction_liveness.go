package accountsdb

// CompactionLivenessObservation covers only candidates selected in this cycle,
// not the whole store. Byte classifications exclude incomplete scans. A complete
// scan can still be followed by a reclamation error in the enclosing cycle.
type CompactionLivenessObservation struct {
	Known              bool
	ScannedCandidates  uint64
	CompleteCandidates uint64
	PartialCandidates  uint64
	RewriteCandidates  uint64
	SparseCandidates   uint64
	SourceBytes        uint64 // selected inventory sizes, including incomplete scans
	ExaminedBytes      uint64 // completely validated source record spans
	LiveBytes          uint64 // current index names this exact record
	DeadBytes          uint64 // neither current-live nor undo-retained
	UndoPinnedBytes    uint64 // undo-only retained records; disjoint from LiveBytes
	EligibleHoleBytes  uint64 // complete dead payload pages, not physical recovery
}

// One optional stack-local result; no record identity or source buffer escapes.
type compactionLivenessScan struct {
	complete                    bool
	examined, live, undo, holes uint64
}

func (o *CompactionLivenessObservation) addCandidate(size int64, sparse bool, scan compactionLivenessScan) {
	if !o.Known {
		return
	}
	// An external length change must not mix the inventory denominator with
	// differently sized records. Keep the selected attempt, but its bytes unknown.
	if size >= 0 && scan.complete && scan.examined != uint64(size) {
		scan = compactionLivenessScan{}
	}
	if size < 0 || scan.live > scan.examined || scan.undo > scan.examined-scan.live ||
		scan.holes > scan.examined-scan.live-scan.undo {
		*o = CompactionLivenessObservation{}
		return
	}
	next := *o
	var complete, partial, rewrite, sparseCount, examined, live, dead, undo, holes uint64
	if sparse {
		sparseCount = 1
	} else {
		rewrite = 1
	}
	if scan.complete {
		complete, examined, live, undo, holes = 1, scan.examined, scan.live, scan.undo, scan.holes
		dead = examined - live - undo
	} else {
		partial = 1
	}
	values := [...]struct {
		dst *uint64
		add uint64
	}{{&next.ScannedCandidates, 1}, {&next.CompleteCandidates, complete}, {&next.PartialCandidates, partial},
		{&next.RewriteCandidates, rewrite}, {&next.SparseCandidates, sparseCount}, {&next.SourceBytes, uint64(size)},
		{&next.ExaminedBytes, examined}, {&next.LiveBytes, live}, {&next.DeadBytes, dead},
		{&next.UndoPinnedBytes, undo}, {&next.EligibleHoleBytes, holes}}
	for _, value := range values {
		if value.add > ^uint64(0)-*value.dst {
			*o = CompactionLivenessObservation{}
			return
		}
		*value.dst += value.add
	}
	*o = next
}

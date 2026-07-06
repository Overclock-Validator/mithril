package sbpf

import (
	"sort"
	"time"

	"github.com/gagliardetto/solana-go"
)

// ProgramStat aggregates interpreter activity for one program id.
type ProgramStat struct {
	ProgramId  solana.PublicKey
	Executions uint64
	Insns      uint64        // interpreted instructions (what a JIT would absorb)
	SelfTime   time.Duration // time in Run excluding nested CPI executions
}

// StatsCollector attributes interpreter time to program ids, subtracting
// nested CPI time from the caller so SelfTime sums to total VM time.
// Single-threaded by design: enable only for sequential replay.
type StatsCollector struct {
	stack []statsFrame
	agg   map[solana.PublicKey]*ProgramStat
}

type statsFrame struct {
	start time.Time
	child time.Duration
}

// Stats, when non-nil, records per-program interpreter activity.
var Stats *StatsCollector

func NewStatsCollector() *StatsCollector {
	return &StatsCollector{agg: make(map[solana.PublicKey]*ProgramStat)}
}

func (s *StatsCollector) enter() {
	s.stack = append(s.stack, statsFrame{start: time.Now()})
}

func (s *StatsCollector) exit(programId solana.PublicKey, insns uint64) {
	frame := s.stack[len(s.stack)-1]
	s.stack = s.stack[:len(s.stack)-1]
	elapsed := time.Since(frame.start)

	if len(s.stack) > 0 {
		s.stack[len(s.stack)-1].child += elapsed
	}

	st := s.agg[programId]
	if st == nil {
		st = &ProgramStat{ProgramId: programId}
		s.agg[programId] = st
	}
	st.Executions++
	st.Insns += insns
	st.SelfTime += elapsed - frame.child
}

// Results returns per-program stats sorted by SelfTime descending.
func (s *StatsCollector) Results() []ProgramStat {
	out := make([]ProgramStat, 0, len(s.agg))
	for _, st := range s.agg {
		out = append(out, *st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SelfTime > out[j].SelfTime })
	return out
}

package dashboard

import "time"

const throughputSamples = 16 // 15 intervals, bounded independently of viewers.

// PublicThroughput measures local replay work, including later-rewound work.
// Rates use replay-observation timestamps and are unavailable until two
// distinct observations establish covered time.
type PublicThroughput struct {
	State                 string                     `json:"state"`
	Samples               int                        `json:"samples"`
	WindowStart           *time.Time                 `json:"window_start"`
	WindowEnd             *time.Time                 `json:"window_end"`
	DurationSeconds       float64                    `json:"duration_seconds"`
	Transactions          *uint64                    `json:"transactions,string"`
	ComputeUnits          *uint64                    `json:"compute_units,string"`
	TransactionsPerSecond *float64                   `json:"transactions_per_second"`
	ComputeUnitsPerSecond *float64                   `json:"compute_units_per_second"`
	History               []PublicThroughputInterval `json:"history"`
}

// PublicThroughputInterval describes one observed interval, not cluster TPS.
type PublicThroughputInterval struct {
	Start                 time.Time `json:"start"`
	End                   time.Time `json:"end"`
	DurationSeconds       float64   `json:"duration_seconds"`
	Transactions          uint64    `json:"transactions,string"`
	ComputeUnits          uint64    `json:"compute_units,string"`
	TransactionsPerSecond float64   `json:"transactions_per_second"`
	ComputeUnitsPerSecond float64   `json:"compute_units_per_second"`
}

type throughputSample struct {
	at, replayAt          time.Time
	transactions, compute uint64
}

// Throughput retains a fixed window; its owner must serialize Observe calls.
type Throughput struct {
	samples                  [throughputSamples]throughputSample
	count                    int
	runID, identity, genesis string
	hasIdentity, hasGenesis  bool
	attempt                  uint64
	startedAt                time.Time
}

// Observe records one fresh feed observation and returns a detached view.
// A stale feed, inactive replay, saturation or a gap clears previous coverage.
func (t *Throughput) Observe(s *Snapshot, state string, now time.Time) PublicThroughput {
	if s == nil || state != "fresh" || s.RunID == "" || s.Replay == nil || s.Replay.Phase != "running" || s.Replay.WorkCountersSaturated ||
		s.SampledAt.IsZero() || s.SampledAt.After(now.Add(time.Second)) || now.Sub(s.SampledAt) > StaleAfter ||
		s.Replay.StartedAt.IsZero() || s.Replay.SampledAt.IsZero() || s.Replay.SampledAt.Before(s.Replay.StartedAt) ||
		s.Replay.SampledAt.After(s.SampledAt.Add(time.Second)) || s.Replay.SampledAt.After(now.Add(time.Second)) ||
		s.SampledAt.Sub(s.Replay.SampledAt) > 3*time.Second || now.Sub(s.Replay.SampledAt) > 3*time.Second {
		*t = Throughput{}
		return PublicThroughput{State: "unavailable"}
	}
	r := s.Replay
	identity, genesis := "", ""
	if s.IdentityKey != nil {
		identity = *s.IdentityKey
	}
	if s.GenesisHash != nil {
		genesis = *s.GenesisHash
	}
	if t.count > 0 && (s.RunID != t.runID || identity != t.identity || genesis != t.genesis ||
		(s.IdentityKey != nil) != t.hasIdentity || (s.GenesisHash != nil) != t.hasGenesis ||
		r.Attempt != t.attempt || !r.StartedAt.Equal(t.startedAt)) {
		*t = Throughput{}
	}
	current := throughputSample{at: s.SampledAt, replayAt: r.SampledAt, transactions: r.ExecutedTransactions, compute: r.ExecutedComputeUnits}
	if t.count > 0 {
		last := t.samples[t.count-1]
		if current.replayAt.Equal(last.replayAt) {
			if current.at.Before(last.at) || current.at.Sub(last.at) > 3*time.Second || current.transactions != last.transactions || current.compute != last.compute {
				*t = Throughput{}
				return PublicThroughput{State: "unavailable"}
			}
			return t.view()
		}
		if current.at.Equal(last.at) {
			if current.transactions != last.transactions || current.compute != last.compute || !current.replayAt.Equal(last.replayAt) {
				*t = Throughput{}
				return PublicThroughput{State: "unavailable"}
			}
			return t.view()
		}
		if current.at.Before(last.at) || current.at.Sub(last.at) > 3*time.Second || current.replayAt.Before(last.replayAt) ||
			current.replayAt.Sub(last.replayAt) > 3*time.Second || current.transactions < last.transactions || current.compute < last.compute {
			*t = Throughput{}
		}
	}
	if t.count == 0 {
		t.runID, t.identity, t.genesis = s.RunID, identity, genesis
		t.hasIdentity, t.hasGenesis = s.IdentityKey != nil, s.GenesisHash != nil
		t.attempt, t.startedAt = r.Attempt, r.StartedAt
	}
	if t.count == len(t.samples) {
		copy(t.samples[:], t.samples[1:])
		t.count--
	}
	t.samples[t.count] = current
	t.count++
	return t.view()
}

func (t *Throughput) view() PublicThroughput {
	v := PublicThroughput{State: "unavailable", Samples: t.count}
	if t.count < 2 {
		return v
	}
	v.State = "available"
	first, last := t.samples[0], t.samples[t.count-1]
	v.WindowStart, v.WindowEnd = &first.replayAt, &last.replayAt
	v.DurationSeconds = last.replayAt.Sub(first.replayAt).Seconds()
	tx, cu := last.transactions-first.transactions, last.compute-first.compute
	txRate, cuRate := float64(tx)/v.DurationSeconds, float64(cu)/v.DurationSeconds
	v.Transactions, v.ComputeUnits, v.TransactionsPerSecond, v.ComputeUnitsPerSecond = &tx, &cu, &txRate, &cuRate
	v.History = make([]PublicThroughputInterval, 0, t.count-1)
	for i := 1; i < t.count; i++ {
		a, b := t.samples[i-1], t.samples[i]
		duration := b.replayAt.Sub(a.replayAt).Seconds()
		tx, cu := b.transactions-a.transactions, b.compute-a.compute
		v.History = append(v.History, PublicThroughputInterval{Start: a.replayAt, End: b.replayAt, DurationSeconds: duration,
			Transactions: tx, ComputeUnits: cu, TransactionsPerSecond: float64(tx) / duration, ComputeUnitsPerSecond: float64(cu) / duration})
	}
	return v
}

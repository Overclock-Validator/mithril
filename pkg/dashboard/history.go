package dashboard

import (
	"bytes"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"math"
	"strconv"
	"time"
)

const (
	historyRecordBytes  = 1400
	historySegmentBytes = 2 << 20
	historySegments     = 31
	historyRecordLimit  = 31 * 24 * 60
	historyRetention    = 30 * 24 * time.Hour
	historyBaseMetrics  = 9
	historyChainMetrics = 6
)

type historyMetric struct {
	Name      string `json:"name"`
	Unit      string `json:"unit"`
	Kind      string `json:"kind"`
	resources bool
}

var historyMetrics = [...]historyMetric{
	{Name: "completed_slot", Unit: "slot", Kind: "uint64"},
	{Name: "durable_slot", Unit: "slot", Kind: "uint64"},
	{Name: "finalized_slot", Unit: "slot", Kind: "uint64"},
	{Name: "published_slot", Unit: "slot", Kind: "uint64"},
	{Name: "local_tps", Unit: "transactions_per_second", Kind: "number"},
	{Name: "cpu_percent", Unit: "percent_one_core", Kind: "number", resources: true},
	{Name: "rss_bytes", Unit: "bytes", Kind: "uint64", resources: true},
	{Name: "known_peers", Unit: "contacts", Kind: "uint64"},
	{Name: "admitted_stake", Unit: "lamports", Kind: "uint64"},
	{Name: "chain_tps", Unit: "transactions_per_bank_second", Kind: "number"},
	{Name: "chain_success_vote_tps", Unit: "transactions_per_bank_second", Kind: "number"},
	{Name: "chain_failed_vote_tps", Unit: "transactions_per_bank_second", Kind: "number"},
	{Name: "chain_success_non_vote_tps", Unit: "transactions_per_bank_second", Kind: "number"},
	{Name: "chain_failed_non_vote_tps", Unit: "transactions_per_bank_second", Kind: "number"},
	{Name: "chain_compute_units", Unit: "compute_units_per_bank_second", Kind: "number"},
}

type historyRange struct {
	Name          string `json:"name"`
	Seconds       int64  `json:"seconds"`
	BucketSeconds int64  `json:"bucket_seconds"`
}

var historyRanges = [...]historyRange{
	{Name: "1h", Seconds: 3600, BucketSeconds: 60},
	{Name: "24h", Seconds: 86400, BucketSeconds: 900},
	{Name: "7d", Seconds: 7 * 86400, BucketSeconds: 7200},
	{Name: "30d", Seconds: 30 * 86400, BucketSeconds: 28800},
}

type historyBinding struct {
	Genesis  string `json:"genesis_hash"`
	Identity string `json:"identity"`
	Run      string `json:"run_id"`
}

func (b historyBinding) matches(other historyBinding) bool {
	if b.Genesis != "" && b.Identity != "" {
		return b.Genesis == other.Genesis && b.Identity == other.Identity
	}
	return b == other && b.Run != ""
}

// historyRecord contains only fixed public scalars. Millisecond dates retain
// source timing without persisting the much larger source snapshot.
type historyRecord struct {
	Version         int                         `json:"v"`
	At              int64                       `json:"at"`
	ObservedAt      int64                       `json:"observed_at"`
	SourceAt        int64                       `json:"source_at"`
	Binding         historyBinding              `json:"binding"`
	Role            string                      `json:"role"`
	Attempt         uint64                      `json:"attempt,string"`
	ReplayStartedAt int64                       `json:"replay_started_at"`
	Rewinds         uint64                      `json:"rewinds,string"`
	Vote            string                      `json:"vote"`
	Values          [historyBaseMetrics]*string `json:"values"`
	GroupAt         [historyBaseMetrics]int64   `json:"group_at"`
	Chain           *historyChainRecord         `json:"chain,omitempty"`
}

// Version 2 extends rather than resizes the version-1 arrays, so old records
// remain byte-for-byte readable. Older binaries fail closed on version 2.
type historyChainRecord struct {
	At     int64                       `json:"at"`
	Values [historyChainMetrics]string `json:"values"`
}

func (r *historyRecord) metricValue(metric int) *string {
	if metric < historyBaseMetrics {
		return r.Values[metric]
	}
	if r.Chain == nil {
		return nil
	}
	return &r.Chain.Values[metric-historyBaseMetrics]
}

func historyPublicRecord(p PublicSnapshot, now time.Time, resources bool) (historyRecord, bool) {
	r := historyRecord{Version: 1, At: now.UTC().Truncate(time.Minute).Unix(), ObservedAt: now.UnixMilli()}
	if p.Node == nil || p.Node.RunID == "" {
		return r, false
	}
	r.Binding.Run, r.Role = p.Node.RunID, p.Node.Mode
	if p.Node.GenesisHash != nil {
		r.Binding.Genesis = *p.Node.GenesisHash
	}
	if p.Node.Identity != nil {
		r.Binding.Identity = *p.Node.Identity
	}
	if p.SampledAt != nil {
		r.SourceAt = p.SampledAt.UnixMilli()
	}
	if p.Replay != nil {
		r.Attempt, r.Rewinds = p.Replay.Attempt, p.Replay.Rewinds
		if !p.Replay.StartedAt.IsZero() {
			r.ReplayStartedAt = p.Replay.StartedAt.UnixMilli()
		}
	}
	if p.Voting != nil {
		r.Vote = p.Voting.VoteAccount
	}
	fresh := func(at time.Time, state string) bool {
		return p.SourceState == "fresh" && p.SampledAt != nil && now.Sub(*p.SampledAt) <= StaleAfter &&
			!p.SampledAt.After(now.Add(time.Second)) && state == "fresh" && !at.IsZero() &&
			!at.After(now.Add(time.Second)) && now.Sub(at) <= 30*time.Second
	}
	putUint := func(i int, value *uint64, at time.Time) {
		if value != nil {
			text := strconv.FormatUint(*value, 10)
			r.Values[i], r.GroupAt[i] = &text, at.UnixMilli()
		}
	}
	putFloat := func(i int, value *float64, at time.Time) {
		if value != nil && *value >= 0 && !math.IsNaN(*value) && !math.IsInf(*value, 0) {
			text := strconv.FormatFloat(*value, 'g', -1, 64)
			r.Values[i], r.GroupAt[i] = &text, at.UnixMilli()
		}
	}
	if v := p.Replay; v != nil && fresh(v.SampledAt, v.State) {
		if v.Completed != nil {
			putUint(0, &v.Completed.Slot, v.SampledAt)
		}
		putUint(1, v.DurableSlot, v.SampledAt)
	}
	if v := p.Consensus; v != nil && fresh(v.SampledAt, v.State) {
		putUint(2, v.FinalizedSlot, v.SampledAt)
	}
	if v := p.RPC; v != nil && fresh(v.SampledAt, v.State) {
		putUint(3, v.PublishedSlot, v.SampledAt)
	}
	if v := p.Throughput; v != nil && v.State == "available" && v.WindowEnd != nil && fresh(*v.WindowEnd, "fresh") {
		putFloat(4, v.TransactionsPerSecond, *v.WindowEnd)
	}
	if v := p.Resources; resources && v != nil && v.SampledAt != nil && fresh(*v.SampledAt, v.State) {
		putUint(6, v.RSSBytes, *v.SampledAt)
		if v.CPU != nil {
			putFloat(5, &v.CPU.PercentOneCore, v.CPU.WindowEnd)
		}
	}
	if p.Input != nil && p.Input.Gossip != nil {
		v := p.Input.Gossip
		if fresh(v.SampledAt, v.State) && v.KnownPeers >= 0 {
			value := uint64(v.KnownPeers)
			putUint(7, &value, v.SampledAt)
		}
	}
	if v := p.Voting; v != nil && v.Identity == r.Binding.Identity && r.Role == "validator" && fresh(v.SampledAt, v.State) {
		putUint(8, v.AdmittedStake, v.SampledAt)
	}
	if p.Replay != nil {
		if a := p.Replay.ChainActivity; a != nil && a.Provenance == "selected_and_folded" && a.TimeSource == "bank_clock" && a.Interval != nil && fresh(a.SampledAt, a.State) {
			v := a.Interval
			if v.StartTime > 0 && v.EndTime > v.StartTime && v.NonVoteTransactions <= v.Transactions && v.FailedTransactions <= v.Transactions &&
				v.FailedNonVote <= v.NonVoteTransactions && v.FailedNonVote <= v.FailedTransactions &&
				v.FailedTransactions-v.FailedNonVote <= v.Transactions-v.NonVoteTransactions {
				seconds := float64(v.EndTime - v.StartTime)
				vote := v.Transactions - v.NonVoteTransactions
				failedVote := v.FailedTransactions - v.FailedNonVote
				counts := [historyChainMetrics]uint64{v.Transactions, vote - failedVote, failedVote,
					v.NonVoteTransactions - v.FailedNonVote, v.FailedNonVote, v.ComputeUnits}
				r.Version, r.Chain = 2, &historyChainRecord{At: a.SampledAt.UnixMilli()}
				for i, count := range counts {
					r.Chain.Values[i] = strconv.FormatFloat(float64(count)/seconds, 'g', -1, 64)
				}
			}
		}
	}
	return r, r.validate() == nil
}

func (r historyRecord) sameSeries(other historyRecord) bool {
	return r.Binding == other.Binding && r.Role == other.Role && r.Attempt == other.Attempt &&
		r.ReplayStartedAt == other.ReplayStartedAt && r.Rewinds == other.Rewinds && r.Vote == other.Vote
}

func (r historyRecord) validate() error {
	if (r.Version != 1 || r.Chain != nil) && (r.Version != 2 || r.Chain == nil) || r.At < 0 || r.At%60 != 0 || r.At > 7258118400 || r.ObservedAt < r.At*1000 ||
		r.ObservedAt >= (r.At+60)*1000 || r.SourceAt < 0 || r.SourceAt > r.ObservedAt+1000 ||
		r.ReplayStartedAt < 0 || r.ReplayStartedAt > r.ObservedAt+1000 ||
		r.Binding.Run == "" || len(r.Binding.Run) > 128 || len(r.Binding.Genesis) > 128 ||
		len(r.Binding.Identity) > 128 || len(r.Vote) > 128 || r.Role != "validator" && r.Role != "verifying" {
		return errors.New("invalid history record")
	}
	for i, value := range r.Values {
		if value == nil {
			if r.GroupAt[i] != 0 {
				return errors.New("date without history value")
			}
			continue
		}
		if r.GroupAt[i] <= 0 || r.GroupAt[i] > r.ObservedAt+1000 || r.ObservedAt-r.GroupAt[i] > 30000 || len(*value) > 24 {
			return errors.New("invalid history value date")
		}
		if historyMetrics[i].Kind == "uint64" {
			n, err := strconv.ParseUint(*value, 10, 64)
			if err != nil || strconv.FormatUint(n, 10) != *value {
				return errors.New("invalid history integer")
			}
		} else {
			n, err := strconv.ParseFloat(*value, 64)
			if err != nil || n < 0 || math.IsInf(n, 0) || math.IsNaN(n) || strconv.FormatFloat(n, 'g', -1, 64) != *value {
				return errors.New("invalid history number")
			}
		}
	}
	if r.Chain != nil {
		if r.Chain.At <= 0 || r.Chain.At > r.ObservedAt+1000 || r.ObservedAt-r.Chain.At > 30000 {
			return errors.New("invalid chain history date")
		}
		for _, value := range r.Chain.Values {
			n, err := strconv.ParseFloat(value, 64)
			if err != nil || len(value) > 24 || math.Signbit(n) || math.IsInf(n, 0) || math.IsNaN(n) || strconv.FormatFloat(n, 'g', -1, 64) != value {
				return errors.New("invalid chain history value")
			}
		}
	}
	return nil
}

var historyChecksumTable = crc32.MakeTable(crc32.Castagnoli)

type historyEnvelope struct {
	Record   json.RawMessage `json:"record"`
	Checksum uint32          `json:"crc32c"`
}

func encodeHistoryRecord(r historyRecord) ([]byte, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	b, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	line, err := json.Marshal(historyEnvelope{Record: b, Checksum: crc32.Checksum(b, historyChecksumTable)})
	if err != nil {
		return nil, err
	}
	line = append(line, '\n')
	if len(line) > historyRecordBytes {
		return nil, errors.New("history record exceeds limit")
	}
	return line, nil
}

func decodeHistoryRecord(line []byte) (historyRecord, error) {
	var r historyRecord
	if len(line) > historyRecordBytes || len(line) == 0 || line[len(line)-1] != '\n' {
		return r, errors.New("invalid history line")
	}
	var envelope historyEnvelope
	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&envelope) != nil || decoder.Decode(new(any)) != io.EOF || crc32.Checksum(envelope.Record, historyChecksumTable) != envelope.Checksum || json.Unmarshal(envelope.Record, &r) != nil {
		return r, errors.New("invalid history checksum or encoding")
	}
	canonical, err := encodeHistoryRecord(r)
	if err != nil || !bytes.Equal(line, canonical) {
		return r, errors.New("noncanonical history record")
	}
	return r, nil
}

type historyBucket struct {
	Start    time.Time  `json:"start"`
	First    *time.Time `json:"first_observed_at"`
	LastAt   *time.Time `json:"last_observed_at"`
	Count    int        `json:"count"`
	Expected int        `json:"expected"`
	Break    bool       `json:"break"`
	Min      *string    `json:"min"`
	Max      *string    `json:"max"`
	Last     *string    `json:"last"`
}

func historyLess(a, b string, metric int) bool {
	if historyMetrics[metric].Kind == "uint64" {
		x, _ := strconv.ParseUint(a, 10, 64)
		y, _ := strconv.ParseUint(b, 10, 64)
		return x < y
	}
	x, _ := strconv.ParseFloat(a, 64)
	y, _ := strconv.ParseFloat(b, 64)
	return x < y
}

func historyBuckets(records []historyRecord, binding historyBinding, metric int, span historyRange, now time.Time) []historyBucket {
	end := now.UTC().Truncate(time.Minute).Add(time.Minute).Unix()
	start := end - span.Seconds
	bins := make([]historyBucket, span.Seconds/span.BucketSeconds)
	for i := range bins {
		bins[i].Start = time.Unix(start+int64(i)*span.BucketSeconds, 0).UTC()
		bins[i].Expected = int(span.BucketSeconds / 60)
	}
	var previous *historyRecord
	mixed := make([]bool, len(bins))
	for i := range records {
		r := &records[i]
		if !binding.matches(r.Binding) || r.At < start || r.At >= end {
			continue
		}
		index := int((r.At - start) / span.BucketSeconds)
		b := &bins[index]
		if previous != nil {
			boundary := !r.sameSeries(*previous)
			if !boundary && metric < 4 && r.metricValue(metric) != nil && previous.metricValue(metric) != nil {
				boundary = historyLess(*r.metricValue(metric), *previous.metricValue(metric), metric)
			}
			if boundary || r.At != previous.At+60 {
				b.Break = true
				if boundary && previous.At >= b.Start.Unix() {
					mixed[index] = true
				}
			}
		}
		previous = r
		value := r.metricValue(metric)
		if value == nil {
			b.Break = true
			continue
		}
		b.Count++
		at := time.UnixMilli(r.ObservedAt).UTC()
		if b.First == nil {
			b.First = &at
		}
		b.LastAt, b.Last = &at, value
		if b.Min == nil || historyLess(*value, *b.Min, metric) {
			b.Min = value
		}
		if b.Max == nil || historyLess(*b.Max, *value, metric) {
			b.Max = value
		}
	}
	for i := range bins {
		b := &bins[i]
		b.Break = b.Break || b.Count != b.Expected
		if mixed[i] {
			b.Min, b.Max, b.Last = nil, nil, nil
		}
	}
	return bins
}

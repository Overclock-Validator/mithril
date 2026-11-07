package cluster

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/mr-tron/base58"
)

func fixtureKey(b byte) string { return base58.Encode(bytes.Repeat([]byte{b}, 32)) }

var testVotes = []string{fixtureKey(1), fixtureKey(2), fixtureKey(3), fixtureKey(4)}
var testIdentities = []string{fixtureKey(5), fixtureKey(6), fixtureKey(7), fixtureKey(8)}

type fixture struct {
	now                time.Time
	solstat            map[string]any
	titan              map[string]any
	peers              map[string]any
	genesisAfter       string
	epochAfter         uint64
	infoAfter          map[string]any
	status             map[string]int
	body               map[string]string
	lanternRetry       bool
	lanternFirst       any
	lanternEmptySecond bool
	performance        any
	performanceCalls   atomic.Int32
	performanceLimit   int
}

func newFixture() *fixture {
	now := time.Date(2026, 10, 31, 7, 0, 0, 0, time.UTC)
	f := &fixture{now: now, status: make(map[string]int), body: make(map[string]string),
		performanceLimit: 1, performance: []any{map[string]any{"slot": 114, "numTransactions": 120, "numNonVoteTransactions": 100, "numSlots": 4, "samplePeriodSecs": 60}}}
	var rows []any
	roster := make(map[string]any)
	for i, vote := range testVotes {
		rows = append(rows, map[string]any{"votePubkey": vote, "nodePubkey": testIdentities[i], "name": "validator", "softwareVersion": "4.4.0", "activatedStake": json.Number("1234567890.123456789"), "commission": 100, "delinquent": false, "lastVote": 115, "rootSlot": 114, "currentEpochCredits": json.Number("9007199254740993"), "voteLagSlots": 1, "leaderSlots": 4, "blocksProduced": 3, "skipRate": 25, "tvcPercent": 99, "voteCreditsRank": i + 1})
		roster[vote] = map[string]any{"identity": testIdentities[i], "name": "Titan roster", "client_version": "4.4.0"}
	}
	f.solstat = map[string]any{"network": "alpenglow", "timestamp": now.UnixMilli(), "epochInfo": map[string]any{"epoch": 1, "absoluteSlot": 115}, "networkStats": map[string]any{"epoch": 1, "totalValidators": 4, "activeValidators": 3, "delinquentValidators": 1, "totalStake": json.Number("1234567890.123456789"), "avgSkipRate": 25, "medianSkipRate": 0}, "validators": rows}
	windows := make(map[string]any)
	for _, window := range []string{"1000", "5000", "epoch"} {
		windows[window] = map[string]any{"actual_slots": 20, "coverage": map[string]any{"first_block_slot": 100, "last_block_slot": 119, "first_observed_at": now.Add(-10 * time.Second), "last_observed_at": now}, "voters": map[string]any{testVotes[0]: []any{10, 9, 1, 90}, testVotes[1]: []any{10, 9, 1, 90}, testVotes[2]: []any{8, 7, 1, 87.5}, testVotes[3]: []any{0, 0, 0, nil}}}
	}
	f.titan = map[string]any{"epoch": 1, "cluster": map[string]any{"id": "alpenglow", "genesis": GenesisHash, "vote_account": testVotes[0]}, "observation_source": map[string]any{"genesis": GenesisHash}, "overview": map[string]any{"observed_at": now}, "roster": roster, "windows": windows}
	f.peers = map[string]any{"root": 115, "identity": testIdentities, "ip": []string{"8.8.8.8", "8.8.4.4", "1.1.1.1", "9.9.9.9"}, "country": []string{"US", "DE", "ZZ", "GB"}, "heard_ago": []uint64{1, 1, 1, 121000}}
	return f
}

func marshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func (f *fixture) collector(t *testing.T) (*collector, *bytes.Buffer) {
	t.Helper()
	var genesisCalls, epochCalls atomic.Int32
	upgrader := websocket.Upgrader{}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if code := f.status[r.URL.Path]; code != 0 {
			w.WriteHeader(code)
			return
		}
		if body, ok := f.body[r.URL.Path]; ok {
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, body)
			return
		}
		if r.URL.Path == "/websocket" {
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer conn.Close()
			var request struct {
				Topic string `json:"topic"`
				Key   string `json:"key"`
				ID    int    `json:"id"`
			}
			if conn.ReadJSON(&request) != nil {
				return
			}
			if request.Topic != "peers" || request.Key != "gossip" {
				t.Error("unexpected mutable websocket request")
				return
			}
			if f.lanternRetry {
				if conn.WriteJSON(map[string]any{"topic": "peers", "key": "gossip", "id": request.ID, "value": f.lanternFirst}) != nil || conn.ReadJSON(&request) != nil {
					return
				}
				if request.Topic != "peers" || request.Key != "gossip" || request.ID != 2 {
					t.Error("unexpected retry request")
					return
				}
			}
			var value any = f.peers
			if f.lanternEmptySecond {
				value = nil
			}
			conn.WriteJSON(map[string]any{"topic": "peers", "key": "gossip", "id": request.ID, "value": value})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if r.Host == "rpc.ag.validblocks.com" {
			var req struct {
				Method string `json:"method"`
				Params []int  `json:"params"`
			}
			json.NewDecoder(r.Body).Decode(&req)
			var result any
			switch req.Method {
			case "getGenesisHash":
				result = GenesisHash
				if genesisCalls.Add(1) > 1 && f.genesisAfter != "" {
					result = f.genesisAfter
				}
			case "getEpochInfo":
				epoch := uint64(1)
				calls := epochCalls.Add(1)
				if calls > 1 && f.epochAfter != 0 {
					epoch = f.epochAfter
				}
				result = map[string]any{"epoch": epoch, "absoluteSlot": 115, "slotIndex": 15, "slotsInEpoch": 100}
				if calls > 1 && f.infoAfter != nil {
					result = f.infoAfter
				}
			case "getVoteAccounts":
				current := make([]voteAccount, 0, len(testVotes))
				for i, v := range testVotes {
					current = append(current, voteAccount{v, testIdentities[i]})
				}
				result = map[string]any{"current": current, "delinquent": []voteAccount{}}
			case "getClusterNodes":
				var nodes []rpcNode
				for i, id := range testIdentities {
					addr := "8.8.8.8:8001"
					nodes = append(nodes, rpcNode{id, &addr})
					if i > 0 {
						addr = "9.9.9.9:8001"
						nodes[i].Gossip = &addr
					}
				}
				result = nodes
			case "getRecentPerformanceSamples":
				f.performanceCalls.Add(1)
				if len(req.Params) != 1 || req.Params[0] != f.performanceLimit {
					t.Errorf("performance request must ask for %d samples", f.performanceLimit)
				}
				result = f.performance
			default:
				t.Errorf("unexpected RPC method %q", req.Method)
				w.WriteHeader(400)
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": result})
			return
		}
		switch r.URL.Path {
		case "/api/validators":
			json.NewEncoder(w).Encode(f.solstat)
		case "/analytics/api/config":
			json.NewEncoder(w).Encode(f.titan["cluster"])
		case "/analytics/api/stream":
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "event: snapshot\ndata: "+string(marshal(t, f.titan))+"\n\n")
		default:
			t.Errorf("unexpected provider path %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(server.Close)
	address := strings.TrimPrefix(server.URL, "https://")
	dial := func(ctx context.Context, network, addr string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}
	transport := &http.Transport{DialContext: dial, TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	t.Cleanup(transport.CloseIdleConnections)
	var logs bytes.Buffer
	c := &collector{http: &http.Client{Transport: transport, Timeout: time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, ws: &websocket.Dialer{NetDialContext: dial, TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, HandshakeTimeout: time.Second}, now: func() time.Time { return f.now }, runID: "fixture", log: slog.New(slog.NewJSONHandler(&logs, nil)).With("entry_point", "collector", "run_id", "fixture")}
	return c, &logs
}

func collectFixture(t *testing.T, f *fixture) (Snapshot, *bytes.Buffer) {
	t.Helper()
	c, logs := f.collector(t)
	s, err := c.collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return s, logs
}

func TestCollectBoundSourcesExactCountsAndCountries(t *testing.T) {
	s, logs := collectFixture(t, newFixture())
	if len(s.Validators) != 4 || s.Epoch == nil || *s.Epoch != "1" {
		t.Fatalf("unexpected directory: %+v", s)
	}
	if s.Reference == nil || s.Reference.Slot != "115" || s.Reference.EpochStart != "100" ||
		s.Reference.SlotsInEpoch != "100" || !s.Reference.ObservedAt.Equal(*s.Providers[0].SourceAt) {
		t.Fatalf("unexpected RPC reference: %+v", s.Reference)
	}
	if s.SolstatSummary == nil || s.SolstatSummary.Epoch != "1" || s.SolstatSummary.TotalValidators != "4" ||
		s.SolstatSummary.ActiveValidators != "3" || s.SolstatSummary.DelinquentValidators != "1" || s.SolstatSummary.TotalStakeSOL != "1234567890.123456789" ||
		s.SolstatSummary.AverageSkipRatePercent == nil || *s.SolstatSummary.AverageSkipRatePercent != 25 ||
		s.SolstatSummary.MedianSkipRatePercent == nil || *s.SolstatSummary.MedianSkipRatePercent != 0 {
		t.Fatalf("unexpected Solstat summary: %+v", s.SolstatSummary)
	}
	for _, p := range s.Providers {
		want := "fresh"
		if p.ID == "puffin" {
			want = "unavailable"
		}
		if p.State != want {
			t.Fatalf("provider %s state %q != %q (%s)", p.ID, p.State, want, p.Reason)
		}
	}
	puffin := provider(&s, "puffin")
	if puffin.Reason != "not_collected" {
		t.Fatalf("Puffin reason = %q", puffin.Reason)
	}
	puffin.State, puffin.Reason = "retired", "provider_retired"
	if err := Validate(s); err != nil {
		t.Fatalf("legacy snapshot rejected: %v", err)
	}
	puffin.State, puffin.Reason = "unavailable", "not_collected"
	byVote := make(map[string]Validator)
	for _, v := range s.Validators {
		byVote[v.VoteAccount] = v
	}
	for i, vote := range testVotes {
		v := byVote[vote]
		if len(v.Inclusion) != 3 || v.StakeSOL == nil || *v.StakeSOL != "1234567890.123456789" {
			t.Fatalf("lost measurements: %+v", v)
		}
		if v.RootSlot == nil || *v.RootSlot != "114" || v.CurrentEpochCredits == nil || *v.CurrentEpochCredits != "9007199254740993" || v.VoteLagSlots == nil || *v.VoteLagSlots != "1" {
			t.Fatalf("lost exact vote observations: %+v", v)
		}
		for _, x := range v.Inclusion {
			if x.CohortSize != "3" {
				t.Fatalf("rank cohort %q != 3", x.CohortSize)
			}
			if i < 2 && (x.Rank == nil || *x.Rank != "1") {
				t.Fatalf("competition tie lost: %+v", x)
			}
			if i == 2 && (x.Rank == nil || *x.Rank != "3") {
				t.Fatalf("competition gap lost: %+v", x)
			}
			if i == 3 && (x.Percent != nil || x.Rank != nil) {
				t.Fatalf("zero-opportunity score/rank: %+v", x)
			}
		}
		if i == 0 && v.CountryCode != "US" {
			t.Fatal("matched country not retained")
		}
		if i > 0 && v.CountryCode != "" {
			t.Fatalf("mismatched/unknown/old country leaked: %+v", v)
		}
	}
	encoded := marshal(t, s)
	for _, secret := range []string{"8.8.8.8", "8.8.4.4", "1.1.1.1", "9.9.9.9", "\"ip\""} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("raw network data in output: %s", secret)
		}
		if strings.Contains(logs.String(), secret) {
			t.Fatalf("raw network data in logs: %s", secret)
		}
	}
	if !bytes.Contains(encoded, []byte(`"opportunities":"10"`)) {
		t.Fatal("counts are not decimal strings")
	}
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var event map[string]any
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatal(err)
		}
		if event["run_id"] != "fixture" || event["entry_point"] != "collector" {
			t.Fatalf("uncorrelated event: %s", line)
		}
	}
}

func TestOptionalRPCPerformanceSample(t *testing.T) {
	f := newFixture()
	c, _ := f.collector(t)
	s, err := c.collect(context.Background())
	if err != nil || s.Performance != nil || f.performanceCalls.Load() != 0 {
		t.Fatalf("default collection changed: sample=%+v calls=%d err=%v", s.Performance, f.performanceCalls.Load(), err)
	}
	c.performanceSamples = true
	s, err = c.collect(context.Background())
	if err != nil || f.performanceCalls.Load() != 1 || s.Performance == nil || s.Performance.Slot != "114" ||
		s.Performance.Transactions != "120" || s.Performance.NonVoteTransactions == nil || *s.Performance.NonVoteTransactions != "100" ||
		s.Performance.Slots != "4" || s.Performance.PeriodSeconds != 60 || !s.Performance.ObservedAt.Equal(f.now) {
		t.Fatalf("optional sample: %+v, %v", s.Performance, err)
	}
	for name, value := range map[string]any{
		"empty": []any{}, "multiple": []any{map[string]any{}, map[string]any{}},
		"future":                 []any{map[string]any{"slot": 116, "numTransactions": 2, "numSlots": 1, "samplePeriodSecs": 60}},
		"zero period":            []any{map[string]any{"slot": 114, "numTransactions": 2, "numSlots": 1, "samplePeriodSecs": 0}},
		"non-vote exceeds total": []any{map[string]any{"slot": 114, "numTransactions": 2, "numNonVoteTransactions": 3, "numSlots": 1, "samplePeriodSecs": 60}},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture()
			f.performance = value
			c, _ := f.collector(t)
			c.performanceSamples = true
			got, err := c.collect(context.Background())
			if err != nil || got.Performance != nil || len(got.Validators) != 4 {
				t.Fatalf("invalid optional sample affected directory: %+v, %v", got.Performance, err)
			}
		})
	}
	f = newFixture()
	f.performance = []any{map[string]any{"slot": 114, "numTransactions": 0, "numNonVoteTransactions": nil, "numSlots": 0, "samplePeriodSecs": 60}}
	c, _ = f.collector(t)
	c.performanceSamples = true
	s, err = c.collect(context.Background())
	if err != nil || s.Performance == nil || s.Performance.Transactions != "0" || s.Performance.NonVoteTransactions != nil {
		t.Fatalf("zero/missing non-vote sample: %+v, %v", s.Performance, err)
	}
}

func TestRPCPerformanceValidation(t *testing.T) {
	f := newFixture()
	c, _ := f.collector(t)
	c.performanceSamples = true
	s, err := c.collect(context.Background())
	if err != nil || s.Performance == nil {
		t.Fatalf("sample fixture: %v", err)
	}
	for name, edit := range map[string]func(*Snapshot){
		"missing reference":  func(s *Snapshot) { s.Reference = nil },
		"future slot":        func(s *Snapshot) { s.Performance.Slot = "116" },
		"noncanonical count": func(s *Snapshot) { s.Performance.Transactions = "01" },
		"zero period":        func(s *Snapshot) { s.Performance.PeriodSeconds = 0 },
		"future receipt":     func(s *Snapshot) { s.Performance.ObservedAt = s.Performance.ObservedAt.Add(time.Second) },
		"old receipt":        func(s *Snapshot) { s.Performance.ObservedAt = s.Performance.ObservedAt.Add(-MaxAge - time.Second) },
		"too many non-votes": func(s *Snapshot) { value := Count("121"); s.Performance.NonVoteTransactions = &value },
	} {
		t.Run(name, func(t *testing.T) {
			copy := s
			value := *s.Performance
			copy.Performance = &value
			edit(&copy)
			if err := Validate(copy); err == nil {
				t.Fatal("invalid performance sample accepted")
			}
		})
	}
}

func TestRPCPerformanceHistoryValidation(t *testing.T) {
	f := newFixture()
	c, _ := f.collector(t)
	c.performanceSamples = true
	s, err := c.collect(context.Background())
	if err != nil || s.Performance == nil {
		t.Fatalf("sample fixture: %v", err)
	}
	s.PerformanceHistory = []RPCPerformance{*s.Performance, *s.Performance}
	s.PerformanceHistory[1].Slot = "113"
	if err := Validate(s); err != nil {
		t.Fatalf("valid descending history: %v", err)
	}
	for name, edit := range map[string]func(*Snapshot){
		"duplicate slot": func(s *Snapshot) { s.PerformanceHistory[1].Slot = "114" },
		"ascending slot": func(s *Snapshot) { s.PerformanceHistory[1].Slot = "115" },
		"invalid count":  func(s *Snapshot) { s.PerformanceHistory[1].Transactions = "01" },
		"different receipt": func(s *Snapshot) {
			s.PerformanceHistory[1].ObservedAt = s.PerformanceHistory[1].ObservedAt.Add(-time.Second)
		},
		"over limit": func(s *Snapshot) { s.PerformanceHistory = make([]RPCPerformance, MaxPerformanceHistory+1) },
	} {
		t.Run(name, func(t *testing.T) {
			copy := s
			copy.PerformanceHistory = append([]RPCPerformance(nil), s.PerformanceHistory...)
			edit(&copy)
			if err := Validate(copy); err == nil {
				t.Fatal("invalid performance history accepted")
			}
		})
	}
}

func TestOptionalRPCPerformanceHistory(t *testing.T) {
	f := newFixture()
	f.performanceLimit = MaxPerformanceHistory
	f.performance = []any{
		map[string]any{"slot": 114, "numTransactions": 120, "numSlots": 4, "samplePeriodSecs": 60},
		map[string]any{"slot": 113, "numTransactions": 0, "numSlots": 1, "samplePeriodSecs": 60},
	}
	c, _ := f.collector(t)
	c.performanceSamples, c.performanceHistory = true, true
	s, err := c.collect(context.Background())
	if err != nil || len(s.PerformanceHistory) != 2 || s.Performance == nil || s.Performance.Slot != "114" || s.PerformanceHistory[1].Transactions != "0" {
		t.Fatalf("history=%+v sample=%+v err=%v", s.PerformanceHistory, s.Performance, err)
	}
	f.performance = []any{
		map[string]any{"slot": 114, "numTransactions": 120, "numSlots": 4, "samplePeriodSecs": 60},
		map[string]any{"slot": 114, "numTransactions": 1, "numSlots": 1, "samplePeriodSecs": 60},
	}
	s, err = c.collect(context.Background())
	if err != nil || len(s.PerformanceHistory) != 0 || s.Performance != nil || len(s.Validators) != 4 {
		t.Fatalf("invalid history should not affect directory: %+v, %v", s.PerformanceHistory, err)
	}
}

func TestSolstatSummaryIsOptionalAndEpochBound(t *testing.T) {
	f := newFixture()
	base := f.solstat["networkStats"].(map[string]any)
	epoch := uint64(1)
	for _, tc := range []struct {
		name   string
		change func(map[string]any)
	}{
		{"wrong epoch", func(v map[string]any) { v["epoch"] = 2 }},
		{"bad counts", func(v map[string]any) { v["activeValidators"] = 4 }},
		{"missing stake", func(v map[string]any) { delete(v, "totalStake") }},
		{"negative stake", func(v map[string]any) { v["totalStake"] = -1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := make(map[string]any)
			for k, value := range base {
				v[k] = value
			}
			tc.change(v)
			if parseSolstatSummary(marshal(t, v), epochInfo{Epoch: &epoch}) != nil {
				t.Fatal("accepted invalid summary")
			}
		})
	}
	base["avgSkipRate"] = "invalid"
	v := parseSolstatSummary(marshal(t, base), epochInfo{Epoch: &epoch})
	if v == nil || v.AverageSkipRatePercent != nil || v.MedianSkipRatePercent == nil || *v.MedianSkipRatePercent != 0 {
		t.Fatalf("optional rate did not remain unknown: %+v", v)
	}
}

func TestSolstatVoteObservationsPreserveMissingZeroAndSource(t *testing.T) {
	f := newFixture()
	rows := f.solstat["validators"].([]any)
	zero := rows[0].(map[string]any)
	zero["rootSlot"], zero["currentEpochCredits"], zero["voteLagSlots"] = 0, 0, 0
	missing := rows[1].(map[string]any)
	delete(missing, "rootSlot")
	delete(missing, "currentEpochCredits")
	delete(missing, "voteLagSlots")
	invalid := rows[2].(map[string]any)
	invalid["rootSlot"], invalid["currentEpochCredits"], invalid["voteLagSlots"] = "not a slot", -1, 1.5
	s, _ := collectFixture(t, f)
	byVote := make(map[string]Validator)
	for _, row := range s.Validators {
		byVote[row.VoteAccount] = row
	}
	z := byVote[testVotes[0]]
	if z.RootSlot == nil || *z.RootSlot != "0" || z.CurrentEpochCredits == nil || *z.CurrentEpochCredits != "0" || z.VoteLagSlots == nil || *z.VoteLagSlots != "0" {
		t.Fatalf("zero values lost: %+v", z)
	}
	m := byVote[testVotes[1]]
	if m.RootSlot != nil || m.CurrentEpochCredits != nil || m.VoteLagSlots != nil {
		t.Fatalf("missing values invented: %+v", m)
	}
	bad := byVote[testVotes[2]]
	if bad.RootSlot != nil || bad.CurrentEpochCredits != nil || bad.VoteLagSlots != nil || provider(&s, "solstat").State != "fresh" {
		t.Fatalf("invalid optional values affected directory: %+v", bad)
	}
	s.Validators[0].DirectorySource = "titan"
	if Validate(s) == nil {
		t.Fatal("accepted Solstat-only fields on a Titan row")
	}
}

func TestRPCReferenceGeometryAndBinding(t *testing.T) {
	for _, after := range []map[string]any{
		{"epoch": 1, "absoluteSlot": 114, "slotIndex": 14, "slotsInEpoch": 100},
		{"epoch": 1, "absoluteSlot": 116, "slotIndex": 15, "slotsInEpoch": 100},
		{"epoch": 1, "absoluteSlot": 116, "slotIndex": 16, "slotsInEpoch": 101},
	} {
		f := newFixture()
		f.infoAfter = after
		s, _ := collectFixture(t, f)
		if s.Reference != nil {
			t.Fatalf("inconsistent RPC observations published: %+v", after)
		}
	}
	s, _ := collectFixture(t, newFixture())
	for _, tc := range []struct {
		name string
		edit func(*RPCReference)
		good bool
	}{
		{"zero", func(r *RPCReference) { r.Slot, r.EpochStart, r.SlotsInEpoch = "0", "0", "1" }, true},
		{"maximum", func(r *RPCReference) {
			r.Slot, r.EpochStart, r.SlotsInEpoch = "18446744073709551615", "18446744073709551615", "1"
		}, true},
		{"out of range", func(r *RPCReference) { r.Slot = "200" }, false},
		{"zero length", func(r *RPCReference) { r.SlotsInEpoch = "0" }, false},
		{"overflow", func(r *RPCReference) { r.EpochStart, r.SlotsInEpoch = "18446744073709551615", "2" }, false},
		{"bad count", func(r *RPCReference) { r.Slot = "01" }, false},
		{"wrong time", func(r *RPCReference) { r.ObservedAt = r.ObservedAt.Add(time.Second) }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			copy := s
			reference := *s.Reference
			copy.Reference = &reference
			tc.edit(copy.Reference)
			if err := Validate(copy); (err == nil) != tc.good {
				t.Fatalf("Validate() = %v, want good=%v", err, tc.good)
			}
		})
	}
	s.Reference = nil
	if err := Validate(s); err != nil {
		t.Fatalf("legacy snapshot rejected: %v", err)
	}
}

func TestCollectRetriesUnreadyLanternResponse(t *testing.T) {
	for _, tc := range []struct {
		name       string
		stale      bool
		staysStale bool
		incomplete bool
		empty      bool
	}{{"not yet collected", false, false, false, false}, {"cached previous epoch", true, false, false, false},
		{"previous epoch after retry", true, true, false, false}, {"incomplete retry", true, false, true, false},
		{"still empty after retry", false, false, false, true}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture()
			f.lanternRetry = true
			f.lanternEmptySecond = tc.empty
			if tc.stale {
				previous := make(map[string]any, len(f.peers))
				for key, value := range f.peers {
					previous[key] = value
				}
				previous["root"] = uint64(99)
				f.lanternFirst = previous
			}
			if tc.staysStale {
				f.peers["root"] = uint64(99)
			}
			if tc.incomplete {
				delete(f.peers, "country")
			}
			c, _ := f.collector(t)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			s, err := c.collect(ctx)
			if err != nil {
				t.Fatal(err)
			}
			p := provider(&s, "lantern")
			if tc.empty {
				if p.State != "unavailable" || p.Reason != "request_failed" {
					t.Fatalf("empty Lantern retry: state=%s reason=%s", p.State, p.Reason)
				}
				return
			}
			if tc.staysStale || tc.incomplete {
				if p.State != "invalid" || p.Reason != "invalid_payload" {
					t.Fatalf("invalid Lantern retry: state=%s reason=%s", p.State, p.Reason)
				}
				return
			}
			if p.State != "fresh" || p.Reason != "" {
				t.Fatalf("refreshed Lantern response: state=%s reason=%s", p.State, p.Reason)
			}
		})
	}
}

func TestLanternRetryStopsOnCancel(t *testing.T) {
	f := newFixture()
	f.lanternRetry = true
	c, _ := f.collector(t)
	epoch, slot, index, slots := uint64(1), uint64(115), uint64(15), uint64(100)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := c.lantern(ctx, epochInfo{Epoch: &epoch, AbsoluteSlot: &slot, SlotIndex: &index, SlotsInEpoch: &slots})
	if err == nil || time.Since(started) > 5*time.Second {
		t.Fatalf("cancelled Lantern retry did not stop promptly: %v", err)
	}
}

func TestCollectFailClosedBindingAndProviderFailures(t *testing.T) {
	cases := []struct {
		name                    string
		mutate                  func(*fixture)
		provider, state, reason string
		rows                    int
	}{
		{"genesis changed", func(f *fixture) { f.genesisAfter = testVotes[1] }, "rpc", "invalid", "rpc_genesis_recheck_failed", 0},
		{"epoch changed", func(f *fixture) { f.epochAfter = 2 }, "rpc", "invalid", "rpc_epoch_changed", 0},
		{"directory wrong network", func(f *fixture) { f.solstat["network"] = "mainnet" }, "solstat", "invalid", "invalid_payload", 4},
		{"directory identity mismatch", func(f *fixture) {
			f.solstat["validators"].([]any)[0].(map[string]any)["nodePubkey"] = testIdentities[1]
		}, "solstat", "invalid", "invalid_payload", 4},
		{"directory missing critical", func(f *fixture) { delete(f.solstat, "timestamp") }, "solstat", "invalid", "invalid_payload", 4},
		{"directory stale", func(f *fixture) { f.solstat["timestamp"] = f.now.Add(-3 * time.Minute).UnixMilli() }, "solstat", "stale", "source_stale", 4},
		{"Titan stale", func(f *fixture) { f.titan["overview"].(map[string]any)["observed_at"] = f.now.Add(-3 * time.Minute) }, "titan", "stale", "source_stale", 4},
		{"directory HTTP failure", func(f *fixture) { f.status["/api/validators"] = 503 }, "solstat", "unavailable", "request_failed", 4},
		{"directory body bound", func(f *fixture) { f.body["/api/validators"] = strings.Repeat(" ", responseLimit+1) }, "solstat", "unavailable", "request_failed", 4},
		{"Titan arithmetic", func(f *fixture) {
			f.titan["windows"].(map[string]any)["epoch"].(map[string]any)["voters"].(map[string]any)[testVotes[0]] = []any{10, 9, 2, 90}
		}, "titan", "invalid", "invalid_payload", 4},
		{"Titan null count", func(f *fixture) {
			f.titan["windows"].(map[string]any)["epoch"].(map[string]any)["voters"].(map[string]any)[testVotes[3]] = []any{nil, 0, 0, nil}
		}, "titan", "invalid", "invalid_payload", 4},
		{"Titan unknown roster", func(f *fixture) {
			f.titan["roster"].(map[string]any)[testVotes[0]].(map[string]any)["identity"] = testIdentities[1]
		}, "titan", "invalid", "invalid_payload", 4},
		{"Lantern unequal columns", func(f *fixture) { f.peers["country"] = []string{"US"} }, "lantern", "invalid", "invalid_payload", 4},
		{"Lantern old epoch", func(f *fixture) { f.peers["root"] = 99 }, "lantern", "invalid", "invalid_payload", 4},
		{"Lantern duplicate identity", func(f *fixture) {
			f.peers["identity"] = []string{testIdentities[0], testIdentities[0], testIdentities[2], testIdentities[3]}
		}, "lantern", "invalid", "invalid_payload", 4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture()
			tc.mutate(f)
			s, logs := collectFixture(t, f)
			p := provider(&s, tc.provider)
			if p.State != tc.state || p.Reason != tc.reason || len(s.Validators) != tc.rows {
				t.Fatalf("state=%s reason=%s rows=%d; want %s/%s/%d", p.State, p.Reason, len(s.Validators), tc.state, tc.reason, tc.rows)
			}
			if (s.Reference == nil) != (tc.provider == "rpc") {
				t.Fatal("RPC reference did not follow its own binding")
			}
			if tc.provider == "titan" {
				for _, v := range s.Validators {
					if len(v.Inclusion) != 0 {
						t.Fatal("invalid Titan data survived")
					}
				}
			}
			if tc.provider == "lantern" {
				detail := map[string]string{"Lantern unequal columns": "columns_invalid", "Lantern old epoch": "root_unavailable_or_outside_epoch", "Lantern duplicate identity": "identity_invalid_or_duplicate"}[tc.name]
				if !strings.Contains(logs.String(), `"detail":"`+detail+`"`) {
					t.Fatalf("missing safe Lantern validation detail %q", detail)
				}
				for _, private := range append(testIdentities, "8.8.8.8", "8.8.4.4", "1.1.1.1", "9.9.9.9") {
					if strings.Contains(logs.String(), private) {
						t.Fatal("Lantern validation log exposed peer data")
					}
				}
				for _, v := range s.Validators {
					if v.CountryCode != "" {
						t.Fatal("invalid country survived")
					}
				}
			}
		})
	}
}

func TestTitanMissingIdentityUsesActualRPCVoteBinding(t *testing.T) {
	f := newFixture()
	delete(f.titan["roster"].(map[string]any)[testVotes[0]].(map[string]any), "identity")
	s, _ := collectFixture(t, f)
	if provider(&s, "titan").State != "fresh" {
		t.Fatal("RPC identity fallback failed")
	}
}

func TestCompareRatiosDoesNotOverflow(t *testing.T) {
	if compareRatio(math.MaxUint64-1, math.MaxUint64, math.MaxUint64-2, math.MaxUint64-1) <= 0 {
		t.Fatal("128-bit cross products compared incorrectly")
	}
	if compareRatio(1, 2, 2, 4) != 0 {
		t.Fatal("equal ratio compared differently")
	}
}

func TestSnapshotAtomicRoundTripAndStrictRead(t *testing.T) {
	s, _ := collectFixture(t, newFixture())
	dir := t.TempDir()
	path := filepath.Join(dir, "cluster.json")
	if err := WriteFile(path, s); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadFile(path); err != nil {
		t.Fatal(err)
	}
	legacy, legacyPath := s, filepath.Join(dir, "legacy.json")
	legacy.Reference = nil
	if err := WriteFile(legacyPath, legacy); err != nil {
		t.Fatal(err)
	}
	if decoded, err := ReadFile(legacyPath); err != nil || decoded.Reference != nil {
		t.Fatalf("legacy cache: reference=%+v error=%v", decoded.Reference, err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0640 {
		t.Fatalf("mode/info %v %v", info, err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	invalid := s
	invalid.Network = "mainnet"
	if WriteFile(path, invalid) == nil {
		t.Fatal("invalid replacement accepted")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("failed write damaged old file")
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadFile(link); err == nil {
		t.Fatal("symlink read accepted")
	}
	if WriteFile(link, s) == nil {
		t.Fatal("symlink write accepted")
	}
	fifo := filepath.Join(dir, "fifo")
	if err := syscall.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadFile(fifo); err == nil {
		t.Fatal("FIFO read accepted")
	}
	if err := WriteFile(fifo, s); err == nil {
		t.Fatal("FIFO write accepted")
	}
	for _, bad := range []string{"relative.json", dir + "/./cache.json", dir + "/../cache.json"} {
		if _, err := ReadFile(bad); err == nil {
			t.Fatalf("unclean/relative read accepted: %s", bad)
		}
		if err := WriteFile(bad, s); err == nil {
			t.Fatalf("unclean/relative write accepted: %s", bad)
		}
	}
	for _, tc := range []struct{ name, body string }{{"unknown field", strings.TrimSuffix(string(before), "}\n") + `,"ip":"8.8.8.8"}`}, {"trailing JSON", string(before) + "{}"}, {"invalid UTF8", string([]byte{0xff})}, {"oversized", strings.Repeat(" ", MaxBytes+1)}} {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(tc.body), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := ReadFile(path); err == nil {
				t.Fatal("unsafe cache accepted")
			}
		})
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".tmp") {
			t.Fatal("temporary file leaked")
		}
	}
}

func TestPuffinUncollectedAndLegacySnapshots(t *testing.T) {
	s := newSnapshot(time.Now().UTC())
	p := provider(&s, "puffin")
	if p.State != "unavailable" || p.Reason != "not_collected" {
		t.Fatalf("unexpected Puffin state: %s/%s", p.State, p.Reason)
	}
	if err := Validate(s); err != nil {
		t.Fatalf("new snapshot rejected: %v", err)
	}
	p.State, p.Reason = "retired", "provider_retired"
	if err := Validate(s); err != nil {
		t.Fatalf("legacy snapshot rejected: %v", err)
	}
	p.State, p.Reason = "unavailable", "provider_retired"
	if err := Validate(s); err == nil {
		t.Fatal("inconsistent state accepted")
	}
}

func TestValidateNegativeBounds(t *testing.T) {
	base, _ := collectFixture(t, newFixture())
	cases := []struct {
		name   string
		mutate func(*Snapshot)
	}{
		{"unknown genesis", func(s *Snapshot) { s.GenesisHash = testVotes[0] }},
		{"duplicate provider", func(s *Snapshot) { s.Providers[1] = s.Providers[0] }},
		{"unknown source URL", func(s *Snapshot) { s.Providers[0].SourceURL = "http://127.0.0.1" }},
		{"raw error reason", func(s *Snapshot) { s.Providers[0].Reason = "connect 8.8.8.8 failed" }},
		{"unverified metadata", func(s *Snapshot) { p := provider(s, "puffin"); p.Binding = "raw peer 8.8.8.8" }},
		{"unsafe count", func(s *Snapshot) { s.Validators[0].Inclusion[0].Included = "18446744073709551616" }},
		{"leading zero", func(s *Snapshot) { s.Validators[0].Inclusion[0].Included = "01" }},
		{"nonfinite percentage", func(s *Snapshot) { n := math.Inf(1); s.Validators[0].CommissionPercent = &n }},
		{"country without source", func(s *Snapshot) { s.Validators[0].CountryCode = "US"; s.Validators[0].CountrySource = "" }},
		{"invented FAST", func(s *Snapshot) { s.Validators[0].Fast = &FastScore{Provider: "puffin"} }},
		{"extra window", func(s *Snapshot) {
			s.Validators[0].Inclusion = append(s.Validators[0].Inclusion, s.Validators[0].Inclusion[0])
		}},
		{"row bound", func(s *Snapshot) { s.Validators = make([]Validator, MaxRows+1) }},
		{"duplicate vote", func(s *Snapshot) { s.Validators[1].VoteAccount = s.Validators[0].VoteAccount }},
		{"future source", func(s *Snapshot) {
			p := provider(s, "solstat")
			future := s.CapturedAt.Add(time.Minute)
			p.SourceAt = &future
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var s Snapshot
			if err := json.Unmarshal(marshal(t, base), &s); err != nil {
				t.Fatal(err)
			}
			tc.mutate(&s)
			if err := Validate(s); err == nil {
				t.Fatal("invalid cache accepted")
			}
		})
	}
	for _, code := range []string{"ZZ", "AA", "us", "USA", "", "<>", "1A"} {
		if countryOK(code) {
			t.Fatalf("invalid country accepted %q", code)
		}
	}
	for _, code := range []string{"US", "DE", "XK"} {
		if !countryOK(code) {
			t.Fatalf("valid country rejected %q", code)
		}
	}
	if _, err := number("18446744073709551615"); err != nil {
		t.Fatal("exact uint64 maximum rejected")
	}
}

func TestCollectLanternHeardAgePresence(t *testing.T) {
	for _, tc := range []struct {
		name        string
		age         any
		missing     bool
		wantState   string
		wantCountry string
	}{
		{"known zero", uint64(0), false, "fresh", "US"},
		{"unknown null", nil, false, "fresh", ""},
		{"missing column", nil, true, "invalid", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture()
			if tc.missing {
				delete(f.peers, "heard_ago")
			} else {
				f.peers["heard_ago"] = []any{tc.age, uint64(1), uint64(1), uint64(121000)}
			}
			s, _ := collectFixture(t, f)
			var country string
			for _, row := range s.Validators {
				if row.VoteAccount == testVotes[0] {
					country = row.CountryCode
				}
			}
			if state := provider(&s, "lantern").State; state != tc.wantState || country != tc.wantCountry {
				t.Errorf("got provider=%q country=%q; want provider=%q country=%q", state, country, tc.wantState, tc.wantCountry)
			}
		})
	}
}

func TestUnsupportedProfileDoesNotCollect(t *testing.T) {
	if _, err := Collect(context.Background(), "http://127.0.0.1/"); err == nil {
		t.Fatal("arbitrary URL accepted as profile")
	}
}

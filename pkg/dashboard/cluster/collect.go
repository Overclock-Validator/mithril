package cluster

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"math/big"
	"math/bits"
	"net"
	"net/http"
	"net/netip"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	responseLimit  = 2 << 20
	titanConfigURL = "https://observatory-origin.titananalytics.io/analytics/api/config"
)

var errStale = errors.New("provider source is stale")

type collector struct {
	http               *http.Client
	log                *slog.Logger
	runID              string
	now                func() time.Time
	performanceSamples bool
	performanceHistory bool
}

type epochInfo struct {
	Epoch        *uint64 `json:"epoch"`
	AbsoluteSlot *uint64 `json:"absoluteSlot"`
	SlotIndex    *uint64 `json:"slotIndex"`
	SlotsInEpoch *uint64 `json:"slotsInEpoch"`
}

type rpcPerformanceSample struct {
	Slot                *uint64 `json:"slot"`
	Transactions        *uint64 `json:"numTransactions"`
	NonVoteTransactions *uint64 `json:"numNonVoteTransactions"`
	Slots               *uint64 `json:"numSlots"`
	PeriodSeconds       *uint16 `json:"samplePeriodSecs"`
}

type rpcNode struct {
	Identity string  `json:"pubkey"`
	Gossip   *string `json:"gossip"`
}

type voteAccount struct {
	Vote     string `json:"votePubkey"`
	Identity string `json:"nodePubkey"`
}

type solstatResponse struct {
	Network      string          `json:"network"`
	Timestamp    *uint64         `json:"timestamp"`
	EpochInfo    *epochInfo      `json:"epochInfo"`
	NetworkStats json.RawMessage `json:"networkStats"`
	Validators   []struct {
		Vote           string          `json:"votePubkey"`
		Identity       string          `json:"nodePubkey"`
		Name           string          `json:"name"`
		Version        string          `json:"softwareVersion"`
		Stake          *json.Number    `json:"activatedStake"`
		Commission     *float64        `json:"commission"`
		Delinquent     *bool           `json:"delinquent"`
		LastVote       *uint64         `json:"lastVote"`
		RootSlot       json.RawMessage `json:"rootSlot"`
		EpochCredits   json.RawMessage `json:"currentEpochCredits"`
		VoteLag        json.RawMessage `json:"voteLagSlots"`
		LeaderSlots    *uint64         `json:"leaderSlots"`
		BlocksProduced *uint64         `json:"blocksProduced"`
		SkipRate       *float64        `json:"skipRate"`
		Efficiency     *float64        `json:"tvcPercent"`
		Rank           *uint64         `json:"voteCreditsRank"`
	} `json:"validators"`
}

type titanCluster struct {
	ID      string `json:"id"`
	Genesis string `json:"genesis"`
	Vote    string `json:"vote_account"`
}

type titanResponse struct {
	Epoch             *uint64      `json:"epoch"`
	Cluster           titanCluster `json:"cluster"`
	ObservationSource struct {
		Genesis string `json:"genesis"`
	} `json:"observation_source"`
	Overview struct {
		ObservedAt time.Time `json:"observed_at"`
	} `json:"overview"`
	Roster map[string]struct {
		Identity string `json:"identity"`
		Name     string `json:"name"`
		Version  string `json:"client_version"`
	} `json:"roster"`
	Windows map[string]struct {
		ActualSlots *uint64                      `json:"actual_slots"`
		Voters      map[string][]json.RawMessage `json:"voters"`
		Coverage    struct {
			FirstSlot       *uint64   `json:"first_block_slot"`
			LastSlot        *uint64   `json:"last_block_slot"`
			FirstObservedAt time.Time `json:"first_observed_at"`
			LastObservedAt  time.Time `json:"last_observed_at"`
		} `json:"coverage"`
	} `json:"windows"`
}

type result[T any] struct {
	value T
	at    time.Time
	err   error
}

// Collect makes one bounded collection for the fixed alpenglow profile.
// Provider failures are valid unavailable/invalid states, not zero measurements.
// Only an unknown profile or an internal output invariant returns an error.
func Collect(ctx context.Context, profile string) (Snapshot, error) {
	return collectProfile(ctx, profile, false, false)
}

// CollectWithPerformance adds one optional RPC sample. Readers and clients
// must support the optional field before this mode writes their shared cache.
func CollectWithPerformance(ctx context.Context, profile string) (Snapshot, error) {
	return collectProfile(ctx, profile, true, false)
}

// CollectWithPerformanceHistory adds up to 120 provider-reported samples.
// Upgrade readers and clients before using this mode with their shared cache.
func CollectWithPerformanceHistory(ctx context.Context, profile string) (Snapshot, error) {
	return collectProfile(ctx, profile, true, true)
}

func collectProfile(ctx context.Context, profile string, performance, history bool) (Snapshot, error) {
	if profile != "alpenglow" {
		return Snapshot{}, errors.New("unsupported cluster profile")
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return Snapshot{}, err
	}
	runID := hex.EncodeToString(id[:])
	transport := &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 8 * time.Second}).DialContext, TLSHandshakeTimeout: 8 * time.Second, ResponseHeaderTimeout: 10 * time.Second, MaxIdleConns: 8, IdleConnTimeout: 20 * time.Second}
	defer transport.CloseIdleConnections()
	c := collector{
		http: &http.Client{Transport: transport, Timeout: 18 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("redirect denied") }},
		log:  slog.Default().With("entry_point", "collector", "run_id", runID), runID: runID, now: func() time.Time { return time.Now().UTC() }, performanceSamples: performance, performanceHistory: history,
	}
	ctx, cancel := context.WithTimeout(ctx, 55*time.Second)
	defer cancel()
	return c.collect(ctx)
}

func newSnapshot(now time.Time) Snapshot {
	s := Snapshot{SchemaVersion: 1, Scope: "independent_cluster", Profile: "alpenglow", Network: "alpenglow-community", GenesisHash: GenesisHash, CapturedAt: now, FastState: "unavailable", Validators: []Validator{}}
	for _, id := range []string{"rpc", "solstat", "titan", "geoip", "puffin"} {
		p := ProviderStatus{ID: id, State: "unavailable", Reason: "not_collected", SourceURL: sourceURLs[id], CapturedAt: now, RowCount: "0"}
		s.Providers = append(s.Providers, p)
	}
	return s
}

func provider(s *Snapshot, id string) *ProviderStatus {
	for i := range s.Providers {
		if s.Providers[i].ID == id {
			return &s.Providers[i]
		}
	}
	panic("internal provider not found")
}

func (c *collector) collect(ctx context.Context) (Snapshot, error) {
	s := newSnapshot(c.now())
	var genesis string
	if err := c.rpc(ctx, "getGenesisHash", &genesis); err != nil || genesis != GenesisHash {
		reason := "rpc_unavailable"
		if err == nil {
			reason = "rpc_genesis_mismatch"
		}
		return c.finish(s, reason)
	}
	var epoch epochInfo
	if err := c.rpc(ctx, "getEpochInfo", &epoch); err != nil || !epochValid(epoch) {
		return c.finish(s, "rpc_epoch_invalid")
	}
	e := count(*epoch.Epoch)
	s.Epoch = &e
	var votes result[map[string]string]
	var nodes result[map[string]netip.Addr]
	var solstat result[solstatResponse]
	var titan result[titanResponse]
	var performance result[[]rpcPerformanceSample]
	var wg sync.WaitGroup
	wg.Add(4)
	if c.performanceSamples {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := time.Now()
			request, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			limit := 1
			if c.performanceHistory {
				limit = MaxPerformanceHistory
			}
			performance.err = c.rpc(request, "getRecentPerformanceSamples", &performance.value, limit)
			performance.at = c.now()
			c.captureLog("rpc", "performance_sample", start, performance.err)
		}()
	}
	go func() {
		defer wg.Done()
		start := time.Now()
		votes.value, votes.err = c.voteAccounts(ctx)
		votes.at = c.now()
		c.captureLog("rpc", "vote_accounts", start, votes.err)
	}()
	go func() {
		defer wg.Done()
		start := time.Now()
		nodes.value, nodes.err = c.clusterNodes(ctx)
		nodes.at = c.now()
		c.captureLog("rpc", "gossip_contacts", start, nodes.err)
	}()
	go func() {
		defer wg.Done()
		start := time.Now()
		solstat.err = c.getJSON(ctx, "solstat", sourceURLs["solstat"], &solstat.value)
		solstat.at = c.now()
		c.captureLog("solstat", "directory", start, solstat.err)
	}()
	go func() {
		defer wg.Done()
		start := time.Now()
		titan.value, titan.err = c.titan(ctx)
		titan.at = c.now()
		c.captureLog("titan", "snapshot", start, titan.err)
	}()
	wg.Wait()
	var after epochInfo
	if err := c.rpc(ctx, "getGenesisHash", &genesis); err != nil || genesis != GenesisHash {
		return c.finish(s, "rpc_genesis_recheck_failed")
	}
	if err := c.rpc(ctx, "getEpochInfo", &after); err != nil || !epochValid(after) || *after.Epoch != *epoch.Epoch {
		return c.finish(s, "rpc_epoch_changed")
	}
	if votes.err != nil {
		return c.finish(s, "rpc_vote_accounts_invalid")
	}
	now := c.now()
	markFresh(provider(&s, "rpc"), now, now, e, uint64(len(votes.value)), "rpc_genesis_before_after", "response")
	// Only publish a reference when both epoch responses describe the same
	// geometry and the later response did not move backwards.
	if *after.SlotsInEpoch == *epoch.SlotsInEpoch &&
		*after.AbsoluteSlot-*after.SlotIndex == *epoch.AbsoluteSlot-*epoch.SlotIndex &&
		*after.AbsoluteSlot >= *epoch.AbsoluteSlot {
		s.Reference = &RPCReference{Slot: count(*after.AbsoluteSlot), EpochStart: count(*after.AbsoluteSlot - *after.SlotIndex),
			SlotsInEpoch: count(*after.SlotsInEpoch), ObservedAt: now}
	}
	if s.Reference != nil && performance.err == nil && fresh(performance.at, now) && len(performance.value) == 1 {
		v := performance.value[0]
		if v.Slot != nil && *v.Slot <= *after.AbsoluteSlot && v.Transactions != nil && v.Slots != nil &&
			v.PeriodSeconds != nil && *v.PeriodSeconds > 0 && (v.NonVoteTransactions == nil || *v.NonVoteTransactions <= *v.Transactions) {
			s.Performance = &RPCPerformance{Slot: count(*v.Slot), Transactions: count(*v.Transactions), NonVoteTransactions: countPointer(v.NonVoteTransactions),
				Slots: count(*v.Slots), PeriodSeconds: *v.PeriodSeconds, ObservedAt: performance.at}
		}
	}
	if c.performanceHistory && s.Reference != nil && performance.err == nil && fresh(performance.at, now) && len(performance.value) > 0 && len(performance.value) <= MaxPerformanceHistory {
		var history []RPCPerformance
		var previous uint64
		for i, v := range performance.value {
			if v.Slot == nil || *v.Slot > *after.AbsoluteSlot || (i > 0 && *v.Slot >= previous) || v.Transactions == nil || v.Slots == nil ||
				v.PeriodSeconds == nil || *v.PeriodSeconds == 0 || (v.NonVoteTransactions != nil && *v.NonVoteTransactions > *v.Transactions) {
				history = nil
				break
			}
			history = append(history, RPCPerformance{Slot: count(*v.Slot), Transactions: count(*v.Transactions), NonVoteTransactions: countPointer(v.NonVoteTransactions), Slots: count(*v.Slots), PeriodSeconds: *v.PeriodSeconds, ObservedAt: performance.at})
			previous = *v.Slot
		}
		if len(history) > 0 {
			s.PerformanceHistory = history
			s.Performance = &s.PerformanceHistory[0]
		}
	}
	rows := make(map[string]Validator)
	sp := provider(&s, "solstat")
	sp.CapturedAt = solstat.at
	if solstat.err != nil {
		sp.Reason = "request_failed"
	} else {
		parsed, source, err := parseSolstat(solstat.value, epoch, votes.value, solstat.at)
		if !source.IsZero() {
			sp.SourceAt, sp.SourceTimeKind = &source, "provider"
		}
		if solstat.value.EpochInfo != nil {
			sp.Epoch = countPointer(solstat.value.EpochInfo.Epoch)
		}
		if err != nil {
			sp.State, sp.Reason = "invalid", "invalid_payload"
			if errors.Is(err, errStale) {
				sp.State, sp.Reason = "stale", "source_stale"
			}
		} else {
			markFresh(sp, solstat.at, source, e, uint64(len(parsed)), "rpc_profile_epoch_checked", "provider")
			s.SolstatSummary = parseSolstatSummary(solstat.value.NetworkStats, epoch)
			for _, row := range parsed {
				rows[row.VoteAccount] = row
			}
		}
	}
	tp := provider(&s, "titan")
	tp.CapturedAt = titan.at
	if titan.err != nil {
		tp.Reason = "request_failed"
	} else {
		parsed, err := parseTitan(titan.value, epoch, votes.value, titan.at)
		if !titan.value.Overview.ObservedAt.IsZero() {
			source := titan.value.Overview.ObservedAt
			tp.SourceAt, tp.SourceTimeKind = &source, "provider"
		}
		tp.Epoch = countPointer(titan.value.Epoch)
		if err != nil {
			tp.State, tp.Reason = "invalid", "invalid_payload"
			if errors.Is(err, errStale) {
				tp.State, tp.Reason = "stale", "source_stale"
			}
		} else {
			markFresh(tp, titan.at, titan.value.Overview.ObservedAt, e, uint64(len(parsed)), "payload_genesis_epoch_checked", "provider")
			for _, row := range parsed {
				if existing, ok := rows[row.VoteAccount]; ok {
					existing.Inclusion = row.Inclusion
					rows[row.VoteAccount] = existing
				} else {
					rows[row.VoteAccount] = row
				}
			}
		}
	}
	gp := provider(&s, "geoip")
	gp.CapturedAt = now
	if nodes.err != nil {
		gp.Reason = "rpc_gossip_unavailable"
	} else if !fresh(nodes.at, now) {
		gp.State, gp.Reason = "stale", "source_stale"
	} else if db, err := countryReader(); err != nil {
		gp.State, gp.Reason = "invalid", "invalid_payload"
	} else {
		countries := make(map[string]string, len(nodes.value))
		for identity, ip := range nodes.value {
			code, err := countryForIP(db, ip)
			if err != nil {
				gp.State, gp.Reason = "invalid", "invalid_payload"
				break
			}
			countries[identity] = code
		}
		if gp.State != "invalid" {
			markFresh(gp, now, nodes.at, e, uint64(len(nodes.value)), "rpc_gossip_local_geoip", "response")
			for key, row := range rows {
				if code := countries[row.Identity]; code != "" {
					row.CountryCode, row.CountrySource = code, "dbip"
					rows[key] = row
				}
			}
		}
	}
	for _, row := range rows {
		s.Validators = append(s.Validators, row)
	}
	sort.Slice(s.Validators, func(i, j int) bool { return s.Validators[i].VoteAccount < s.Validators[j].VoteAccount })
	return c.finish(s, "")
}

func (c *collector) finish(s Snapshot, failure string) (Snapshot, error) {
	s.CapturedAt = c.now()
	if failure != "" {
		s.Validators = []Validator{}
		for i := range s.Providers {
			p := &s.Providers[i]
			if p.ID == "puffin" {
				continue
			}
			p.State, p.Reason, p.CapturedAt = "invalid", failure, s.CapturedAt
			if failure == "rpc_unavailable" {
				p.State = "unavailable"
			}
			p.RowCount = "0"
		}
	}
	if err := Validate(s); err != nil {
		return Snapshot{}, err
	}
	for _, p := range s.Providers {
		c.log.Info("cluster_provider_result", "provider", p.ID, "state", p.State, "reason", p.Reason, "rows", p.RowCount)
	}
	c.log.Info("cluster_capture_complete", "rows", len(s.Validators), "binding_failure", failure)
	return s, nil
}

func markFresh(p *ProviderStatus, captured, source time.Time, epoch Count, rows uint64, binding, timeKind string) {
	p.State, p.Reason, p.CapturedAt, p.SourceAt, p.Epoch, p.GenesisHash, p.Binding, p.SourceTimeKind, p.RowCount = "fresh", "", captured, &source, &epoch, GenesisHash, binding, timeKind, count(rows)
}

func epochValid(e epochInfo) bool {
	return e.Epoch != nil && e.AbsoluteSlot != nil && e.SlotIndex != nil && e.SlotsInEpoch != nil && *e.SlotsInEpoch > 0 && *e.SlotIndex < *e.SlotsInEpoch && *e.AbsoluteSlot >= *e.SlotIndex && *e.AbsoluteSlot-*e.SlotIndex <= math.MaxUint64-(*e.SlotsInEpoch-1)
}

func slotInEpoch(slot uint64, e epochInfo) bool {
	return epochValid(e) && slot >= *e.AbsoluteSlot-*e.SlotIndex && slot-(*e.AbsoluteSlot-*e.SlotIndex) < *e.SlotsInEpoch
}

func fresh(source, now time.Time) bool {
	return !source.IsZero() && !source.After(now.Add(5*time.Second)) && now.Sub(source) <= MaxAge
}

func (c *collector) captureLog(id, operation string, start time.Time, err error) {
	c.log.Info("cluster_provider_capture", "provider", id, "operation", operation, "duration_ms", time.Since(start).Milliseconds(), "failed", err != nil)
}

func (c *collector) request(ctx context.Context, id, method, url string, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mithril-dashboard-collector/1")
	req.Header.Set("X-Request-ID", c.runID)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	start := time.Now()
	res, err := c.http.Do(req)
	status := 0
	if res != nil {
		status = res.StatusCode
	}
	c.log.Info("cluster_provider_request", "provider", id, "status", status, "headers_duration_ms", time.Since(start).Milliseconds(), "failed", err != nil || status != http.StatusOK)
	if err != nil {
		return nil, errors.New("provider request failed")
	}
	if res.StatusCode != http.StatusOK {
		res.Body.Close()
		return nil, errors.New("provider HTTP status")
	}
	return res, nil
}

func decode(body []byte, out any) error {
	if !utf8.Valid(body) {
		return errors.New("provider JSON is not UTF-8")
	}
	d := json.NewDecoder(bytes.NewReader(body))
	if err := d.Decode(out); err != nil {
		return errors.New("invalid provider JSON")
	}
	if d.Decode(new(any)) != io.EOF {
		return errors.New("trailing provider JSON")
	}
	return nil
}

func (c *collector) getJSON(ctx context.Context, id, url string, out any) error {
	res, err := c.request(ctx, id, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	b, err := io.ReadAll(io.LimitReader(res.Body, responseLimit+1))
	if err != nil {
		return errors.New("provider body read failed")
	}
	if len(b) > responseLimit {
		return errors.New("provider body limit")
	}
	return decode(b, out)
}

func (c *collector) rpc(ctx context.Context, method string, out any, params ...any) error {
	body, err := json.Marshal(struct {
		JSONRPC string `json:"jsonrpc"`
		ID      int    `json:"id"`
		Method  string `json:"method"`
		Params  []any  `json:"params,omitempty"`
	}{"2.0", 1, method, params})
	if err != nil {
		return err
	}
	res, err := c.request(ctx, "rpc", http.MethodPost, sourceURLs["rpc"], body)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	b, err := io.ReadAll(io.LimitReader(res.Body, responseLimit+1))
	if err != nil || len(b) > responseLimit {
		return errors.New("RPC body limit or read failure")
	}
	var envelope struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      int             `json:"id"`
		Result  json.RawMessage `json:"result"`
		Error   json.RawMessage `json:"error"`
	}
	if err := decode(b, &envelope); err != nil {
		return err
	}
	if envelope.JSONRPC != "2.0" || envelope.ID != 1 || len(envelope.Error) > 0 || len(envelope.Result) == 0 || string(envelope.Result) == "null" {
		return errors.New("invalid RPC response")
	}
	return decode(envelope.Result, out)
}

func (c *collector) voteAccounts(ctx context.Context) (map[string]string, error) {
	var v struct {
		Current    []voteAccount `json:"current"`
		Delinquent []voteAccount `json:"delinquent"`
	}
	if err := c.rpc(ctx, "getVoteAccounts", &v); err != nil {
		return nil, err
	}
	if v.Current == nil || v.Delinquent == nil || len(v.Current)+len(v.Delinquent) > MaxRows {
		return nil, errors.New("invalid vote account list")
	}
	rows := make(map[string]string, len(v.Current)+len(v.Delinquent))
	for _, a := range append(v.Current, v.Delinquent...) {
		if !keyOK(a.Vote) || !keyOK(a.Identity) || rows[a.Vote] != "" {
			return nil, errors.New("invalid vote account binding")
		}
		rows[a.Vote] = a.Identity
	}
	return rows, nil
}

func (c *collector) clusterNodes(ctx context.Context) (map[string]netip.Addr, error) {
	var v []rpcNode
	if err := c.rpc(ctx, "getClusterNodes", &v); err != nil {
		return nil, err
	}
	if v == nil || len(v) > MaxRows {
		return nil, errors.New("invalid gossip list")
	}
	rows := make(map[string]netip.Addr, len(v))
	seen := make(map[string]bool, len(v))
	for _, a := range v {
		if !keyOK(a.Identity) || seen[a.Identity] {
			return nil, errors.New("invalid gossip identity")
		}
		seen[a.Identity] = true
		if a.Gossip == nil {
			continue
		}
		host, _, err := net.SplitHostPort(*a.Gossip)
		if err != nil {
			continue
		}
		ip, err := netip.ParseAddr(host)
		if err == nil && ip.IsGlobalUnicast() && !ip.IsPrivate() {
			rows[a.Identity] = ip.Unmap()
		}
	}
	return rows, nil
}

func countPointer(n *uint64) *Count {
	if n == nil {
		return nil
	}
	v := count(*n)
	return &v
}

// Optional provider fields must not invalidate the rest of the directory.
func optionalCount(raw json.RawMessage) *Count {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil
	}
	var n uint64
	if json.Unmarshal(raw, &n) != nil {
		return nil
	}
	return countPointer(&n)
}

func optionalPercent(raw json.RawMessage) *float64 {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil
	}
	var value float64
	if json.Unmarshal(raw, &value) != nil || !percentOK(&value) {
		return nil
	}
	return &value
}

// providerSOL preserves exact numeric values while accepting JSON exponent
// notation. Values outside the existing decimal precision or size stay unknown.
func providerSOL(s string) (string, bool) {
	if decimalSOL.MatchString(s) {
		return s, true
	}
	i := strings.IndexAny(s, "eE")
	if len(s) > 64 || i < 0 || strings.HasPrefix(s, "-") || !json.Valid([]byte(s)) {
		return "", false
	}
	exponent, err := strconv.Atoi(s[i+1:])
	if err != nil || exponent < -64 || exponent > 64 {
		return "", false
	}
	r, ok := new(big.Rat).SetString(s)
	if !ok || r.Sign() < 0 {
		return "", false
	}
	units := new(big.Int).Mul(r.Num(), big.NewInt(1_000_000_000_000_000_000))
	whole, remainder := new(big.Int).QuoRem(units, r.Denom(), new(big.Int))
	if remainder.Sign() != 0 {
		return "", false
	}
	digits := whole.String()
	if len(digits) <= 18 {
		digits = strings.Repeat("0", 19-len(digits)) + digits
	}
	value := digits[:len(digits)-18] + "." + digits[len(digits)-18:]
	value = strings.TrimRight(strings.TrimRight(value, "0"), ".")
	return value, decimalSOL.MatchString(value)
}

func parseSolstatSummary(raw json.RawMessage, epoch epochInfo) *SolstatSummary {
	var source struct {
		Epoch      json.RawMessage `json:"epoch"`
		Total      json.RawMessage `json:"totalValidators"`
		Active     json.RawMessage `json:"activeValidators"`
		Delinquent json.RawMessage `json:"delinquentValidators"`
		Stake      json.RawMessage `json:"totalStake"`
		Average    json.RawMessage `json:"avgSkipRate"`
		Median     json.RawMessage `json:"medianSkipRate"`
	}
	if epoch.Epoch == nil || len(raw) == 0 || json.Unmarshal(raw, &source) != nil {
		return nil
	}
	e, total, active, delinquent := optionalCount(source.Epoch), optionalCount(source.Total), optionalCount(source.Active), optionalCount(source.Delinquent)
	if e == nil || total == nil || active == nil || delinquent == nil {
		return nil
	}
	stake, ok := providerSOL(string(bytes.TrimSpace(source.Stake)))
	if !ok {
		return nil
	}
	v := &SolstatSummary{Epoch: *e, TotalValidators: *total, ActiveValidators: *active, DelinquentValidators: *delinquent,
		TotalStakeSOL: stake, AverageSkipRatePercent: optionalPercent(source.Average), MedianSkipRatePercent: optionalPercent(source.Median)}
	expected := count(*epoch.Epoch)
	if validateSolstatSummary(v, &expected) != nil {
		return nil
	}
	return v
}

func parseSolstat(v solstatResponse, e epochInfo, votes map[string]string, now time.Time) ([]Validator, time.Time, error) {
	if v.Network != "alpenglow" || v.Timestamp == nil || *v.Timestamp > math.MaxInt64 || v.EpochInfo == nil || v.EpochInfo.Epoch == nil || *v.EpochInfo.Epoch != *e.Epoch || v.EpochInfo.AbsoluteSlot == nil || !slotInEpoch(*v.EpochInfo.AbsoluteSlot, e) || v.Validators == nil || len(v.Validators) > MaxRows {
		return nil, time.Time{}, errors.New("invalid directory envelope")
	}
	source := time.UnixMilli(int64(*v.Timestamp)).UTC()
	if !fresh(source, now) {
		return nil, source, errStale
	}
	rows := make([]Validator, 0, len(v.Validators))
	seen := make(map[string]bool, len(v.Validators))
	for _, a := range v.Validators {
		if !keyOK(a.Vote) || !keyOK(a.Identity) || votes[a.Vote] != a.Identity || seen[a.Vote] || a.Stake == nil || a.Commission == nil || a.Delinquent == nil || !percentOK(a.Commission) || !percentOK(a.SkipRate) || !percentOK(a.Efficiency) {
			return nil, source, errors.New("invalid directory row")
		}
		stake, ok := providerSOL(a.Stake.String())
		if !ok {
			return nil, source, errors.New("invalid directory stake")
		}
		seen[a.Vote] = true
		if a.BlocksProduced != nil && a.LeaderSlots != nil && *a.BlocksProduced > *a.LeaderSlots {
			return nil, source, errors.New("invalid production arithmetic")
		}
		if a.Rank != nil && (*a.Rank == 0 || *a.Rank > MaxRows) {
			return nil, source, errors.New("invalid efficiency rank")
		}
		row := Validator{Identity: a.Identity, VoteAccount: a.Vote, DirectorySource: "solstat", StakeSOL: &stake, CommissionPercent: a.Commission, Delinquent: a.Delinquent, LastVote: countPointer(a.LastVote), RootSlot: optionalCount(a.RootSlot), CurrentEpochCredits: optionalCount(a.EpochCredits), VoteLagSlots: optionalCount(a.VoteLag), LeaderSlots: countPointer(a.LeaderSlots), BlocksProduced: countPointer(a.BlocksProduced), SkipRatePercent: a.SkipRate, EfficiencyPercent: a.Efficiency, EfficiencyRank: countPointer(a.Rank), Inclusion: []Inclusion{}}
		if textOK(a.Name, 128) {
			row.Name = a.Name
		}
		if textOK(a.Version, 64) {
			row.Version = a.Version
		}
		rows = append(rows, row)
	}
	return rows, source, nil
}

func (c *collector) titan(ctx context.Context) (titanResponse, error) {
	var out titanResponse
	var cfg titanCluster
	if err := c.getJSON(ctx, "titan", titanConfigURL, &cfg); err != nil {
		return out, err
	}
	if cfg.ID != "alpenglow" || cfg.Genesis != GenesisHash || !keyOK(cfg.Vote) {
		return out, errors.New("Titan config binding mismatch")
	}
	res, err := c.request(ctx, "titan", http.MethodGet, sourceURLs["titan"]+"?vote_account="+cfg.Vote, nil)
	if err != nil {
		return out, err
	}
	defer res.Body.Close()
	if !strings.HasPrefix(res.Header.Get("Content-Type"), "text/event-stream") {
		return out, errors.New("not an SSE stream")
	}
	scan := bufio.NewScanner(io.LimitReader(res.Body, responseLimit+1))
	scan.Buffer(make([]byte, 4096), responseLimit)
	event := ""
	var data strings.Builder
	total := 0
	for scan.Scan() {
		line := scan.Text()
		total += len(line) + 1
		if total > responseLimit {
			return out, errors.New("SSE body limit")
		}
		if line == "" {
			if event == "snapshot" && data.Len() > 0 {
				if err := decode([]byte(data.String()), &out); err != nil {
					return out, err
				}
				if out.Cluster.ID != cfg.ID || out.Cluster.Genesis != cfg.Genesis || out.Cluster.Vote != cfg.Vote {
					return out, errors.New("Titan stream binding mismatch")
				}
				return out, nil
			}
			event = ""
			data.Reset()
			continue
		}
		if strings.HasPrefix(line, "event:") {
			event = strings.TrimSpace(line[6:])
		} else if strings.HasPrefix(line, "data:") {
			if data.Len() > 0 {
				data.WriteByte('\n')
			}
			data.WriteString(strings.TrimPrefix(line[5:], " "))
		}
	}
	return out, errors.New("complete SSE snapshot unavailable")
}

func parseTitan(v titanResponse, e epochInfo, votes map[string]string, now time.Time) ([]Validator, error) {
	if v.Epoch == nil || *v.Epoch != *e.Epoch || v.Cluster.ID != "alpenglow" || v.Cluster.Genesis != GenesisHash || v.ObservationSource.Genesis != GenesisHash || v.Roster == nil || len(v.Roster) > MaxRows || len(v.Windows) != 3 {
		return nil, errors.New("invalid Titan envelope")
	}
	if !fresh(v.Overview.ObservedAt, now) {
		return nil, errStale
	}
	rows := make(map[string]Validator, len(v.Roster))
	for vote, a := range v.Roster {
		id := a.Identity
		if id == "" {
			id = votes[vote]
		}
		if !keyOK(vote) || !keyOK(id) || votes[vote] != id {
			return nil, errors.New("invalid Titan roster binding")
		}
		row := Validator{Identity: id, VoteAccount: vote, DirectorySource: "titan", Inclusion: []Inclusion{}}
		if textOK(a.Name, 128) {
			row.Name = a.Name
		}
		if textOK(a.Version, 64) {
			row.Version = a.Version
		}
		rows[vote] = row
	}
	for _, window := range []string{"1000", "5000", "epoch"} {
		w, ok := v.Windows[window]
		coverage := w.Coverage
		if !ok || w.ActualSlots == nil || w.Voters == nil || len(w.Voters) > MaxRows || coverage.FirstSlot == nil || coverage.LastSlot == nil || *coverage.FirstSlot > *coverage.LastSlot || !slotInEpoch(*coverage.FirstSlot, e) || !slotInEpoch(*coverage.LastSlot, e) || coverage.FirstObservedAt.IsZero() || coverage.FirstObservedAt.After(coverage.LastObservedAt) || !fresh(coverage.LastObservedAt, now) || *w.ActualSlots > *e.SlotsInEpoch || *w.ActualSlots > *coverage.LastSlot-*coverage.FirstSlot+1 {
			return nil, errors.New("invalid Titan window")
		}
		if (window == "1000" && *w.ActualSlots > 1000) || (window == "5000" && *w.ActualSlots > 5000) {
			return nil, errors.New("oversized Titan window")
		}
		type ranked struct {
			vote         string
			op, included uint64
		}
		var rankedRows []ranked
		measures := make(map[string]Inclusion, len(w.Voters))
		for vote, values := range w.Voters {
			if _, ok := rows[vote]; !ok || len(values) != 4 {
				return nil, errors.New("unknown Titan voter")
			}
			var op, included, missed uint64
			if string(values[0]) == "null" || string(values[1]) == "null" || string(values[2]) == "null" || decode(values[0], &op) != nil || decode(values[1], &included) != nil || decode(values[2], &missed) != nil || included > op || missed != op-included || op > *w.ActualSlots {
				return nil, errors.New("invalid Titan arithmetic")
			}
			var percent *float64
			if decode(values[3], &percent) != nil || !percentOK(percent) {
				return nil, errors.New("invalid Titan percentage")
			}
			if op == 0 {
				percent = nil
			} else {
				if percent == nil || math.Abs(*percent-float64(included)/float64(op)*100) > 0.0001 {
					return nil, errors.New("Titan percentage mismatch")
				}
				rankedRows = append(rankedRows, ranked{vote, op, included})
			}
			measures[vote] = Inclusion{Window: window, Epoch: count(*e.Epoch), Opportunities: count(op), Included: count(included), Missed: count(missed), Percent: percent, Observations: count(*w.ActualSlots), RankPolicy: RankPolicy, FirstSlot: count(*coverage.FirstSlot), LastSlot: count(*coverage.LastSlot), FirstObservedAt: coverage.FirstObservedAt, LastObservedAt: coverage.LastObservedAt}
		}
		sort.Slice(rankedRows, func(i, j int) bool {
			a, b := rankedRows[i], rankedRows[j]
			cmp := compareRatio(a.included, a.op, b.included, b.op)
			if cmp != 0 {
				return cmp > 0
			}
			if a.op != b.op {
				return a.op > b.op
			}
			return a.vote < b.vote
		})
		var rank Count
		for i, a := range rankedRows {
			if i == 0 || a.op != rankedRows[i-1].op || a.included != rankedRows[i-1].included {
				rank = count(uint64(i + 1))
			}
			m := measures[a.vote]
			r := rank
			m.Rank = &r
			measures[a.vote] = m
		}
		for vote, m := range measures {
			m.CohortSize = count(uint64(len(rankedRows)))
			row := rows[vote]
			row.Inclusion = append(row.Inclusion, m)
			rows[vote] = row
		}
	}
	out := make([]Validator, 0, len(rows))
	for _, row := range rows {
		out = append(out, row)
	}
	return out, nil
}

func compareRatio(a, b, c, d uint64) int {
	ah, al := bits.Mul64(a, d)
	ch, cl := bits.Mul64(c, b)
	if ah > ch || (ah == ch && al > cl) {
		return 1
	}
	if ah < ch || (ah == ch && al < cl) {
		return -1
	}
	return 0
}

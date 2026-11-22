package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/netip"
	"sort"
	"time"
)

// mainnetVote is the subset of getVoteAccounts needed by the directory.
type mainnetVote struct {
	Identity   string     `json:"nodePubkey"`
	Vote       string     `json:"votePubkey"`
	Stake      *uint64    `json:"activatedStake"`
	Commission *uint64    `json:"commission"`
	LastVote   *uint64    `json:"lastVote"`
	RootSlot   *uint64    `json:"rootSlot"`
	Credits    [][]uint64 `json:"epochCredits"`
}

type mainnetNode struct {
	Identity string          `json:"pubkey"`
	Version  *string         `json:"version"`
	Gossip   *string         `json:"gossip"`
	RPC      json.RawMessage `json:"rpc,omitempty"`
}

func mainnetVersions(nodes []mainnetNode) (map[string]string, error) {
	if nodes == nil || len(nodes) > MaxRPCNodes {
		return nil, errors.New("invalid mainnet contact list size")
	}
	versions := make(map[string]string, len(nodes))
	for _, node := range nodes {
		if !keyOK(node.Identity) {
			return nil, errors.New("invalid gossip identity")
		}
		if _, exists := versions[node.Identity]; exists {
			return nil, errors.New("duplicate gossip identity")
		}
		version := ""
		if node.Version != nil {
			if !textOK(*node.Version, 64) {
				return nil, errors.New("invalid gossip version")
			}
			version = *node.Version
		}
		versions[node.Identity] = version
	}
	return versions, nil
}

func mainnetRows(current, delinquent []mainnetVote, epoch uint64) ([]Validator, error) {
	if current == nil || delinquent == nil || len(current)+len(delinquent) > MaxRows {
		return nil, errors.New("invalid mainnet vote account list size")
	}
	rows := make([]Validator, 0, len(current)+len(delinquent))
	seen := make(map[string]bool, len(current)+len(delinquent))
	appendRows := func(votes []mainnetVote, isDelinquent bool) error {
		for _, vote := range votes {
			if !keyOK(vote.Identity) || !keyOK(vote.Vote) || seen[vote.Vote] || (vote.Commission != nil && *vote.Commission > 100) {
				return errors.New("invalid vote account")
			}
			seen[vote.Vote] = true
			row := Validator{Identity: vote.Identity, VoteAccount: vote.Vote, DirectorySource: "rpc", Delinquent: &isDelinquent, LastVote: countPointer(vote.LastVote), RootSlot: countPointer(vote.RootSlot), Inclusion: []Inclusion{}}
			if vote.Stake != nil {
				// Integer formatting keeps lamport precision without float rounding.
				sol := fmt.Sprintf("%d.%09d", *vote.Stake/1_000_000_000, *vote.Stake%1_000_000_000)
				row.StakeSOL = &sol
			}
			if vote.Commission != nil {
				percent := float64(*vote.Commission)
				row.CommissionPercent = &percent
			}
			for _, credits := range vote.Credits {
				if len(credits) != 3 {
					return errors.New("invalid epoch credits")
				}
				if credits[0] == epoch {
					if credits[1] < credits[2] || row.CurrentEpochCredits != nil {
						return errors.New("invalid epoch credits")
					}
					value := count(credits[1] - credits[2])
					row.CurrentEpochCredits = &value
				}
			}
			rows = append(rows, row)
		}
		return nil
	}
	if err := appendRows(current, false); err != nil {
		return nil, err
	}
	if err := appendRows(delinquent, true); err != nil {
		return nil, err
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].VoteAccount < rows[j].VoteAccount })
	return rows, nil
}

func (c *collector) collectMainnet(ctx context.Context) (Snapshot, error) {
	s := Snapshot{SchemaVersion: 1, Scope: "independent_cluster", Profile: "mainnet", Network: "mainnet-beta", GenesisHash: MainnetGenesisHash, CapturedAt: c.now(), FastState: "unavailable", Validators: []Validator{}}
	for _, id := range []string{"rpc", "solstat", "titan", "geoip", "puffin"} {
		sourceURL := mainnetSourceURLs[id]
		if id == "rpc" && c.rpcURL == mainnetPublicRPC {
			sourceURL = mainnetPublicRPC
		}
		s.Providers = append(s.Providers, ProviderStatus{ID: id, State: "unavailable", Reason: "not_collected", SourceURL: sourceURL, CapturedAt: s.CapturedAt, RowCount: "0"})
	}
	var genesis string
	if err := c.rpc(ctx, "getGenesisHash", &genesis); err != nil {
		return c.finish(s, "rpc_unavailable")
	}
	if genesis != MainnetGenesisHash {
		return c.finish(s, "rpc_genesis_mismatch")
	}
	var epoch epochInfo
	if err := c.rpc(ctx, "getEpochInfo", &epoch); err != nil || !epochValid(epoch) {
		return c.finish(s, "rpc_epoch_invalid")
	}
	e := count(*epoch.Epoch)
	s.Epoch = &e
	var votes struct {
		Current    []mainnetVote `json:"current"`
		Delinquent []mainnetVote `json:"delinquent"`
	}
	if err := c.rpc(ctx, "getVoteAccounts", &votes); err != nil {
		return c.finish(s, "rpc_vote_accounts_invalid")
	}
	votesAt := c.now()
	rows, err := mainnetRows(votes.Current, votes.Delinquent, *epoch.Epoch)
	if err != nil {
		return c.finish(s, "rpc_vote_accounts_invalid")
	}
	// Optional observations share a short budget and leave time for both
	// mandatory rechecks even if earlier required requests were slow.
	until := time.Now().Add(2 * time.Second)
	if deadline, ok := ctx.Deadline(); ok {
		if reserve := deadline.Add(-2 * collectorHTTPTimeout); reserve.Before(until) {
			until = reserve
		}
	}
	optional, cancelOptional := context.WithDeadline(ctx, until)
	defer cancelOptional()
	var nodes []mainnetNode
	var nodesAt time.Time
	var versions map[string]string
	contactsDone := make(chan struct{})
	go func() {
		defer close(contactsDone)
		start := time.Now()
		err := optional.Err()
		if err == nil {
			err = c.rpc(optional, "getClusterNodes", &nodes)
		}
		if err == nil {
			versions, err = mainnetVersions(nodes)
			if err == nil {
				nodesAt = c.now()
			}
		}
		c.captureLog("rpc", "gossip_contacts", start, err)
	}()
	var samples []rpcPerformanceSample
	var samplesAt time.Time
	if c.performanceSamples && optional.Err() == nil {
		limit := 1
		if c.performanceHistory {
			limit = MaxPerformanceHistory
		}
		start := time.Now()
		err := c.rpc(optional, "getRecentPerformanceSamples", &samples, limit)
		c.captureLog("rpc", "performance_sample", start, err)
		if err == nil {
			samplesAt = c.now()
		}
	}
	<-contactsDone
	cancelOptional()
	var after epochInfo
	if err := c.rpc(ctx, "getGenesisHash", &genesis); err != nil || genesis != MainnetGenesisHash {
		return c.finish(s, "rpc_genesis_recheck_failed")
	}
	if err := c.rpc(ctx, "getEpochInfo", &after); err != nil || !epochValid(after) || *after.Epoch != *epoch.Epoch {
		return c.finish(s, "rpc_epoch_changed")
	}
	if *after.SlotsInEpoch != *epoch.SlotsInEpoch || *after.AbsoluteSlot-*after.SlotIndex != *epoch.AbsoluteSlot-*epoch.SlotIndex || *after.AbsoluteSlot < *epoch.AbsoluteSlot {
		return c.finish(s, "rpc_epoch_changed")
	}
	now := c.now()
	if !fresh(votesAt, now) {
		return c.finish(s, "rpc_vote_accounts_invalid")
	}
	geo := provider(&s, "geoip")
	if fresh(nodesAt, now) && !nodesAt.After(now) {
		for i := range rows {
			rows[i].Version = versions[rows[i].Identity]
		}
		// Reuse the same response; only a detached count/time survives collection.
		contacts := make([]rpcNode, len(nodes))
		for i, node := range nodes {
			contacts[i] = rpcNode{Identity: node.Identity, RPC: node.RPC}
		}
		if advertised := advertisedRPCNodes(contacts); advertised != nil {
			s.RPCNodes = &RPCNodes{Advertised: *advertised, ObservedAt: nodesAt}
		}
		if db, err := countryReader(); err == nil {
			countries := make(map[string]string)
			valid := true
			for _, node := range nodes {
				if node.Gossip == nil {
					continue
				}
				host, _, err := net.SplitHostPort(*node.Gossip)
				if err != nil {
					continue
				}
				ip, err := netip.ParseAddr(host)
				if err != nil || !ip.IsGlobalUnicast() || ip.IsPrivate() {
					continue
				}
				country, err := countryForIP(db, ip.Unmap())
				if err != nil {
					valid = false
					break
				}
				countries[node.Identity] = country
			}
			if valid {
				var matched uint64
				for i := range rows {
					if country := countries[rows[i].Identity]; country != "" {
						rows[i].CountryCode, rows[i].CountrySource = country, "dbip"
						matched++
					}
				}
				markFresh(geo, MainnetGenesisHash, now, nodesAt, e, matched, "rpc_gossip_local_geoip", "response")
			}
		}
	}
	markFresh(provider(&s, "rpc"), MainnetGenesisHash, now, votesAt, e, uint64(len(rows)), "rpc_genesis_before_after", "response")
	s.Reference = &RPCReference{Slot: count(*after.AbsoluteSlot), EpochStart: count(*after.AbsoluteSlot - *after.SlotIndex), SlotsInEpoch: count(*after.SlotsInEpoch), ObservedAt: now}
	if fresh(samplesAt, now) && !samplesAt.After(now) && len(samples) > 0 && len(samples) <= MaxPerformanceHistory {
		var history []RPCPerformance
		var previous uint64 = math.MaxUint64
		for _, sample := range samples {
			if sample.Slot == nil || *sample.Slot > *after.AbsoluteSlot || *sample.Slot >= previous || sample.Transactions == nil || sample.Slots == nil || sample.PeriodSeconds == nil || *sample.PeriodSeconds == 0 || (sample.NonVoteTransactions != nil && *sample.NonVoteTransactions > *sample.Transactions) {
				history = nil
				break
			}
			history = append(history, RPCPerformance{Slot: count(*sample.Slot), Transactions: count(*sample.Transactions), NonVoteTransactions: countPointer(sample.NonVoteTransactions), Slots: count(*sample.Slots), PeriodSeconds: *sample.PeriodSeconds, ObservedAt: samplesAt})
			previous = *sample.Slot
		}
		if len(history) > 0 {
			s.Performance = &history[0]
			if c.performanceHistory {
				s.PerformanceHistory = history
			}
		}
	}
	s.Validators = rows
	return c.finish(s, "")
}

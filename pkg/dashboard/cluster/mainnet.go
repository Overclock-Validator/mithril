package cluster

import (
	"context"
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
	Identity string  `json:"pubkey"`
	Version  *string `json:"version"`
	Gossip   *string `json:"gossip"`
}

func mainnetRows(current, delinquent []mainnetVote, nodes []mainnetNode, epoch uint64) ([]Validator, error) {
	if current == nil || delinquent == nil || len(current)+len(delinquent) > MaxRows || len(nodes) > 8192 {
		return nil, errors.New("invalid mainnet directory size")
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
	rows := make([]Validator, 0, len(current)+len(delinquent))
	seen := make(map[string]bool, len(current)+len(delinquent))
	appendRows := func(votes []mainnetVote, isDelinquent bool) error {
		for _, vote := range votes {
			if !keyOK(vote.Identity) || !keyOK(vote.Vote) || seen[vote.Vote] || (vote.Commission != nil && *vote.Commission > 100) {
				return errors.New("invalid vote account")
			}
			seen[vote.Vote] = true
			row := Validator{Identity: vote.Identity, VoteAccount: vote.Vote, DirectorySource: "rpc", Version: versions[vote.Identity], Delinquent: &isDelinquent, LastVote: countPointer(vote.LastVote), RootSlot: countPointer(vote.RootSlot), Inclusion: []Inclusion{}}
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
	var nodes []mainnetNode
	var nodesAt time.Time
	// Versions are optional. A transient gossip failure must not invent values.
	if err := c.rpc(ctx, "getClusterNodes", &nodes); err != nil {
		nodes = nil
	} else if nodes != nil {
		nodesAt = c.now()
	}
	rows, err := mainnetRows(votes.Current, votes.Delinquent, nodes, *epoch.Epoch)
	if err != nil {
		return c.finish(s, "rpc_vote_accounts_invalid")
	}
	var samples []rpcPerformanceSample
	var samplesAt time.Time
	if c.performanceSamples {
		limit := 1
		if c.performanceHistory {
			limit = MaxPerformanceHistory
		}
		if c.rpc(ctx, "getRecentPerformanceSamples", &samples, limit) == nil {
			samplesAt = c.now()
		}
	}
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
	if fresh(nodesAt, now) {
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
				geo.State, geo.Reason, geo.CapturedAt, geo.SourceAt, geo.Epoch, geo.GenesisHash, geo.Binding, geo.SourceTimeKind, geo.RowCount = "fresh", "", now, &nodesAt, &e, MainnetGenesisHash, "rpc_gossip_local_geoip", "response", count(matched)
			}
		}
	}
	p := provider(&s, "rpc")
	p.State, p.Reason, p.CapturedAt, p.SourceAt, p.Epoch, p.GenesisHash, p.Binding, p.SourceTimeKind, p.RowCount = "fresh", "", now, &votesAt, &e, MainnetGenesisHash, "rpc_genesis_before_after", "response", count(uint64(len(rows)))
	if *after.SlotsInEpoch == *epoch.SlotsInEpoch && *after.AbsoluteSlot-*after.SlotIndex == *epoch.AbsoluteSlot-*epoch.SlotIndex && *after.AbsoluteSlot >= *epoch.AbsoluteSlot {
		s.Reference = &RPCReference{Slot: count(*after.AbsoluteSlot), EpochStart: count(*after.AbsoluteSlot - *after.SlotIndex), SlotsInEpoch: count(*after.SlotsInEpoch), ObservedAt: now}
	}
	if s.Reference != nil && fresh(samplesAt, now) && len(samples) > 0 && len(samples) <= MaxPerformanceHistory {
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

package dashboard

import (
	"strconv"
	"strings"
	"time"
)

// PublicPeers is an opt-in sample, not a complete directory or health verdict.
// It is bounded to eight recent adverts, regardless of cluster size.
type PublicPeers struct {
	State     string       `json:"state"`
	SampledAt *time.Time   `json:"sampled_at"`
	StartedAt *time.Time   `json:"started_at"`
	Contacts  []PublicPeer `json:"contacts"`
}

type PublicPeer struct {
	Identity     string    `json:"identity"`
	Version      string    `json:"version"`
	Client       uint16    `json:"client"`
	ShredVersion uint16    `json:"shred_version"`
	ReceivedAt   time.Time `json:"received_at"`
	Repair       bool      `json:"repair"`
	TVU          bool      `json:"tvu"`
	Alpenglow    bool      `json:"alpenglow"`
}

func publicPeers(s *Snapshot, state string, now time.Time) *PublicPeers {
	p := &PublicPeers{State: "unavailable"}
	if s == nil || state == "unavailable" || s.BlockSource == nil || !s.BlockSource.LiveConnected || s.BlockSource.Gossip == nil {
		return p
	}
	g := s.BlockSource.Gossip
	if !validPublicTime(g.SampledAt, now) || !validPublicTime(g.StartedAt, now) || g.StartedAt.Before(s.StartedAt) || g.StartedAt.After(g.SampledAt) ||
		g.SampledAt.After(s.SampledAt.Add(time.Second)) || !validPublicTime(s.BlockSource.SampledAt, now) || len(g.RecentPeers) > 8 {
		return p
	}
	contacts := make([]PublicPeer, 0, len(g.RecentPeers))
	seen := make(map[string]bool, len(g.RecentPeers))
	for _, c := range g.RecentPeers {
		if !validPeerIdentity(c.Identity) || !validPeerVersion(c.Version) || seen[c.Identity] ||
			c.ReceivedAt.Before(g.StartedAt) || c.ReceivedAt.After(g.SampledAt) || g.SampledAt.Sub(c.ReceivedAt) > 15*time.Minute {
			return p
		}
		seen[c.Identity] = true
		contacts = append(contacts, PublicPeer{Identity: c.Identity, Version: c.Version, Client: c.Client, ShredVersion: c.ShredVersion,
			ReceivedAt: c.ReceivedAt, Repair: c.Repair, TVU: c.TVU, Alpenglow: c.Alpenglow})
	}
	at, start := g.SampledAt, g.StartedAt
	p.State, p.SampledAt, p.StartedAt, p.Contacts = publicObservation(at, state, now).State, &at, &start, contacts
	return p
}

func validPeerIdentity(s string) bool {
	return len(s) >= 32 && len(s) <= 44 && strings.Trim(s, "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz") == ""
}

func validPeerVersion(s string) bool {
	parts := strings.Split(s, ".")
	if len(parts) != 3 {
		return false
	}
	for _, part := range parts {
		n, err := strconv.ParseUint(part, 10, 16)
		if err != nil || strconv.FormatUint(n, 10) != part {
			return false
		}
	}
	return true
}

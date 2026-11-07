package dashboard

import (
	"bytes"
	"math"
	"testing"
	"time"
)

func TestPublicElapsedDuration(t *testing.T) {
	now := time.Now().UTC()
	s := publicTestSnapshot(now)
	for _, elapsed := range []*uint64{nil, new(uint64), func() *uint64 { v := uint64(math.MaxInt64 - 1); return &v }()} {
		s.ElapsedNanoseconds = elapsed
		if err := s.validate(); err != nil {
			t.Fatal(err)
		}
		p, _ := publicValue(t, &s, "fresh", now)
		if p.Node == nil || (p.Node.ElapsedNanoseconds == nil) != (elapsed == nil) || elapsed != nil && *p.Node.ElapsedNanoseconds != *elapsed {
			t.Fatal("sampled duration lost absence, zero or width")
		}
	}
	invalid := uint64(math.MaxInt64)
	s.ElapsedNanoseconds = &invalid
	if s.validate() == nil {
		t.Fatal("saturated elapsed duration accepted")
	}
}

func TestPublicJoinedLiveMilestone(t *testing.T) {
	now := time.Now().UTC()
	s := publicTestSnapshot(now)
	for _, joined := range []*bool{nil, new(bool), func() *bool { v := true; return &v }()} {
		s.Consensus.Voting.JoinedLive = joined
		p, body := publicValue(t, &s, "fresh", now)
		if p.Voting == nil || (p.Voting.JoinedLive == nil) != (joined == nil) ||
			joined != nil && *p.Voting.JoinedLive != *joined || bytes.Contains(body, []byte(`"joined_live"`)) != (joined != nil) {
			t.Fatalf("legacy/known joined-live value changed: %+v", p.Voting)
		}
	}
	p, _ := publicValue(t, &s, "stale", now.Add(StaleAfter+time.Second))
	if p.Voting == nil || p.Voting.State != "stale" || p.Voting.JoinedLive == nil || !*p.Voting.JoinedLive {
		t.Fatal("historical voter milestone was redated or discarded")
	}
	s.Mode = "verifying"
	if p, _ := publicValue(t, &s, "fresh", now); p.Voting != nil {
		t.Fatal("full node received a voter milestone")
	}
	s.Mode, s.Consensus.Voting.Identity = "validator", "different-identity"
	if p, _ := publicValue(t, &s, "fresh", now); p.Voting != nil {
		t.Fatal("old identity milestone labelled the current node")
	}
}

func TestPublicLastNetworkLandedTime(t *testing.T) {
	now := time.Now().UTC()
	s := publicTestSnapshot(now)
	v := s.Consensus.Voting
	slot, observed := uint64(42), now.Add(-time.Second)
	v.LastNetworkLandedSlot, v.LastNetworkLandedAt, v.NetworkLanded = &slot, &observed, 1
	p, body := publicValue(t, &s, "fresh", now)
	if p.Voting == nil || p.Voting.LastNetworkAt == nil || !p.Voting.LastNetworkAt.Equal(observed) || !bytes.Contains(body, []byte(`"last_network_landed_at"`)) {
		t.Fatal("original certificate observation time was not projected")
	}
	p, _ = publicValue(t, &s, "stale", now.Add(StaleAfter+time.Second))
	if p.Voting == nil || p.Voting.State != "stale" || p.Voting.LastNetworkAt == nil || !p.Voting.LastNetworkAt.Equal(observed) {
		t.Fatal("stale observation was lost or redated")
	}
	for name, mutate := range map[string]func(*Snapshot){
		"missing time": func(s *Snapshot) { s.Consensus.Voting.LastNetworkLandedAt = nil },
		"missing slot": func(s *Snapshot) { s.Consensus.Voting.LastNetworkLandedSlot = nil },
		"zero matches": func(s *Snapshot) { s.Consensus.Voting.NetworkLanded = 0 },
		"before run": func(s *Snapshot) {
			at := s.StartedAt.Add(-time.Nanosecond)
			s.Consensus.Voting.LastNetworkLandedAt = &at
		},
		"future collector": func(s *Snapshot) {
			at := now.Add(time.Second + time.Nanosecond)
			s.Consensus.Voting.LastNetworkLandedAt = &at
		},
		"future voter sample": func(s *Snapshot) { s.Consensus.Voting.SampledAt = now.Add(-3 * time.Second) },
	} {
		t.Run(name, func(t *testing.T) {
			s := publicTestSnapshot(now)
			s.Consensus.Voting.LastNetworkLandedSlot, s.Consensus.Voting.LastNetworkLandedAt, s.Consensus.Voting.NetworkLanded = &slot, &observed, 1
			mutate(&s)
			p, body := publicValue(t, &s, "fresh", now)
			if p.Voting == nil || p.Voting.LastNetworkAt != nil || bytes.Contains(body, []byte(`"last_network_landed_at"`)) {
				t.Fatal("invalid or missing observation time was exposed")
			}
		})
	}
	s.Mode = "verifying"
	if p, _ := publicValue(t, &s, "fresh", now); p.Voting != nil {
		t.Fatal("verifying node exposed voter activity")
	}
}

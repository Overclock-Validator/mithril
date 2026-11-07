package node

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/gagliardetto/solana-go"
	"github.com/spf13/viper"
)

func localPeerNodeFixture() (dashboard.NodeNetworkBinding, gossip.PeerDatasetSnapshot, time.Time) {
	at := time.Now()
	genesis := (solana.Hash{9}).String()
	binding := dashboard.NodeNetworkBinding{RunID: "local-peer-node", NodeStartedAt: at.Add(-time.Hour).UTC(), GenesisHash: &genesis}
	s := gossip.PeerDatasetSnapshot{Generation: 3, StartedAt: at.Add(-time.Minute), PublicationSequence: math.MaxUint64, SampledAt: at,
		AcceptedAdverts: 1<<53 + 1, RetainedAdverts: 1, Rows: []gossip.PeerDatasetRow{{
			PeerObservation: gossip.PeerObservation{Pubkey: gossip.Pubkey{1}, ReceivedAt: at.Add(-time.Second).UTC(),
				Version:      gossip.Version{Major: 4, Minor: 0x8002, Patch: 99, Commit: math.MaxUint32, FeatureSet: math.MaxUint32, Client: math.MaxUint16},
				ShredVersion: math.MaxUint16, Repair: true, TVU: true}, Outset: math.MaxUint64, Wallclock: math.MaxUint64}}}
	return binding, s, at
}

func TestDashboardLocalPeersMappingAndLease(t *testing.T) {
	binding, source, at := localPeerNodeFixture()
	clientStarted := at.Add(-2 * time.Minute).UTC()
	capture, err := localPeerMeasurement(binding, 2, clientStarted, source, at)
	if err != nil {
		t.Fatal(err)
	}
	var detail dashboard.LocalPeerDetail
	if err := json.Unmarshal([]byte(capture.Data.Body()), &detail); err != nil {
		t.Fatal(err)
	}
	row := detail.Rows[0]
	if detail.ClientGeneration != 2 || detail.RunGeneration != source.Generation || detail.PublicationSequence != source.PublicationSequence || !detail.ClientStartedAt.Equal(clientStarted) ||
		!detail.RunStartedAt.Equal(source.StartedAt) || !detail.SampledAt.Equal(source.SampledAt) || !detail.CapturedAt.Equal(source.SampledAt) ||
		!row.ObservedAt.Equal(source.Rows[0].ReceivedAt) || row.Identity != (solana.PublicKey{1}).String() ||
		row.Outset != math.MaxUint64 || row.Wallclock != math.MaxUint64 || detail.AcceptedAdverts != 1<<53+1 ||
		row.Version.Major != 4 || row.Version.Minor != 2 || row.Version.Patch != 0 || row.Version.Commit != math.MaxUint32 ||
		row.Version.FeatureSet != math.MaxUint32 || row.Version.Client != math.MaxUint16 || row.ShredVersion != math.MaxUint16 ||
		!row.Repair || !row.TVU || row.Alpenglow {
		t.Fatal("projection changed source metadata, precision or base-version semantics")
	}
	wantUntil := source.SampledAt.Add(dashboard.NetworkDetailFreshFor)
	if capture.FreshUntil != wantUntil || capture.FreshUntil == capture.FreshUntil.Round(0) {
		t.Fatal("original monotonic lease was reconstructed or refreshed")
	}
	repeated, err := localPeerMeasurement(binding, 2, clientStarted, source, at.Add(time.Second))
	if err != nil || repeated.FreshUntil != wantUntil || repeated.Data.Body() != capture.Data.Body() || repeated.Data.Digest() != capture.Data.Digest() {
		t.Fatal("repeated cached publication changed its bytes or lease")
	}
	body := capture.Data.Body()
	source.Rows[0].Pubkey[0] = 99
	*binding.GenesisHash = (solana.Hash{8}).String()
	if capture.Data.Body() != body || *capture.Data.Binding().GenesisHash != (solana.Hash{9}).String() {
		t.Fatal("prepared result retained caller-owned headers or rows")
	}
}

func TestDashboardLocalPeersUnknownAndBounds(t *testing.T) {
	for name, mutate := range map[string]func(*dashboard.NodeNetworkBinding, *gossip.PeerDatasetSnapshot, *time.Time){
		"missing sample": func(_ *dashboard.NodeNetworkBinding, s *gossip.PeerDatasetSnapshot, _ *time.Time) {
			s.SampledAt = time.Time{}
		},
		"utc sample": func(_ *dashboard.NodeNetworkBinding, s *gossip.PeerDatasetSnapshot, _ *time.Time) {
			s.SampledAt = s.SampledAt.UTC()
		},
		"utc run": func(_ *dashboard.NodeNetworkBinding, s *gossip.PeerDatasetSnapshot, _ *time.Time) {
			s.StartedAt = s.StartedAt.UTC()
		},
		"future": func(_ *dashboard.NodeNetworkBinding, s *gossip.PeerDatasetSnapshot, now *time.Time) {
			s.SampledAt = now.Add(time.Second)
		},
		"expired": func(_ *dashboard.NodeNetworkBinding, _ *gossip.PeerDatasetSnapshot, now *time.Time) {
			*now = now.Add(dashboard.NetworkDetailFreshFor)
		},
		"unknown run": func(_ *dashboard.NodeNetworkBinding, s *gossip.PeerDatasetSnapshot, _ *time.Time) { s.Generation = 0 },
		"unknown publication": func(_ *dashboard.NodeNetworkBinding, s *gossip.PeerDatasetSnapshot, _ *time.Time) {
			s.PublicationSequence = 0
		},
		"unknown genesis": func(b *dashboard.NodeNetworkBinding, _ *gossip.PeerDatasetSnapshot, _ *time.Time) {
			b.GenesisHash = nil
		},
		"zero genesis": func(b *dashboard.NodeNetworkBinding, _ *gossip.PeerDatasetSnapshot, _ *time.Time) {
			*b.GenesisHash = (solana.Hash{}).String()
		},
		"oversize": func(_ *dashboard.NodeNetworkBinding, s *gossip.PeerDatasetSnapshot, _ *time.Time) {
			s.Rows = make([]gossip.PeerDatasetRow, gossip.PeerDatasetCapacity+1)
		},
	} {
		t.Run(name, func(t *testing.T) {
			binding, source, at := localPeerNodeFixture()
			clientStarted := at.Add(-2 * time.Minute)
			mutate(&binding, &source, &at)
			capture, err := localPeerMeasurement(binding, 2, clientStarted, source, at)
			if err == nil || capture.Data != nil || !capture.FreshUntil.IsZero() {
				t.Fatal("invalid or expired source became a leased dataset")
			}
		})
	}
	binding, source, at := localPeerNodeFixture()
	source.Rows = nil
	source.AcceptedAdverts, source.RetainedAdverts = 0, 0
	capture, err := localPeerMeasurement(binding, 2, at.Add(-2*time.Minute), source, at)
	if err != nil {
		t.Fatal(err)
	}
	var empty dashboard.LocalPeerDetail
	if json.Unmarshal([]byte(capture.Data.Body()), &empty) != nil || empty.Rows == nil || len(empty.Rows) != 0 || empty.Limited {
		t.Fatal("known empty dataset became unknown or null rows")
	}
	source.CountersSaturated, source.Limited, source.AcceptedAdverts = true, true, math.MaxUint64
	if _, err := localPeerMeasurement(binding, math.MaxUint64, at.Add(-2*time.Minute), source, at); err != nil {
		t.Fatal("source saturation or last nonwrapping client generation was discarded", err)
	}
}

func TestDashboardLocalPeersOwnerFences(t *testing.T) {
	binding, source, at := localPeerNodeFixture()
	client := &gossip.Client{}
	d := &nodeDashboard{networkDetails: true, localPeers: true, gossipClient: client, gossipGeneration: 2,
		state: dashboard.Snapshot{RunID: binding.RunID, StartedAt: binding.NodeStartedAt, GenesisHash: binding.GenesisHash}}
	if !d.localPeerBindingCurrent(client, 2, binding) || !localPeerRunStable(source, source) {
		t.Fatal("unchanged owner was rejected")
	}
	for _, mutate := range []func(*nodeDashboard){
		func(d *nodeDashboard) { d.localPeers = false },
		func(d *nodeDashboard) { d.networkDetails = false },
		func(d *nodeDashboard) { d.networkRetired = true },
		func(d *nodeDashboard) { d.state.Phase = "stopping" },
		func(d *nodeDashboard) { d.gossipClient = &gossip.Client{} },
		func(d *nodeDashboard) { d.gossipGeneration++ },
		func(d *nodeDashboard) { d.state.RunID = "replacement" },
		func(d *nodeDashboard) { d.state.StartedAt = at },
		func(d *nodeDashboard) { value := (solana.Hash{8}).String(); d.state.GenesisHash = &value },
	} {
		candidate := &nodeDashboard{networkDetails: true, localPeers: true, gossipClient: client, gossipGeneration: 2, state: d.state}
		mutate(candidate)
		if candidate.localPeerBindingCurrent(client, 2, binding) {
			t.Fatal("replaced or retired node context accepted an earlier copy")
		}
	}
	next := source
	next.SampledAt = at.Add(time.Second)
	if !localPeerRunStable(source, next) {
		t.Fatal("a new publication was confused with a new Run")
	}
	next.Generation++
	if localPeerRunStable(source, next) {
		t.Fatal("new socket Run accepted an earlier copy")
	}
	next = source
	next.StartedAt = at
	if localPeerRunStable(source, next) {
		t.Fatal("changed Run start accepted an earlier copy")
	}
	var absent *nodeDashboard
	for _, owner := range []*nodeDashboard{absent, {}, d} {
		capture, err := owner.sampleLocalPeers(at)
		if err == nil || capture.Data != nil || !capture.FreshUntil.IsZero() {
			t.Fatal("nil, disabled or unpublished client fabricated a dataset")
		}
	}
}

func TestDashboardLocalPeersConfiguration(t *testing.T) {
	for _, tc := range []struct {
		enabled, network bool
		socket, reason   string
	}{
		{},
		{enabled: true, network: true, socket: "/private/node.sock"},
		{enabled: true, network: true, reason: "dashboard.local_peers requires dashboard.socket"},
		{enabled: true, network: true, socket: " \t", reason: "dashboard.local_peers requires dashboard.socket"},
		{enabled: true, socket: "/private/node.sock", reason: "dashboard.local_peers requires dashboard.network_details"},
	} {
		err := validateDashboardLocalPeers(tc.enabled, tc.socket, tc.network)
		if tc.reason == "" && err != nil || tc.reason != "" && (err == nil || err.Error() != tc.reason) {
			t.Fatalf("local-peer prerequisites: got %v, want %q", err, tc.reason)
		}
	}
	flag := Run.Flags().Lookup("dashboard-local-peers")
	if flag == nil || flag.DefValue != "false" {
		t.Fatal("local peers is not explicitly default off")
	}
	oldValue, oldChanged := flag.Value.String(), flag.Changed
	t.Cleanup(func() {
		if err := flag.Value.Set(oldValue); err != nil {
			t.Error(err)
		}
		flag.Changed = oldChanged
	})
	if err := flag.Value.Set("false"); err != nil {
		t.Fatal(err)
	}
	flag.Changed = false
	config := viper.New()
	resolve := func() bool {
		return resolveBoolOption(flag, config.IsSet("dashboard.local_peers"), config.GetBool("dashboard.local_peers"))
	}
	config.Set("dashboard.network_details", true)
	if resolve() {
		t.Fatal("network details implicitly enabled the dataset")
	}
	config.Set("dashboard.local_peers", true)
	if !resolve() {
		t.Fatal("explicit local-peer configuration was ignored")
	}
	if err := Run.Flags().Set("dashboard-local-peers", "false"); err != nil {
		t.Fatal(err)
	}
	if resolve() {
		t.Fatal("explicit CLI false did not override configuration")
	}
	config.Set("dashboard.local_peers", false)
	if err := Run.Flags().Set("dashboard-local-peers", "true"); err != nil {
		t.Fatal(err)
	}
	if !resolve() {
		t.Fatal("explicit CLI true did not override configuration")
	}
}

func TestDashboardLocalPeersConstructorOptIn(t *testing.T) {
	for _, role := range []string{"verifying", "validator"} {
		for _, enabled := range []bool{false, true} {
			t.Run(role+map[bool]string{false: "_disabled", true: "_enabled"}[enabled], func(t *testing.T) {
				dir, err := os.MkdirTemp("/tmp", "node-local-peers-")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := os.RemoveAll(dir); err != nil {
						t.Error(err)
					}
				})
				path := filepath.Join(dir, "feed.sock")
				d := startNodeDashboard(context.Background(), time.Now(), path, "", "peer-option", "isolated", role,
					true, false, false, true, false, false, false, false, false, false, enabled)
				if d == nil || d.localPeers != enabled {
					t.Fatal("constructor did not preserve the explicit option")
				}
				t.Cleanup(d.close)
				transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, "unix", path)
				}}
				t.Cleanup(transport.CloseIdleConnections)
				client := &http.Client{Transport: transport, Timeout: time.Second}
				response, err := client.Get("http://node/details/peers")
				if err != nil {
					t.Fatal(err)
				}
				_, readErr := io.Copy(io.Discard, io.LimitReader(response.Body, 513))
				closeErr := response.Body.Close()
				want := http.StatusNotFound
				if enabled {
					want = http.StatusServiceUnavailable
				}
				if readErr != nil || closeErr != nil || response.StatusCode != want {
					t.Fatalf("unstarted local peers status=%d want=%d read=%v close=%v", response.StatusCode, want, readErr, closeErr)
				}
			})
		}
	}
}

func TestDashboardLocalPeersObservedClientRun(t *testing.T) {
	peer, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	started := time.Now()
	client, err := gossip.NewClient(gossip.Config{Entrypoint: peer.LocalAddr().String(), BindAddr: "127.0.0.1:0",
		TVUAddr: peer.LocalAddr().String(), AdvertisedIP: "127.0.0.1", ShredVersion: 42})
	if err != nil {
		t.Fatal(err)
	}
	genesis := (solana.Hash{9}).String()
	d := &nodeDashboard{networkDetails: true, localPeers: true, state: dashboard.Snapshot{
		RunID: "peer-run", StartedAt: started.UTC(), GenesisHash: &genesis}}
	d.observeGossipClient(client)
	ctx, cancel := context.WithTimeout(context.Background(), 16*time.Second)
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("owned Gossip Run did not stop")
		}
	})
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		capture, err := d.sampleLocalPeers(time.Time{})
		if err == nil {
			var detail dashboard.LocalPeerDetail
			if json.Unmarshal([]byte(capture.Data.Body()), &detail) != nil || detail.ClientGeneration != 1 || detail.RunGeneration != 1 || detail.PublicationSequence == 0 ||
				detail.Rows == nil || len(detail.Rows) != 0 || detail.Limited || detail.AcceptedAdverts != 0 {
				t.Fatal("actual opt-in Run did not publish a known-empty local observation")
			}
			source, known := client.PeerDatasetSnapshot()
			if !known || detail.PublicationSequence != source.PublicationSequence || capture.FreshUntil != source.SampledAt.Add(dashboard.NetworkDetailFreshFor) {
				t.Fatal("actual Run's original lease changed in the adapter")
			}
			d.stopping()
			if _, err := d.sampleLocalPeers(time.Time{}); err == nil {
				t.Fatal("stopped node admitted the previous source")
			}
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("real diagnostics cycle did not publish the opted-in dataset", err)
		case <-ticker.C:
		}
	}
}

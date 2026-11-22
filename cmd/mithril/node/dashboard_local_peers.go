package node

import (
	"errors"
	"strings"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/gagliardetto/solana-go"
)

func validateDashboardLocalPeers(enabled bool, socket string, networkDetails bool) error {
	if !enabled {
		return nil
	}
	if strings.TrimSpace(socket) == "" {
		return errors.New("dashboard.local_peers requires dashboard.socket")
	}
	if !networkDetails {
		return errors.New("dashboard.local_peers requires dashboard.network_details")
	}
	return nil
}

// sampleLocalPeers runs only in the joined detail worker. It copies the selected
// client's published cache, never its live contact map or network endpoints.
func (d *nodeDashboard) sampleLocalPeers(_ time.Time) (dashboard.LocalPeerCapture, error) {
	if d == nil {
		return dashboard.LocalPeerCapture{}, errors.New("source_unavailable")
	}
	d.mu.Lock()
	client, generation := d.gossipClient, d.gossipGeneration
	enabled := d.localPeers && d.networkDetails && !d.networkRetired && d.state.Phase != "stopping"
	binding := dashboard.NodeNetworkBinding{RunID: d.state.RunID, NodeStartedAt: d.state.StartedAt.UTC()}
	if d.state.GenesisHash != nil {
		genesis := *d.state.GenesisHash
		binding.GenesisHash = &genesis
	}
	d.mu.Unlock()
	if !enabled {
		return dashboard.LocalPeerCapture{}, errors.New("source_unavailable")
	}
	if client == nil || generation == 0 || binding.GenesisHash == nil {
		return dashboard.LocalPeerCapture{}, errors.New("waiting_source")
	}
	monitoring, known := client.MonitoringSnapshot()
	if !known {
		return dashboard.LocalPeerCapture{}, errors.New("waiting_source")
	}
	before, known := client.PeerDatasetSnapshot()
	if !known {
		return dashboard.LocalPeerCapture{}, errors.New("waiting_source")
	}
	capture, err := localPeerMeasurement(binding, generation, monitoring.StartedAt, before, time.Now())
	if err != nil {
		return dashboard.LocalPeerCapture{}, err
	}
	// A Client may reenter Run without changing the selected pointer. A newer
	// publication within the same Run is fine; it cannot renew this copy's lease.
	after, known := client.PeerDatasetSnapshot()
	if !known || !localPeerRunStable(before, after) || !d.localPeerBindingCurrent(client, generation, binding) {
		return dashboard.LocalPeerCapture{}, errors.New("binding_changed")
	}
	if !capture.FreshUntil.After(time.Now()) {
		return dashboard.LocalPeerCapture{}, errors.New("source_time_invalid")
	}
	return capture, nil
}

func localPeerRunStable(before, after gossip.PeerDatasetSnapshot) bool {
	return before.Generation != 0 && before.Generation == after.Generation && !before.StartedAt.IsZero() &&
		before.StartedAt.Equal(after.StartedAt)
}

func (d *nodeDashboard) localPeerBindingCurrent(client *gossip.Client, generation uint64, binding dashboard.NodeNetworkBinding) bool {
	if d == nil || client == nil || generation == 0 {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.localPeers && d.networkDetails && !d.networkRetired && d.state.Phase != "stopping" &&
		d.gossipClient == client && d.gossipGeneration == generation && d.state.RunID == binding.RunID &&
		d.state.StartedAt.Equal(binding.NodeStartedAt) && sameReplayDetailGenesis(d.state.GenesisHash, binding.GenesisHash)
}

func localPeerMeasurement(binding dashboard.NodeNetworkBinding, clientGeneration uint64, clientStartedAt time.Time,
	s gossip.PeerDatasetSnapshot, now time.Time) (dashboard.LocalPeerCapture, error) {
	if binding.GenesisHash == nil {
		return dashboard.LocalPeerCapture{}, errors.New("waiting_source")
	}
	genesis, err := solana.HashFromBase58(*binding.GenesisHash)
	if err != nil || genesis.IsZero() || genesis.String() != *binding.GenesisHash {
		return dashboard.LocalPeerCapture{}, errors.New("source_invalid")
	}
	// Only the source's original same-process clocks establish the lease. UTC
	// wire dates are descriptive and cannot reconstruct or extend it.
	if s.StartedAt.IsZero() || s.SampledAt.IsZero() || s.StartedAt == s.StartedAt.Round(0) ||
		s.SampledAt == s.SampledAt.Round(0) || now.Before(s.SampledAt) || s.SampledAt.Before(s.StartedAt) ||
		!s.SampledAt.Add(dashboard.NetworkDetailFreshFor).After(now) {
		return dashboard.LocalPeerCapture{}, errors.New("source_time_invalid")
	}
	if len(s.Rows) > gossip.PeerDatasetCapacity {
		return dashboard.LocalPeerCapture{}, errors.New("response_limit")
	}
	detail := dashboard.LocalPeerDetail{SchemaVersion: 1, Family: "local_peers", Scope: "local_signed_contacts",
		Binding: binding, CapturedAt: s.SampledAt.UTC(), ClientGeneration: clientGeneration, ClientStartedAt: clientStartedAt.UTC(),
		RunGeneration: s.Generation, RunStartedAt: s.StartedAt.UTC(), PublicationSequence: s.PublicationSequence,
		SampledAt: s.SampledAt.UTC(), Capacity: gossip.PeerDatasetCapacity,
		Limited: s.Limited, CountersSaturated: s.CountersSaturated, AcceptedAdverts: s.AcceptedAdverts,
		RetainedAdverts: s.RetainedAdverts, CapacityRefusals: s.CapacityRefusals,
		Rows: make([]dashboard.LocalPeerRow, 0, len(s.Rows))}
	for _, row := range s.Rows {
		// Match the existing recent-peer projection: Agave prerelease bits do
		// not become an ordinary semantic minor or patch version.
		minor, patch := row.Version.Minor&0x3fff, row.Version.Patch
		if row.Version.Minor>>14 != 0 {
			patch = 0
		}
		detail.Rows = append(detail.Rows, dashboard.LocalPeerRow{Identity: solana.PublicKey(row.Pubkey).String(),
			ObservedAt: row.ReceivedAt.UTC(), Outset: row.Outset, Wallclock: row.Wallclock,
			Version: dashboard.LocalPeerVersion{Major: row.Version.Major, Minor: minor, Patch: patch,
				Commit: row.Version.Commit, FeatureSet: row.Version.FeatureSet, Client: row.Version.Client},
			ShredVersion: row.ShredVersion, Repair: row.Repair, TVU: row.TVU, Alpenglow: row.Alpenglow})
	}
	data, err := dashboard.PrepareLocalPeers(detail)
	if err != nil {
		return dashboard.LocalPeerCapture{}, err
	}
	return dashboard.LocalPeerCapture{Data: data, FreshUntil: s.SampledAt.Add(dashboard.NetworkDetailFreshFor)}, nil
}

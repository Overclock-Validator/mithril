package dashboard

import (
	"context"
	"net/http"
	"testing"
	"time"
)

// Exercise all independent readers against the shared, rate-limited feed.
func TestFeedDetailsShareRequestLimit(t *testing.T) {
	now := time.Now()
	snapshot, network := networkDetailFixture(now)
	_, machine := machineSourceFixture(t, now, 1)
	d, _, _, _ := maximumMachineDetail(t)
	d.Binding.RunID, d.Binding.NodeStartedAt, d.Binding.SourceGeneration = snapshot.RunID, snapshot.StartedAt, 1
	d.CapturedAt = now.UTC()
	snapshot.GenesisHash = &d.Binding.GenesisHash
	network.Detail.Binding.GenesisHash = snapshot.GenesisHash
	preparedMachine, err := PrepareMachineDetail(d)
	if err != nil {
		t.Fatal(err)
	}
	machine.Prepared = preparedMachine

	peers := localPeerTestDetail(1)
	peers.Binding = NodeNetworkBinding{RunID: snapshot.RunID, NodeStartedAt: snapshot.StartedAt, GenesisHash: snapshot.GenesisHash}
	peers.ClientGeneration, peers.RunGeneration = 1, 1
	peers.ClientStartedAt, peers.RunStartedAt = snapshot.StartedAt, snapshot.StartedAt
	peers.CapturedAt, peers.SampledAt, peers.Rows[0].ObservedAt = now.UTC(), now.UTC(), now.UTC()
	preparedPeers, err := PrepareLocalPeers(peers)
	if err != nil {
		t.Fatal(err)
	}
	path := socketPath(t)
	feed, err := ListenFeedWithDetails(context.Background(), path, "", func() Snapshot { return snapshot }, DetailSamplers{
		Network:    func(time.Time) (NetworkDetailCapture, error) { return network, nil },
		Machine:    func(time.Time) (MachineDetailCapture, error) { return machine, nil },
		LocalPeers: func(time.Time) (LocalPeerCapture, error) { return LocalPeerCapture{Data: preparedPeers, FreshUntil: now.Add(30 * time.Second)}, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	address, cancel, done := replayDetailPublicServer(t, path, PublicOptions{Resources: true, NetworkDetails: true, MachineDetails: true, LocalPeers: true})
	t.Cleanup(func() { replayDetailStopPublic(t, cancel, done) })
	allPaths := []string{"/api/v1/details/network", machineDetailPublicPath, localPeerPublicPath}
	paths := append([]string(nil), allPaths...)
	deadline := time.Now().Add(20 * time.Second)
	for len(paths) > 0 && time.Now().Before(deadline) {
		pending := paths[:0]
		for _, path := range paths {
			status, _, _ := replayDetailPublicRequest(t, address, path, "stats.example", "")
			if status != http.StatusOK {
				pending = append(pending, path)
			}
		}
		paths = pending
		if len(paths) != 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
	if len(paths) != 0 {
		t.Fatalf("detail readers did not become available: %v", paths)
	}
	for range 3 {
		time.Sleep(time.Second)
		for _, path := range allPaths {
			status, _, _ := replayDetailPublicRequest(t, address, path, "stats.example", "")
			if status != http.StatusOK {
				t.Fatalf("%s became unavailable during polling: %d", path, status)
			}
		}
	}
}

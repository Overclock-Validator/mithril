package blockstream

import (
	"net"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
)

func TestPrewarmGossipObserverReceivesActualBootstrapClient(t *testing.T) {
	peer, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	tvu, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	bind := tvu.LocalAddr().String()
	if err := tvu.Close(); err != nil {
		t.Fatal(err)
	}
	var attached *gossip.Client
	pw, err := StartTurbinePrewarm(TurbinePrewarmConfig{BindAddr: bind, GossipEntrypoint: peer.LocalAddr().String(), GossipBindAddr: "127.0.0.1:0",
		AdvertisedIP: "127.0.0.1", ShredVersion: 42, FloorSlot: 1, GossipObserver: func(client *gossip.Client) {
			if client == nil || attached != nil {
				t.Fatal("bootstrap attached a nil or repeated owner")
			}
			attached = client
			client.EnableMonitoring()
			if _, known := client.MonitoringSnapshot(); known {
				t.Fatal("attachment fabricated a diagnostic sample before Run")
			}
		}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pw.Stop)
	if attached == nil {
		t.Fatal("bootstrap client was not attached before replay exists")
	}
}

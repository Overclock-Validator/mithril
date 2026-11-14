package gossip

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"net"
	"testing"
	"time"
)

func TestPeerDatasetClientRun(t *testing.T) {
	entrypoint, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = entrypoint.Close() })
	c, err := NewClient(Config{Entrypoint: entrypoint.LocalAddr().String(), BindAddr: "127.0.0.1:0", TVUAddr: entrypoint.LocalAddr().String(), AdvertisedIP: "127.0.0.1", ShredVersion: 42})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := c.PeerDatasetSnapshot(); ok {
		t.Fatal("default-off dataset was available")
	}
	c.EnablePeerDataset()
	if _, ok := c.PeerDatasetSnapshot(); ok {
		t.Fatal("enable fabricated a sample before Run")
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	info, err := NewContactInfo(Pubkey(pub), 42, entrypoint.LocalAddr().(*net.UDPAddr), nil)
	if err != nil {
		t.Fatal(err)
	}
	value, err := signCrdsContactInfo(info, priv)
	if err != nil {
		t.Fatal(err)
	}
	packet, err := encodePushMessage(Pubkey(pub), []CrdsValue{value})
	if err != nil {
		t.Fatal(err)
	}
	forged := value
	forged.Signature[0] ^= 1
	forgedPacket, err := encodePushMessage(Pubkey(pub), []CrdsValue{forged})
	if err != nil {
		t.Fatal(err)
	}
	var previous uint64
	for range 2 {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		joined := false
		go func() { done <- c.Run(ctx) }()
		t.Cleanup(func() {
			cancel()
			if !joined {
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					t.Error("Run did not stop during cleanup")
				}
			}
		})
		deadline := time.Now().Add(3 * time.Second)
		var session *peerDatasetSession
		for session == nil && time.Now().Before(deadline) {
			session = c.peerDataset.Load()
			time.Sleep(time.Millisecond)
		}
		if session == nil {
			cancel()
			<-done
			joined = true
			t.Fatal("Run did not attach a dataset")
		}
		if session.dataset.generation <= previous {
			t.Fatal("Run reused a source generation")
		}
		previous = session.dataset.generation
		if err := c.handlePacket(session.conn, forgedPacket, nil, nil); err != nil {
			t.Fatal(err)
		}
		if err := c.handlePacket(&net.UDPConn{}, packet, nil, nil); err != nil {
			t.Fatal(err)
		}
		c.publishMonitoringWithSocket(time.Now(), Stats{}, session.conn, previous)
		if snapshot, ok := c.PeerDatasetSnapshot(); !ok || len(snapshot.Rows) != 0 || snapshot.AcceptedAdverts != 0 {
			t.Fatal("forged or different-socket contact reached the dataset")
		}
		if _, err := entrypoint.WriteToUDP(packet, session.conn.LocalAddr().(*net.UDPAddr)); err != nil {
			t.Fatal(err)
		}
		var found bool
		for time.Now().Before(deadline) {
			c.publishMonitoringWithSocket(time.Now(), Stats{}, session.conn, previous)
			if snapshot, ok := c.PeerDatasetSnapshot(); ok && len(snapshot.Rows) == 1 && snapshot.Rows[0].Pubkey == Pubkey(pub) {
				found = true
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		cancel()
		select {
		case err := <-done:
			joined = true
			if err != nil && !errors.Is(err, net.ErrClosed) {
				t.Fatal(err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("Run did not stop")
		}
		if !found {
			t.Fatal("accepted signed contact did not reach the dataset")
		}
		if _, ok := c.PeerDatasetSnapshot(); ok {
			t.Fatal("returned Run retained its dataset")
		}
	}
}

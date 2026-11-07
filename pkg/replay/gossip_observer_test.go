package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/blockstream"
	"github.com/Overclock-Validator/mithril/pkg/gossip"
)

func TestProgressGossipObserverFencesAbandonedSources(t *testing.T) {
	var disabled *Progress
	disabled.SetGossipObserver(nil)
	disabled.observeSourceGossip(nil, nil)
	p := &Progress{}
	var attached []*gossip.Client
	p.SetGossipObserver(func(client *gossip.Client) { attached = append(attached, client) })
	first := blockstream.NewBlockSource(&blockstream.BlockSourceOpts{DisableRPCBlockFetch: true})
	second := blockstream.NewBlockSource(&blockstream.BlockSourceOpts{DisableRPCBlockFetch: true})
	t.Cleanup(first.Stop)
	t.Cleanup(second.Stop)
	client, replacement := &gossip.Client{}, &gossip.Client{}
	p.begin(1, nil)
	p.sourceStarted(first)
	p.observeSourceGossip(first, client)
	if len(attached) != 1 || attached[0] != client {
		t.Fatal("current source client was not attached")
	}
	p.begin(2, nil)
	p.observeSourceGossip(first, client)
	if len(attached) != 1 {
		t.Fatal("a reset source attached after its attempt ended")
	}
	p.sourceStarted(second)
	p.observeSourceGossip(first, client)
	p.observeSourceGossip(second, replacement)
	p.sourceStarted(nil)
	p.observeSourceGossip(second, replacement)
	if len(attached) != 2 || attached[1] != replacement {
		t.Fatal("source replacement lost the observer or detached a newer client")
	}
}

func TestProgressGossipObserverOrdersReplacement(t *testing.T) {
	p := &Progress{}
	first := blockstream.NewBlockSource(&blockstream.BlockSourceOpts{DisableRPCBlockFetch: true})
	second := blockstream.NewBlockSource(&blockstream.BlockSourceOpts{DisableRPCBlockFetch: true})
	t.Cleanup(first.Stop)
	t.Cleanup(second.Stop)
	client, replacement := &gossip.Client{}, &gossip.Client{}
	entered, release, attached := make(chan struct{}), make(chan struct{}), make(chan *gossip.Client, 2)
	p.SetGossipObserver(func(current *gossip.Client) {
		if p.gossipMu.TryLock() {
			p.gossipMu.Unlock()
			t.Error("attachment is not ordered against source replacement")
		}
		if current == client {
			close(entered)
			<-release
		}
		attached <- current
	})
	p.sourceStarted(first)
	oldDone := make(chan struct{})
	go func() { defer close(oldDone); p.observeSourceGossip(first, client) }()
	<-entered
	replaced := make(chan struct{})
	go func() { defer close(replaced); p.sourceStarted(second) }()
	close(release)
	<-oldDone
	<-replaced
	p.observeSourceGossip(first, client)
	p.observeSourceGossip(second, replacement)
	if len(attached) != 2 || <-attached != client || <-attached != replacement {
		t.Fatal("an abandoned source replaced the current client")
	}
}

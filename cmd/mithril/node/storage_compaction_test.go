package node

import (
	"context"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
)

type blockedCompactor struct{ started, release chan struct{} }

func (c *blockedCompactor) CompactOnce(accountsdb.CompactionConfig) (accountsdb.CompactStats, error) {
	close(c.started)
	<-c.release
	return accountsdb.CompactStats{}, nil
}

func TestAccountsCompactorShutdownJoinsInFlightCycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &blockedCompactor{started: make(chan struct{}), release: make(chan struct{})}
	stop := startAccountsCompactor(ctx, c, accountsdb.CompactionConfig{}, time.Hour)
	select {
	case <-c.started:
	case <-time.After(5 * time.Second):
		t.Fatal("first compaction never started")
	}
	cancel()
	done := make(chan struct{})
	go func() { stop(); close(done) }()
	select {
	case <-done:
		t.Fatal("shutdown returned with compactor still using the DB")
	case <-time.After(20 * time.Millisecond):
	}
	close(c.release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not join completed compaction")
	}
}

package blockprod

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func newTestShredSink(broadcast func([]turbine.Entry) error) *ShredSink {
	s := &ShredSink{broadcast: broadcast}
	s.cond = sync.NewCond(&s.mu)
	go s.loop()
	return s
}

func TestShredSinkOnEntryBatchDoesNotBlockOnBroadcast(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	s := newTestShredSink(func([]turbine.Entry) error {
		close(started)
		<-release
		return nil
	})
	defer s.Discard()

	done := make(chan struct{})
	go func() {
		s.OnEntryBatch([]turbine.Entry{{NumHashes: 1}}, 0)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("OnEntryBatch blocked on broadcast")
	}
	<-started
	close(release)
	s.Wait()
}

func TestShredSinkWaitDrainsFIFO(t *testing.T) {
	var order []uint64
	var mu sync.Mutex
	s := newTestShredSink(func(entries []turbine.Entry) error {
		mu.Lock()
		order = append(order, entries[0].NumHashes)
		mu.Unlock()
		time.Sleep(5 * time.Millisecond)
		return nil
	})
	defer s.Discard()

	s.OnEntryBatch([]turbine.Entry{{NumHashes: 1, Hash: solana.Hash{1}}}, 0)
	s.OnEntryBatch([]turbine.Entry{{NumHashes: 2, Hash: solana.Hash{2}}}, 0)
	s.OnEntryBatch([]turbine.Entry{{NumHashes: 3, Hash: solana.Hash{3}}}, 0)
	s.Wait()

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []uint64{1, 2, 3}, order)
}

func TestShredSinkDiscardDropsQueuedBatches(t *testing.T) {
	var started atomic.Int32
	firstEntered := make(chan struct{})
	release := make(chan struct{})
	s := newTestShredSink(func([]turbine.Entry) error {
		if started.Add(1) == 1 {
			close(firstEntered)
		}
		<-release
		return nil
	})

	s.OnEntryBatch([]turbine.Entry{{NumHashes: 1}}, 0)
	s.OnEntryBatch([]turbine.Entry{{NumHashes: 2}}, 0)
	<-firstEntered

	done := make(chan struct{})
	go func() {
		s.Discard()
		close(done)
	}()
	for {
		s.mu.Lock()
		dropped := s.closed && len(s.queue) == 0
		s.mu.Unlock()
		if dropped {
			break
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	<-done
	require.Equal(t, int32(1), started.Load())
}

func TestShredSinkBroadcastErrorStopsQueue(t *testing.T) {
	var started atomic.Int32
	s := newTestShredSink(func([]turbine.Entry) error {
		n := started.Add(1)
		if n == 1 {
			return errors.New("shred failed")
		}
		return nil
	})
	defer s.Discard()

	s.OnEntryBatch([]turbine.Entry{{NumHashes: 1}}, 0)
	s.OnEntryBatch([]turbine.Entry{{NumHashes: 2}}, 0)
	s.Wait()
	require.EqualError(t, s.Err(), "shred failed")
	require.Equal(t, int32(1), started.Load())
}

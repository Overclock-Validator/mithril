package turbine

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestEntryPipelineTraceRequiresCompleteBatchAndBoundary(t *testing.T) {
	source := &entryPipelineTrace{arrivals: map[uint32]int64{0: 10, 1: 100, 2: 20, 3: 30, 4: 40}, discovered: map[uint32]int64{0: 110, 3: 120}, sealed: true}
	first := &prefetchedShredBatch{start: 0, end: 2}
	later := &prefetchedShredBatch{start: 3, end: 4}
	r := entryPipelineReport{source: source, all: []*prefetchedShredBatch{first, later}, retained: []*prefetchedShredBatch{later}}
	r.finish()
	require.Equal(t, int64(100), r.Batches[0].Available)
	require.Equal(t, int64(40), r.Batches[1].Available)
	require.Equal(t, int64(80), r.Batches[1].Discovered-r.Batches[1].Available, "a gap in the earlier batch delays discovery, not availability of the later one")
	require.False(t, r.Batches[0].Retained)
	require.True(t, r.Batches[1].Retained)
	delete(source.arrivals, 2)
	r.Batches = nil
	r.finish()
	require.False(t, r.Batches[1].AvailabilityKnown, "the preceding boundary must also have been observed")
}

func TestEntryVerificationTraceJoinsWorkerTimings(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	v := newTransactionVerifier(1, 8, func(*solana.Transaction) error {
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
		return nil
	})
	defer v.closeAndWait()
	ctx := withEntryPipelineTrace(context.Background(), &entryPipelineTrace{})
	r, err := v.submitTransactions(ctx, []*solana.Transaction{{}})
	require.NoError(t, err)
	<-entered
	require.Nil(t, completedEntryVerification(r), "unfinished mutable metrics must not be read")
	close(release)
	_, err = r.wait()
	require.NoError(t, err)
	m := completedEntryVerification(r)
	require.NotNil(t, m)
	require.Equal(t, 1, m.Jobs)
	require.LessOrEqual(t, m.Submit, m.Admitted)
	require.LessOrEqual(t, m.Admitted, m.FirstWorker)
	require.Less(t, m.FirstWorker, m.LastWorker)
	require.LessOrEqual(t, m.LastWorker, m.Finished)
	require.Positive(t, m.WorkerSum)
	require.GreaterOrEqual(t, m.JobWaitSum, int64(0))
	plain, err := v.submitTransactions(context.Background(), []*solana.Transaction{{}})
	require.NoError(t, err)
	_, err = plain.wait()
	require.NoError(t, err)
	require.Nil(t, plain.trace, "ordinary verification does not collect job timestamps")
}

func TestEntryPipelineTraceSealedGeneration(t *testing.T) {
	s := &slotState{pipelineTrace: &entryPipelineTrace{arrivals: map[uint32]int64{1: 42}, discovered: map[uint32]int64{}, sealed: true}}
	s.traceAcceptedShred(&Shred{Type: ShredTypeData, Index: 2})
	require.Len(t, s.pipelineTrace.arrivals, 1, "completion/retry cannot mutate a report's frozen arrival map")
}

func TestEntryCriticalShredIncludesBoundaryAndSource(t *testing.T) {
	source := &entryPipelineTrace{arrivals: map[uint32]int64{2: 100, 3: 20, 4: 30}, sources: map[uint32]entryShredSource{2: {Path: "fec_recovery", FEC: 0, TriggerIndex: 12, TriggerCoding: true, TriggerRepair: true, AdmissionEntered: 80}}}
	r := entryPipelineReport{source: source, all: []*prefetchedShredBatch{{start: 3, end: 4}}}
	r.finish()
	require.Equal(t, uint32(2), *r.Batches[0].CriticalIndex)
	require.Equal(t, "fec_recovery", r.Batches[0].CriticalSource.Path)
	require.True(t, r.Batches[0].CriticalSource.TriggerRepair)
	require.Equal(t, 1, r.Batches[0].CriticalTies)
	source.arrivals[3] = 100
	r.Batches = nil
	r.finish()
	require.Equal(t, 2, r.Batches[0].CriticalTies)
	require.Equal(t, uint32(2), *r.Batches[0].CriticalIndex, "ties select the lowest index deterministically")
}

func TestEntryTracePreservesFirstAdmission(t *testing.T) {
	s := &slotState{pipelineTrace: &entryPipelineTrace{arrivals: make(map[uint32]int64)}}
	sh := &Shred{Type: ShredTypeData, Index: 1}
	s.traceAcceptedShred(sh, entryShredSource{Path: "repair"})
	first := s.pipelineTrace.arrivals[1]
	s.traceAcceptedShred(sh, entryShredSource{Path: "non_repair"})
	require.Equal(t, first, s.pipelineTrace.arrivals[1])
	require.Equal(t, "repair", s.pipelineTrace.sources[1].Path)
	s.pipelineTrace.sealed = true
	s.traceAcceptedShred(&Shred{Type: ShredTypeData, Index: 2}, entryShredSource{Path: "repair"})
	require.Len(t, s.pipelineTrace.sources, 1)
}

func TestEntryRepairTraceBoundedAndExplicit(t *testing.T) {
	old := entryTraceConfig
	defer func() { entryTraceConfig = old }()
	entryTraceConfig = entryTraceSettings{repairs: make(chan entryRepairTrace, 1)}
	traceRepairSend(15, 10, repairRequestWindowIndex, 2, 100, true)
	r := <-entryTraceConfig.repairs
	require.Equal(t, "repair_send", r.Event)
	require.Equal(t, uint64(15), r.Slot)
	require.False(t, r.Highest)
	require.True(t, r.Success)
	require.Equal(t, uint8(2), r.Attempt)
	traceRepairSend(15, 10, repairRequestHighestWindowIndex, 0, 100, false)
	dropped := entryTraceDropped.Load()
	traceRepairSend(15, 11, repairRequestWindowIndex, 0, 100, true)
	require.Equal(t, dropped+1, entryTraceDropped.Load(), "full diagnostic queue never blocks repair")
	r = <-entryTraceConfig.repairs
	require.True(t, r.Highest)
	require.False(t, r.Success)
	traceRepairSend(15, 1, repairRequestWindowIndex, 0, 0, true)
	require.Empty(t, entryTraceConfig.repairs, "unsampled sends are ignored")
}

func TestEntryTraceSelectionIsBounded(t *testing.T) {
	old := entryTraceConfig
	defer func() { entryTraceConfig = old }()
	entryTraceConfig = entryTraceSettings{modulo: 5, until: time.Now().Add(time.Minute)}
	require.True(t, entryTraceSelected(15))
	require.False(t, entryTraceSelected(16))
	entryTraceConfig.until = time.Now().Add(-time.Second)
	require.False(t, entryTraceSelected(15))
	entryTraceConfig = entryTraceSettings{}
	require.False(t, entryTraceSelected(15))
}

func TestEntryRepairResponseCorrelation(t *testing.T) {
	old := entryTraceConfig
	defer func() { entryTraceConfig = old }()
	entryTraceConfig = entryTraceSettings{modulo: 1, until: time.Now().Add(time.Minute), repairs: make(chan entryRepairTrace, 8)}
	from := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8000}
	addr, _ := repairAddressKeyFromUDP(from)
	for _, late := range []bool{false, true} {
		c := newPacingTestClient(t)
		key := repairRequestKey{kind: repairRequestWindowIndex, slot: 42, index: 3}
		rec := outstandingRepairRequest{key: key, nonce: 777, addr: addr, sentAt: time.Now().Add(-time.Second), accountAt: time.Now().Add(time.Second)}
		responseKey := repairResponseKey{addr: addr, nonce: 777}
		if late {
			c.expiredCur[responseKey] = rec
		} else {
			c.outstanding[key] = rec
			c.byResponse[responseKey] = key
		}
		require.False(t, c.observeShredResponse(nil, nonceTrailer(777), from, &Shred{Slot: 42, Index: 4, Type: ShredTypeData}))
		require.Empty(t, entryTraceConfig.repairs, "wrong index cannot be reported as matched")
		require.True(t, c.observeShredResponse(nil, nonceTrailer(777), from, &Shred{Slot: 42, Index: 3, Type: ShredTypeData}))
		r := <-entryTraceConfig.repairs
		require.Equal(t, "repair_response", r.Event)
		require.Equal(t, uint32(777), r.Nonce)
		require.Equal(t, from.String(), r.Peer)
		require.Equal(t, late, r.Late)
		require.Equal(t, uint32(3), r.ReturnedIndex)
		require.Greater(t, r.ResponseAt, r.RequestedAt)
		require.False(t, c.observeShredResponse(nil, nonceTrailer(777), from, &Shred{Slot: 42, Index: 3, Type: ShredTypeData}))
		require.Empty(t, entryTraceConfig.repairs, "consumed nonce cannot count twice")
	}
}

func TestEntryFECDeficit(t *testing.T) {
	s := newRepairSelectionSlot(42)
	require.Equal(t, -1, traceFECDeficit(s, 0))
	addCodedSet(s, 0, 32, 32, seq(0, 19), 8)
	require.Equal(t, 4, traceFECDeficit(s, 0))
	s.fecSets[0].data[20] = &Shred{}
	require.Equal(t, 3, traceFECDeficit(s, 0))
	for i := uint32(21); i < 32; i++ {
		s.fecSets[0].data[i] = &Shred{}
	}
	require.Zero(t, traceFECDeficit(s, 0), "enough shards is not a negative deficit")
}

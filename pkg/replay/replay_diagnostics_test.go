package replay

import (
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func diagnosticProgress(t *testing.T) *Progress {
	t.Helper()
	p := &Progress{}
	require.NoError(t, p.EnableReplayDiagnostics("test-run"))
	_, ok := p.ReplayDiagnosticsSnapshot()
	require.False(t, ok)
	p.begin(1, nil)
	return p
}

func publishDiagnostic(p *Progress, slot uint64, hash byte, row *metrics.BlockReplay) {
	s, _ := p.Snapshot()
	at := s.StartedAt.Add(time.Duration(s.ExecutedBlocks+1) * time.Millisecond)
	p.completed(CompletedBlock{
		At: at, Slot: slot, Bankhash: solana.Hash{hash}, Epoch: 3, BlockHeight: slot,
		Transactions: 2, ComputeUnits: 30,
	}, 0, false)
	row.Slot = slot
	p.recordReplayDiagnostic(at.Add(time.Microsecond), row, nil)
}

func TestReplayDiagnosticsDisabledAndConfiguration(t *testing.T) {
	var nilProgress *Progress
	var disabled Progress
	row := &metrics.BlockReplay{ReplayStages: map[string]metrics.Timing{"tx_loop": {Count: 1}}}
	at := time.Unix(1, 0)
	allocations := testing.AllocsPerRun(100, func() {
		nilProgress.recordReplayDiagnostic(at, row, nil)
		disabled.recordReplayDiagnostic(at, row, nil)
		_, _ = nilProgress.ReplayDiagnosticsSnapshot()
		_, _ = disabled.ReplayDiagnosticsSnapshot()
	})
	require.Zero(t, allocations)
	require.Error(t, nilProgress.EnableReplayDiagnostics("run"))
	require.Error(t, disabled.EnableReplayDiagnostics(""))
	require.Error(t, disabled.EnableReplayDiagnostics(strings.Repeat("x", 129)))
	require.NoError(t, disabled.EnableReplayDiagnostics("run"))
	require.Error(t, disabled.EnableReplayDiagnostics("again"))
	p := &Progress{}
	p.begin(1, nil)
	require.Error(t, p.EnableReplayDiagnostics("late"))
}

func TestReplayDiagnosticsRejectsIncompleteAndDuplicateRows(t *testing.T) {
	p := diagnosticProgress(t)
	at := time.Unix(7, 0)
	row := &metrics.BlockReplay{Slot: 4}
	p.recordReplayDiagnostic(at, row, nil)
	p.completed(CompletedBlock{Slot: 4, At: at}, 0, false)
	p.recordReplayDiagnostic(at, row, nil)
	p.completed(CompletedBlock{Slot: 5, At: at, Bankhash: solana.Hash{1}}, 0, false)
	p.recordReplayDiagnostic(at, row, nil)
	p.completed(CompletedBlock{Slot: 4, Bankhash: solana.Hash{1}}, 0, false)
	p.recordReplayDiagnostic(at, row, nil)
	s, _ := p.ReplayDiagnosticsSnapshot()
	require.Empty(t, s.Records)
	publishDiagnostic(p, 4, 1, row)
	first, _ := p.ReplayDiagnosticsSnapshot()
	p.recordReplayDiagnostic(at.Add(time.Hour), row, nil)
	again, _ := p.ReplayDiagnosticsSnapshot()
	require.Equal(t, first, again, "another read of the same completed bank must not redate or reinsert it")
	row.ReplayStages = map[string]metrics.Timing{"future_stage": {Count: 1}}
	publishDiagnostic(p, 4, 2, row)
	s, _ = p.ReplayDiagnosticsSnapshot()
	require.Len(t, s.Records, 2)
	require.Equal(t, solana.Hash{1}, s.Records[0].Bankhash)
	require.Equal(t, solana.Hash{2}, s.Records[1].Bankhash)
	require.True(t, s.Records[1].UnknownStages)
	require.Zero(t, s.Records[1].Stages)
	// Observation UTC can move backwards while elapsed producer durations remain valid.
	p.completed(CompletedBlock{Slot: 6, At: at, Bankhash: solana.Hash{3}}, 0, false)
	row.Slot = 6
	row.SlotReplay = metrics.Timing{Count: 1, SumNanoseconds: 99}
	p.recordReplayDiagnostic(at.Add(-time.Second), row, nil)
	s, _ = p.ReplayDiagnosticsSnapshot()
	require.Equal(t, uint64(99), s.Records[2].Timings.SlotReplay.SumNanoseconds)
	require.True(t, s.Records[2].ObservedAt.Before(s.Records[2].CompletedAt))
}

func TestReplayDiagnosticsRetentionAndResets(t *testing.T) {
	p := diagnosticProgress(t)
	row := &metrics.BlockReplay{}
	for i := uint64(1); i <= ReplayDiagnosticCapacity+5; i++ {
		publishDiagnostic(p, i, 1, row)
	}
	s, _ := p.ReplayDiagnosticsSnapshot()
	require.Equal(t, "test-run", s.RunID)
	require.Len(t, s.Records, ReplayDiagnosticCapacity)
	require.Equal(t, uint64(5), s.Evicted)
	require.Equal(t, uint64(6), s.FirstSequence)
	require.Equal(t, uint64(261), s.LastSequence)
	for i, r := range s.Records {
		require.Equal(t, uint64(i+6), r.Sequence)
		require.Equal(t, r.Sequence, r.Slot)
	}
	p.rewound()
	reset, _ := p.ReplayDiagnosticsSnapshot()
	require.Empty(t, reset.Records)
	require.Equal(t, s.Generation+1, reset.Generation)
	require.Equal(t, s.Attempt, reset.Attempt)
	require.Equal(t, uint64(1), reset.Rewinds)
	require.Zero(t, reset.Evicted)
	require.Zero(t, reset.FirstSequence)
	require.Zero(t, reset.LastSequence)
	p.recordReplayDiagnostic(time.Now(), row, nil)
	reset, _ = p.ReplayDiagnosticsSnapshot()
	require.Empty(t, reset.Records, "rewind cleared the completed-bank binding")
	publishDiagnostic(p, 3, 9, row)
	reset, _ = p.ReplayDiagnosticsSnapshot()
	require.Equal(t, uint64(1), reset.Records[0].Sequence)
	require.Equal(t, uint64(262), reset.Records[0].CompletedIndex)
	require.Equal(t, uint64(1), reset.Records[0].Rewinds)
	p.begin(9, nil)
	attempt, _ := p.ReplayDiagnosticsSnapshot()
	require.Empty(t, attempt.Records)
	require.Equal(t, reset.Generation+1, attempt.Generation)
	require.Equal(t, reset.Attempt+1, attempt.Attempt)
	require.Zero(t, attempt.Rewinds)
}

func TestReplayDiagnosticsCounterExhaustion(t *testing.T) {
	for _, counter := range []string{"sequence", "generation", "completed"} {
		t.Run(counter, func(t *testing.T) {
			p := diagnosticProgress(t)
			d := p.diagnostics.Load()
			switch counter {
			case "sequence":
				d.sequence = math.MaxUint64
			case "generation":
				d.generation = math.MaxUint64
				p.rewound()
			case "completed":
				s, _ := p.Snapshot()
				s.ExecutedBlocks = math.MaxUint64
				p.latest.Store(&s)
			}
			publishDiagnostic(p, 1, 1, &metrics.BlockReplay{})
			s, _ := p.ReplayDiagnosticsSnapshot()
			require.True(t, s.Exhausted)
			require.Empty(t, s.Records)
			p.begin(5, nil)
			publishDiagnostic(p, 5, 1, &metrics.BlockReplay{})
			s, _ = p.ReplayDiagnosticsSnapshot()
			require.True(t, s.Exhausted)
			require.Empty(t, s.Records)
		})
	}
}

func TestReplayDiagnosticsConcurrentDetachedSnapshots(t *testing.T) {
	p := diagnosticProgress(t)
	done := make(chan struct{})
	var readers sync.WaitGroup
	for i := 0; i < 3; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-done:
					return
				default:
					s, ok := p.ReplayDiagnosticsSnapshot()
					if !ok || len(s.Records) > ReplayDiagnosticCapacity {
						t.Error("missing or oversized snapshot")
						return
					}
					for j, r := range s.Records {
						if r.Attempt != s.Attempt || r.Rewinds != s.Rewinds || r.Sequence != s.FirstSequence+uint64(j) {
							t.Error("mixed generation or torn record")
							return
						}
						s.Records[j].Slot = 0
					}
				}
			}
		}()
	}
	for i := uint64(1); i <= 600; i++ {
		publishDiagnostic(p, i, 1, &metrics.BlockReplay{})
		if i == 300 {
			p.rewound()
		}
		if i == 450 {
			p.begin(i, nil)
		}
	}
	close(done)
	readers.Wait()
}

func TestReplayDiagnosticsDoesNotReadSharedExecutionTimers(t *testing.T) {
	p := diagnosticProgress(t)
	row := &metrics.BlockReplay{}
	shared := []*metrics.Timing{
		&row.InstructionsAndAccountMetasFromTx, &row.ComputeBudgetExecutionInstructions,
		&row.AccountsFromTx, &row.CalcAndDeductFees, &row.IxLoop,
		&row.SbpfInterpreterNew, &row.SbpfInterpreterRun, &row.AddProgramToCache,
		&row.GetProgramAccount, &row.GetProgramDataUncachedAccountsDb,
		&row.GetProgramDataUncachedAccounts, &row.GetProgramDataUncachedMarshal,
	}
	done, joined := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(joined)
		for {
			select {
			case <-done:
				return
			default:
				for _, timing := range shared {
					timing.AddTiming(time.Nanosecond)
				}
			}
		}
	}()
	for i := uint64(0); i < 300; i++ {
		publishDiagnostic(p, i, 1, row)
	}
	close(done)
	<-joined
	s, ok := p.ReplayDiagnosticsSnapshot()
	require.True(t, ok)
	require.Len(t, s.Records, ReplayDiagnosticCapacity)
	for _, r := range s.Records {
		require.True(t, r.SharedExecutionTimingsUnavailable)
	}
}

func TestReplayDiagnosticsFromExecutedSerialAndStreamedBanks(t *testing.T) {
	priorMetrics, priorStreaming := metrics.GlobalBlockReplay, StreamingExecutionCfg
	t.Cleanup(func() { metrics.GlobalBlockReplay, StreamingExecutionCfg = priorMetrics, priorStreaming })
	for _, streamed := range []bool{false, true} {
		p := diagnosticProgress(t)
		metrics.GlobalBlockReplay = metrics.BlockReplay{}
		txs := transferTransactions(t, 8, 989_000)
		var outcome lifecycleOutcome
		if streamed {
			StreamingExecutionCfg = StreamingExecutionConfig{Enabled: true}
			outcome, _ = lifecycleStream(t, newLifecycleEnv(t), txs, []int{3}, 2, 4)
		} else {
			outcome = lifecycleWholeBlock(t, newLifecycleEnv(t), txs, 4)
		}
		require.NotEmpty(t, outcome.bankhash)
		metrics.GlobalBlockReplay.Slot = lifecycleSlot
		p.completed(CompletedBlock{At: time.Now(), Slot: lifecycleSlot, Bankhash: solana.HashFromBytes(outcome.bankhash), Transactions: uint64(len(txs))}, 0, false)
		p.recordReplayDiagnostic(time.Now(), &metrics.GlobalBlockReplay, nil)
		s, ok := p.ReplayDiagnosticsSnapshot()
		require.True(t, ok)
		require.Len(t, s.Records, 1)
		r := s.Records[0]
		require.Equal(t, solana.HashFromBytes(outcome.bankhash), r.Bankhash)
		require.Equal(t, metrics.GlobalBlockReplay.AccountLoader, r.AccountLoader)
		require.Positive(t, r.Stages[ReplayStageTransactionLoop].Count)
		require.Positive(t, r.Stages[ReplayStageBankHash].Count)
		require.False(t, r.UnknownStages)
		if streamed {
			require.Positive(t, r.Stages[ReplayStageStreamingWait].Count)
		}
	}
}

func TestReplayDiagnosticDiscardCategories(t *testing.T) {
	for _, tc := range []struct {
		reason string
		want   ReplayDiagnosticDiscardReason
	}{
		{"other_block", ReplayDiscardOtherBlock}, {"update_parent", ReplayDiscardParentChanged},
		{"fork_switch", ReplayDiscardForkSwitch}, {"timeout", ReplayDiscardTimeout},
		{"gone", ReplayDiscardGenerationGone}, {"shutdown", ReplayDiscardShutdown},
		{"decode_error", ReplayDiscardInputRejected}, {"versions", ReplayDiscardInputRejected},
		{"plan", ReplayDiscardInputRejected}, {"prefix_bookkeeping", ReplayDiscardInputRejected},
		{"execution_mask", ReplayDiscardInputRejected}, {"status_validation", ReplayDiscardInputRejected},
		{"quarantined", ReplayDiscardInputRejected}, {"cancelled:untrusted text", ReplayDiscardCancelled},
		{"prefix_mismatch:transaction 27 identity", ReplayDiscardInputRejected},
		{"finalize:tail", ReplayDiscardFinalization}, {"arbitrary producer error", ReplayDiscardOther},
		{"", ReplayDiscardOther},
	} {
		require.Equal(t, tc.want, replayDiagnosticDiscardReason(true, tc.reason))
		require.NotEmpty(t, tc.want.String())
		require.Equal(t, ReplayDiscardNone, replayDiagnosticDiscardReason(false, tc.reason))
	}
	require.Empty(t, ReplayDiagnosticDiscardReason(255).String())
	require.Empty(t, ReplayDiagnosticScope(0).String())
	require.Empty(t, ReplayDiagnosticScope(255).String())
}

func TestReplayDiagnosticRecordHasNoMutableReferences(t *testing.T) {
	var check func(reflect.Type)
	check = func(typ reflect.Type) {
		if typ == reflect.TypeOf(time.Time{}) {
			return // Both dates are normalized to UTC before publication.
		}
		switch typ.Kind() {
		case reflect.Struct:
			for i := 0; i < typ.NumField(); i++ {
				check(typ.Field(i).Type)
			}
		case reflect.Array:
			check(typ.Elem())
		case reflect.Uint64, reflect.Bool, reflect.Uint8:
		default:
			t.Fatalf("record contains mutable or variable-sized field %s", typ)
		}
	}
	check(reflect.TypeOf(ReplayDiagnostic{}))
	t.Logf("record=%d bytes; fixed owner=%d bytes", unsafe.Sizeof(ReplayDiagnostic{}), unsafe.Sizeof(replayDiagnostics{}))
}

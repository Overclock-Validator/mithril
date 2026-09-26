package replay

import (
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/stretchr/testify/require"
)

type testCheckpointEncoder func() ([]byte, error)

func (f testCheckpointEncoder) MarshalBinary() ([]byte, error) { return f() }

func testCheckpointBytes(payload []byte) TransactionStatusSnapshot {
	owned := append([]byte(nil), payload...)
	return testCheckpointEncoder(func() ([]byte, error) { return append([]byte(nil), owned...), nil })
}

func TestAsyncCheckpointEncodingDoesNotRunDuringJobBuild(t *testing.T) {
	fc := &fakeCommitter{durable: accounts.NewMemAccounts()}
	tail := asyncTestTail(fc, 5, 6)
	started, release := make(chan struct{}), make(chan struct{})
	blockEncoding := false
	rootDir := t.TempDir()
	require.NoError(t, tail.SetTransactionStatusCheckpointHooks(TransactionStatusCheckpointHooks{
		Capture: func(through uint64) (TransactionStatusSnapshot, error) {
			require.Equal(t, uint64(6), through)
			return testCheckpointEncoder(func() ([]byte, error) {
				if !blockEncoding {
					return nil, errors.New("encoder ran during job construction")
				}
				close(started)
				<-release
				return []byte("encoded-on-worker"), nil
			}), nil
		},
		Install: func(through uint64, payload []byte) (*state.TransactionStatusCheckpointRef, error) {
			return PrepareTransactionStatusCheckpoint(rootDir, through, payload)
		},
	}))
	job, err := tail.buildFoldJob(6, false)
	require.NoError(t, err)
	select {
	case <-started:
		t.Fatal("job construction ran the encoder")
	default:
	}
	promoter := newAsyncPromoter(fc)
	// Always unblock the worker before draining it, including a failed assertion.
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer promoter.stop()
	defer unblock()
	blockEncoding = true
	promoter.enqueue(job)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("checkpoint worker did not start encoding")
	}
	// Replay can publish a later bank while the worker's encoder is blocked.
	tail.Add(7, []*accounts.Account{testAccount(3, 7)}, testHashBytes(7))
	tail.SetContext(7, &state.ResumeContext{Slot: 7})
	require.Equal(t, 3, tail.overlay.HeldSlots())
	require.Nil(t, promoter.poll())

	unblock()
	result := promoter.drain()
	require.NotNil(t, result)
	require.NoError(t, result.err)
	require.Nil(t, result.job.transactionStatusSnapshot)
	tail.applyFoldJob(result.job)
	require.Equal(t, 1, tail.overlay.HeldSlots())
}

func TestCheckpointCaptureFailureOrdering(t *testing.T) {
	cases := []struct {
		name       string
		capture    func(uint64) (TransactionStatusSnapshot, error)
		want       string
		buildFails bool
	}{
		{"nil capture", func(uint64) (TransactionStatusSnapshot, error) { return nil, nil }, "capture is nil", true},
		{"capture error", func(uint64) (TransactionStatusSnapshot, error) { return nil, errors.New("capture failed") }, "capture failed", true},
		{"encode error", func(uint64) (TransactionStatusSnapshot, error) {
			return testCheckpointEncoder(func() ([]byte, error) { return nil, errors.New("encode failed") }), nil
		}, "encode failed", false},
		{"empty encoding", func(uint64) (TransactionStatusSnapshot, error) { return testCheckpointBytes(nil), nil }, "snapshot is empty", false},
	}
	for _, tc := range cases {
		for _, forced := range []bool{false, true} {
			name := tc.name + "/async"
			if forced {
				name = tc.name + "/forced"
			}
			t.Run(name, func(t *testing.T) {
				fc := &fakeCommitter{durable: accounts.NewMemAccounts()}
				tail := asyncTestTail(fc, 5, 6)
				installed := false
				require.NoError(t, tail.SetTransactionStatusCheckpointHooks(TransactionStatusCheckpointHooks{
					Capture: tc.capture,
					Install: func(uint64, []byte) (*state.TransactionStatusCheckpointRef, error) {
						installed = true
						return nil, errors.New("unexpected install")
					},
				}))
				if forced {
					through, _, err := tail.flush(6)
					require.ErrorContains(t, err, tc.want)
					require.Zero(t, through)
				} else {
					job, err := tail.buildFoldJob(6, false)
					if tc.buildFails {
						require.ErrorContains(t, err, tc.want)
						require.Nil(t, job)
					} else {
						require.NoError(t, err)
						require.ErrorContains(t, runFoldJob(fc, job), tc.want)
						require.Nil(t, job.transactionStatusSnapshot, "failed result retained its captured deltas")
					}
				}
				require.False(t, installed)
				require.Empty(t, fc.throughs)
				require.Equal(t, 2, tail.overlay.HeldSlots())
			})
		}
	}
}

func TestFoldCheckpointKeepsCapturedRootAfterLiveCacheAdvances(t *testing.T) {
	c := importedStatusCacheForTest(t)
	for slot := uint64(301); slot <= 350; slot++ {
		require.NoError(t, c.CommitBlock(captureTestBlock(slot, 1)))
	}
	want, err := legacyStatusSnapshotForTest(c, 320)
	require.NoError(t, err)
	fc := &fakeCommitter{durable: accounts.NewMemAccounts()}
	tail := asyncTestTail(fc, 319, 320, 321)
	rootDir := t.TempDir()
	require.NoError(t, tail.SetTransactionStatusCheckpointHooks(TransactionStatusCheckpointHooks{
		Capture: c.CaptureSnapshotThrough,
		Install: func(through uint64, payload []byte) (*state.TransactionStatusCheckpointRef, error) {
			return PrepareTransactionStatusCheckpoint(rootDir, through, payload)
		},
	}))
	job, err := tail.buildFoldJob(321, false)
	require.NoError(t, err)
	for slot := uint64(351); slot <= 660; slot++ {
		require.NoError(t, c.CommitBlock(captureTestBlock(slot, 1)))
		c.Root(slot - 20)
	}
	require.NoError(t, runFoldJob(fc, job))
	require.Nil(t, job.transactionStatusSnapshot, "completed result retained captured deltas")
	require.Positive(t, job.checkpointCaptureTime)
	require.Positive(t, job.checkpointEncodeTime)
	require.Equal(t, len(want), job.checkpointBytes)
	var manifest state.ResumeContext
	require.NoError(t, json.Unmarshal(fc.ctxs[320], &manifest))
	require.Equal(t, uint64(320), manifest.TransactionStatusCheckpoint.Root)
	got, err := ReadTransactionStatusCheckpoint(rootDir, manifest.TransactionStatusCheckpoint)
	require.NoError(t, err)
	require.Equal(t, want, got)
	restored, err := NewTransactionStatusCacheFromSnapshot(got)
	require.NoError(t, err)
	require.Equal(t, uint64(320), restored.RootedThrough())
	require.True(t, restored.CoverageComplete())
}

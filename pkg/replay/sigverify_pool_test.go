package replay

import (
	"crypto/ed25519"
	"crypto/rand"
	"strings"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/sigverifytelemetry"
	"github.com/gagliardetto/solana-go"
)

func signedTestSnapshot(t *testing.T, corrupt bool) *sigverifySnapshot {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	message := []byte("sigverify pool test message")
	sig := ed25519.Sign(priv, message)
	if corrupt {
		sig[0] ^= 0xFF
	}
	var signer solana.PublicKey
	copy(signer[:], pub)
	var signature solana.Signature
	copy(signature[:], sig)
	return &sigverifySnapshot{
		slot:       9,
		signers:    []solana.PublicKey{signer},
		signatures: []solana.Signature{signature},
		firstKeys:  []solana.PublicKey{signer},
		message:    message,
	}
}

// The pool verifies valid snapshots and releases the block's WaitGroup —
// the join contract ProcessBlock relies on.
func TestSigverifyPoolVerifiesAndJoins(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		enqueueSigverify(signedTestSnapshot(t, false), &wg)
	}
	wg.Wait() // hangs (test timeout) if any worker fails to Done()
}

func TestVerifySignaturesTelemetryPreservesVerdict(t *testing.T) {
	sigverifytelemetry.Enable(8)
	t.Cleanup(sigverifytelemetry.Disable)
	var wg sync.WaitGroup
	wg.Add(1)
	verifySignatures(signedTestSnapshot(t, false), &wg)
	wg.Wait()

	s := sigverifytelemetry.Current()
	if s.Transactions != 1 || s.Signatures != 1 || s.VerificationAttempts != 1 || s.ExactDuplicateHits != 0 {
		t.Fatalf("telemetry snapshot = %+v", s)
	}
	if len(s.VerificationTrace) != 1 || s.VerificationTrace[0].Outcome != sigverifytelemetry.VerificationOutcomeValid {
		t.Fatalf("telemetry trace = %+v", s.VerificationTrace)
	}
}

func TestCollectSigverifyTelemetryBatchPreservesJobBoundaries(t *testing.T) {
	first := sigverifyJob{snapshot: &sigverifySnapshot{signatures: make([]solana.Signature, 1)}}
	in := make(chan sigverifyJob, 2)
	in <- sigverifyJob{snapshot: &sigverifySnapshot{signatures: make([]solana.Signature, 2)}}
	in <- sigverifyJob{snapshot: &sigverifySnapshot{signatures: make([]solana.Signature, 3)}}
	close(in)

	jobs, counts, queuedBefore, queuedAfter := collectSigverifyTelemetryBatch(first, in)
	if len(jobs) != 3 || queuedBefore != 2 || queuedAfter != 0 {
		t.Fatalf("batch shape: jobs=%d queued=%d->%d", len(jobs), queuedBefore, queuedAfter)
	}
	want := []uint16{1, 2, 3}
	for i := range want {
		if counts[i] != want[i] {
			t.Fatalf("signature counts = %v, want %v", counts, want)
		}
	}
}

func TestPassiveTelemetryPreservesOneJobWorkerClaims(t *testing.T) {
	sigverifytelemetry.Enable(32)
	t.Cleanup(sigverifytelemetry.Disable)
	in := make(chan sigverifyJob, 3)
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		in <- sigverifyJob{snapshot: signedTestSnapshot(t, false), wg: &wg}
	}
	close(in)
	runSigverifyJobs(in)
	wg.Wait()

	s := sigverifytelemetry.Current()
	if s.Mode != sigverifytelemetry.CollectionPassive || len(s.DispatchTrace) != 3 || len(s.VerificationTrace) != 3 {
		t.Fatalf("passive trace = %+v", s)
	}
	for i, dispatch := range s.DispatchTrace {
		if dispatch.Mode != sigverifytelemetry.CollectionPassive || len(dispatch.JobSignatures) != 1 || dispatch.JobSignatures[0] != 1 {
			t.Fatalf("dispatch[%d] = %+v", i, dispatch)
		}
		attempt := s.VerificationTrace[i]
		if attempt.DispatchID != dispatch.DispatchID || attempt.JobIndex != 0 || attempt.LaneIndex != 0 {
			t.Fatalf("attempt[%d] correlation = %+v, dispatch=%+v", i, attempt, dispatch)
		}
	}
}

func TestSchedulingSimulationClaimsVisibleReplayGroup(t *testing.T) {
	sigverifytelemetry.EnableSchedulingSimulation(32)
	t.Cleanup(sigverifytelemetry.Disable)
	in := make(chan sigverifyJob, 3)
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		in <- sigverifyJob{snapshot: signedTestSnapshot(t, false), wg: &wg}
	}
	close(in)
	runSigverifyJobs(in)
	wg.Wait()

	s := sigverifytelemetry.Current()
	if s.Mode != sigverifytelemetry.CollectionSchedulingSimulation || len(s.DispatchTrace) != 1 || len(s.VerificationTrace) != 3 {
		t.Fatalf("simulation trace = %+v", s)
	}
	dispatch := s.DispatchTrace[0]
	if dispatch.Mode != sigverifytelemetry.CollectionSchedulingSimulation || dispatch.SignatureLanes != 3 || len(dispatch.JobSignatures) != 3 {
		t.Fatalf("simulation dispatch = %+v", dispatch)
	}
	for i, attempt := range s.VerificationTrace {
		if attempt.DispatchID != dispatch.DispatchID || attempt.JobIndex != uint32(i) || attempt.LaneIndex != 0 {
			t.Fatalf("attempt[%d] correlation = %+v", i, attempt)
		}
	}
}

// An invalid signature still halts — same deliberate panic semantics as the
// per-goroutine version this pool replaced.
func TestVerifySignaturesPanicsOnBadSignature(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on invalid signature")
		}
		if msg, ok := r.(string); !ok || !strings.Contains(msg, "invalid signature") {
			t.Fatalf("panic = %v, want invalid-signature message", r)
		}
	}()
	var wg sync.WaitGroup
	wg.Add(1)
	verifySignatures(signedTestSnapshot(t, true), &wg)
}

// Failure diagnostics render from raw snapshot data on demand — the base58
// work deliberately deferred off the execution path.
func TestSigverifyDiagContextRendersFromRawData(t *testing.T) {
	s := signedTestSnapshot(t, false)
	diag := s.diagContext()
	if !strings.Contains(diag, s.txSigString()) || !strings.Contains(diag, s.signers[0].String()) {
		t.Fatalf("diag context missing identifiers: %s", diag)
	}
}

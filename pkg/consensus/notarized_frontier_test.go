package consensus

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/gagliardetto/solana-go"
)

func requireEngineFrontier(t *testing.T, e *AlpenglowObserverEngine, want uint64, known bool) {
	t.Helper()
	s := e.MonitoringSnapshot()
	if s.NotarizedSlot != want || s.HasNotarizedSlot != known {
		t.Fatalf("monitored frontier = (%d, %t), want (%d, %t)", s.NotarizedSlot, s.HasNotarizedSlot, want, known)
	}
	if got, ok := e.ensureObserver().NotarizedSlot(); got != want || ok != known {
		t.Fatalf("observer frontier = (%d, %t), want (%d, %t)", got, ok, want, known)
	}
}

func TestEngineNotarizedFrontierVerifiedKinds(t *testing.T) {
	for _, kind := range []alpenglow.CertificateType{alpenglow.CertificateNotarize, alpenglow.CertificateNotarizeFallback, alpenglow.CertificateFinalizeFast, alpenglow.CertificateSkip} {
		t.Run(string(kind), func(t *testing.T) {
			e := lifecycleEngine(t, true, true)
			requireEngineFrontier(t, e, 0, false)
			for i, slot := range []uint64{0, 42} {
				cert := lifecycleSignedCertificate(t, kind, lifecycleID(slot, 1))
				// This entry point performs actual aggregate BLS and stake checks.
				e.observeVotorMessage(alpenglow.NewCertificateMessage(cert))
				requireEngineFrontier(t, e, slot, true)
				if got := e.ensureObserver().Snapshot(); got.CertificatesObserved != uint64(i+1) || got.SignatureVerifiedCertificates != uint64(i+1) || got.StakeCheckedCertificates != uint64(i+1) {
					t.Fatalf("certificate did not traverse verified accepted output: %+v", got)
				}
				e.observeVotorMessage(alpenglow.NewCertificateMessage(cert))
				requireEngineFrontier(t, e, slot, true)
			}
			// Stay within the pool window retained after fast finalization.
			lower := lifecycleSignedCertificate(t, kind, lifecycleID(41, 1))
			e.ObserveFooterCertificates([]alpenglow.Certificate{lower})
			requireEngineFrontier(t, e, 42, true)
			if got := e.ensureObserver().Snapshot().CertificatesObserved; got != 3 {
				t.Fatalf("lower verified footer not accepted exactly once: %d", got)
			}
			if e.MonitoringSnapshot().SafetyFault {
				t.Fatal("valid certificate sequence caused a safety fault")
			}
		})
	}
}

func TestEngineNotarizedFrontierFinalityIsSeparate(t *testing.T) {
	t.Run("slow-pair", func(t *testing.T) {
		e := lifecycleEngine(t, true, true)
		id := lifecycleID(42, 1)
		e.ObserveFooterCertificates([]alpenglow.Certificate{lifecycleSignedCertificate(t, alpenglow.CertificateFinalize, id)})
		requireEngineFrontier(t, e, 0, false)
		if e.ensureObserver().Snapshot().CertificatesObserved != 1 {
			t.Fatal("standalone finalize was not accepted")
		}
		if _, ok := e.ensureChain().FinalizedBlockAt(id.Slot); ok {
			t.Fatal("standalone finalize selected a block")
		}
		e.ObserveFooterCertificates([]alpenglow.Certificate{lifecycleSignedCertificate(t, alpenglow.CertificateNotarize, id)})
		requireEngineFrontier(t, e, id.Slot, true)
		if got, ok := e.ensureChain().FinalizedBlockAt(id.Slot); !ok || got != id {
			t.Fatalf("complete slow pair did not finalize exact block: %+v, %t", got, ok)
		}
		e.ObserveFooterCertificates([]alpenglow.Certificate{lifecycleSignedCertificate(t, alpenglow.CertificateFinalize, lifecycleID(100, 2))})
		requireEngineFrontier(t, e, id.Slot, true)
	})
	t.Run("genesis", func(t *testing.T) {
		e := lifecycleEngine(t, true, true)
		id := lifecycleID(0, 1)
		e.ObserveFooterCertificates([]alpenglow.Certificate{lifecycleSignedCertificate(t, alpenglow.CertificateGenesis, id)})
		requireEngineFrontier(t, e, 0, false)
		if got, ok := e.ensureChain().FinalizedBlockAt(0); !ok || got != id {
			t.Fatalf("genesis certificate did not reach finality: %+v, %t", got, ok)
		}
	})
}

func TestEngineNotarizedFrontierRejectsUnacceptedInputs(t *testing.T) {
	for _, name := range []string{"bad-signature", "missing-stakes", "below-root", "missing-signature-verification", "missing-stake-verification", "invalid-pool-input"} {
		t.Run(name, func(t *testing.T) {
			e := lifecycleEngine(t, true, name != "missing-stakes")
			cert := lifecycleSignedCertificate(t, alpenglow.CertificateSkip, lifecycleID(42, 1))
			switch name {
			case "bad-signature":
				cert.Signature[0] ^= 0xff
				e.observeVotorMessage(alpenglow.NewCertificateMessage(cert))
			case "missing-stakes":
				e.observeVotorMessage(alpenglow.NewCertificateMessage(cert))
			case "below-root":
				e.SetAlpenglowRoot(lifecycleID(100, 2))
				e.ObserveFooterCertificates([]alpenglow.Certificate{cert})
			default:
				// Exercise the pool's trust contract directly, not cryptographic verification.
				cert.SignatureVerified, cert.StakeVerified = true, true
				switch name {
				case "missing-signature-verification":
					cert.SignatureVerified = false
				case "missing-stake-verification":
					cert.StakeVerified = false
				case "invalid-pool-input":
					cert.BlockHash = solana.Hash{1}
				}
				if update, accepted := e.acceptVerifiedCertificate(cert); accepted || len(update.Certificates) != 0 {
					t.Fatal("pool accepted invalid or unverified input")
				}
			}
			requireEngineFrontier(t, e, 0, false)
			if got := e.ensureObserver().Snapshot().CertificatesObserved; got != 0 {
				t.Fatalf("rejected input reached observer: %d", got)
			}
			if name == "missing-stakes" {
				if err := e.SetAlpenglowValidatorSet(testAlpenglowValidatorSet()); err != nil {
					t.Fatal(err)
				}
				requireEngineFrontier(t, e, cert.Slot, true)
			}
		})
	}
}

func TestEngineNotarizedFrontierRejectsUnderThreshold(t *testing.T) {
	e := lifecycleEngine(t, true, false)
	set := voterTestValidatorSet(t, voterTestKey(51), voterTestKey(52), solana.PublicKey{1})
	set.TotalStake = 90 // Three real BLS keys, each with stake 30.
	e.SetAlpenglowEpochLookup(func(uint64) uint64 { return set.Epoch })
	if err := e.SetAlpenglowValidatorSet(set); err != nil {
		t.Fatal(err)
	}
	cert := voterTestAggregateCertificate(t, 42, solana.Hash{1}, []int{0}, 0)
	e.observeVotorMessage(alpenglow.NewCertificateMessage(cert))
	requireEngineFrontier(t, e, 0, false)
	if e.ensureObserver().Snapshot().CertificatesObserved != 0 {
		t.Fatal("under-threshold certificate reached observer")
	}
	// A sufficient aggregate for the same logical key must still be admitted.
	cert = voterTestAggregateCertificate(t, 42, solana.Hash{1}, []int{0, 1}, 0)
	e.observeVotorMessage(alpenglow.NewCertificateMessage(cert))
	requireEngineFrontier(t, e, 42, true)
}

func TestEngineNotarizedFrontierSurvivesPruningAndNewEngineResets(t *testing.T) {
	e := lifecycleEngine(t, true, true)
	cert := lifecycleSignedCertificate(t, alpenglow.CertificateNotarizeFallback, lifecycleID(42, 1))
	e.ObserveFooterCertificates([]alpenglow.Certificate{cert})
	requireEngineFrontier(t, e, 42, true)
	e.applyAlpenglowDurableRoot(100)
	if retained := e.ensurePool().CertificatesAtOrAfter(0); len(retained) != 0 {
		t.Fatalf("pool did not prune old certificate: %+v", retained)
	}
	before := e.ensureObserver().Snapshot()
	for range 10 {
		requireEngineFrontier(t, e, 42, true)
	}
	if after := e.ensureObserver().Snapshot(); before != after {
		t.Fatal("monitoring reads mutated observer history")
	}
	fresh := lifecycleEngine(t, true, true)
	requireEngineFrontier(t, fresh, 0, false)
}

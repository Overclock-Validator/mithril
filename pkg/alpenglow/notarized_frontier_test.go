package alpenglow

import (
	"math"
	"sync"
	"testing"
)

// These observer tests supply trusted, basic-valid inputs. Signature and stake
// verification belong to the engine-boundary tests, not to this getter.
func notarizedFrontierCertificate(kind CertificateType, slot uint64) Certificate {
	c := Certificate{Type: kind, Slot: slot}
	if kind.HasBlock() {
		c.BlockHash = testHash(7)
	}
	return c
}

func requireObserverFrontier(t *testing.T, o *Observer, want uint64, known bool) {
	t.Helper()
	if got, ok := o.NotarizedSlot(); got != want || ok != known {
		t.Fatalf("notarized frontier = (%d, %t), want (%d, %t)", got, ok, want, known)
	}
}

func TestObserverNotarizedFrontierKindsAndZero(t *testing.T) {
	for _, kind := range []CertificateType{CertificateNotarize, CertificateNotarizeFallback, CertificateFinalizeFast, CertificateSkip} {
		t.Run(string(kind), func(t *testing.T) {
			o := NewObserver()
			requireObserverFrontier(t, o, 0, false)
			for _, slot := range []uint64{0, 42, math.MaxUint64} {
				cert := notarizedFrontierCertificate(kind, slot)
				if _, err := o.ObserveCertificate(cert); err != nil {
					t.Fatal(err)
				}
				requireObserverFrontier(t, o, slot, true)
				if obs, err := o.ObserveCertificate(cert); err != nil || obs.New {
					t.Fatalf("duplicate observation: new=%t err=%v", obs.New, err)
				}
				requireObserverFrontier(t, o, slot, true)
			}
			if _, err := o.ObserveCertificate(notarizedFrontierCertificate(kind, 1)); err != nil {
				t.Fatal(err)
			}
			requireObserverFrontier(t, o, math.MaxUint64, true)
		})
	}
}

func TestObserverNotarizedFrontierExcludesOtherEvidence(t *testing.T) {
	o := NewObserver()
	for _, kind := range []CertificateType{CertificateFinalize, CertificateGenesis} {
		if _, err := o.ObserveCertificate(notarizedFrontierCertificate(kind, 90)); err != nil {
			t.Fatal(err)
		}
		requireObserverFrontier(t, o, 0, false)
	}
	if _, err := o.ObserveVote(VoteMessage{Vote: NewSkipVote(100)}); err != nil {
		t.Fatal(err)
	}
	o.ObserveReplayBlock(ReplayBlockObservation{Block: BlockID{Slot: 101, Hash: testHash(8)}})
	requireObserverFrontier(t, o, 0, false)
	if _, err := o.ObserveCertificate(Certificate{Type: CertificateNotarize, Slot: 102}); err == nil {
		t.Fatal("hashless notarization was not rejected")
	}
	requireObserverFrontier(t, o, 0, false)
}

func TestObserverNotarizedFrontierSurvivesRetention(t *testing.T) {
	for _, limit := range []int{0, 1} {
		o := NewObserverWithConfig(ObserverConfig{MaxTrackedCertificates: limit})
		for i, kind := range []CertificateType{CertificateNotarize, CertificateNotarizeFallback, CertificateFinalizeFast, CertificateSkip} {
			if _, err := o.ObserveCertificate(notarizedFrontierCertificate(kind, uint64(10+i))); err != nil {
				t.Fatal(err)
			}
			requireObserverFrontier(t, o, uint64(10+i), true)
		}
		if _, err := o.ObserveCertificate(notarizedFrontierCertificate(CertificateFinalize, 100)); err != nil {
			t.Fatal(err)
		}
		if got := len(o.certificates); got != limit {
			t.Fatalf("retained %d certificates, want %d", got, limit)
		}
		// The scalar getter must not invoke pending-history reconciliation.
		o.pendingStatsValid = false
		if allocs := testing.AllocsPerRun(100, func() { o.NotarizedSlot() }); allocs != 0 {
			t.Fatalf("scalar getter allocated %g times", allocs)
		}
		requireObserverFrontier(t, o, 13, true)
		if o.pendingStatsValid {
			t.Fatal("scalar getter recomputed retained pending-certificate statistics")
		}
	}
	requireObserverFrontier(t, NewObserver(), 0, false)
}

func TestObserverNotarizedFrontierConcurrent(t *testing.T) {
	o := NewObserverWithConfig(ObserverConfig{MaxTrackedCertificates: 2})
	var wg sync.WaitGroup
	for _, kind := range []CertificateType{CertificateNotarize, CertificateNotarizeFallback, CertificateFinalizeFast, CertificateSkip} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for slot := uint64(0); slot < 128; slot++ {
				if _, err := o.ObserveCertificate(notarizedFrontierCertificate(kind, slot)); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var previous uint64
			var seen bool
			for range 512 {
				slot, known := o.NotarizedSlot()
				if (seen && (!known || slot < previous)) || slot > 127 {
					t.Errorf("frontier regressed or escaped inputs: previous=(%d,%t), current=(%d,%t)", previous, seen, slot, known)
					return
				}
				previous, seen = slot, known
			}
		}()
	}
	wg.Wait()
	requireObserverFrontier(t, o, 127, true)
}

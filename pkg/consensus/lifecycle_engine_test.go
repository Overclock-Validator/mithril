package consensus

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/block"
)

func lifecycleObserve(t *testing.T, e *AlpenglowObserverEngine, id alpenglow.BlockID, trustedIngress bool) {
	t.Helper()
	now := time.Now()
	b := &block.Block{Slot: id.Slot, AlpenglowBlockID: [32]byte(id.Hash), HasAlpenglowBlockID: id.HasHash()}
	if trustedIngress {
		b.MarkTurbineIngressTimings(block.TurbineIngressTimings{AssemblyFirstAcceptedAt: now, AssemblyFullAt: now, ReadyAt: now})
	} else {
		// Public wall fields alone must not establish trusted live ingress.
		b.ShredFirstNanos, b.ShredFullNanos = now.UnixNano(), now.UnixNano()
	}
	if err := e.ObserveBlock(context.Background(), BlockObservation{Block: b, Source: "turbine", At: now}); err != nil {
		t.Fatal(err)
	}
}

func lifecycleEngineSnapshot(t *testing.T, e *AlpenglowObserverEngine) LifecycleSnapshot {
	t.Helper()
	s := e.MonitoringSnapshot().Lifecycle
	if s == nil {
		t.Fatal("enabled monitoring omitted lifecycle snapshot")
	}
	return *s
}

func TestLifecycleEngineRequiresExactReplayIdentity(t *testing.T) {
	for _, name := range []string{"exact", "mismatch", "hashless", "hashless-candidate", "missing-candidate", "untrusted-ingress", "replaced-candidate", "disabled"} {
		t.Run(name, func(t *testing.T) {
			e := lifecycleEngine(t, name != "disabled", false)
			id := lifecycleID(42, 1)
			if name == "hashless-candidate" {
				lifecycleObserve(t, e, alpenglow.BlockID{Slot: id.Slot}, true)
			} else if name != "missing-candidate" {
				lifecycleObserve(t, e, id, name != "untrusted-ingress")
			}
			if name == "replaced-candidate" {
				lifecycleObserve(t, e, lifecycleID(id.Slot, 2), true)
			}
			result := SlotReplayResult{Slot: id.Slot, Bankhash: [32]byte(id.Hash), AlpenglowBlockID: [32]byte(id.Hash), At: time.Now()}
			switch name {
			case "mismatch":
				result.AlpenglowBlockID = [32]byte(lifecycleID(id.Slot, 2).Hash)
			case "hashless":
				result.AlpenglowBlockID = [32]byte{}
			}
			if err := e.OnReplayResult(context.Background(), result); err != nil {
				t.Fatalf("legacy replay behavior changed: %v", err)
			}
			if name == "disabled" {
				if e.lifecycle != nil || e.MonitoringSnapshot().Lifecycle != nil {
					t.Fatal("disabled monitoring allocated/published lifecycle")
				}
				return
			}
			s := lifecycleEngineSnapshot(t, e)
			if name == "missing-candidate" || name == "hashless-candidate" {
				if s.Retained != 0 {
					t.Fatalf("unstaged result created lifecycle evidence: %+v", s)
				}
				return
			}
			row := lifecycleFind(t, s, id)
			wantReplay := name == "exact" || name == "untrusted-ingress"
			if row.ReplayAcceptedAt.IsZero() == wantReplay {
				t.Fatalf("exact replay requirement not enforced: %+v", row)
			}
			if row.HasIngress != (name != "untrusted-ingress") {
				t.Fatalf("ingress trust marker ignored: %+v", row)
			}
			if !row.DirectFinalizedAt.IsZero() {
				t.Fatal("successful replay manufactured finality")
			}
			if name == "replaced-candidate" {
				other := lifecycleFind(t, s, lifecycleID(id.Slot, 2))
				if !other.ReplayAcceptedAt.IsZero() {
					t.Fatal("replacement candidate borrowed another block's replay result")
				}
			}
		})
	}
}

func TestLifecycleEngineSafetyFaultPreventsReplayTiming(t *testing.T) {
	e := lifecycleEngine(t, true, true)
	id := lifecycleID(42, 1)
	lifecycleObserve(t, e, id, true)
	e.ObserveFooterCertificates([]alpenglow.Certificate{lifecycleSignedCertificate(t, alpenglow.CertificateFinalizeFast, id)})
	before := lifecycleEngineSnapshot(t, e)
	if err := e.ObserveObjectivelyInvalidAlpenglowBlock(id, "test invalid finalized block"); err == nil {
		t.Fatal("invalidating a verified-finalized block did not latch a safety fault")
	}
	if err := e.OnReplayResult(context.Background(), SlotReplayResult{Slot: id.Slot, AlpenglowBlockID: [32]byte(id.Hash), At: time.Now()}); err == nil {
		t.Fatal("replay callback succeeded through safety fault")
	}
	if !e.MonitoringSnapshot().SafetyFault || !reflect.DeepEqual(before, lifecycleEngineSnapshot(t, e)) {
		t.Fatal("failed callback changed historical lifecycle or hid safety fault")
	}
}

func TestLifecycleEngineVerifiedFinalityJoinsOnlyMatchingFork(t *testing.T) {
	for _, finalityFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "replay-first", true: "certificate-first"}[finalityFirst], func(t *testing.T) {
			e := lifecycleEngine(t, true, true)
			id, other := lifecycleID(42, 1), lifecycleID(42, 2)
			cert := lifecycleSignedCertificate(t, alpenglow.CertificateFinalizeFast, id)
			beforeFinality := time.Now()
			if finalityFirst {
				e.ObserveFooterCertificates([]alpenglow.Certificate{cert})
			}
			lifecycleObserve(t, e, other, true)
			if err := e.OnReplayResult(context.Background(), SlotReplayResult{Slot: other.Slot, AlpenglowBlockID: [32]byte(other.Hash), At: time.Now()}); err != nil {
				t.Fatal(err)
			}
			lifecycleObserve(t, e, id, true)
			if err := e.OnReplayResult(context.Background(), SlotReplayResult{Slot: id.Slot, AlpenglowBlockID: [32]byte(id.Hash), At: time.Now()}); err != nil {
				t.Fatal(err)
			}
			if !finalityFirst {
				beforeFinality = time.Now()
				e.ObserveFooterCertificates([]alpenglow.Certificate{cert})
			}
			afterFinality := time.Now()
			s := lifecycleEngineSnapshot(t, e)
			row := lifecycleFind(t, s, id)
			if row.DirectFinalizedAt.Before(beforeFinality) || row.DirectFinalizedAt.After(afterFinality) || row.FinalityKind != alpenglow.CertificateFinalizeFast || row.FinalityBeforeReplay != finalityFirst {
				t.Fatalf("wrong verified finality time or order: %+v", row)
			}
			if sibling := lifecycleFind(t, s, other); !sibling.DirectFinalizedAt.IsZero() || sibling.ReplayAcceptedAt.IsZero() {
				t.Fatalf("same-slot alternate borrowed finality: %+v", sibling)
			}
			if e.MonitoringSnapshot().VotingEnabled {
				t.Fatal("non-voting observer unexpectedly enabled voting")
			}
			e.ObserveFooterCertificates([]alpenglow.Certificate{cert})
			if !reflect.DeepEqual(s, lifecycleEngineSnapshot(t, e)) {
				t.Fatal("duplicate certificate redated accepted finality")
			}
		})
	}
}

func TestLifecycleEngineSlowFinalityRequiresCertificatePair(t *testing.T) {
	for _, finalizationFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "notarization-first", true: "finalization-first"}[finalizationFirst], func(t *testing.T) {
			e := lifecycleEngine(t, true, true)
			id := lifecycleID(43, 9)
			notar := lifecycleSignedCertificate(t, alpenglow.CertificateNotarize, id)
			finalize := lifecycleSignedCertificate(t, alpenglow.CertificateFinalize, id)
			first, second := notar, finalize
			if finalizationFirst {
				first, second = finalize, notar
			}
			e.ObserveFooterCertificates([]alpenglow.Certificate{first})
			if s := lifecycleEngineSnapshot(t, e); s.Retained != 0 {
				t.Fatalf("incomplete slow pair became finalized: %+v", s)
			}
			e.ObserveFooterCertificates([]alpenglow.Certificate{second})
			row := lifecycleFind(t, lifecycleEngineSnapshot(t, e), id)
			if row.FinalityKind != alpenglow.CertificateFinalize || row.DirectFinalizedAt.IsZero() || !row.ReplayAcceptedAt.IsZero() || row.HasIngress {
				t.Fatalf("incorrect slow-pair lifecycle: %+v", row)
			}
		})
	}
}

func TestLifecycleEngineVerifiesGenesisAndDeferredCertificates(t *testing.T) {
	t.Run("genesis", func(t *testing.T) {
		e := lifecycleEngine(t, true, true)
		id := lifecycleID(0, 5)
		e.ObserveFooterCertificates([]alpenglow.Certificate{lifecycleSignedCertificate(t, alpenglow.CertificateGenesis, id)})
		row := lifecycleFind(t, lifecycleEngineSnapshot(t, e), id)
		if row.FinalityKind != alpenglow.CertificateGenesis || row.DirectFinalizedAt.IsZero() || !row.ReplayAcceptedAt.IsZero() {
			t.Fatalf("incorrect genesis evidence: %+v", row)
		}
	})
	t.Run("deferred", func(t *testing.T) {
		e := lifecycleEngine(t, true, false)
		id := lifecycleID(42, 6)
		e.ObserveFooterCertificates([]alpenglow.Certificate{lifecycleSignedCertificate(t, alpenglow.CertificateFinalizeFast, id)})
		if s := lifecycleEngineSnapshot(t, e); s.Retained != 0 {
			t.Fatalf("unverified pending certificate produced finality: %+v", s)
		}
		installedAt := time.Now()
		if err := e.SetAlpenglowValidatorSet(testAlpenglowValidatorSet()); err != nil {
			t.Fatal(err)
		}
		row := lifecycleFind(t, lifecycleEngineSnapshot(t, e), id)
		if row.DirectFinalizedAt.Before(installedAt) || row.FinalityKind != alpenglow.CertificateFinalizeFast {
			t.Fatalf("deferred finality uses pre-verification receipt time: %+v", row)
		}
	})
}

func TestLifecycleEngineRejectsInvalidAndPrunedFinality(t *testing.T) {
	for _, name := range []string{"invalid-signature", "invalid-block", "pruned"} {
		t.Run(name, func(t *testing.T) {
			e := lifecycleEngine(t, true, true)
			id := lifecycleID(42, 4)
			cert := lifecycleSignedCertificate(t, alpenglow.CertificateFinalizeFast, id)
			switch name {
			case "invalid-signature":
				cert.Signature[0] ^= 0xff
			case "invalid-block":
				if err := e.ObserveObjectivelyInvalidAlpenglowBlock(id, "test invalid block"); err != nil {
					t.Fatal(err)
				}
			case "pruned":
				// Exercise the chain's silent old-decision return without relying
				// on an earlier pool-floor rejection to protect the monitoring hook.
				e.ensureChain().PruneBeforeSlot(100)
			}
			e.ObserveFooterCertificates([]alpenglow.Certificate{cert})
			if s := lifecycleEngineSnapshot(t, e); s.Retained != 0 {
				t.Fatalf("%s produced finality timing: %+v", name, s)
			}
		})
	}
}

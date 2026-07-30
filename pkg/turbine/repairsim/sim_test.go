package repairsim

import (
	"reflect"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/turbine"
)

func testLedger(t *testing.T, slots, fecs int) *Ledger {
	t.Helper()
	ledger, err := GenerateLedger(LedgerConfig{
		StartSlot:     20_000,
		Slots:         slots,
		FECsPerSlot:   fecs,
		Seed:          7,
		ShredVersion:  11,
		ReferenceTick: 63,
	})
	if err != nil {
		t.Fatal(err)
	}
	return ledger
}

func deterministicConfig(scenario Scenario) Config {
	cfg := DefaultConfig(scenario)
	cfg.RepairLatency = 10 * time.Millisecond
	cfg.RepairJitter = time.Millisecond
	cfg.DuplicateProbability = 0
	cfg.BandwidthBytesPerSec = 0
	cfg.MaxConcurrent = 32
	cfg.Seed = 19
	return cfg
}

func TestGenerateLedgerHasExactFECCountAndValidPackets(t *testing.T) {
	ledger := testLedger(t, 2, 3)
	if ledger.Config.EntriesPerSlot == 0 {
		t.Fatal("entry count was not resolved")
	}
	for _, slot := range ledger.Slots {
		if len(slot.FECs) != 3 {
			t.Fatalf("slot %d FECs=%d, want 3", slot.Number, len(slot.FECs))
		}
		for _, fec := range slot.FECs {
			if len(fec.Data) != 32 || len(fec.Coding) != 32 {
				t.Fatalf("slot %d FEC %d shape=%d+%d", slot.Number, fec.Index, len(fec.Data), len(fec.Coding))
			}
			for _, packet := range append(append([]Packet(nil), fec.Data...), fec.Coding...) {
				shred, err := parseAndVerify(packet, ledger)
				if err != nil {
					t.Fatalf("slot %d FEC %d: %v", slot.Number, fec.Index, err)
				}
				if shred.Slot != slot.Number || shred.FECSetIndex != fec.Index {
					t.Fatalf("packet routing got slot=%d FEC=%d", shred.Slot, shred.FECSetIndex)
				}
			}
		}
	}
}

func TestNearTipRepairCompletesAndTraceIsDeterministic(t *testing.T) {
	ledger := testLedger(t, 3, 2)
	cfg := deterministicConfig(ScenarioNearTip)
	cfg.NaturalLateShreds = true

	first, err := Run(ledger, cfg)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Run(ledger, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if first.CompletedSlots != 3 || first.SpoolCompleteSlots != 3 {
		t.Fatalf("completed=%d spool=%d, want 3", first.CompletedSlots, first.SpoolCompleteSlots)
	}
	if first.LocallyRecoveredDataShreds == 0 || first.FECDecodes == 0 {
		t.Fatalf("recovered=%d decodes=%d, want both nonzero", first.LocallyRecoveredDataShreds, first.FECDecodes)
	}
	if first.CanceledOrLateResponses == 0 {
		t.Fatal("natural late-shred scenario did not produce canceled/late repair work")
	}
	if !reflect.DeepEqual(first.Trace, second.Trace) {
		t.Fatal("same seed/config produced different logical traces")
	}
}

func TestNearTipWithoutRepairRemainsIncomplete(t *testing.T) {
	ledger := testLedger(t, 2, 2)
	cfg := deterministicConfig(ScenarioNearTip)
	cfg.RepairEnabled = false
	cfg.NaturalLateShreds = false
	result, err := Run(ledger, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if result.CompletedSlots != 0 || result.SpoolCompleteSlots != 0 {
		t.Fatalf("completed=%d spool=%d without repair", result.CompletedSlots, result.SpoolCompleteSlots)
	}
}

func TestCompleteDeliveryEstablishesZeroRepairBaseline(t *testing.T) {
	ledger := testLedger(t, 2, 2)
	cfg := deterministicConfig(ScenarioNearTip)
	cfg.Availability = AvailabilityComplete
	cfg.RepairEnabled = false
	cfg.NaturalLateShreds = false
	result, err := Run(ledger, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if result.CompletedSlots != 2 {
		t.Fatalf("completed=%d, want 2", result.CompletedSlots)
	}
	if result.RepairRequests != 0 || result.LocallyRecoveredDataShreds != 0 {
		t.Fatalf("baseline requests=%d recovered=%d, want zero", result.RepairRequests, result.LocallyRecoveredDataShreds)
	}
	if result.ShredEd25519Verifications != 4 || result.ShredSignatureCacheHits != 124 {
		t.Fatalf("signature cache verifies=%d hits=%d, want 4/124 for four FEC roots",
			result.ShredEd25519Verifications, result.ShredSignatureCacheHits)
	}
}

func TestDeepCatchupMixedUsesThresholdRecovery(t *testing.T) {
	ledger := testLedger(t, 8, 2)
	cfg := deterministicConfig(ScenarioDeepCatchup)
	cfg.Availability = AvailabilityMixed
	cfg.NaturalLateShreds = false
	result, err := Run(ledger, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if result.CompletedSlots != len(ledger.Slots) {
		t.Fatalf("completed=%d, want %d", result.CompletedSlots, len(ledger.Slots))
	}
	if result.LocallyRecoveredDataShreds <= result.UsefulNetworkDataShreds {
		t.Fatalf("local recovery=%d, network data=%d; mixed threshold scenario should recover most losses locally", result.LocallyRecoveredDataShreds, result.UsefulNetworkDataShreds)
	}
	if result.RepairRequests == 0 || result.RepairBytesReceived == 0 {
		t.Fatal("deep catch-up completed without exercising repair")
	}
}

func TestCorruptRepairResponseRejectedThenRetried(t *testing.T) {
	ledger := testLedger(t, 1, 1)
	cfg := deterministicConfig(ScenarioDeepCatchup)
	cfg.Availability = AvailabilityMixed
	cfg.CorruptResponses = 1
	result, err := Run(ledger, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if result.CompletedSlots != 1 {
		t.Fatalf("completed=%d, want 1", result.CompletedSlots)
	}
	if result.RejectedCorruptResponses != 1 {
		t.Fatalf("rejected corrupt=%d, want 1", result.RejectedCorruptResponses)
	}
	if result.RepairRequests < 2 {
		t.Fatalf("requests=%d, want retry after corruption", result.RepairRequests)
	}
}

func parseAndVerify(packet Packet, ledger *Ledger) (*turbine.Shred, error) {
	shred, err := turbine.ParseShred(packet.Bytes)
	if err != nil {
		return nil, err
	}
	if err := shred.VerifySignature(ledger.LeaderPub); err != nil {
		return nil, err
	}
	return shred, nil
}

func BenchmarkScenarios(b *testing.B) {
	ledger, err := GenerateLedger(LedgerConfig{
		StartSlot: 30_000, Slots: 8, FECsPerSlot: 2, Seed: 23, ShredVersion: 1, ReferenceTick: 63,
	})
	if err != nil {
		b.Fatal(err)
	}
	tests := []struct {
		name         string
		scenario     Scenario
		availability Availability
	}{
		{name: "near-tip", scenario: ScenarioNearTip, availability: AvailabilityNearLoss},
		{name: "deep-mixed", scenario: ScenarioDeepCatchup, availability: AvailabilityMixed},
		{name: "deep-sparse", scenario: ScenarioDeepCatchup, availability: AvailabilitySparse},
	}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			cfg := DefaultConfig(tt.scenario)
			cfg.Availability = tt.availability
			cfg.RepairLatency = 0
			cfg.RepairJitter = 0
			cfg.DuplicateProbability = 0
			cfg.BandwidthBytesPerSec = 0
			cfg.NaturalLateShreds = false
			cfg.CollectTrace = false
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				result, err := Run(ledger, cfg)
				if err != nil {
					b.Fatal(err)
				}
				if result.CompletedSlots != len(ledger.Slots) {
					b.Fatalf("completed=%d", result.CompletedSlots)
				}
				b.ReportMetric(float64(result.RepairRequests), "repair-requests/op")
				b.ReportMetric(float64(result.LocallyRecoveredDataShreds), "recovered-shreds/op")
			}
		})
	}
}

package node

import (
	"math"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/consensus"
)

func TestConsensusNotarizedSlotPresenceAndPrecision(t *testing.T) {
	for _, slot := range []uint64{0, 1<<53 + 1, math.MaxUint64} {
		s := consensus.MonitoringSnapshot{NotarizedSlot: slot}
		if consensusMeasurement(s, time.Now()).NotarizedSlot != nil {
			t.Fatal("absent evidence became a zero or supplied slot")
		}
		s.HasNotarizedSlot = true
		m := consensusMeasurement(s, time.Now())
		if m.NotarizedSlot == nil || *m.NotarizedSlot != slot || m.FinalizedSlot != nil {
			t.Fatal("notarization evidence was lost or became direct finality")
		}
	}
}

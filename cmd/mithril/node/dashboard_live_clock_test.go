package node

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/consensus"
	"github.com/Overclock-Validator/mithril/pkg/dashboard"
)

func TestDashboardLiveClockActualEngineWithoutReceiver(t *testing.T) {
	engine := func() *consensus.AlpenglowObserverEngine {
		e, err := consensus.NewEngine(consensus.Config{MonitoringEnabled: true})
		if err != nil {
			t.Fatal(err)
		}
		if err = e.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := e.Close(); err != nil {
				t.Error(err)
			}
		})
		return e
	}
	e := engine()
	d := &nodeDashboard{networkDetails: true, state: dashboard.Snapshot{RunID: "clock-run", StartedAt: time.Now().Add(-time.Minute).UTC(), Alpenglow: true, Mode: "verifying"}}
	d.consensusStarted(e)
	binding := dashboard.NodeNetworkBinding{RunID: d.state.RunID, NodeStartedAt: d.state.StartedAt}
	if c, _ := d.captureLiveClock(binding); c != nil {
		t.Fatal("unanchored engine fabricated an estimate")
	}
	e.ObserveAlpenglowFirstShred(9007199254740993) // Controlled source event, not signature-verification proof.
	var original time.Time
	for _, role := range []string{"verifying", "validator"} {
		d.state.Mode = role
		c, until := d.captureLiveClock(binding)
		if c == nil || c.Generation != 1 || c.AnchorSlot != 9007199254740993 || !until.After(time.Now()) || until.Sub(c.SampledAt) > dashboard.NetworkDetailFreshFor {
			t.Fatalf("clock owner projection missing: %+v", c)
		}
		if original.IsZero() {
			original = c.AnchorAt
		} else if c.AnchorAt != original {
			t.Fatal("sampling redated the anchor")
		}
		c.EstimatedSlot = 0
		combined, err := d.sampleNodeNetwork(time.Now())
		if err != nil || combined.Detail.LiveClock == nil || combined.Detail.LiveClock.EstimatedSlot < 9007199254740993 ||
			combined.Detail.Receiver != nil || combined.Detail.Votor != nil || combined.Detail.VotorPayload != nil || combined.Detail.Interfaces != nil ||
			d.votorSocket.latest != nil || d.votorSocket.payloadLatest != nil || e.MonitoringSnapshot().Receiver != nil {
			t.Fatal("clock-only observation required or sampled a socket/receiver")
		}
	}
	wrong := binding
	wrong.RunID = "another-run"
	if c, _ := d.captureLiveClock(wrong); c != nil {
		t.Fatal("wrong node context accepted")
	}
	replacement := engine()
	d.consensusStarted(replacement)
	if c, _ := d.captureLiveClock(binding); c != nil {
		t.Fatal("replacement retained old clock")
	}
	replacement.ObserveAlpenglowFirstShred(1)
	if c, _ := d.captureLiveClock(binding); c == nil || c.Generation != 2 || c.AnchorSlot != 1 {
		t.Fatal("new lifetime lost lower anchor or generation")
	}
	d.state.Phase = "stopping"
	if c, _ := d.captureLiveClock(binding); c != nil {
		t.Fatal("stopping node published new clock")
	}
	d.state.Phase = "replaying"
	d.networkDetails = false
	if c, _ := d.captureLiveClock(binding); c != nil {
		t.Fatal("disabled details sampled clock")
	}
	d.networkDetails = true
	d.consensusStarted(nil)
	if c, _ := d.captureLiveClock(binding); c != nil {
		t.Fatal("detached engine published clock")
	}
	d.votorSocketGeneration = math.MaxUint64
	d.consensusStarted(e)
	if c, _ := d.captureLiveClock(binding); c != nil {
		t.Fatal("exhausted owner generation reused clock")
	}
}

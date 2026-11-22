package dashboard

import (
	"fmt"
	"testing"
	"time"
)

// Exercise the publication leases with the source and reader on different
// phases, as they are when the reader starts after the node.
func TestReplayDetailReaderStaggeredCaptureAvailability(t *testing.T) {
	for phase := 0; phase < int(replayDetailCaptureInterval/time.Second); phase++ {
		t.Run(fmt.Sprintf("phase_%d", phase), func(t *testing.T) {
			testReplayDetailReaderPhase(t, phase)
		})
	}
}

func testReplayDetailReaderPhase(t *testing.T, phase int) {
	t.Helper()
	core := replayDetailTestCore(0)
	start := core.CapturedAt
	r := replayDetailReaderFixture(t, core, start, "/unused")
	var generation string
	for seconds := 0; seconds <= 120; seconds++ {
		now := start.Add(time.Duration(seconds) * time.Second)
		gate := replayDetailTestGate(core, now)
		r.gate.Store(&gate)
		if seconds%int(replayDetailCaptureInterval/time.Second) == 0 {
			core.CapturedAt = now.UTC()
			var err error
			generation, err = ReplayDetailGeneration(core)
			if err != nil {
				t.Fatal(err)
			}
		}
		if seconds >= phase && (seconds-phase)%int(replayDetailReaderInterval/time.Second) == 0 {
			candidate, reason := r.prepare(core, generation, gate, now, core.CapturedAt.Add(replayDetailFreshFor), now)
			if reason != "" {
				t.Fatalf("second %d: prepare: %s", seconds, reason)
			}
			if reason := r.commit(candidate, now); reason != "" {
				t.Fatalf("second %d: commit: %s", seconds, reason)
			}
		}
		if seconds >= phase {
			if reply := r.lookup("", false, now); reply.status != 200 {
				t.Fatalf("second %d: current capture unavailable: %s", seconds, reply.body)
			}
		}
	}
}

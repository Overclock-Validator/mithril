package node

import (
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/metrics"
)

func TestMachineResolvedWorkers(t *testing.T) {
	at := time.Now()
	f := &machineMonitorFixture{at: at}
	m := newMachineDashboardMonitor(true)
	m.captureMetadata = f.capture
	defer m.stop()
	m.observe(machineMonitorBinding(at), 1, metrics.RSSHistoryPoint{At: at, Bytes: 100, Known: true}, 2)
	first, original := machineMonitorHeader(t, m)
	if first.PoolSettingsState != "not_collected" || len(first.Workers) != 0 {
		t.Fatal("unknown constructors were reported as configured")
	}
	m.resolvedWorker("replay_transactions", 0)
	m.resolvedWorker("tpu_sigverify", 4)
	m.resolvedWorker("harmonic_bundles", 8)
	m.resolvedWorker("other", 10)
	m.resolvedWorker("tpu_sigverify", -1)
	f.at = at.Add(10 * time.Second)
	m.observe(machineMonitorBinding(at), 2, metrics.RSSHistoryPoint{At: f.at, Bytes: 100, Known: true}, 2)
	current, _ := machineMonitorHeader(t, m)
	if current.PoolSettingsState != "resolved_constructors" || len(current.Workers) != 3 ||
		current.Workers[0].Name != "replay_transactions" || current.Workers[0].Enabled || current.Workers[0].Resolved != 0 ||
		current.Workers[1].Name != "tpu_sigverify" || !current.Workers[1].Enabled || current.Workers[1].Resolved != 4 ||
		current.Workers[1].Affinity != "not_bound_by_node" || current.Workers[2].Name != "harmonic_bundles" || current.Workers[2].Resolved != 8 {
		t.Fatalf("wrong resolved worker projection: %+v", current.Workers)
	}
	m.resolvedWorker("tpu_sigverify", 8)
	if current.Workers[1].Resolved != 4 {
		t.Fatal("updating a constructor mutated the preceding publication")
	}
	if previous, ok := original.Prepared.Page(0); !ok || previous == "" {
		t.Fatal("updating constructors invalidated a prepared publication")
	}
	m.stop()
	m.resolvedWorker("tpu_sigverify", 9)
	if _, err := m.sample(); err == nil {
		t.Fatal("stopped monitor republished worker data")
	}
}

package node

import (
	"bytes"
	"encoding/json"
	"math"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
)

type machineMonitorFixture struct {
	at     time.Time
	calls  uint64
	failed bool
}

func (f *machineMonitorFixture) capture() machineDashboardMetadata {
	f.calls++
	g, at := f.calls, f.at
	t := metrics.TopologyObservation{Generation: g, StartedAt: at, SampledAt: at.Add(time.Millisecond), CPUState: metrics.TopologyAvailable, NUMAState: metrics.TopologyUnsupported,
		CPUs: []metrics.TopologyCPU{{ID: 1, Present: true, Online: true, Package: metrics.TopologyID{State: metrics.TopologyUnsupported}, Die: metrics.TopologyID{State: metrics.TopologyUnsupported}, Core: metrics.TopologyID{State: metrics.TopologyUnsupported}, SiblingsState: metrics.TopologyUnsupported}}}
	a := metrics.ThreadAffinityObservation{Generation: g, TopologyGeneration: g, StartedAt: at.Add(2 * time.Millisecond), SampledAt: at.Add(3 * time.Millisecond), State: metrics.TopologyUnsupported, ThreadsState: metrics.TopologyUnsupported}
	l := metrics.LinkTopologyObservation{Generation: g, TopologyGeneration: g, AffinityGeneration: g, MetadataStartedAt: at, StartedAt: at.Add(4 * time.Millisecond), SampledAt: at.Add(5 * time.Millisecond), State: metrics.TopologyAvailable,
		Links: []metrics.LinkTopology{{Index: 7, OperationalState: "unknown", Kind: "not_reported", XDPMode: "unknown"}}}
	r := metrics.RouteSummaryObservation{Generation: g, TopologyGeneration: g, AffinityGeneration: g, LinkGeneration: g, MetadataStartedAt: at, NetworkStartedAt: l.StartedAt, StartedAt: at.Add(6 * time.Millisecond), SampledAt: at.Add(7 * time.Millisecond), State: metrics.TopologyAvailable,
		IPv4: metrics.RouteFamilySummary{RouteCounts: metrics.RouteCounts{MainUnicast: 1, Default: 1}}, Interfaces: []metrics.RouteInterfaceSummary{{Index: 7, IPv4: metrics.RouteCounts{MainUnicast: 1, Default: 1}}}}
	if f.failed {
		t.CPUState = metrics.TopologyReadFailed
		t.CPUs = nil
		l.State = metrics.TopologyDenied
		l.Links = nil
		r.State = metrics.TopologyDenied
		r.Interfaces = nil
		r.IPv4 = metrics.RouteFamilySummary{}
	}
	return machineDashboardMetadata{t, a, l, r}
}

func machineMonitorBinding(at time.Time) metrics.RSSHistoryBinding {
	return metrics.RSSHistoryBinding{RunID: "machine-run", StartedAt: at.Add(-2 * time.Hour), GenesisHash: [32]byte{1}}
}

func machineMonitorHeader(t *testing.T, m *machineDashboardMonitor) (dashboard.MachineDetail, dashboard.MachineDetailCapture) {
	t.Helper()
	c, err := m.sample()
	if err != nil {
		t.Fatal(err)
	}
	body, ok := c.Prepared.Page(0)
	if !ok {
		t.Fatal("missing header")
	}
	var page dashboard.MachineDetailPage
	if err = json.Unmarshal([]byte(body), &page); err != nil || page.Header == nil {
		t.Fatal("invalid header", err)
	}
	return *page.Header, c
}

func TestMachineMonitorDisabled(t *testing.T) {
	var m *machineDashboardMonitor
	if newMachineDashboardMonitor(false) != nil {
		t.Fatal("disabled constructor allocated")
	}
	if allocs := testing.AllocsPerRun(100, func() { m.observe(metrics.RSSHistoryBinding{}, 0, metrics.RSSHistoryPoint{}, 0); m.stop() }); allocs != 0 {
		t.Fatal("disabled owner allocated")
	}
	if _, err := m.sample(); err == nil {
		t.Fatal("nil source became available")
	}
	m = newMachineDashboardMonitor(true)
	defer m.stop()
	if m.metadata != nil || m.latest.Load() != nil || !m.metadataAttemptAt.IsZero() {
		t.Fatal("construction captured or published")
	}
	if _, err := m.sample(); err == nil {
		t.Fatal("unobserved owner available")
	}
}

func TestMachineMonitorCadenceAndOriginalRSS(t *testing.T) {
	at := time.Now()
	b := machineMonitorBinding(at)
	f := &machineMonitorFixture{at: at}
	m := newMachineDashboardMonitor(true)
	m.captureMetadata = f.capture
	defer m.stop()
	for i := uint64(0); i < 4; i++ {
		point := metrics.RSSHistoryPoint{At: at.Add(time.Duration(i) * 10 * time.Second), Bytes: 900 + i, Known: true}
		f.at = point.At
		m.observe(b, i+1, point, 4)
		d, c := machineMonitorHeader(t, m)
		if d.Binding.SourceGeneration != i+1 || d.CapturedAt.Before(at) || d.CapturedAt.After(time.Now()) || d.RSSHour.Current.Bytes == nil || *d.RSSHour.Current.Bytes != point.Bytes || !d.RSSHour.Current.At.Equal(point.At) || !c.FreshUntil.Equal(point.At.Add(30*time.Second)) || c.FreshUntil == c.FreshUntil.Round(0) || d.GOMAXPROCS == nil || *d.GOMAXPROCS != 4 || d.PoolSettingsState != "not_collected" || len(d.Workers) != 0 {
			t.Fatal("caller point, completion date, binding, original lease or uncollected pools changed")
		}
		want := uint64(1)
		if i == 3 {
			want = 2
		}
		if f.calls != want || d.Topology.Generation != want {
			t.Fatal("metadata sampled more than every thirty seconds")
		}
		for range 50 {
			again, err := m.sample()
			if err != nil || again.Prepared != c.Prepared || again.FreshUntil != c.FreshUntil {
				t.Fatal("cached read renewed or rebuilt publication")
			}
		}
	}
}

func TestMachineMonitorBindingAndGeneration(t *testing.T) {
	at := time.Now()
	b := machineMonitorBinding(at)
	f := &machineMonitorFixture{at: at}
	m := newMachineDashboardMonitor(true)
	m.captureMetadata = f.capture
	defer m.stop()
	point := metrics.RSSHistoryPoint{At: at, Bytes: 1, Known: true}
	m.observe(b, 1, point, 1)
	_, first := machineMonitorHeader(t, m)
	point.At = at.Add(10 * time.Second)
	m.observe(b, 1, point, 1)
	if _, err := m.sample(); err == nil || f.calls != 1 {
		t.Fatal("repeat generation renewed")
	}
	m.observe(b, 2, point, 1)
	_, second := machineMonitorHeader(t, m)
	if second.Prepared == first.Prepared {
		t.Fatal("new publication reused old binding counter")
	}
	b.GenesisHash = [32]byte{}
	point.At = at.Add(20 * time.Second)
	m.observe(b, 3, point, 1)
	if _, err := m.sample(); err == nil || f.calls != 1 {
		t.Fatal("unknown binding sampled or retained old value")
	}
	b.GenesisHash = [32]byte{2}
	point.At = at.Add(21 * time.Second)
	f.at = point.At
	m.observe(b, 4, point, 1)
	d, _ := machineMonitorHeader(t, m)
	if f.calls != 2 || d.RSSHour.Known || d.RSSHour.Reason != "insufficient_history" || !d.Topology.StartedAt.Equal(point.At) {
		t.Fatal("new context retained old history or metadata")
	}
	point.At = at.Add(22 * time.Second)
	m.observe(b, math.MaxUint64-1, point, 1)
	d, _ = machineMonitorHeader(t, m)
	if d.Binding.SourceGeneration != math.MaxUint64-1 {
		t.Fatal("last representable publication refused")
	}
	m.observe(b, math.MaxUint64, point, 1)
	m.observe(b, 1, point, 1)
	if _, err := m.sample(); err == nil || f.calls != 2 {
		t.Fatal("exhausted generation wrapped or sampled")
	}
}

func TestMachineMonitorFailuresAndLease(t *testing.T) {
	at := time.Now()
	b := machineMonitorBinding(at)
	f := &machineMonitorFixture{at: at}
	m := newMachineDashboardMonitor(true)
	m.captureMetadata = f.capture
	defer m.stop()
	m.observe(b, 1, metrics.RSSHistoryPoint{At: at, Bytes: 99, Known: true}, 1)
	_, old := machineMonitorHeader(t, m)
	original := old.Prepared.Manifest()
	f.failed = true
	f.at = at.Add(30 * time.Second)
	m.observe(b, 2, metrics.RSSHistoryPoint{At: f.at, Bytes: 0, Known: true}, 0)
	d, _ := machineMonitorHeader(t, m)
	if d.Topology.CPUState != "read_failed" || d.Links.State != "denied" || d.Routes.IPv4 != nil || d.RSSHour.Current.Bytes != nil || d.RSSHour.Reason != "sample_unavailable" || d.GOMAXPROCS != nil || old.Prepared.Manifest() != original {
		t.Fatal("failure became zero, changed old bytes, or renewed old rows")
	}
	meta := (&machineMonitorFixture{at: at}).capture()
	until, ok := machineDashboardDeadline(at.Add(80*time.Second), &meta)
	if !ok || !until.Equal(at.Add(90*time.Second)) {
		t.Fatal("older usable metadata lease renewed")
	}
	meta.topology.StartedAt = meta.topology.StartedAt.UTC()
	if _, ok = machineDashboardDeadline(at, &meta); ok {
		t.Fatal("UTC reconstruction acquired monotonic lease")
	}
	m.observe(b, 3, metrics.RSSHistoryPoint{At: f.at.UTC(), Bytes: 1, Known: true}, 1)
	if _, err := m.sample(); err == nil || f.calls != 2 {
		t.Fatal("invalid caller clock performed metadata I/O")
	}
	m.observe(b, 4, metrics.RSSHistoryPoint{At: at.Add(31 * time.Second), Bytes: 1, Known: true}, 1)
	if _, err := m.sample(); err == nil || f.calls != 2 {
		t.Fatal("clock recovery bypassed metadata cadence")
	}
	f.at = at.Add(60 * time.Second)
	m.observe(b, 5, metrics.RSSHistoryPoint{At: f.at, Bytes: 1, Known: true}, 1)
	if _, err := m.sample(); err != nil || f.calls != 3 {
		t.Fatal("original cadence did not recover", err)
	}
}

func TestMachineMonitorRSSHourAndReset(t *testing.T) {
	at := time.Now()
	b := machineMonitorBinding(at)
	f := &machineMonitorFixture{at: at}
	m := newMachineDashboardMonitor(true)
	m.captureMetadata = f.capture
	defer m.stop()
	for i := uint64(0); i <= 360; i++ {
		f.at = at.Add(time.Duration(i) * 10 * time.Second)
		m.observe(b, i+1, metrics.RSSHistoryPoint{At: f.at, Bytes: math.MaxUint64 - i, Known: true}, 2)
	}
	d, _ := machineMonitorHeader(t, m)
	if !d.RSSHour.Known || !*d.RSSHour.Negative || *d.RSSHour.MagnitudeBytes != 360 || *d.RSSHour.ElapsedNanoseconds != uint64(time.Hour) || !d.RSSHour.Baseline.At.Equal(at) {
		t.Fatal("actual supplied RSS history lost sign, precision or original interval")
	}
	b.RunID = "replacement"
	f.at = f.at.Add(time.Second)
	m.observe(b, 362, metrics.RSSHistoryPoint{At: f.at, Bytes: 1, Known: true}, 2)
	d, _ = machineMonitorHeader(t, m)
	if d.RSSHour.Known || d.RSSHour.Baseline != nil {
		t.Fatal("run replacement retained hour baseline")
	}
}

func TestMachineMonitorStopJoinsCapture(t *testing.T) {
	at := time.Now()
	b := machineMonitorBinding(at)
	f := &machineMonitorFixture{at: at}
	m := newMachineDashboardMonitor(true)
	entered, release, observed, stopped := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var released sync.Once
	defer released.Do(func() { close(release) })
	m.captureMetadata = func() machineDashboardMetadata { close(entered); <-release; return f.capture() }
	go func() {
		defer close(observed)
		m.observe(b, 1, metrics.RSSHistoryPoint{At: at, Bytes: 1, Known: true}, 1)
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("capture did not begin")
	}
	go func() { m.stop(); close(stopped) }()
	// Observe the actual stop fence; a bounded wait also makes a broken join fail.
	limit := time.Now().Add(2 * time.Second)
	for !m.stopped.Load() {
		if time.Now().After(limit) {
			t.Fatal("stop did not fence")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := m.sample(); err == nil {
		t.Fatal("stopped source still available")
	}
	select {
	case <-stopped:
		t.Fatal("stop did not join capture")
	default:
	}
	released.Do(func() { close(release) })
	select {
	case <-observed:
	case <-time.After(2 * time.Second):
		t.Fatal("capture did not return")
	}
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("stop did not join")
	}
	m.observe(b, 2, metrics.RSSHistoryPoint{At: at.Add(time.Second), Bytes: 2, Known: true}, 1)
	if f.calls != 1 || m.latest.Load() != nil {
		t.Fatal("late publication or restart after stop")
	}
}

func TestMachineMonitorMaximumProjectionAndReaders(t *testing.T) {
	at := time.Now()
	f := &machineMonitorFixture{at: at}
	value := f.capture()
	value.topology.CPUs = make([]metrics.TopologyCPU, 256)
	value.topology.Nodes = make([]metrics.TopologyNode, 64)
	value.topology.NUMAState = metrics.TopologyAvailable
	value.links.Links = make([]metrics.LinkTopology, 256)
	value.routes.Interfaces = make([]metrics.RouteInterfaceSummary, 256)
	value.affinity.State = metrics.TopologyAvailable
	value.affinity.ThreadsState = metrics.TopologyAvailable
	value.affinity.CPUDictionary = make([]uint16, 256)
	value.affinity.NodeDictionary = make([]uint16, 64)
	value.affinity.HasCPUDictionary = true
	value.affinity.HasNodeDictionary = true
	value.affinity.ReportedThreads = 256
	value.affinity.ListedThreads = 256
	value.affinity.ObservedThreads = 256
	value.affinity.MembershipStable = true
	value.affinity.Complete = true
	value.affinity.HasCPUIntersection = true
	value.affinity.HasNodeIntersection = true
	value.affinity.Groups = make([]metrics.ThreadAffinityGroup, 256)
	value.routes.IPv4 = metrics.RouteFamilySummary{RouteCounts: metrics.RouteCounts{MainUnicast: 1024, Default: 1024}, UnknownOutput: 1024, UnknownDefaultOutput: 1024}
	value.routes.IPv6 = value.routes.IPv4
	for i := range value.topology.CPUs {
		value.topology.CPUs[i] = metrics.TopologyCPU{ID: uint16(i), Present: true, Online: true, Package: metrics.TopologyID{State: metrics.TopologyAvailable, Value: math.MaxInt32}, Die: metrics.TopologyID{State: metrics.TopologyUnsupported}, Core: metrics.TopologyID{State: metrics.TopologyUnsupported}, SiblingsState: metrics.TopologyUnsupported}
		value.links.Links[i] = metrics.LinkTopology{Index: uint32(i + 1), OperationalState: "lower_layer_down", Kind: "wireguard", XDPMode: "unknown"}
		value.routes.Interfaces[i] = metrics.RouteInterfaceSummary{Index: uint32(i + 1), IPv4: metrics.RouteCounts{MainUnicast: 1024, Default: 1024}, IPv6: metrics.RouteCounts{MainUnicast: 1024, Default: 1024}}
		value.affinity.CPUDictionary[i] = uint16(i)
		g := metrics.ThreadAffinityGroup{CPUState: metrics.TopologyAvailable, NodeState: metrics.TopologyAvailable, Threads: 1}
		g.CPUs[i/8] = 1 << uint(i%8)
		g.Nodes[(i%64)/8] = 1 << uint(i%8)
		value.affinity.Groups[i] = g
	}
	for i := range value.topology.Nodes {
		value.topology.Nodes[i] = metrics.TopologyNode{ID: uint16(i), Online: true, CPUsState: metrics.TopologyUnsupported, MemoryState: metrics.TopologyAvailable, Memory: metrics.TopologyNodeMemory{TotalBytes: math.MaxUint64, FreeBytes: math.MaxUint64, ShmemBytes: math.MaxUint64, HasShmem: true}}
	}
	for i := range value.affinity.NodeDictionary {
		value.affinity.NodeDictionary[i] = uint16(i)
	}
	slices.SortFunc(value.affinity.Groups, func(a, b metrics.ThreadAffinityGroup) int { return bytes.Compare(a.CPUs[:], b.CPUs[:]) })
	m := newMachineDashboardMonitor(true)
	m.captureMetadata = func() machineDashboardMetadata { return value }
	defer m.stop()
	m.observe(machineMonitorBinding(at), 1, metrics.RSSHistoryPoint{At: at, Bytes: math.MaxUint64, Known: true}, math.MaxUint32)
	_, c := machineMonitorHeader(t, m)
	var manifest dashboard.MachineDetailManifest
	if err := json.Unmarshal([]byte(c.Prepared.Manifest()), &manifest); err != nil || manifest.Bytes > dashboard.MaxMachineDetailBytes || len(manifest.Pages) > 16 {
		t.Fatal("maximum projection exceeded fixed bounds", err)
	}
	rows := map[string]int{}
	for i, desc := range manifest.Pages {
		body, ok := c.Prepared.Page(i)
		if !ok || len(body) != desc.Bytes || len(body) > dashboard.MaxMachinePageBytes {
			t.Fatal("prepared page cap or descriptor differs")
		}
		rows[desc.Kind] += desc.Count
	}
	if rows["cpus"] != 256 || rows["numa"] != 64 || rows["links"] != 256 || rows["thread_groups"] != 256 {
		t.Fatal("maximum rows truncated")
	}
	before, _ := c.Prepared.Page(1)
	value.topology.CPUs[0].ID++
	value.affinity.Groups[0].Threads++
	value.links.Links[0].Index++
	value.routes.Interfaces[0].IPv4.MainUnicast++
	if after, _ := c.Prepared.Page(1); after != before {
		t.Fatal("prepared publication aliases mutable native metadata")
	}
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 10 {
				got, err := m.sample()
				if err != nil || got.Prepared != c.Prepared || got.FreshUntil != c.FreshUntil {
					t.Error("reader changed immutable capture")
				}
			}
		}()
	}
	wg.Wait()
}

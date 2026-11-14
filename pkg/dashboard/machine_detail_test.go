package dashboard

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"math"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/Overclock-Validator/mithril/pkg/metrics"
)

func TestMachineFamilyEmptyRowsHaveOneEncoding(t *testing.T) {
	at := time.Now()
	topology := metrics.TopologyObservation{Generation: 1, StartedAt: at, SampledAt: at, CPUState: metrics.TopologyUnsupported, NUMAState: metrics.TopologyUnsupported}
	affinity := metrics.ThreadAffinityObservation{Generation: 1, State: metrics.TopologyUnsupported}
	links := metrics.LinkTopologyObservation{Generation: 1, State: metrics.TopologyUnsupported}
	d, err := NewMachineDetail(MachineDetailBinding{RunID: "empty-machine", NodeStartedAt: at.UTC(), GenesisHash: "JEKNVnkbo3jma5nREBBJCDoXFVeKkD56V3xKrvRmWxFG", SourceGeneration: 1}, at, &topology, &affinity, &links, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareMachineDetail(d); err != nil {
		t.Fatal("canonical empty arrays rejected", err)
	}
	for _, name := range []string{"cpus", "numa", "thread_groups", "links"} {
		t.Run(name, func(t *testing.T) {
			copy := d
			topology, affinity, links := *d.Topology, *d.Affinity, *d.Links
			copy.Topology, copy.Affinity, copy.Links = &topology, &affinity, &links
			switch name {
			case "cpus":
				copy.Topology.CPUs = nil
			case "numa":
				copy.Topology.Nodes = nil
			case "thread_groups":
				copy.Affinity.Groups = nil
			case "links":
				copy.Links.Rows = nil
			}
			if _, err := PrepareMachineDetail(copy); err == nil {
				t.Fatal("accepted null rows that cannot be distinguished from [] by header-only transport")
			}
		})
	}
}

func TestMachineRSSBreakdownBudgetAndClock(t *testing.T) {
	d, _, _, _ := maximumMachineDetail(t)
	d.RSSBreakdown = &ProcessRSSBreakdown{SampledAt: d.CapturedAt, AnonymousBytes: math.MaxUint64, FileBytes: 0, SharedBytes: math.MaxUint64}
	prepared, err := PrepareMachineDetail(d)
	if err != nil || prepared == nil {
		t.Fatalf("maximum machine capture with RSS split: %v", err)
	}
	page, ok := prepared.Page(0)
	var header MachineDetailPage
	if !ok || json.Unmarshal([]byte(page), &header) != nil || header.Header == nil ||
		header.Header.RSSBreakdown == nil || *header.Header.RSSBreakdown != *d.RSSBreakdown {
		t.Fatal("machine header did not preserve RSS split")
	}
	d.RSSBreakdown.SampledAt = d.CapturedAt.Add(time.Nanosecond)
	if _, err := PrepareMachineDetail(d); err == nil {
		t.Fatal("accepted RSS sample after capture")
	}
}

func maximumMachineDetail(t *testing.T) (MachineDetail, metrics.TopologyObservation, metrics.ThreadAffinityObservation, metrics.LinkTopologyObservation) {
	t.Helper()
	at := time.Date(9999, 12, 31, 23, 59, 57, 999999999, time.UTC)
	topology := metrics.TopologyObservation{Generation: math.MaxUint64 - 1, StartedAt: at, SampledAt: at.Add(time.Millisecond), CPUState: metrics.TopologyAvailable, NUMAState: metrics.TopologyAvailable, CPUs: make([]metrics.TopologyCPU, 256), Nodes: make([]metrics.TopologyNode, 64)}
	affinity := metrics.ThreadAffinityObservation{Generation: math.MaxUint64 - 1, TopologyGeneration: topology.Generation, StartedAt: at.Add(2 * time.Millisecond), SampledAt: at.Add(3 * time.Millisecond), State: metrics.TopologyAvailable, CPUDictionary: make([]uint16, 256), NodeDictionary: make([]uint16, 64), HasCPUDictionary: true, HasNodeDictionary: true, ReportedThreads: 256, ListedThreads: 256, ObservedThreads: 256, ThreadsState: metrics.TopologyAvailable, MembershipStable: true, Complete: true, HasCPUIntersection: true, HasNodeIntersection: true, Groups: make([]metrics.ThreadAffinityGroup, 256)}
	links := metrics.LinkTopologyObservation{Generation: math.MaxUint64 - 1, TopologyGeneration: topology.Generation, AffinityGeneration: affinity.Generation, MetadataStartedAt: at, StartedAt: at.Add(4 * time.Millisecond), SampledAt: at.Add(5 * time.Millisecond), State: metrics.TopologyAvailable, Links: make([]metrics.LinkTopology, 256)}
	for i := range topology.Nodes {
		topology.Nodes[i] = metrics.TopologyNode{ID: uint16(i * 127), Online: true, CPUsState: metrics.TopologyAvailable, MemoryState: metrics.TopologyAvailable, Memory: metrics.TopologyNodeMemory{TotalBytes: math.MaxUint64, FreeBytes: math.MaxUint64, ShmemBytes: math.MaxUint64, HasShmem: true}}
		topology.Nodes[i].ID = uint16(8128 + i)
		affinity.NodeDictionary[i] = topology.Nodes[i].ID
	}
	for i := range topology.CPUs {
		id := metrics.TopologyID{Value: math.MaxInt32, State: metrics.TopologyAvailable}
		c := metrics.TopologyCPU{ID: uint16(i * 31), Present: true, Online: true, Package: id, Die: id, Core: id, SiblingsState: metrics.TopologyAvailable, NUMANode: topology.Nodes[i%64].ID, HasNUMANode: true}
		c.Siblings[i/8] = 1 << uint(i%8)
		topology.CPUs[i] = c
		topology.Nodes[i%64].CPUs[i/8] |= 1 << uint(i%8)
		topology.CPUs[i].ID = uint16(7936 + i)
		affinity.CPUDictionary[i] = topology.CPUs[i].ID
		g := metrics.ThreadAffinityGroup{CPUState: metrics.TopologyAvailable, NodeState: metrics.TopologyAvailable, CPUs: c.Siblings, Threads: 1}
		g.Nodes[(i%64)/8] = 1 << uint(i%8)
		affinity.Groups[i] = g
		links.Links[i] = metrics.LinkTopology{Index: uint32(math.MaxInt32 - 255 + i), AdminUp: true, MTU: math.MaxUint32, HasMTU: true, OperationalState: "lower_layer_down", Carrier: true, HasCarrier: true, MasterIndex: math.MaxUint32, HasMaster: true, LinkIndex: math.MaxUint32, HasLink: true, Kind: "wireguard", XDPMode: "offload"}
	}
	rss := metrics.RSSHourObservation{Known: true, Reason: "known", Current: metrics.RSSHistoryPoint{At: at, Known: true, Bytes: 0}, Baseline: metrics.RSSHistoryPoint{At: at.Add(-time.Hour), Known: true, Bytes: math.MaxUint64}, Elapsed: time.Hour, Negative: true, Magnitude: math.MaxUint64}
	binding := MachineDetailBinding{RunID: strings.Repeat("<", 128), NodeStartedAt: at.Add(-2 * time.Hour).UTC(), GenesisHash: "JEKNVnkbo3jma5nREBBJCDoXFVeKkD56V3xKrvRmWxFG", SourceGeneration: math.MaxUint64 - 1}
	slices.SortFunc(affinity.Groups, func(a, b metrics.ThreadAffinityGroup) int { return bytes.Compare(a.CPUs[:], b.CPUs[:]) })
	routes := maximumMachineRoutes(topology, affinity, links)
	d, err := NewMachineDetail(binding, at.Add(time.Second), &topology, &affinity, &links, &routes, &rss)
	if err != nil {
		t.Fatal(err)
	}
	d.GOMAXPROCS = machinePointer(uint32(math.MaxInt32))
	d.PoolSettingsState = "resolved_constructors"
	for _, name := range []string{"replay_transactions", "replay_streaming", "tpu_sigverify", "shred_sigverify", "harmonic_transactions", "harmonic_bundles", "block_production", "transaction_ingress"} {
		d.Workers = append(d.Workers, MachineWorker{Name: name, Enabled: true, Resolved: math.MaxUint32, Affinity: "not_bound_by_node"})
	}
	return d, topology, affinity, links
}

func maximumMachineRoutes(topology metrics.TopologyObservation, affinity metrics.ThreadAffinityObservation, links metrics.LinkTopologyObservation) metrics.RouteSummaryObservation {
	r := metrics.RouteSummaryObservation{Generation: math.MaxUint64 - 1, TopologyGeneration: topology.Generation, AffinityGeneration: affinity.Generation, LinkGeneration: links.Generation, MetadataStartedAt: topology.StartedAt, NetworkStartedAt: links.StartedAt, StartedAt: links.SampledAt, SampledAt: links.SampledAt.Add(time.Millisecond), State: metrics.TopologyAvailable, IPv4: metrics.RouteFamilySummary{RouteCounts: metrics.RouteCounts{MainUnicast: 1024, Default: 1024}, UnknownOutput: 1024, UnknownDefaultOutput: 1024}, IPv6: metrics.RouteFamilySummary{RouteCounts: metrics.RouteCounts{MainUnicast: 1024, Default: 1024}, UnknownOutput: 1024, UnknownDefaultOutput: 1024}, Interfaces: make([]metrics.RouteInterfaceSummary, len(links.Links))}
	for i, l := range links.Links {
		r.Interfaces[i] = metrics.RouteInterfaceSummary{Index: l.Index, IPv4: metrics.RouteCounts{MainUnicast: 1024, Default: 1024}, IPv6: metrics.RouteCounts{MainUnicast: 1024, Default: 1024}}
	}
	return r
}

func TestMachineFamilyMaximumAndOwnership(t *testing.T) {
	d, topology, affinity, links := maximumMachineDetail(t)
	expected, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	p, err := PrepareMachineDetail(d)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeMachineDetail(expected); err != nil {
		t.Fatal(err)
	}
	var manifest MachineDetailManifest
	if json.Unmarshal([]byte(p.Manifest()), &manifest) != nil || len(p.Manifest()) > MaxMachineManifestBytes || len(expected) > MaxMachineDetailBytes || len(manifest.Pages) != 14 || len(manifest.Pages) > machineMaxPages {
		t.Fatal("maximum family lost rows or exceeded manifest/family/page-count limits")
	}
	hash := sha256.Sum256(expected)
	if p.Generation() != hex.EncodeToString(hash[:]) || manifest.Generation != p.Generation() || manifest.Bytes != len(expected) {
		t.Fatal("complete family digest does not match the sealed bytes")
	}
	var restored MachineDetail
	for i, desc := range manifest.Pages {
		body, ok := p.Page(i)
		sum := sha256.Sum256([]byte(body))
		var page MachineDetailPage
		if !ok || len(body) > MaxMachinePageBytes || len(body) != desc.Bytes || desc.Digest != hex.EncodeToString(sum[:]) || json.Unmarshal([]byte(body), &page) != nil || page.Generation != p.Generation() || page.Page != i || page.Kind != desc.Kind || page.Offset != desc.Offset {
			t.Fatalf("page %d is not independently bounded/bound", i)
		}
		switch page.Kind {
		case "header":
			restored = *page.Header
		case "cpus":
			restored.Topology.CPUs = append(restored.Topology.CPUs, page.CPUs...)
		case "numa":
			restored.Topology.Nodes = append(restored.Topology.Nodes, page.Nodes...)
		case "thread_groups":
			restored.Affinity.Groups = append(restored.Affinity.Groups, page.Groups...)
		case "links":
			restored.Links.Rows = append(restored.Links.Rows, page.Links...)
		default:
			t.Fatal("unexpected page section")
		}
	}
	raw, err := json.Marshal(restored)
	if err != nil || !bytes.Equal(raw, expected) {
		t.Fatal("typed pages cannot reproduce every full-family field")
	}
	if _, err := decodeMachineDetail(raw); err != nil {
		t.Fatal("reassembled family did not pass complete validation", err)
	}
	// This is a modeled allocation envelope, not RSS or production concurrency proof:
	// two DTOs, five prepared holders (current, old, incoming and two retired
	// handlers), existing detached source inputs,
	// two full 1-MiB encoder workspaces and one page scratch buffer.
	owned := func(v any) uintptr { return reflect.TypeOf(v).Size() + machineDynamicBytes(reflect.ValueOf(v)) }
	modeled := 2*owned(d) + 5*owned(*p) + owned(topology) + owned(affinity) + owned(links) + owned(maximumMachineRoutes(topology, affinity, links)) + 2*MaxMachineDetailBytes + MaxMachinePageBytes
	// The detached native link owner also retains private validated names.
	// Include those unseen fields and the two capture row sets, receive buffer
	// and a conservative 64-KiB parser/map workspace reservation.
	identitySize := unsafe.Sizeof(struct {
		Index uint32
		Name  string
	}{})
	modeled += 256*(identitySize+15) + 2*256*(unsafe.Sizeof(metrics.LinkTopology{})+identitySize) + 64<<10 + 64<<10
	if modeled > 4<<20 {
		t.Fatalf("modeled source holders exceed 4MiB: %d", modeled)
	}
	beforeManifest, beforeGeneration := p.Manifest(), p.Generation()
	beforePages := slices.Clone(p.pages)
	topology.CPUs[0].ID++
	affinity.CPUDictionary[0]++
	links.Links[0].Index++
	d.Topology.CPUs[0].ID++
	*d.RSSHour.MagnitudeBytes = 0
	if p.Manifest() != beforeManifest || p.Generation() != beforeGeneration || !slices.Equal(p.pages, beforePages) || bytes.Contains(expected, []byte("Cpus_allowed_list")) || bytes.Contains(expected, []byte("/proc/")) || bytes.Contains(expected, []byte("namespace")) && !bytes.Contains(expected, []byte("visible_namespace_metadata")) {
		t.Fatal("prepared source aliases mutable input or leaks private source detail")
	}
	if _, ok := p.Page(-1); ok {
		t.Fatal("negative page accepted")
	}
	if _, ok := p.Page(16); ok {
		t.Fatal("out-of-range page accepted")
	}
	t.Logf("family=%d manifest=%d pages=%d modeled_five_prepared_holders=%d/4194304 native_CPU_row=%d", len(expected), len(p.Manifest()), len(manifest.Pages), modeled, unsafe.Sizeof(metrics.TopologyCPU{}))
}

func machineDynamicBytes(v reflect.Value) uintptr {
	if v.Type() == reflect.TypeFor[time.Time]() {
		return 0
	}
	switch v.Kind() {
	case reflect.Pointer:
		if !v.IsNil() {
			return v.Elem().Type().Size() + machineDynamicBytes(v.Elem())
		}
	case reflect.String:
		return uintptr(v.Len())
	case reflect.Slice:
		n := uintptr(v.Cap()) * v.Type().Elem().Size()
		for i := 0; i < v.Len(); i++ {
			n += machineDynamicBytes(v.Index(i))
		}
		return n
	case reflect.Struct:
		var n uintptr
		for i := 0; i < v.NumField(); i++ {
			n += machineDynamicBytes(v.Field(i))
		}
		return n
	}
	return 0
}

func TestMachineFamilyRefusesMalformedFacts(t *testing.T) {
	for name, mutate := range map[string]func(*MachineDetail){
		"run UTF8":                func(d *MachineDetail) { d.Binding.RunID = string([]byte{0xff}) },
		"row cap":                 func(d *MachineDetail) { d.Topology.CPUs = append(d.Topology.CPUs, d.Topology.CPUs[0]) },
		"duplicate dictionary":    func(d *MachineDetail) { d.Topology.CPUDictionary[1] = d.Topology.CPUDictionary[0] },
		"mask size":               func(d *MachineDetail) { d.Topology.CPUs[0].Siblings = "AQ==" },
		"negative hierarchy":      func(d *MachineDetail) { d.Topology.CPUs[0].Package.Value = machinePointer(int32(-1)) },
		"available without value": func(d *MachineDetail) { d.Topology.CPUs[0].Package.Value = nil },
		"cross generation":        func(d *MachineDetail) { d.Affinity.TopologyGeneration-- },
		"false complete":          func(d *MachineDetail) { d.Affinity.MembershipStable = false },
		"false intersection": func(d *MachineDetail) {
			v := make([]byte, 32)
			v[0] = 1
			d.Affinity.CPUIntersection = machinePointer(base64.StdEncoding.EncodeToString(v))
		},
		"raw link kind":                 func(d *MachineDetail) { d.Links.Rows[0].Kind = "private-interface-name" },
		"RSS arithmetic":                func(d *MachineDetail) { *d.RSSHour.MagnitudeBytes = 1 },
		"RSS sign":                      func(d *MachineDetail) { *d.RSSHour.Negative = false },
		"missing RSS clock":             func(d *MachineDetail) { d.RSSHour.Current.At = nil },
		"route unknown count":           func(d *MachineDetail) { d.Routes.IPv4.UnknownOutput = 2048 },
		"route default":                 func(d *MachineDetail) { d.Routes.IPv4.Default = 2048 },
		"route unknown default":         func(d *MachineDetail) { d.Routes.IPv4.UnknownDefaultOutput = 2048 },
		"route impossible intersection": func(d *MachineDetail) { d.Routes.IPv4.UnknownDefaultOutput = 0 },
		"route link generation":         func(d *MachineDetail) { d.Routes.LinkGeneration-- },
		"route interface over global":   func(d *MachineDetail) { d.Links.Rows[0].Routes.IPv4.MainUnicast = 2048 },
		"route missing represented row": func(d *MachineDetail) { d.Links.Rows[0].Routes = nil },
		"invented pool":                 func(d *MachineDetail) { d.Workers[0].Name = "goroutine-42" },
	} {
		t.Run(name, func(t *testing.T) {
			d, _, _, _ := maximumMachineDetail(t)
			mutate(&d)
			if _, err := PrepareMachineDetail(d); err == nil {
				t.Fatal("invalid source facts accepted")
			}
		})
	}
	d, _, _, _ := maximumMachineDetail(t)
	body, _ := json.Marshal(d)
	for _, bad := range [][]byte{append([]byte(" "), body...), bytes.Replace(body, []byte(`"family":"machine"`), []byte(`"family":"machine","unknown":0`), 1), bytes.Replace(body, []byte(`"family":"machine"`), []byte(`"family":"machine","family":"machine"`), 1)} {
		if _, err := decodeMachineDetail(bad); err == nil {
			t.Fatal("noncanonical/unknown/duplicate source member accepted")
		}
	}
}

func TestMachineFamilyMasksAndPartialAffinity(t *testing.T) {
	at := time.Now()
	binding := MachineDetailBinding{RunID: "run", NodeStartedAt: at, GenesisHash: "JEKNVnkbo3jma5nREBBJCDoXFVeKkD56V3xKrvRmWxFG", SourceGeneration: 1}
	topology := metrics.TopologyObservation{Generation: 1, StartedAt: at, SampledAt: at, CPUState: metrics.TopologyPartial, NUMAState: metrics.TopologyUnsupported, CPUs: []metrics.TopologyCPU{{ID: 7, Present: true, SiblingsState: metrics.TopologyAvailable, Siblings: [32]byte{1}}}}
	affinity := metrics.ThreadAffinityObservation{Generation: 1, TopologyGeneration: 1, StartedAt: at, SampledAt: at, State: metrics.TopologyPartial, CPUDictionary: []uint16{7}, HasCPUDictionary: true, ReportedThreads: 1, ListedThreads: 1, ObservedThreads: 1, ThreadsState: metrics.TopologyAvailable, MembershipStable: true, HasCPUIntersection: true, CPUIntersection: [32]byte{1}, Groups: []metrics.ThreadAffinityGroup{{CPUState: metrics.TopologyAvailable, NodeState: metrics.TopologyUnsupported, CPUs: [32]byte{1}, Threads: 1}}}
	d, err := NewMachineDetail(binding, at, &topology, &affinity, nil, nil, nil)
	if err != nil || d.Affinity.CPUIntersection == nil || d.Affinity.NodeIntersection != nil || d.Topology.CPUs[0].Die.Value != nil || d.Topology.CPUs[0].Online {
		t.Fatalf("independent partial knownness lost: %v", err)
	}
	for _, foreign := range []int{0, 1, 31} {
		copy := topology
		copy.CPUs = append([]metrics.TopologyCPU(nil), topology.CPUs...)
		copy.CPUs[0].Siblings[foreign] |= 0x80
		if _, err := NewMachineDetail(binding, at, &copy, nil, nil, nil, nil); err == nil {
			t.Fatalf("foreign/tail mask bit in byte%d discarded", foreign)
		}
	}
	// One failed thread read belongs in a group, but is not an observed read.
	affinity.ObservedThreads, affinity.HasCPUIntersection = 0, false
	affinity.Groups[0] = metrics.ThreadAffinityGroup{CPUState: metrics.TopologyReadFailed, NodeState: metrics.TopologyReadFailed, Threads: 1}
	if _, err := NewMachineDetail(binding, at, &topology, &affinity, nil, nil, nil); err != nil {
		t.Fatal("partial sampled thread failure discarded good topology", err)
	}
}

func TestMachineFamilyActualFailedSamplersAndExhaustion(t *testing.T) {
	at := time.Now()
	binding := MachineDetailBinding{RunID: "run", NodeStartedAt: at.Add(-time.Minute), GenesisHash: "JEKNVnkbo3jma5nREBBJCDoXFVeKkD56V3xKrvRmWxFG", SourceGeneration: 1}
	// This actual sampler path refuses its exhausted inherited time budget before
	// any host read, on Linux and other systems alike. ThreadsState remains zero.
	topology := metrics.TopologyObservation{Generation: 1, StartedAt: at.Add(-2 * time.Second), SampledAt: at.Add(-2 * time.Second), CPUState: metrics.TopologyAvailable, NUMAState: metrics.TopologyUnsupported, CPUs: []metrics.TopologyCPU{{ID: 0, Present: true}}}
	affinity, ok := metrics.NewThreadAffinitySampler(true).Sample(topology)
	if !ok || affinity.State != metrics.TopologyCaptureExpired || affinity.ThreadsState != "" {
		t.Fatalf("unexpected actual refusal: %+v", affinity)
	}
	links, ok := metrics.NewLinkTopologySampler(true).Sample(topology, affinity)
	if !ok {
		t.Fatal("enabled link sampler omitted failed capture")
	}
	d, err := NewMachineDetail(binding, at, &topology, &affinity, &links, nil, nil)
	if err != nil || d.Topology == nil || d.Affinity.ThreadsState != "not_collected" || d.Affinity.SampledAt != nil {
		t.Fatalf("actual failed source invalidated unrelated CPU facts: %v", err)
	}
	// Exhaustion retains the top-level generation but has no child clocks/refs.
	affinity = metrics.ThreadAffinityObservation{Generation: math.MaxUint64 - 1, State: metrics.TopologyLimitExceeded}
	links = metrics.LinkTopologyObservation{Generation: math.MaxUint64 - 1, State: metrics.TopologyLimitExceeded}
	if _, err := NewMachineDetail(binding, at, &topology, &affinity, &links, nil, nil); err != nil {
		t.Fatal("retired empty subordinate source references rejected", err)
	}
	unknown := metrics.RSSHourObservation{Reason: "insufficient_history", Current: metrics.RSSHistoryPoint{At: at, Known: true, Bytes: 17}}
	d, err = NewMachineDetail(binding, at, nil, nil, nil, nil, &unknown)
	if err != nil || d.RSSHour.Known || d.RSSHour.Baseline != nil || d.PoolSettingsState != "not_collected" || d.Routes != nil {
		t.Fatal("missing history/routes/pools became known", err)
	}
	if runtime.GOOS != "linux" {
		topology, _ = metrics.NewTopologySampler(true).Sample()
		affinity, _ = metrics.NewThreadAffinitySampler(true).Sample(topology)
		links, _ = metrics.NewLinkTopologySampler(true).Sample(topology, affinity)
		d, err = NewMachineDetail(binding, time.Now(), &topology, &affinity, &links, nil, nil)
		if err != nil || d.Topology.CPUState != "unsupported" || d.Affinity.State != "unsupported" || d.Affinity.ThreadsState != "not_collected" || d.Links.State != "unsupported" {
			t.Fatal("actual non-Linux uncollected subfields rejected", err)
		}
	}
}

func TestMachineFamilyRouteAssociationAndUnknowns(t *testing.T) {
	d, topology, affinity, links := maximumMachineDetail(t)
	routes := maximumMachineRoutes(topology, affinity, links)
	if d.Routes == nil || d.Links.Rows[255].Routes == nil || d.Routes.LinkGeneration != d.Links.Generation {
		t.Fatal("complete represented route joins missing")
	}
	// Multipath per-interface counts overlap and are deliberately not summed.
	if uint64(len(d.Links.Rows))*uint64(d.Links.Rows[0].Routes.IPv4.MainUnicast) <= uint64(d.Routes.IPv4.MainUnicast) {
		t.Fatal("fixture does not exercise overlapping interface counts")
	}
	for _, kind := range []string{"generation", "missing", "duplicate", "foreign"} {
		r := routes
		r.Interfaces = slices.Clone(routes.Interfaces)
		switch kind {
		case "generation":
			r.LinkGeneration--
		case "missing":
			r.Interfaces = r.Interfaces[:255]
		case "duplicate":
			r.Interfaces[1].Index = r.Interfaces[0].Index
		case "foreign":
			r.Interfaces[0].Index--
		}
		if _, err := NewMachineDetail(d.Binding, d.CapturedAt, &topology, &affinity, &links, &r, nil); err == nil {
			t.Fatalf("invalid route association %s accepted", kind)
		}
	}
	for _, state := range []metrics.TopologyState{metrics.TopologyDenied, metrics.TopologyReadFailed, metrics.TopologyLimitExceeded, metrics.TopologyBindingChanged, metrics.TopologyCaptureExpired} {
		r := metrics.RouteSummaryObservation{Generation: 1, State: state}
		got, err := NewMachineDetail(d.Binding, d.CapturedAt, &topology, &affinity, &links, &r, nil)
		if err != nil || got.Routes == nil || got.Routes.IPv4 != nil || got.Routes.IPv6 != nil || got.Links.Rows[0].Routes != nil {
			t.Fatalf("failed route capture fabricated zeros or discarded links: %s %v", state, err)
		}
	}
	zero := routes
	zero.IPv4, zero.IPv6 = metrics.RouteFamilySummary{}, metrics.RouteFamilySummary{}
	zero.Interfaces = slices.Clone(routes.Interfaces)
	for i := range zero.Interfaces {
		zero.Interfaces[i].IPv4, zero.Interfaces[i].IPv6 = metrics.RouteCounts{}, metrics.RouteCounts{}
	}
	got, err := NewMachineDetail(d.Binding, d.CapturedAt, &topology, &affinity, &links, &zero, nil)
	if err != nil || got.Routes.IPv4 == nil || got.Routes.IPv4.MainUnicast != 0 || got.Links.Rows[0].Routes == nil {
		t.Fatal("complete known empty route dumps did not remain known zero", err)
	}
	got, err = NewMachineDetail(d.Binding, d.CapturedAt, &topology, &affinity, &links, nil, nil)
	if err != nil || got.Routes != nil || got.Links.Rows[0].Routes != nil {
		t.Fatal("uncollected routes must remain nil", err)
	}
}

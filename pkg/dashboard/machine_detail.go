package dashboard

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Overclock-Validator/mithril/pkg/metrics"
)

const (
	// MaxMachineDetailBytes bounds the complete canonical source projection.
	MaxMachineDetailBytes = 1 << 20
	// MaxMachineManifestBytes bounds metadata independently of row pages.
	MaxMachineManifestBytes = 8 << 10
	// MaxMachinePageBytes bounds each typed transport page.
	MaxMachinePageBytes = 64 << 10
	machinePageRows     = 64
	machineMaxPages     = 16
)

// MachineDetailBinding identifies a metadata publication, independently of replay.
type MachineDetailBinding struct {
	RunID            string    `json:"run_id"`
	NodeStartedAt    time.Time `json:"node_started_at"`
	GenesisHash      string    `json:"genesis_hash"`
	SourceGeneration uint64    `json:"source_generation,string"`
}

// MachineStamp preserves source dates. Missing failed-capture clocks stay null.
type MachineStamp struct {
	Generation uint64     `json:"generation,string"`
	StartedAt  *time.Time `json:"started_at"`
	SampledAt  *time.Time `json:"sampled_at"`
}

// MachineID distinguishes an unavailable hierarchy identity from known zero.
type MachineID struct {
	State string `json:"state"`
	Value *int32 `json:"value"`
}

// MachineCPU uses raw CPU IDs; sibling bits index CPUDictionary, not those IDs.
type MachineCPU struct {
	ID            uint16    `json:"id"`
	Present       bool      `json:"present"`
	Online        bool      `json:"online"`
	Package       MachineID `json:"package"`
	Die           MachineID `json:"die"`
	Core          MachineID `json:"core"`
	SiblingsState string    `json:"siblings_state"`
	Siblings      string    `json:"siblings"`
	NUMANode      *uint16   `json:"numa_node"`
}

// MachineNUMA reports visible node capacity, not process page residence.
type MachineNUMA struct {
	ID          uint16  `json:"id"`
	Online      bool    `json:"online"`
	CPUsState   string  `json:"cpus_state"`
	CPUs        string  `json:"cpus"`
	MemoryState string  `json:"memory_state"`
	TotalBytes  *uint64 `json:"total_bytes,string"`
	FreeBytes   *uint64 `json:"free_bytes,string"`
	ShmemBytes  *uint64 `json:"shmem_bytes,string"`
}

// MachineTopology carries the complete possible-ID dictionaries, including offline CPUs.
type MachineTopology struct {
	MachineStamp
	CPUState       string        `json:"cpu_state"`
	NUMAState      string        `json:"numa_state"`
	CPUDictionary  []uint16      `json:"cpu_dictionary"`
	NodeDictionary []uint16      `json:"node_dictionary"`
	CPUs           []MachineCPU  `json:"cpus"`
	Nodes          []MachineNUMA `json:"nodes"`
}

// MachineThreadGroup describes sampled allowed masks, without OS thread identities.
type MachineThreadGroup struct {
	CPUState  string `json:"cpu_state"`
	NodeState string `json:"node_state"`
	CPUs      string `json:"cpus"`
	Nodes     string `json:"nodes"`
	Threads   uint32 `json:"threads"`
}

// MachineAffinity describes a sampled thread set, not fixed worker pinning.
type MachineAffinity struct {
	MachineStamp
	State              string               `json:"state"`
	TopologyGeneration uint64               `json:"topology_generation,string"`
	CPUDictionary      []uint16             `json:"cpu_dictionary"`
	NodeDictionary     []uint16             `json:"node_dictionary"`
	HasCPUDictionary   bool                 `json:"has_cpu_dictionary"`
	HasNodeDictionary  bool                 `json:"has_node_dictionary"`
	ReportedThreads    uint32               `json:"reported_threads"`
	ListedThreads      uint32               `json:"listed_threads"`
	ObservedThreads    uint32               `json:"observed_threads"`
	ThreadsState       string               `json:"threads_state"`
	MembershipStable   bool                 `json:"membership_stable"`
	Complete           bool                 `json:"complete"`
	CPUIntersection    *string              `json:"cpu_intersection"`
	NodeIntersection   *string              `json:"node_intersection"`
	Groups             []MachineThreadGroup `json:"groups"`
}

// MachineLink contains only passive numeric topology and fixed kernel enums.
type MachineLink struct {
	Index            uint32             `json:"index"`
	AdminUp          bool               `json:"admin_up"`
	MTU              *uint32            `json:"mtu"`
	OperationalState string             `json:"operational_state"`
	Carrier          *bool              `json:"carrier"`
	MasterIndex      *uint32            `json:"master_index"`
	LinkIndex        *uint32            `json:"link_index"`
	Kind             string             `json:"kind"`
	XDPMode          string             `json:"xdp_mode"`
	Routes           *MachineLinkRoutes `json:"routes"`
}

// MachineRouteCounts counts main-table unicast dump records, not reachability.
type MachineRouteCounts struct {
	MainUnicast uint32 `json:"main_unicast"`
	Default     uint32 `json:"default"`
}

// MachineLinkRoutes counts records once per interface. Multipath records may
// appear on several interfaces, so these counts must not be summed as global.
type MachineLinkRoutes struct {
	IPv4 MachineRouteCounts `json:"ipv4"`
	IPv6 MachineRouteCounts `json:"ipv6"`
}

// MachineRouteFamily counts each route once globally, including unresolved outputs.
type MachineRouteFamily struct {
	MachineRouteCounts
	UnknownOutput        uint32 `json:"unknown_output"`
	UnknownDefaultOutput uint32 `json:"unknown_default_output"`
}

// MachineRoutes preserves both dump coverage and its original link association.
// Failed captures have null counts; a nil MachineDetail.Routes is uncollected.
type MachineRoutes struct {
	MachineStamp
	State              string              `json:"state"`
	TopologyGeneration uint64              `json:"topology_generation,string"`
	AffinityGeneration uint64              `json:"affinity_generation,string"`
	LinkGeneration     uint64              `json:"link_generation,string"`
	MetadataStartedAt  *time.Time          `json:"metadata_started_at"`
	NetworkStartedAt   *time.Time          `json:"network_started_at"`
	IPv4               *MachineRouteFamily `json:"ipv4"`
	IPv6               *MachineRouteFamily `json:"ipv6"`
}

// MachineLinks has no interface names, addresses or private namespace tokens.
type MachineLinks struct {
	MachineStamp
	State              string        `json:"state"`
	TopologyGeneration uint64        `json:"topology_generation,string"`
	AffinityGeneration uint64        `json:"affinity_generation,string"`
	MetadataStartedAt  *time.Time    `json:"metadata_started_at"`
	Rows               []MachineLink `json:"rows"`
}

// MachineRSSPoint preserves one original approximate RSS read and its knownness.
type MachineRSSPoint struct {
	At    *time.Time `json:"at"`
	Bytes *uint64    `json:"bytes,string"`
}

// MachineRSSHour is a sign/magnitude difference, avoiding int64 overflow.
type MachineRSSHour struct {
	Scope              string           `json:"scope"`
	Known              bool             `json:"known"`
	Reason             string           `json:"reason"`
	Current            MachineRSSPoint  `json:"current"`
	Baseline           *MachineRSSPoint `json:"baseline"`
	ElapsedNanoseconds *uint64          `json:"elapsed_nanoseconds,string"`
	Negative           *bool            `json:"negative"`
	MagnitudeBytes     *uint64          `json:"magnitude_bytes,string"`
}

// MachineWorker is a resolved constructor limit, never an active OS-thread count.
type MachineWorker struct {
	Name     string `json:"name"`
	Enabled  bool   `json:"enabled"`
	Resolved uint32 `json:"resolved"`
	Affinity string `json:"affinity"`
}

// MachineDetail is a redacted source projection. Null sections mean uncollected.
// Pool settings remain uncollected until actual constructors supply them.
type MachineDetail struct {
	SchemaVersion             int                  `json:"schema_version"`
	Family                    string               `json:"family"`
	Scope                     string               `json:"scope"`
	Binding                   MachineDetailBinding `json:"binding"`
	CapturedAt                time.Time            `json:"captured_at"`
	Topology                  *MachineTopology     `json:"topology"`
	Affinity                  *MachineAffinity     `json:"affinity"`
	Links                     *MachineLinks        `json:"links"`
	RSSHour                   *MachineRSSHour      `json:"rss_hour"`
	GOMAXPROCS                *uint32              `json:"gomaxprocs"`
	Workers                   []MachineWorker      `json:"workers"`
	PoolSettingsState         string               `json:"pool_settings_state"`
	Routes                    *MachineRoutes       `json:"routes"`
	ProcessNUMAResidenceState string               `json:"process_numa_residence_state"`
	RSSBreakdown              *ProcessRSSBreakdown `json:"rss_breakdown,omitempty"`
}

// ProcessRSSBreakdown contains approximate Linux process-resident components.
type ProcessRSSBreakdown struct {
	SampledAt      time.Time `json:"sampled_at"`
	AnonymousBytes uint64    `json:"anonymous_bytes,string"`
	FileBytes      uint64    `json:"file_bytes,string"`
	SharedBytes    uint64    `json:"shared_bytes,string"`
}

func machinePointer[T any](v T) *T { return &v }
func machineDate(at time.Time) *time.Time {
	at = at.UTC()
	if !validReplayDetailTime(at) {
		return nil
	}
	return machinePointer(at.UTC())
}
func machineState(s metrics.TopologyState) string {
	if s == "" {
		return "not_collected"
	}
	return string(s)
}
func machineStamp(g uint64, start, at time.Time) MachineStamp {
	return MachineStamp{g, machineDate(start), machineDate(at)}
}
func machineMask(raw []byte, count int, state string) string {
	if state != "available" {
		return ""
	}
	n := (count + 7) / 8
	for _, v := range raw[n:] {
		if v != 0 {
			// Preserve foreign bits; the typed validator rejects this full length.
			return base64.StdEncoding.EncodeToString(raw)
		}
	}
	return base64.StdEncoding.EncodeToString(raw[:n])
}
func machineID(v metrics.TopologyID) MachineID {
	d := MachineID{State: machineState(v.State)}
	if v.State == metrics.TopologyAvailable {
		d.Value = machinePointer(v.Value)
	}
	return d
}

// NewMachineDetail copies existing observations without clocks, I/O or source reads.
// Workers are intentionally uncollected here; callers may attach actual resolved rows.
func NewMachineDetail(binding MachineDetailBinding, capturedAt time.Time, topology *metrics.TopologyObservation, affinity *metrics.ThreadAffinityObservation, links *metrics.LinkTopologyObservation, routes *metrics.RouteSummaryObservation, rss *metrics.RSSHourObservation) (MachineDetail, error) {
	binding.NodeStartedAt = binding.NodeStartedAt.UTC()
	d := MachineDetail{SchemaVersion: 1, Family: "machine", Scope: "visible_namespace_metadata", Binding: binding, CapturedAt: capturedAt.UTC(), PoolSettingsState: "not_collected", ProcessNUMAResidenceState: "not_collected"}
	if topology != nil {
		o := topology
		if len(o.CPUs) > 256 || len(o.Nodes) > 64 {
			return MachineDetail{}, errors.New("source_invalid")
		}
		t := &MachineTopology{MachineStamp: machineStamp(o.Generation, o.StartedAt, o.SampledAt), CPUState: machineState(o.CPUState), NUMAState: machineState(o.NUMAState), CPUDictionary: make([]uint16, len(o.CPUs)), NodeDictionary: make([]uint16, len(o.Nodes)), CPUs: make([]MachineCPU, len(o.CPUs)), Nodes: make([]MachineNUMA, len(o.Nodes))}
		for i, v := range o.CPUs {
			t.CPUDictionary[i] = v.ID
			t.CPUs[i] = MachineCPU{ID: v.ID, Present: v.Present, Online: v.Online, Package: machineID(v.Package), Die: machineID(v.Die), Core: machineID(v.Core), SiblingsState: machineState(v.SiblingsState), Siblings: machineMask(v.Siblings[:], len(o.CPUs), machineState(v.SiblingsState))}
			if v.HasNUMANode {
				t.CPUs[i].NUMANode = machinePointer(v.NUMANode)
			}
		}
		for i, v := range o.Nodes {
			t.NodeDictionary[i] = v.ID
			n := MachineNUMA{ID: v.ID, Online: v.Online, CPUsState: machineState(v.CPUsState), CPUs: machineMask(v.CPUs[:], len(o.CPUs), machineState(v.CPUsState)), MemoryState: machineState(v.MemoryState)}
			if v.MemoryState == metrics.TopologyAvailable {
				n.TotalBytes = machinePointer(v.Memory.TotalBytes)
				n.FreeBytes = machinePointer(v.Memory.FreeBytes)
				if v.Memory.HasShmem {
					n.ShmemBytes = machinePointer(v.Memory.ShmemBytes)
				}
			}
			t.Nodes[i] = n
		}
		d.Topology = t
	}
	if affinity != nil {
		o := affinity
		if len(o.CPUDictionary) > 256 || len(o.NodeDictionary) > 64 || len(o.Groups) > 256 {
			return MachineDetail{}, errors.New("source_invalid")
		}
		a := &MachineAffinity{MachineStamp: machineStamp(o.Generation, o.StartedAt, o.SampledAt), State: machineState(o.State), TopologyGeneration: o.TopologyGeneration, CPUDictionary: slices.Clone(o.CPUDictionary), NodeDictionary: slices.Clone(o.NodeDictionary), HasCPUDictionary: o.HasCPUDictionary, HasNodeDictionary: o.HasNodeDictionary, ReportedThreads: o.ReportedThreads, ListedThreads: o.ListedThreads, ObservedThreads: o.ObservedThreads, ThreadsState: machineState(o.ThreadsState), MembershipStable: o.MembershipStable, Complete: o.Complete, Groups: make([]MachineThreadGroup, len(o.Groups))}
		for i, v := range o.Groups {
			a.Groups[i] = MachineThreadGroup{CPUState: machineState(v.CPUState), NodeState: machineState(v.NodeState), CPUs: machineMask(v.CPUs[:], len(o.CPUDictionary), machineState(v.CPUState)), Nodes: machineMask(v.Nodes[:], len(o.NodeDictionary), machineState(v.NodeState)), Threads: v.Threads}
		}
		if o.HasCPUIntersection {
			a.CPUIntersection = machinePointer(machineMask(o.CPUIntersection[:], len(o.CPUDictionary), "available"))
		}
		if o.HasNodeIntersection {
			a.NodeIntersection = machinePointer(machineMask(o.NodeIntersection[:], len(o.NodeDictionary), "available"))
		}
		d.Affinity = a
	}
	if links != nil {
		o := links
		if len(o.Links) > 256 {
			return MachineDetail{}, errors.New("source_invalid")
		}
		l := &MachineLinks{MachineStamp: machineStamp(o.Generation, o.StartedAt, o.SampledAt), State: machineState(o.State), TopologyGeneration: o.TopologyGeneration, AffinityGeneration: o.AffinityGeneration, MetadataStartedAt: machineDate(o.MetadataStartedAt), Rows: make([]MachineLink, len(o.Links))}
		for i, v := range o.Links {
			row := MachineLink{Index: v.Index, AdminUp: v.AdminUp, OperationalState: v.OperationalState, Kind: v.Kind, XDPMode: v.XDPMode}
			if v.HasMTU {
				row.MTU = machinePointer(v.MTU)
			}
			if v.HasCarrier {
				row.Carrier = machinePointer(v.Carrier)
			}
			if v.HasMaster {
				row.MasterIndex = machinePointer(v.MasterIndex)
			}
			if v.HasLink {
				row.LinkIndex = machinePointer(v.LinkIndex)
			}
			l.Rows[i] = row
		}
		d.Links = l
	}
	if routes != nil {
		o := routes
		r := &MachineRoutes{MachineStamp: machineStamp(o.Generation, o.StartedAt, o.SampledAt), State: machineState(o.State), TopologyGeneration: o.TopologyGeneration, AffinityGeneration: o.AffinityGeneration, LinkGeneration: o.LinkGeneration, MetadataStartedAt: machineDate(o.MetadataStartedAt), NetworkStartedAt: machineDate(o.NetworkStartedAt)}
		if o.State == metrics.TopologyAvailable {
			if d.Links == nil || o.LinkGeneration != d.Links.Generation || len(o.Interfaces) != len(d.Links.Rows) || len(o.Interfaces) > 256 {
				return MachineDetail{}, errors.New("source_invalid")
			}
			project := func(v metrics.RouteFamilySummary) *MachineRouteFamily {
				return &MachineRouteFamily{MachineRouteCounts: MachineRouteCounts{v.MainUnicast, v.Default}, UnknownOutput: v.UnknownOutput, UnknownDefaultOutput: v.UnknownDefaultOutput}
			}
			r.IPv4, r.IPv6 = project(o.IPv4), project(o.IPv6)
			for i, v := range o.Interfaces {
				if v.Index != d.Links.Rows[i].Index {
					return MachineDetail{}, errors.New("source_invalid")
				}
				d.Links.Rows[i].Routes = &MachineLinkRoutes{IPv4: MachineRouteCounts{v.IPv4.MainUnicast, v.IPv4.Default}, IPv6: MachineRouteCounts{v.IPv6.MainUnicast, v.IPv6.Default}}
			}
		}
		d.Routes = r
	}
	if rss != nil {
		o := rss
		r := &MachineRSSHour{Scope: "approximately_1h", Known: o.Known, Reason: o.Reason, Current: MachineRSSPoint{At: machineDate(o.Current.At)}}
		if o.Current.Known {
			r.Current.Bytes = machinePointer(o.Current.Bytes)
		}
		if o.Known {
			r.Baseline = &MachineRSSPoint{At: machineDate(o.Baseline.At), Bytes: machinePointer(o.Baseline.Bytes)}
			r.ElapsedNanoseconds = machinePointer(uint64(o.Elapsed))
			r.Negative = machinePointer(o.Negative)
			r.MagnitudeBytes = machinePointer(o.Magnitude)
		}
		d.RSSHour = r
	}
	return d, validateMachineDetail(d)
}

func validMachineState(s string, optional bool) bool {
	switch s {
	case "available", "partial", "unsupported", "denied", "read_failed", "limit_exceeded", "binding_changed", "capture_expired":
		return true
	case "not_collected":
		return optional
	}
	return false
}
func machineUsable(s string) bool { return s == "available" || s == "partial" }
func validMachineStamp(s MachineStamp, needed bool) bool {
	return s.Generation > 0 && s.Generation < math.MaxUint64 && (!needed || s.StartedAt != nil && s.SampledAt != nil) && (s.StartedAt == nil || validReplayDetailTime(*s.StartedAt)) && (s.SampledAt == nil || validReplayDetailTime(*s.SampledAt))
}
func validMachineDictionary(ids []uint16, maximum int) bool {
	if len(ids) > maximum {
		return false
	}
	for i, id := range ids {
		if id > 8191 || i > 0 && id <= ids[i-1] {
			return false
		}
	}
	return true
}
func validMachineMask(encoded string, count int, known bool) bool {
	if !known {
		return encoded == ""
	}
	raw, err := base64.StdEncoding.Strict().DecodeString(encoded)
	return err == nil && len(raw) == (count+7)/8 && base64.StdEncoding.EncodeToString(raw) == encoded && (count%8 == 0 || raw[len(raw)-1]>>uint(count%8) == 0)
}
func validMachineID(v MachineID) bool {
	return validMachineState(v.State, true) && (v.State == "available") == (v.Value != nil) && (v.Value == nil || *v.Value >= 0)
}

func compareMachineGroups(a, b MachineThreadGroup) int {
	for _, pair := range [][2]string{{a.CPUs, b.CPUs}, {a.Nodes, b.Nodes}} {
		left, _ := base64.StdEncoding.Strict().DecodeString(pair[0])
		right, _ := base64.StdEncoding.Strict().DecodeString(pair[1])
		if n := bytes.Compare(left, right); n != 0 {
			return n
		}
	}
	if n := strings.Compare(a.CPUState, b.CPUState); n != 0 {
		return n
	}
	return strings.Compare(a.NodeState, b.NodeState)
}

func validateMachineDetail(d MachineDetail) error {
	b := d.Binding
	if d.SchemaVersion != 1 || d.Family != "machine" || d.Scope != "visible_namespace_metadata" || !utf8.ValidString(b.RunID) || b.RunID == "" || len(b.RunID) > 128 || !validReplayDetailTime(b.NodeStartedAt) || !validReplayDetailHash(b.GenesisHash, true) || b.SourceGeneration == 0 || b.SourceGeneration == math.MaxUint64 || !validReplayDetailTime(d.CapturedAt) || d.ProcessNUMAResidenceState != "not_collected" || d.GOMAXPROCS != nil && *d.GOMAXPROCS == 0 || len(d.Workers) > 8 {
		return errors.New("source_invalid")
	}
	if r := d.RSSBreakdown; r != nil && (!validReplayDetailTime(r.SampledAt) || r.SampledAt.Before(b.NodeStartedAt) || r.SampledAt.After(d.CapturedAt)) {
		return errors.New("source_invalid")
	}
	// Header pages omit row arrays. Reassembly always restores empty arrays for
	// present sections, so the full family must have that same unique encoding.
	if d.Topology != nil && (d.Topology.CPUs == nil || d.Topology.Nodes == nil) ||
		d.Affinity != nil && d.Affinity.Groups == nil || d.Links != nil && d.Links.Rows == nil {
		return errors.New("source_invalid")
	}
	if d.PoolSettingsState != "not_collected" && d.PoolSettingsState != "resolved_constructors" || (d.PoolSettingsState == "not_collected") != (len(d.Workers) == 0) {
		return errors.New("source_invalid")
	}
	workerNames := []string{"replay_transactions", "replay_streaming", "tpu_sigverify", "shred_sigverify", "harmonic_transactions", "harmonic_bundles", "block_production", "transaction_ingress"}
	var seen uint16
	for _, w := range d.Workers {
		i := slices.Index(workerNames, w.Name)
		if i < 0 || seen&(1<<i) != 0 || w.Affinity != "not_bound_by_node" || w.Enabled && w.Resolved == 0 || !w.Enabled && w.Resolved != 0 {
			return errors.New("source_invalid")
		}
		seen |= 1 << i
	}
	if t := d.Topology; t != nil {
		if !validMachineState(t.CPUState, false) || !validMachineState(t.NUMAState, false) || !validMachineStamp(t.MachineStamp, machineUsable(t.CPUState) || machineUsable(t.NUMAState)) || !validMachineDictionary(t.CPUDictionary, 256) || !validMachineDictionary(t.NodeDictionary, 64) || len(t.CPUs) != len(t.CPUDictionary) || len(t.Nodes) != len(t.NodeDictionary) || !machineUsable(t.CPUState) && len(t.CPUs) != 0 || !machineUsable(t.NUMAState) && len(t.Nodes) != 0 {
			return errors.New("source_invalid")
		}
		for i, c := range t.CPUs {
			if c.ID != t.CPUDictionary[i] || c.Online && !c.Present || !validMachineID(c.Package) || !validMachineID(c.Die) || !validMachineID(c.Core) || !validMachineState(c.SiblingsState, true) || !validMachineMask(c.Siblings, len(t.CPUs), c.SiblingsState == "available") || c.NUMANode != nil && !slices.Contains(t.NodeDictionary, *c.NUMANode) {
				return errors.New("source_invalid")
			}
		}
		for i, n := range t.Nodes {
			if n.ID != t.NodeDictionary[i] || !validMachineState(n.CPUsState, true) || !validMachineState(n.MemoryState, true) || !validMachineMask(n.CPUs, len(t.CPUs), n.CPUsState == "available") || (n.MemoryState == "available") != (n.TotalBytes != nil && n.FreeBytes != nil) || n.MemoryState != "available" && (n.TotalBytes != nil || n.FreeBytes != nil || n.ShmemBytes != nil) || n.FreeBytes != nil && *n.FreeBytes > *n.TotalBytes || !n.Online && (n.CPUsState != "not_collected" || n.MemoryState != "not_collected") {
				return errors.New("source_invalid")
			}
		}
	}
	if a := d.Affinity; a != nil {
		if !validMachineState(a.State, false) || !validMachineState(a.ThreadsState, true) || !validMachineStamp(a.MachineStamp, machineUsable(a.State)) || machineUsable(a.State) && a.TopologyGeneration == 0 || !validMachineDictionary(a.CPUDictionary, 256) || !validMachineDictionary(a.NodeDictionary, 64) || !a.HasCPUDictionary && len(a.CPUDictionary) != 0 || !a.HasNodeDictionary && len(a.NodeDictionary) != 0 || len(a.Groups) > 256 || a.ListedThreads > 256 || a.ObservedThreads > a.ListedThreads || a.Complete && (!a.MembershipStable || a.ObservedThreads != a.ListedThreads || a.State != "available") {
			return errors.New("source_invalid")
		}
		if t := d.Topology; t != nil && (a.TopologyGeneration != 0 && a.TopologyGeneration != t.Generation || a.HasCPUDictionary && !slices.Equal(a.CPUDictionary, t.CPUDictionary) || a.HasNodeDictionary && !slices.Equal(a.NodeDictionary, t.NodeDictionary)) {
			return errors.New("source_invalid")
		}
		var count uint64
		for i, g := range a.Groups {
			if g.Threads == 0 || !validMachineState(g.CPUState, false) || !validMachineState(g.NodeState, false) || g.CPUState == "available" && !a.HasCPUDictionary || g.NodeState == "available" && !a.HasNodeDictionary || !validMachineMask(g.CPUs, len(a.CPUDictionary), g.CPUState == "available") || !validMachineMask(g.Nodes, len(a.NodeDictionary), g.NodeState == "available") {
				return errors.New("source_invalid")
			}
			if i > 0 && compareMachineGroups(a.Groups[i-1], g) >= 0 {
				return errors.New("source_invalid")
			}
			count += uint64(g.Threads)
		}
		completeReads := a.MembershipStable && a.ThreadsState == "available" && a.ReportedThreads == a.ListedThreads && a.ObservedThreads == a.ListedThreads
		if machineUsable(a.State) && count != uint64(a.ListedThreads) || !machineUsable(a.State) && (len(a.Groups) != 0 || a.CPUIntersection != nil || a.NodeIntersection != nil) || a.CPUIntersection != nil && (!completeReads || !a.HasCPUDictionary || !validMachineMask(*a.CPUIntersection, len(a.CPUDictionary), true)) || a.NodeIntersection != nil && (!completeReads || !a.HasNodeDictionary || !validMachineMask(*a.NodeIntersection, len(a.NodeDictionary), true)) {
			return errors.New("source_invalid")
		}
		for _, entry := range []struct {
			pointer *string
			cpu     bool
		}{{a.CPUIntersection, true}, {a.NodeIntersection, false}} {
			if entry.pointer == nil {
				continue
			}
			var intersection []byte
			for i, g := range a.Groups {
				state, mask := g.NodeState, g.Nodes
				if entry.cpu {
					state, mask = g.CPUState, g.CPUs
				}
				if state != "available" {
					return errors.New("source_invalid")
				}
				raw, _ := base64.StdEncoding.Strict().DecodeString(mask)
				if i == 0 {
					intersection = raw
				} else {
					for j, v := range raw {
						intersection[j] &= v
					}
				}
			}
			if base64.StdEncoding.EncodeToString(intersection) != *entry.pointer {
				return errors.New("source_invalid")
			}
		}
	}
	if l := d.Links; l != nil {
		if !validMachineState(l.State, false) || !validMachineStamp(l.MachineStamp, machineUsable(l.State)) || machineUsable(l.State) && (l.TopologyGeneration == 0 || l.AffinityGeneration == 0) || l.MetadataStartedAt != nil && !validReplayDetailTime(*l.MetadataStartedAt) || len(l.Rows) > 256 || l.State != "available" && len(l.Rows) != 0 || d.Topology != nil && l.TopologyGeneration != 0 && l.TopologyGeneration != d.Topology.Generation || d.Affinity != nil && l.AffinityGeneration != 0 && l.AffinityGeneration != d.Affinity.Generation {
			return errors.New("source_invalid")
		}
		for i, row := range l.Rows {
			if row.Index == 0 || i > 0 && row.Index <= l.Rows[i-1].Index || !slices.Contains([]string{"not_reported", "unknown", "not_present", "down", "lower_layer_down", "testing", "dormant", "up"}, row.OperationalState) || !slices.Contains([]string{"none", "driver", "generic", "offload", "multi", "unknown"}, row.XDPMode) || !slices.Contains([]string{"not_reported", "other", "bond", "bridge", "vlan", "veth", "dummy", "gre", "gretap", "ip6gre", "ip6gretap", "ipip", "sit", "ip6tnl", "vti", "vti6", "vxlan", "geneve", "tun", "wireguard", "macvlan", "macvtap", "ipvlan", "ipvtap", "vrf"}, row.Kind) {
				return errors.New("source_invalid")
			}
		}
	}
	if r := d.Routes; r != nil {
		if !validMachineState(r.State, false) || !validMachineStamp(r.MachineStamp, machineUsable(r.State)) || r.MetadataStartedAt != nil && !validReplayDetailTime(*r.MetadataStartedAt) || r.NetworkStartedAt != nil && !validReplayDetailTime(*r.NetworkStartedAt) || d.Topology != nil && r.TopologyGeneration != 0 && r.TopologyGeneration != d.Topology.Generation || d.Affinity != nil && r.AffinityGeneration != 0 && r.AffinityGeneration != d.Affinity.Generation || d.Links != nil && r.LinkGeneration != 0 && r.LinkGeneration != d.Links.Generation {
			return errors.New("source_invalid")
		}
		if r.State == "available" {
			if d.Links == nil || d.Links.State != "available" || r.TopologyGeneration == 0 || r.AffinityGeneration == 0 || r.LinkGeneration == 0 || r.MetadataStartedAt == nil || r.NetworkStartedAt == nil || r.IPv4 == nil || r.IPv6 == nil || !validMachineRouteFamily(*r.IPv4) || !validMachineRouteFamily(*r.IPv6) || uint64(r.IPv4.MainUnicast)+uint64(r.IPv6.MainUnicast) > 2048 || d.Topology != nil && d.Topology.StartedAt != nil && !r.MetadataStartedAt.Equal(*d.Topology.StartedAt) || d.Links.StartedAt != nil && !r.NetworkStartedAt.Equal(*d.Links.StartedAt) {
				return errors.New("source_invalid")
			}
		} else if r.IPv4 != nil || r.IPv6 != nil {
			return errors.New("source_invalid")
		}
	}
	if l := d.Links; l != nil {
		for _, row := range l.Rows {
			if d.Routes == nil || d.Routes.State != "available" {
				if row.Routes != nil {
					return errors.New("source_invalid")
				}
				continue
			}
			if row.Routes == nil || !validMachineRouteCounts(row.Routes.IPv4) || !validMachineRouteCounts(row.Routes.IPv6) || row.Routes.IPv4.MainUnicast > d.Routes.IPv4.MainUnicast || row.Routes.IPv4.Default > d.Routes.IPv4.Default || row.Routes.IPv6.MainUnicast > d.Routes.IPv6.MainUnicast || row.Routes.IPv6.Default > d.Routes.IPv6.Default {
				return errors.New("source_invalid")
			}
		}
	}
	if r := d.RSSHour; r != nil {
		validPoint := func(p MachineRSSPoint) bool {
			return (p.At == nil || validReplayDetailTime(*p.At)) && (p.Bytes == nil || p.At != nil)
		}
		if r.Scope != "approximately_1h" || !validPoint(r.Current) {
			return errors.New("source_invalid")
		}
		if !r.Known {
			if !slices.Contains([]string{"binding_unavailable", "clock_unavailable", "insufficient_history", "clock_discontinuity", "sample_unavailable", "coverage_gap"}, r.Reason) || r.Baseline != nil || r.ElapsedNanoseconds != nil || r.Negative != nil || r.MagnitudeBytes != nil {
				return errors.New("source_invalid")
			}
		} else {
			if r.Reason != "known" || r.Baseline == nil || !validPoint(*r.Baseline) || r.Baseline.Bytes == nil || r.Current.Bytes == nil || r.ElapsedNanoseconds == nil || *r.ElapsedNanoseconds < uint64(time.Hour-15*time.Second) || *r.ElapsedNanoseconds > uint64(time.Hour+15*time.Second) || r.Negative == nil || r.MagnitudeBytes == nil {
				return errors.New("source_invalid")
			}
			current, baseline := *r.Current.Bytes, *r.Baseline.Bytes
			negative := current < baseline
			magnitude := current - baseline
			if negative {
				magnitude = baseline - current
			}
			if *r.Negative != negative || *r.MagnitudeBytes != magnitude {
				return errors.New("source_invalid")
			}
		}
	}
	return nil
}

func validMachineRouteCounts(c MachineRouteCounts) bool {
	return c.Default <= c.MainUnicast && c.MainUnicast <= 2048
}
func validMachineRouteFamily(c MachineRouteFamily) bool {
	return validMachineRouteCounts(c.MachineRouteCounts) && c.UnknownOutput <= c.MainUnicast && c.UnknownDefaultOutput <= c.Default && c.UnknownDefaultOutput <= c.UnknownOutput && uint64(c.Default)+uint64(c.UnknownOutput) <= uint64(c.MainUnicast)+uint64(c.UnknownDefaultOutput)
}

// MachinePageDescriptor binds one small typed page to the complete family digest.
type MachinePageDescriptor struct {
	Page   int    `json:"page"`
	Kind   string `json:"kind"`
	Offset int    `json:"offset"`
	Count  int    `json:"count"`
	Bytes  int    `json:"bytes"`
	Digest string `json:"digest"`
}

// MachineDetailManifest binds every typed page to the complete source digest.
type MachineDetailManifest struct {
	SchemaVersion int                     `json:"schema_version"`
	Family        string                  `json:"family"`
	Binding       MachineDetailBinding    `json:"binding"`
	CapturedAt    time.Time               `json:"captured_at"`
	Generation    string                  `json:"generation"`
	Bytes         int                     `json:"bytes"`
	Pages         []MachinePageDescriptor `json:"pages"`
}

// MachineDetailPage holds one header or consecutive rows of one fixed section.
// Its header omits row arrays: validate the complete detail only after assembly.
type MachineDetailPage struct {
	SchemaVersion int                  `json:"schema_version"`
	Family        string               `json:"family"`
	Generation    string               `json:"generation"`
	Page          int                  `json:"page"`
	Kind          string               `json:"kind"`
	Offset        int                  `json:"offset"`
	Header        *MachineDetail       `json:"header,omitempty"`
	CPUs          []MachineCPU         `json:"cpus,omitempty"`
	Nodes         []MachineNUMA        `json:"nodes,omitempty"`
	Groups        []MachineThreadGroup `json:"groups,omitempty"`
	Links         []MachineLink        `json:"links,omitempty"`
}

// PreparedMachineDetail retains encoded strings only, without source slices or pointers.
// Its dates and digest are immutable source facts, not a new freshness lease.
type PreparedMachineDetail struct {
	manifest, generation string
	pages                []string
}

// Manifest returns immutable page descriptors without renewing source dates.
func (p *PreparedMachineDetail) Manifest() string {
	if p == nil {
		return ""
	}
	return p.manifest
}

// Generation returns the SHA-256 digest of the complete canonical source detail.
func (p *PreparedMachineDetail) Generation() string {
	if p == nil {
		return ""
	}
	return p.generation
}

// Page returns an existing immutable page, with no source work or body copy.
func (p *PreparedMachineDetail) Page(index int) (string, bool) {
	if p == nil || index < 0 || index >= len(p.pages) {
		return "", false
	}
	return p.pages[index], true
}

// PrepareMachineDetail seals one complete generation. It does not read owners,
// establish a lease, publish a route or renew a previously captured observation.
func PrepareMachineDetail(d MachineDetail) (*PreparedMachineDetail, error) {
	if err := validateMachineDetail(d); err != nil {
		return nil, err
	}
	body, err := json.Marshal(d)
	if err != nil || len(body) > MaxMachineDetailBytes {
		return nil, errors.New("response_limit")
	}
	hash := sha256.Sum256(body)
	p := &PreparedMachineDetail{generation: hex.EncodeToString(hash[:])}
	m := MachineDetailManifest{SchemaVersion: 1, Family: "machine", Binding: d.Binding, CapturedAt: d.CapturedAt, Generation: p.generation, Bytes: len(body)}
	add := func(page MachineDetailPage, count int) error {
		page.SchemaVersion, page.Family, page.Generation, page.Page = 1, "machine", p.generation, len(p.pages)
		raw, e := json.Marshal(page)
		if e != nil || len(raw) > MaxMachinePageBytes || len(p.pages) >= machineMaxPages {
			return errors.New("response_limit")
		}
		sum := sha256.Sum256(raw)
		m.Pages = append(m.Pages, MachinePageDescriptor{page.Page, page.Kind, page.Offset, count, len(raw), hex.EncodeToString(sum[:])})
		p.pages = append(p.pages, string(raw))
		return nil
	}
	header := d
	if d.Topology != nil {
		v := *d.Topology
		v.CPUs, v.Nodes = nil, nil
		header.Topology = &v
	}
	if d.Affinity != nil {
		v := *d.Affinity
		v.Groups = nil
		header.Affinity = &v
	}
	if d.Links != nil {
		v := *d.Links
		v.Rows = nil
		header.Links = &v
	}
	if err = add(MachineDetailPage{Kind: "header", Header: &header}, 1); err != nil {
		return nil, err
	}
	for section := range 4 {
		count := 0
		switch section {
		case 0:
			if d.Topology != nil {
				count = len(d.Topology.CPUs)
			}
		case 1:
			if d.Topology != nil {
				count = len(d.Topology.Nodes)
			}
		case 2:
			if d.Affinity != nil {
				count = len(d.Affinity.Groups)
			}
		case 3:
			if d.Links != nil {
				count = len(d.Links.Rows)
			}
		}
		for offset := 0; offset < count; offset += machinePageRows {
			end := min(offset+machinePageRows, count)
			page := MachineDetailPage{Offset: offset}
			switch section {
			case 0:
				page.Kind, page.CPUs = "cpus", d.Topology.CPUs[offset:end]
			case 1:
				page.Kind, page.Nodes = "numa", d.Topology.Nodes[offset:end]
			case 2:
				page.Kind, page.Groups = "thread_groups", d.Affinity.Groups[offset:end]
			case 3:
				page.Kind, page.Links = "links", d.Links.Rows[offset:end]
			}
			if err = add(page, end-offset); err != nil {
				return nil, err
			}
		}
	}
	manifest, err := json.Marshal(m)
	if err != nil || len(manifest) > MaxMachineManifestBytes {
		return nil, errors.New("response_limit")
	}
	p.manifest = string(manifest)
	return p, nil
}

func decodeMachineDetail(body []byte) (MachineDetail, error) {
	var d MachineDetail
	if len(body) == 0 || len(body) > MaxMachineDetailBytes || json.Unmarshal(body, &d) != nil || validateMachineDetail(d) != nil {
		return d, errors.New("source_invalid")
	}
	canonical, err := json.Marshal(d)
	if err != nil || !bytes.Equal(body, canonical) {
		return MachineDetail{}, errors.New("source_invalid")
	}
	return d, nil
}

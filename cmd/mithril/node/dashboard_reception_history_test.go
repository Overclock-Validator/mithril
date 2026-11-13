package node

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/Overclock-Validator/mithril/pkg/blockstream"
	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
	"github.com/spf13/viper"
)

func TestDashboardShredHistoryConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name            string
		enabled         bool
		socket          string
		replay, network bool
		source, reason  string
	}{
		{name: "disabled"},
		{name: "complete", enabled: true, socket: "/private/node.sock", replay: true, network: true, source: "turbine"},
		{name: "missing_socket", enabled: true, replay: true, network: true, source: "turbine", reason: "dashboard.shred_history requires dashboard.socket"},
		{name: "blank_socket", enabled: true, socket: " \t\n", replay: true, network: true, source: "turbine", reason: "dashboard.shred_history requires dashboard.socket"},
		{name: "missing_replay", enabled: true, socket: "/private/node.sock", network: true, source: "turbine", reason: "dashboard.shred_history requires dashboard.replay_details"},
		{name: "missing_network", enabled: true, socket: "/private/node.sock", replay: true, source: "turbine", reason: "dashboard.shred_history requires dashboard.network_details"},
		{name: "unresolved_source", enabled: true, socket: "/private/node.sock", replay: true, network: true, reason: "dashboard.shred_history requires block.source=turbine"},
		{name: "rpc", enabled: true, socket: "/private/node.sock", replay: true, network: true, source: "rpc", reason: "dashboard.shred_history requires block.source=turbine"},
		{name: "lightbringer", enabled: true, socket: "/private/node.sock", replay: true, network: true, source: "lightbringer", reason: "dashboard.shred_history requires block.source=turbine"},
		{name: "noncanonical_source", enabled: true, socket: "/private/node.sock", replay: true, network: true, source: "Turbine", reason: "dashboard.shred_history requires block.source=turbine"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateDashboardShredHistory(tc.enabled, tc.socket, tc.replay, tc.network, tc.source)
			if tc.reason == "" && err != nil || tc.reason != "" && (err == nil || err.Error() != tc.reason) {
				t.Fatalf("configuration error = %v, want %q", err, tc.reason)
			}
		})
	}
	flag := Run.Flags().Lookup("dashboard-shred-history")
	if flag == nil || flag.DefValue != "false" {
		t.Fatal("shred history flag is not explicitly default off")
	}
	oldValue, oldChanged := flag.Value.String(), flag.Changed
	t.Cleanup(func() {
		if err := flag.Value.Set(oldValue); err != nil {
			t.Error(err)
		}
		flag.Changed = oldChanged
	})
	if err := flag.Value.Set("false"); err != nil {
		t.Fatal(err)
	}
	flag.Changed = false
	config := viper.New()
	resolve := func() bool {
		return resolveBoolOption(flag, config.IsSet("dashboard.shred_history"), config.GetBool("dashboard.shred_history"))
	}
	config.Set("dashboard.network_details", true)
	config.Set("dashboard.replay_details", true)
	if resolve() {
		t.Fatal("other dashboard options enabled history implicitly")
	}
	config.Set("dashboard.shred_history", true)
	if !resolve() {
		t.Fatal("explicit configuration enable was ignored")
	}
	if err := Run.Flags().Set("dashboard-shred-history", "false"); err != nil {
		t.Fatal(err)
	}
	if resolve() {
		t.Fatal("explicit CLI false did not override configuration")
	}
	config.Set("dashboard.shred_history", false)
	if err := Run.Flags().Set("dashboard-shred-history", "true"); err != nil {
		t.Fatal(err)
	}
	if !resolve() {
		t.Fatal("explicit CLI true did not override configuration")
	}
}

func TestDashboardShredHistoryConstructorOptIn(t *testing.T) {
	for _, role := range []string{"verifying", "validator"} {
		for _, enabled := range []bool{false, true} {
			t.Run(role+map[bool]string{false: "_disabled", true: "_enabled"}[enabled], func(t *testing.T) {
				dir, err := os.MkdirTemp("/tmp", "node-reception-option-")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := os.RemoveAll(dir); err != nil {
						t.Error(err)
					}
				})
				path := filepath.Join(dir, "feed.sock")
				d := startNodeDashboard(context.Background(), time.Now(), path, "", "reception-option", "isolated", role,
					true, false, true, true, false, false, false, enabled, false, false, false)
				if d == nil {
					t.Fatal("node feed did not start")
				}
				t.Cleanup(d.close)
				d.mu.Lock()
				configured, owner := d.receptionEnabled, d.reception
				d.mu.Unlock()
				if configured != enabled || owner != nil {
					t.Fatal("constructor ignored option or allocated a recorder")
				}
				transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, "unix", path)
				}}
				t.Cleanup(transport.CloseIdleConnections)
				client := &http.Client{Transport: transport, Timeout: time.Second}
				for _, knownGenesis := range []bool{false, true} {
					if knownGenesis {
						d.bootstrap((solana.Hash{1}).String())
					}
					response, err := client.Get("http://unix/details/reception")
					if err != nil {
						t.Fatal(err)
					}
					body, readErr := io.ReadAll(io.LimitReader(response.Body, 513))
					closeErr := response.Body.Close()
					if readErr != nil || closeErr != nil || len(body) > 512 {
						t.Fatalf("bounded response: %v %v", readErr, closeErr)
					}
					if !enabled && response.StatusCode != http.StatusNotFound {
						t.Fatalf("disabled reception route status %d", response.StatusCode)
					}
					if enabled {
						var unavailable struct {
							Schema int    `json:"schema_version"`
							Family string `json:"family"`
							State  string `json:"state"`
						}
						if response.StatusCode != http.StatusServiceUnavailable || json.Unmarshal(body, &unavailable) != nil ||
							unavailable.Schema != 1 || unavailable.Family != "reception" || unavailable.State != "unavailable" ||
							response.Header.Get("Mithril-Detail-Remaining-Milliseconds") != "" {
							t.Fatal("unstarted recorder was exposed as known or fresh")
						}
					}
					_, sampleErr := d.sampleReceptionHistory()
					want := "source_unavailable"
					if enabled {
						want = "waiting_source"
					}
					if sampleErr == nil || sampleErr.Error() != want {
						t.Fatalf("unstarted sampler error = %v, want %s", sampleErr, want)
					}
					d.mu.Lock()
					owner = d.reception
					d.mu.Unlock()
					if owner != nil {
						t.Fatal("HTTP, sampler or genesis arrival started the recorder")
					}
				}
				d.close()
			})
		}
	}
}

func receptionNodeForTest(t *testing.T) *nodeDashboard {
	t.Helper()
	at := time.Now()
	genesis := (solana.Hash{1}).String()
	d := &nodeDashboard{runStartedAt: at, state: dashboard.Snapshot{
		RunID: "reception-node", StartedAt: at.UTC(), GenesisHash: &genesis,
		Version: "dev", Network: "isolated", Mode: "verifying", Phase: "bootstrap",
	}}
	t.Cleanup(d.close)
	return d
}

func TestDashboardReceptionHistoryLifecycle(t *testing.T) {
	var absent *nodeDashboard
	if absent.enableReceptionHistory() != nil || !absent.closeReceptionHistoryAfterProducers() {
		t.Fatal("nil dashboard was not inert")
	}
	absent.interruptReceptionHistory()
	if h, err := absent.startReceptionHistory(); h != nil || err != nil {
		t.Fatal("nil dashboard allocated an owner")
	}
	if _, err := absent.sampleReceptionHistory(); err == nil || err.Error() != "source_unavailable" {
		t.Fatal("nil dashboard supplied known history")
	}
	d := receptionNodeForTest(t)
	if h, err := d.startReceptionHistory(); h != nil || err != nil || d.reception != nil {
		t.Fatal("default-off allocated an owner")
	}
	if _, err := d.sampleReceptionHistory(); err == nil || err.Error() != "source_unavailable" {
		t.Fatal("default-off sampler became enabled")
	}
	genesis := d.state.GenesisHash
	d.state.GenesisHash = nil
	sampler := d.enableReceptionHistory()
	if sampler == nil || d.reception != nil {
		t.Fatal("enable must supply callbacks without starting a recorder")
	}
	if h, err := d.startReceptionHistory(); h != nil || err == nil || err.Error() != "waiting_source" {
		t.Fatal("unknown genesis started a recorder")
	}
	if _, err := sampler.Metadata(); err == nil || err.Error() != "waiting_source" {
		t.Fatal("unstarted recording did not remain waiting")
	}
	d.state.GenesisHash = genesis
	h, err := d.startReceptionHistory()
	if err != nil || h == nil {
		t.Fatalf("start: %v", err)
	}
	if again, err := d.startReceptionHistory(); err != nil || again != h {
		t.Fatal("repeat startup replaced the owner")
	}
	c, err := sampler.Metadata()
	if err != nil || c.Metadata.Binding.RecordingGeneration != 1 || c.ConsumedThrough != 0 || len(c.Chunks) != 0 ||
		c.Metadata.StartupAtCapture.State != "open" || c.Closed || c.Interrupted || c.Exhausted {
		t.Fatalf("known empty capture: %+v %v", c, err)
	}
	if c.PublishedAt == c.PublishedAt.Round(0) || !c.PublishedAt.Equal(c.Metadata.PublishedAt) || c.Metadata.PublishedAt.Location() != time.UTC {
		t.Fatal("source publication lost its original clock or UTC wire copy")
	}
	if !d.closeReceptionHistoryAfterProducers() || !d.closeReceptionHistoryAfterProducers() {
		t.Fatal("empty joined drain was not repeatable")
	}
	closed, err := sampler.Metadata()
	if err != nil || !closed.Closed || closed.Interrupted {
		t.Fatal("terminal empty history became unavailable")
	}
	d.interruptReceptionHistory()
	if after, err := sampler.Metadata(); err != nil || !reflect.DeepEqual(after, closed) {
		t.Fatal("later fallback interrupt changed an already drained capture")
	}
	if d.enableReceptionHistory() != nil {
		t.Fatal("stopped dashboard re-enabled capture")
	}
	if again, err := d.startReceptionHistory(); again != nil || err == nil {
		t.Fatal("stopped dashboard restarted its recorder")
	}
	stoppedBeforeStart := receptionNodeForTest(t)
	stoppedBeforeStart.interruptReceptionHistory()
	if stoppedBeforeStart.enableReceptionHistory() != nil || stoppedBeforeStart.reception != nil {
		t.Fatal("stop-before-start did not prohibit activation")
	}
}

func TestDashboardReceptionHistoryInvalidBinding(t *testing.T) {
	for name, mutate := range map[string]func(*nodeDashboard){
		"empty_run":           func(d *nodeDashboard) { d.state.RunID = "" },
		"long_run":            func(d *nodeDashboard) { d.state.RunID = strings.Repeat("x", 129) },
		"invalid_utf8":        func(d *nodeDashboard) { d.state.RunID = string([]byte{0xff}) },
		"zero_start":          func(d *nodeDashboard) { d.state.StartedAt = time.Time{} },
		"different_raw_start": func(d *nodeDashboard) { d.runStartedAt = d.runStartedAt.Add(time.Second) },
		"non_utc_start":       func(d *nodeDashboard) { d.state.StartedAt = d.state.StartedAt.In(time.FixedZone("offset", 3600)) },
		"zero_genesis":        func(d *nodeDashboard) { value := (solana.Hash{}).String(); d.state.GenesisHash = &value },
		"bad_genesis":         func(d *nodeDashboard) { value := "not-a-hash"; d.state.GenesisHash = &value },
	} {
		t.Run(name, func(t *testing.T) {
			d := receptionNodeForTest(t)
			d.enableReceptionHistory()
			mutate(d)
			if h, err := d.startReceptionHistory(); h != nil || err == nil || d.reception != nil {
				t.Fatal("invalid startup binding created an owner")
			}
		})
	}
}

// Typed submissions exercise the actual recorder and node adapter, not UDP
// authentication or the runLive bootstrap/recovery call graph.
func populatedReceptionNode(t *testing.T) (*nodeDashboard, dashboard.ReceptionHistoryCapture) {
	t.Helper()
	d := receptionNodeForTest(t)
	d.enableReceptionHistory()
	h, err := d.startReceptionHistory()
	if err != nil {
		t.Fatal(err)
	}
	at := d.runStartedAt
	prewarm, ok := h.RegisterSource(turbine.ShredHistoryPrewarm, at)
	if !ok {
		t.Fatal("prewarm registration refused")
	}
	live, ok := h.RegisterSource(turbine.ShredHistoryLive, at.Add(time.Millisecond))
	if !ok {
		t.Fatal("live registration refused")
	}
	if !prewarm.TryReceipt(turbine.ShredHistoryReceipt{At: at, Slot: 7, Origin: turbine.ShredHistoryTVU, Root: solana.Hash{3}}) ||
		!live.TryReceipt(turbine.ShredHistoryReceipt{At: at.Add(-time.Second), Slot: 0, Origin: turbine.ShredHistoryRepairMatched}) ||
		!live.TryReceipt(turbine.ShredHistoryReceipt{At: at.Add(-2 * time.Second), Slot: math.MaxUint64,
			Index: math.MaxUint32, FECSetIndex: math.MaxUint32, Version: math.MaxUint16, Variant: math.MaxUint8,
			Origin: turbine.ShredHistoryRepairUnmatched, Root: solana.Hash{4}}) ||
		!live.TryMarker(turbine.ShredHistoryMarker{At: at.Add(-3 * time.Second), Slot: 9, Bankhash: solana.Hash{5}, ReplayAttempt: 2, Rewinds: 1, CompletedIndex: 3}) {
		t.Fatal("bounded typed source fixture refused")
	}
	prewarm.Retire()
	live.Retire()
	if !d.closeReceptionHistoryAfterProducers() {
		t.Fatal("joined source failed to drain")
	}
	c, err := d.sampleReceptionHistory()
	if err != nil || len(c.Chunks) == 0 {
		t.Fatalf("sample: %v", err)
	}
	return d, c
}

func TestDashboardReceptionHistoryActualOwnerCopy(t *testing.T) {
	d, c := populatedReceptionNode(t)
	source, ok := d.reception.owner.Metadata()
	if !ok || c.PublishedAt != source.PublishedAt || c.Metadata.PublishedAt != source.PublishedAt.UTC() ||
		c.ConsumedThrough != 3 || !c.Closed || c.Interrupted || c.Exhausted || c.Metadata.Revision != 0 {
		t.Fatal("node changed source clocks, terminal state or revision ownership")
	}
	startup := c.Metadata.StartupAtCapture
	if startup.State != "closed" || startup.PrefixCutoff != 3 || startup.Limited || startup.Marker == nil ||
		startup.Marker.At != source.Marker.At.UTC() || startup.Marker.SourceGeneration != 2 || startup.Marker.Slot != 9 ||
		startup.Marker.Bankhash != (solana.Hash{5}).String() || startup.Marker.ReplayAttempt != 2 ||
		startup.Marker.Rewinds != 1 || startup.Marker.CompletedIndex != 3 {
		t.Fatal("node changed the historical marker")
	}
	if len(c.Metadata.Sources) != 2 || c.Metadata.Sources[0].Kind != "prewarm" || c.Metadata.Sources[1].Kind != "live" ||
		!c.Metadata.Sources[0].Retired || !c.Metadata.Sources[1].Retired {
		t.Fatal("source lifetimes were not copied")
	}
	for i, slot := range []uint64{7, 0, math.MaxUint64} {
		if !reflect.DeepEqual(c.Metadata.Ranges[i], []dashboard.ReceptionRange{{First: slot, Last: slot}}) {
			t.Fatalf("origin %d ranges changed", i)
		}
	}
	for i, chunk := range source.Chunks {
		want, exists := d.reception.owner.ChunkBody(chunk.FirstSequence, chunk.LastSequence, chunk.Digest)
		got, copied := d.receptionHistoryChunk(c.Metadata.Binding, c.Chunks[i])
		if !exists || !copied || got != want || c.Chunks[i].Digest != hex.EncodeToString(chunk.Digest[:]) ||
			c.Chunks[i].MinAt != chunk.MinAt || c.Chunks[i].MaxAt != chunk.MaxAt || c.Chunks[i].Count != chunk.Count {
			t.Fatal("node changed an immutable chunk or its descriptor")
		}
	}
	c.Metadata.Sources[0].Kind = "changed"
	c.Metadata.Ranges[0][0].First = 99
	c.Metadata.StartupAtCapture.Marker.Slot = 99
	c.Chunks[0].Digest = "changed"
	again, err := d.sampleReceptionHistory()
	if err != nil || again.Metadata.Sources[0].Kind != "prewarm" || again.Metadata.Ranges[0][0].First != 7 ||
		again.Metadata.StartupAtCapture.Marker.Slot != 9 || again.Chunks[0].Digest == "changed" || again.PublishedAt != source.PublishedAt {
		t.Fatal("caller mutation leaked into the owner or renewed its clock")
	}
	prepared := again.Metadata
	prepared.Revision = 1
	if _, err := dashboard.PrepareReceptionMetadata(prepared); err != nil {
		t.Fatalf("mapped source metadata is not valid wire input: %v", err)
	}
}

func TestDashboardReceptionHistoryFencesAndInterrupt(t *testing.T) {
	d, c := populatedReceptionNode(t)
	before := d.reception
	for name, mutate := range map[string]func(*dashboard.ReceptionRecordingBinding){
		"run":        func(b *dashboard.ReceptionRecordingBinding) { b.RunID += "-other" },
		"start":      func(b *dashboard.ReceptionRecordingBinding) { b.NodeStartedAt = b.NodeStartedAt.Add(time.Nanosecond) },
		"genesis":    func(b *dashboard.ReceptionRecordingBinding) { b.GenesisHash = (solana.Hash{2}).String() },
		"generation": func(b *dashboard.ReceptionRecordingBinding) { b.RecordingGeneration++ },
	} {
		t.Run(name, func(t *testing.T) {
			binding := c.Metadata.Binding
			mutate(&binding)
			if _, ok := d.receptionHistoryChunk(binding, c.Chunks[0]); ok {
				t.Fatal("foreign recording body accepted")
			}
		})
	}
	for name, mutate := range map[string]func(*dashboard.ReceptionChunkDescriptor){
		"first":               func(c *dashboard.ReceptionChunkDescriptor) { c.FirstSequence++ },
		"last":                func(c *dashboard.ReceptionChunkDescriptor) { c.LastSequence++ },
		"digest":              func(c *dashboard.ReceptionChunkDescriptor) { c.Digest = strings.Repeat("0", 64) },
		"noncanonical_digest": func(c *dashboard.ReceptionChunkDescriptor) { c.Digest = strings.Repeat("A", 64) },
		"bytes":               func(c *dashboard.ReceptionChunkDescriptor) { c.Bytes++ },
	} {
		t.Run(name, func(t *testing.T) {
			chunk := c.Chunks[0]
			mutate(&chunk)
			if _, ok := d.receptionHistoryChunk(c.Metadata.Binding, chunk); ok {
				t.Fatal("different chunk identity accepted")
			}
		})
	}
	for name, mutate := range map[string]func(*dashboard.Snapshot){
		"run":             func(s *dashboard.Snapshot) { s.RunID += "-other" },
		"start":           func(s *dashboard.Snapshot) { s.StartedAt = s.StartedAt.Add(time.Nanosecond) },
		"genesis":         func(s *dashboard.Snapshot) { value := (solana.Hash{2}).String(); s.GenesisHash = &value },
		"unknown_genesis": func(s *dashboard.Snapshot) { s.GenesisHash = nil },
	} {
		t.Run("node_"+name, func(t *testing.T) {
			old := d.state
			mutate(&d.state)
			if d.receptionHistoryCurrent(before) {
				t.Fatal("post-copy context recheck accepted changed node binding")
			}
			if _, err := d.sampleReceptionHistory(); err == nil || err.Error() != "binding_changed" {
				t.Fatal("sampler accepted changed node binding")
			}
			if _, ok := d.receptionHistoryChunk(c.Metadata.Binding, c.Chunks[0]); ok {
				t.Fatal("body callback accepted changed node binding")
			}
			d.state = old
		})
	}
	d.reception = &nodeReceptionHistory{binding: before.binding, owner: before.owner}
	if d.receptionHistoryCurrent(before) {
		t.Fatal("post-copy pointer fence accepted a replacement")
	}
	d.reception = before
	next := receptionNodeForTest(t)
	next.state.RunID += "-replacement"
	next.enableReceptionHistory()
	if _, err := next.startReceptionHistory(); err != nil {
		t.Fatal(err)
	}
	nextCapture, err := next.sampleReceptionHistory()
	if err != nil || nextCapture.ConsumedThrough != 0 || nextCapture.Metadata.Binding.RecordingGeneration != 1 {
		t.Fatal("new run inherited old history")
	}
	if _, ok := next.receptionHistoryChunk(c.Metadata.Binding, c.Chunks[0]); ok {
		t.Fatal("new run exposed old recording bytes")
	}
	interrupted := receptionNodeForTest(t)
	interrupted.enableReceptionHistory()
	h, err := interrupted.startReceptionHistory()
	if err != nil || !h.MarkMarkerUnknown() {
		t.Fatal("interrupt fixture failed")
	}
	interrupted.close()
	interrupted.close()
	if interrupted.closeReceptionHistoryAfterProducers() {
		t.Fatal("interruption later claimed full drain")
	}
	got, err := interrupted.sampleReceptionHistory()
	if err != nil || !got.Closed || !got.Interrupted || got.Metadata.StartupAtCapture.State != "unknown" || !got.Metadata.StartupAtCapture.Limited {
		t.Fatal("terminal unknown was hidden or relabeled during interruption")
	}
}

func TestDashboardReceptionHistoryConcurrentCopiesAndStop(t *testing.T) {
	d := receptionNodeForTest(t)
	d.enableReceptionHistory()
	h, err := d.startReceptionHistory()
	if err != nil {
		t.Fatal(err)
	}
	source, ok := h.RegisterSource(turbine.ShredHistoryLive, time.Now())
	if !ok {
		t.Fatal("source registration refused")
	}
	var wg sync.WaitGroup
	wg.Go(func() {
		defer source.Retire()
		for i := range 128 {
			if !source.TryReceipt(turbine.ShredHistoryReceipt{At: d.runStartedAt, Slot: uint64(i), Origin: turbine.ShredHistoryTVU}) {
				t.Error("bounded producer refused input")
				return
			}
		}
	})
	for range 2 {
		wg.Go(func() {
			for range 128 {
				c, err := d.sampleReceptionHistory()
				if err != nil {
					t.Error(err)
					return
				}
				for _, chunk := range c.Chunks {
					if _, ok := d.receptionHistoryChunk(c.Metadata.Binding, chunk); !ok {
						t.Error("non-evicted source body unavailable")
						return
					}
				}
			}
		})
	}
	wg.Wait() // Producer and readers join before the promised complete drain.
	for range 2 {
		wg.Go(func() {
			if !d.closeReceptionHistoryAfterProducers() {
				t.Error("concurrent joined close failed")
			}
		})
	}
	wg.Wait()
	c, err := d.sampleReceptionHistory()
	if err != nil || c.ConsumedThrough != 128 || !c.Closed || c.Interrupted {
		t.Fatal("joined close lost accepted inputs")
	}
}

func TestDashboardReceptionHistoryStopFencesPendingCapture(t *testing.T) {
	for _, interrupt := range []bool{false, true} {
		t.Run(map[bool]string{false: "drain", true: "interrupt"}[interrupt], func(t *testing.T) {
			d := receptionNodeForTest(t)
			d.enableReceptionHistory()
			h, err := d.startReceptionHistory()
			if err != nil {
				t.Fatal(err)
			}
			owner := d.reception
			live, ok := h.Metadata()
			if !ok || live.Closed || live.Interrupted || live.Exhausted {
				t.Fatal("fixture did not capture a live original source publication")
			}
			if interrupt {
				d.interruptReceptionHistory()
			} else if !d.closeReceptionHistoryAfterProducers() {
				t.Fatal("empty producer-joined close failed")
			}
			pending, err := receptionHistoryCapture(live, owner.binding)
			if err != nil || pending.Closed || pending.Interrupted || pending.Exhausted || pending.PublishedAt != live.PublishedAt {
				t.Fatal("pre-stop value changed before the admission check")
			}
			// Use the same metadata admission seam as sampleReceptionHistory.
			// Historical chunk-body admission must remain independent of stop.
			if d.receptionHistoryCaptureCurrent(owner, pending) {
				t.Fatal("pre-stop live capture passed post-stop admission")
			}
			terminal, err := d.sampleReceptionHistory()
			if err != nil || !terminal.Closed || !d.receptionHistoryCurrent(owner) {
				t.Fatal("stop made terminal capture or historical bodies unavailable")
			}
		})
	}
}

func TestDashboardReceptionHistoryRealUnixFeed(t *testing.T) {
	d, capture := populatedReceptionNode(t)
	dir, err := os.MkdirTemp("/tmp", "node-reception-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	path := filepath.Join(dir, "feed.sock")
	sampler := dashboard.ReceptionHistorySampler{Metadata: d.sampleReceptionHistory, ChunkBody: d.receptionHistoryChunk}
	feed, err := dashboard.ListenFeedWithDetails(context.Background(), path, "", d.snapshot, dashboard.DetailSamplers{Reception: &sampler})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", path)
	}}
	client := &http.Client{Transport: transport, Timeout: time.Second}
	t.Cleanup(transport.CloseIdleConnections)
	get := func(path string, limit int) (int, []byte, http.Header) {
		t.Helper()
		r, err := client.Get("http://unix" + path)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, int64(limit+1)))
		closeErr := r.Body.Close()
		if err != nil || closeErr != nil || len(body) > limit {
			t.Fatalf("bounded body read: %v %v", err, closeErr)
		}
		return r.StatusCode, body, r.Header
	}
	var root dashboard.ReceptionHistoryRoot
	deadline := time.Now().Add(3 * time.Second)
	for {
		status, body, header := get("/details/reception", dashboard.MaxReceptionRootBytes)
		if status == http.StatusOK {
			if err := json.Unmarshal(body, &root); err != nil || root.State != "closed" || root.Binding != capture.Metadata.Binding ||
				!root.PublishedAt.Equal(capture.PublishedAt) || root.ConsumedThrough != 3 || header.Get("Mithril-Detail-Remaining-Milliseconds") != "" {
				t.Fatalf("node sampler root changed source facts: %v", err)
			}
			break
		}
		if status != http.StatusServiceUnavailable || time.Now().After(deadline) {
			t.Fatalf("node sampler never published: status %d", status)
		}
		time.Sleep(50 * time.Millisecond)
	}
	for _, chunk := range capture.Chunks {
		want, ok := d.receptionHistoryChunk(capture.Metadata.Binding, chunk)
		status, body, header := get("/details/reception/chunk/"+root.RecordingID+"/"+chunk.Digest, dashboard.MaxReceptionChunkBytes)
		if !ok || status != http.StatusOK || string(body) != want || header.Get("ETag") != `"`+chunk.Digest+`"` {
			t.Fatal("actual node sampler/Feed bytes differ from retained source bytes")
		}
	}
	if after, err := d.sampleReceptionHistory(); err != nil || !reflect.DeepEqual(after, capture) {
		t.Fatal("Feed polling or HTTP viewing changed the stopped source capture")
	}
	feed.Close()
}

// startReceptionNodePrewarm uses the real constructor and one signed loopback
// TVU packet. It does not execute runLive or the replay recovery wrapper.
func startReceptionNodePrewarm(t *testing.T, h *turbine.ShredHistory, wantSequence uint64) *blockstream.TurbinePrewarm {
	t.Helper()
	peer, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	reserved, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	address := reserved.LocalAddr().(*net.UDPAddr)
	if err := reserved.Close(); err != nil {
		t.Fatal(err)
	}
	var seed [ed25519.SeedSize]byte
	seed[0] = 1
	leader := solana.PrivateKey(ed25519.NewKeyFromSeed(seed[:]))
	generator := turbine.ShredGenerator{Slot: 42, ParentSlot: 41, Version: 7}
	packets, _, _, _, err := generator.MakeShredsFromData(leader, nil, false, solana.Hash{1}, 0, 0)
	if err != nil || len(packets) == 0 {
		t.Fatalf("signed packet fixture: %v", err)
	}
	pw, err := blockstream.StartTurbinePrewarm(blockstream.TurbinePrewarmConfig{
		BindAddr: address.String(), GossipEntrypoint: peer.LocalAddr().String(), GossipBindAddr: "127.0.0.1:0",
		AdvertisedIP: "127.0.0.1", ShredVersion: 7, ShredHistory: h,
		LeaderForSlot: func(uint64) (solana.PublicKey, bool) { return leader.PublicKey(), true },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pw.Stop)
	sender, err := net.DialUDP("udp4", nil, address)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	if err := sender.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if n, err := sender.Write(packets[0]); err != nil || n != len(packets[0]) {
		t.Fatalf("signed loopback send: bytes=%d, error=%v", n, err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		m, ok := h.Metadata()
		if ok && m.LastSequence == wantSequence {
			return pw
		}
		if time.Now().After(deadline) {
			t.Fatalf("prewarm did not record expected sequence %d", wantSequence)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestDashboardReceptionHistoryPrewarmFinish(t *testing.T) {
	for _, joined := range []bool{true, false} {
		t.Run(map[bool]string{true: "joined_drain", false: "bootstrap_interrupt"}[joined], func(t *testing.T) {
			d := receptionNodeForTest(t)
			d.enableReceptionHistory()
			h, err := d.startReceptionHistory()
			if err != nil {
				t.Fatal(err)
			}
			pw := startReceptionNodePrewarm(t, h, 1)
			before, ok := h.Metadata()
			if !ok || len(before.Chunks) != 1 || len(before.Sources) != 1 || before.Sources[0].Retired {
				t.Fatal("live prewarm did not publish its original receipt")
			}
			chunk := before.Chunks[0]
			body, ok := h.ChunkBody(chunk.FirstSequence, chunk.LastSequence, chunk.Digest)
			if !ok {
				t.Fatal("original body missing")
			}
			var lifetime nodePrewarmLifetime
			lifetime.mu.Lock() // The existing manifest callback still owns startup.
			started, done := make(chan struct{}, 2), make(chan bool, 2)
			for range 2 {
				go func() {
					started <- struct{}{}
					done <- d.finishReceptionHistory(&lifetime, joined)
				}()
			}
			<-started
			<-started
			select {
			case <-done:
				lifetime.mu.Unlock()
				t.Fatal("finish bypassed the existing startup critical section")
			case <-time.After(20 * time.Millisecond):
			}
			// The callback installs its actual collector before releasing the lock.
			lifetime.collector = pw
			lifetime.mu.Unlock()
			for range 2 {
				select {
				case drained := <-done:
					if drained != joined {
						t.Fatalf("drained=%v, want %v", drained, joined)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("prewarm/recorder join did not complete")
				}
			}
			after, ok := h.Metadata()
			if !ok || !after.Closed || after.Interrupted == joined || after.LastSequence != 1 ||
				len(after.Sources) != 1 || !after.Sources[0].Retired || after.Sources[0].Kind != turbine.ShredHistoryPrewarm ||
				after.Sources[0].StartedAt != before.Sources[0].StartedAt || len(after.Chunks) != 1 || after.Chunks[0] != chunk {
				t.Fatal("recorder finished before receiver retirement or changed retained source facts")
			}
			if got, ok := h.ChunkBody(chunk.FirstSequence, chunk.LastSequence, chunk.Digest); !ok || got != body {
				t.Fatal("finish changed the historical body")
			}
			if lifetime.collector != nil || !lifetime.closing || d.finishReceptionHistory(&lifetime, !joined) != joined {
				t.Fatal("repeated cleanup changed the original shutdown decision")
			}
			if terminal, err := d.sampleReceptionHistory(); err != nil || !terminal.Closed || terminal.Interrupted == joined {
				t.Fatal("terminal node callback was unavailable or mislabeled")
			}
		})
	}
}

func TestDashboardReceptionHistoryPrewarmOwnerReuse(t *testing.T) {
	d := receptionNodeForTest(t)
	d.enableReceptionHistory()
	h, err := d.startReceptionHistory()
	if err != nil {
		t.Fatal(err)
	}
	first := startReceptionNodePrewarm(t, h, 1)
	lifetime := nodePrewarmLifetime{collector: first}
	if lifetime.forReplay(false) != first || lifetime.closing {
		t.Fatal("default-off selection changed callback admission")
	}
	if lifetime.forReplay(true) != first || !lifetime.closing {
		t.Fatal("opted-in handoff did not close late producer admission")
	}
	if blocks, dropped := first.Handover(); len(blocks) != 0 || dropped != 0 {
		t.Fatal("partial signed receipt fabricated a completed block")
	}
	lifetime.mu.Lock()
	lifetime.collector = nil
	lifetime.mu.Unlock()
	if again, err := d.startReceptionHistory(); err != nil || again != h {
		t.Fatal("handover replaced the node-run owner")
	}
	// Reuse the concrete prewarm constructor, not a claimed ReplayBlocks retry.
	// The existing blockstream retry test owns actual live-receiver retry coverage.
	second := startReceptionNodePrewarm(t, h, 2)
	second.Stop()
	if !d.finishReceptionHistory(&lifetime, true) {
		t.Fatal("joined reused owner did not drain")
	}
	m, ok := h.Metadata()
	if !ok || m.LastSequence != 2 || len(m.Sources) != 2 || m.Binding.RunID != d.state.RunID {
		t.Fatal("constructor reuse lost run history")
	}
	for i, source := range m.Sources {
		if source.Generation != uint64(i+1) || !source.Retired || source.Kind != turbine.ShredHistoryPrewarm {
			t.Fatal("constructor reuse did not keep distinct retired source generations")
		}
	}
}

func TestDashboardReceptionHistoryFinishDisabled(t *testing.T) {
	var absent *nodeDashboard
	var lifetime nodePrewarmLifetime
	if !absent.finishReceptionHistory(&lifetime, false) {
		t.Fatal("nil dashboard was not inert")
	}
	d := receptionNodeForTest(t)
	for _, joined := range []bool{false, true} {
		if !d.finishReceptionHistory(&lifetime, joined) || lifetime.closing || d.receptionStopped || d.reception != nil {
			t.Fatal("default-off cleanup changed prewarm admission or allocated history")
		}
	}
}

func TestDashboardReceptionHistoryHeaderBound(t *testing.T) {
	// Charge the concrete owner, prewarm lifetime, sampler and added dashboard
	// fields, plus a conservative 256 bytes for method/defer closure captures.
	// This is a typed model allowance, not measured heap usage or whole-node RSS.
	typed := unsafe.Sizeof(nodeReceptionHistory{}) + unsafe.Sizeof(nodePrewarmLifetime{}) +
		unsafe.Sizeof(dashboard.ReceptionHistorySampler{}) + 2*unsafe.Sizeof(bool(false)) +
		2*unsafe.Sizeof((*turbine.ShredHistory)(nil))
	const closureAllowance = 256
	if typed+closureAllowance > 1024 {
		t.Fatalf("node reception headers=%d + closures=%d exceed 1024", typed, closureAllowance)
	}
	t.Logf("node reception typed headers=%d, closure allowance=%d, total=%d", typed, closureAllowance, typed+closureAllowance)
}

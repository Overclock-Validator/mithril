package rpcserver

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

func TestMonitoringCoversEveryInstrumentedMethod(t *testing.T) {
	s := NewRpcServer(nil, 0, &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32}, solana.Hash{1})
	defer s.listener.Close()
	s.EnableMonitoring()
	endpoint := httptest.NewServer(s)
	defer endpoint.Close()
	errorsByMethod := make(map[string]uint64)
	for _, name := range instrumentedRPCMethods {
		if _, ok := supportedRPCMethods[name]; !ok {
			t.Fatalf("instrumented method %q is not registered", name)
		}
		// Invalid method parameters exercise dispatch without accounts, signing,
		// transaction simulation or forwarding to an external cluster.
		request := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":%q,"params":[false]}`, name)
		response, err := endpoint.Client().Post(endpoint.URL, "application/json", strings.NewReader(request))
		if err != nil {
			t.Fatal(err)
		}
		var result struct {
			Error json.RawMessage `json:"error"`
		}
		err = json.NewDecoder(response.Body).Decode(&result)
		response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Error) != 0 && string(result.Error) != "null" {
			errorsByMethod[name] = 1
		}
	}
	stats := s.MonitoringSnapshot()
	if len(stats.Methods) != len(instrumentedRPCMethods) {
		t.Fatalf("method count: got %d, want %d", len(stats.Methods), len(instrumentedRPCMethods))
	}
	for _, method := range stats.Methods {
		if method.Completed != 1 || method.Errors != errorsByMethod[method.Name] || method.Active != 0 {
			t.Fatalf("unobserved method or response/counter mismatch: %+v", method)
		}
	}
}

func TestMonitoringDoesNotInventCountersForNewRegistryMethods(t *testing.T) {
	extra := []string{
		"getMultipleAccounts", "getBalance", "getBlock", "getBlocks", "getBlockProduction",
		"getFirstAvailableBlock", "getHealth", "getIdentity", "getLeaderSchedule",
		"getSignatureStatuses", "getSlot", "getTransaction", "getVoteAccounts",
		"getVersion", "minimumLedgerSlot",
	}
	instrumented := make(map[string]bool, len(instrumentedRPCMethods))
	for _, name := range instrumentedRPCMethods {
		instrumented[name] = true
	}
	var unwrapped []string
	for _, name := range extra {
		if instrumented[name] {
			continue
		}
		unwrapped = append(unwrapped, name)
		if _, exists := supportedRPCMethods[name]; !exists {
			supportedRPCMethods[name] = struct{}{}
			t.Cleanup(func() { delete(supportedRPCMethods, name) })
		}
	}
	s := &RpcServer{}
	s.EnableMonitoring()
	h := rpcHandler{monitoring: &s.monitoring}
	for _, name := range unwrapped {
		value, err := observeRPC(h, name, func() (int, error) { return 42, nil })
		if err != nil || value != 42 {
			t.Fatalf("uninstrumented method %q did not run unchanged", name)
		}
	}
	methods := s.MonitoringSnapshot().Methods
	if len(methods) != len(instrumentedRPCMethods) {
		t.Fatalf("unexpected number of instrumented methods: got %d, want %d", len(methods), len(instrumentedRPCMethods))
	}
	for _, method := range methods {
		for _, name := range unwrapped {
			if method.Name == name {
				t.Fatalf("uninstrumented RPC %q was reported as having a zero counter", name)
			}
		}
	}
}

func BenchmarkRPCMonitoring(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		name := "disabled"
		s := &RpcServer{}
		if enabled {
			name = "enabled"
			s.EnableMonitoring()
		}
		h := rpcHandler{rpcMethods: s, monitoring: &s.monitoring}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_, _ = h.GetGenesisHash(b.Context(), nil)
			}
		})
	}
}

func TestMonitoringDoesNotChangeRPCResponses(t *testing.T) {
	s := NewRpcServer(nil, 0, &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32}, solana.Hash{1})
	_ = s.listener.Close()
	endpoint := httptest.NewServer(s)
	defer endpoint.Close()
	request := `[{"jsonrpc":"2.0","id":1,"method":"getGenesisHash","params":[]},{"jsonrpc":"2.0","id":2,"method":"getGenesisHash","params":[true]}]`
	call := func() string {
		t.Helper()
		r, err := endpoint.Client().Post(endpoint.URL, "application/json", strings.NewReader(request))
		if err != nil {
			t.Fatal(err)
		}
		defer r.Body.Close()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		return string(body)
	}
	before := call()
	if s.monitoring.Load() != nil {
		t.Fatal("monitoring enabled by a request")
	}
	s.EnableMonitoring()
	startedAt := s.MonitoringSnapshot().MonitoringStartedAt
	if startedAt.IsZero() {
		t.Fatal("enabled RPC counters have no start time")
	}
	if after := call(); after != before {
		t.Fatalf("monitoring changed response: %s / %s", before, after)
	}
	s.EnableMonitoring()
	stats := s.MonitoringSnapshot()
	if !stats.MonitoringStartedAt.Equal(startedAt) {
		t.Fatal("repeated enable reset RPC counter scope")
	}
	for _, m := range stats.Methods {
		if m.Name != "getGenesisHash" {
			continue
		}
		if m.Completed != 2 || m.Errors != 1 || m.Active != 0 {
			t.Fatalf("incorrect batch counters: %+v", m)
		}
		var n uint64
		for _, count := range m.DurationBuckets {
			n += count
		}
		if n != 2 {
			t.Fatalf("histogram total %d", n)
		}
	}
	for _, name := range []string{"EnableMonitoring", "MonitoringSnapshot"} {
		if _, ok := reflect.TypeOf(rpcHandler{}).MethodByName(name); ok {
			t.Fatalf("internal method exposed: %s", name)
		}
	}
	if stats.HasPublishedSlot || stats.Serving {
		t.Fatal("unstarted node reported serving/publication")
	}
	s.SetSlotCtx(&sealevel.SlotCtx{Slot: 0})
	stats = s.MonitoringSnapshot()
	if !stats.HasPublishedSlot || stats.PublishedAt.IsZero() || stats.PublishedSlot != 0 {
		t.Fatal("valid zero publication lost")
	}
	s.SetSlotCtx(nil)
	if s.MonitoringSnapshot().HasPublishedSlot {
		t.Fatal("publication reset retained data")
	}
}

func TestMonitoringConcurrentCallsAndReads(t *testing.T) {
	s := NewRpcServer(nil, 0, &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32}, solana.Hash{1})
	defer s.listener.Close()
	s.EnableMonitoring()
	h := rpcHandler{rpcMethods: s, monitoring: &s.monitoring}
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 100 {
				_, _ = h.GetGenesisHash(t.Context(), nil)
				s.MonitoringSnapshot()
			}
		})
	}
	wg.Wait()
	for _, m := range s.MonitoringSnapshot().Methods {
		if m.Name == "getGenesisHash" && (m.Completed != 400 || m.Active != 0 || m.Errors != 0) {
			t.Fatalf("counter mismatch: %+v", m)
		}
	}
}

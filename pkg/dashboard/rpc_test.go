package dashboard

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"
)

func rpcFixture(at time.Time) *RPC {
	return &RPC{SampledAt: at, MonitoringStartedAt: at.Add(-time.Minute), Methods: []RPCMethod{
		{Name: "getGenesisHash", Completed: 2, Errors: 1, Active: 3, DurationBuckets: [6]string{"1", "0", "0", "0", "0", "1"}},
		{Name: "getAccountInfo", Completed: 1, DurationBuckets: [6]string{"0", "1", "0", "0", "0", "0"}},
	}}
}

func TestPublicRPCActivity(t *testing.T) {
	at := time.Now().UTC()
	s := testSnapshot(at)
	s.RPCConfigured, s.RPC = true, rpcFixture(at)
	p, wire := publicValue(t, &s, "fresh", at)
	a := p.RPC.Activity
	if a == nil || a.Completed != 3 || a.Errors != 1 || a.Active != 3 || len(a.Methods) != 2 ||
		a.Methods[0].Name != "getAccountInfo" || a.Methods[1].Name != "getGenesisHash" || a.DurationBuckets != [6]string{"1", "1", "0", "0", "0", "1"} {
		t.Fatalf("incorrect aggregate or method attribution: %+v", a)
	}
	if strings.Contains(string(wire), "history_recovery_pending") {
		t.Fatal("private RPC field escaped")
	}
	s.RPC.Methods[0].Completed = 99
	if a.Methods[1].Completed != 2 {
		t.Fatal("public methods alias their producer")
	}
	s.RPC = rpcFixture(at)
	p, _ = publicValue(t, &s, "stale", at)
	if p.RPC.State != "stale" || p.RPC.Activity == nil {
		t.Fatal("stale counters lost their source state")
	}
	s.RPCConfigured = false
	if p, _ = publicValue(t, &s, "fresh", at); p.RPC != nil {
		t.Fatal("disabled RPC exposed activity")
	}

	r := rpcFixture(at)
	r.Methods = r.Methods[:1]
	r.Methods[0].Completed, r.Methods[0].Errors = math.MaxUint64, math.MaxUint64
	r.Methods[0].DurationBuckets = [6]string{"18446744073709551615", "0", "0", "0", "0", "0"}
	a = publicRPCActivity(r, s.StartedAt)
	b, err := json.Marshal(a)
	if err != nil || a == nil || !strings.Contains(string(b), `"completed":"18446744073709551615"`) {
		t.Fatalf("RPC counter precision lost: %s %v", b, err)
	}
}

func TestPublicRPCActivityRejectsInconsistentMeasurements(t *testing.T) {
	at := time.Now().UTC()
	for name, mutate := range map[string]func(*RPC){
		"absent start":            func(r *RPC) { r.MonitoringStartedAt = time.Time{} },
		"old run":                 func(r *RPC) { r.MonitoringStartedAt = at.Add(-2 * time.Hour) },
		"future start":            func(r *RPC) { r.MonitoringStartedAt = at.Add(time.Second) },
		"absent methods":          func(r *RPC) { r.Methods = nil },
		"too many":                func(r *RPC) { r.Methods = make([]RPCMethod, 10) },
		"untrusted name":          func(r *RPC) { r.Methods[0].Name = "private/query" },
		"duplicate":               func(r *RPC) { r.Methods[1].Name = r.Methods[0].Name },
		"errors exceed completed": func(r *RPC) { r.Methods[0].Errors = 3 },
		"malformed bin":           func(r *RPC) { r.Methods[0].DurationBuckets[0] = "01" },
		"missing bin":             func(r *RPC) { r.Methods[0].DurationBuckets[0] = "" },
		"histogram mismatch":      func(r *RPC) { r.Methods[0].DurationBuckets[0] = "0" },
		"bin overflow":            func(r *RPC) { r.Methods[0].DurationBuckets[0] = "18446744073709551615" },
		"total overflow": func(r *RPC) {
			r.Methods[0].Completed = math.MaxUint64
			r.Methods[0].DurationBuckets = [6]string{"18446744073709551615", "0", "0", "0", "0", "0"}
		},
		"active overflow": func(r *RPC) { r.Methods[0].Active = math.MaxUint64; r.Methods[1].Active = 1 },
	} {
		t.Run(name, func(t *testing.T) {
			r := rpcFixture(at)
			mutate(r)
			if got := publicRPCActivity(r, at.Add(-time.Hour)); got != nil {
				t.Fatalf("accepted invalid measurements: %+v", got)
			}
		})
	}
}

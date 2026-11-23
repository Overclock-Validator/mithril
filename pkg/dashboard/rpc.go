package dashboard

import (
	"math"
	"sort"
	"strconv"
	"time"
)

// PublicRPCActivity counts instrumented method invocations since StartedAt.
// Transport/envelope rejection is excluded; a batch can contain several calls.
type PublicRPCActivity struct {
	StartedAt time.Time `json:"started_at"`
	Completed uint64    `json:"completed,string"`
	Errors    uint64    `json:"errors,string"`
	Active    uint64    `json:"active,string"`
	// Non-cumulative bins: <=1ms, <=10ms, <=100ms, <=1s, <=10s, >10s.
	DurationBuckets [6]string         `json:"duration_buckets"`
	Methods         []PublicRPCMethod `json:"methods"`
}

// PublicRPCMethod exposes only a fixed method name and its invocation counts.
type PublicRPCMethod struct {
	Name      string `json:"name"`
	Completed uint64 `json:"completed,string"`
	Errors    uint64 `json:"errors,string"`
	Active    uint64 `json:"active,string"`
}

func publicRPCActivity(r *RPC, runStarted time.Time) *PublicRPCActivity {
	if r.MonitoringStartedAt.IsZero() || r.MonitoringStartedAt.Before(runStarted) || r.MonitoringStartedAt.After(r.SampledAt) || len(r.Methods) == 0 || len(r.Methods) > 9 {
		return nil
	}
	p := &PublicRPCActivity{StartedAt: r.MonitoringStartedAt}
	var bins [6]uint64
	seen := make(map[string]bool, len(r.Methods))
	add := func(total *uint64, value uint64) bool {
		if value > math.MaxUint64-*total {
			return false
		}
		*total += value
		return true
	}
	for _, m := range r.Methods {
		switch m.Name {
		case "getAccountInfo", "getBankHash", "getBlockHeight", "getEpochInfo", "getGenesisHash", "getInflationReward", "getLatestBlockhash", "sendTransaction", "simulateTransaction":
		default:
			return nil
		}
		if seen[m.Name] || m.Errors > m.Completed || !add(&p.Completed, m.Completed) || !add(&p.Errors, m.Errors) || !add(&p.Active, m.Active) {
			return nil
		}
		seen[m.Name] = true
		var completed uint64
		for i, raw := range m.DurationBuckets {
			n, err := strconv.ParseUint(raw, 10, 64)
			if err != nil || strconv.FormatUint(n, 10) != raw || !add(&completed, n) || !add(&bins[i], n) {
				return nil
			}
		}
		if completed != m.Completed {
			return nil
		}
		p.Methods = append(p.Methods, PublicRPCMethod{Name: m.Name, Completed: m.Completed, Errors: m.Errors, Active: m.Active})
	}
	for i, n := range bins {
		p.DurationBuckets[i] = strconv.FormatUint(n, 10)
	}
	sort.Slice(p.Methods, func(i, j int) bool { return p.Methods[i].Name < p.Methods[j].Name })
	return p
}

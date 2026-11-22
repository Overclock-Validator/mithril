package dashboard

import (
	"bytes"
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func productionDetailFixture() ProductionDetail {
	at := time.Date(2026, 11, 1, 12, 0, 0, 123456789, time.UTC)
	hash := "JEKNVnkbo3jma5nREBBJCDoXFVeKkD56V3xKrvRmWxFG"
	return ProductionDetail{SchemaVersion: 1, Family: "production", Scope: "producer_session_latest_terminal", State: "fresh",
		Binding: ProductionDetailBinding{RunID: "production-run", NodeStartedAt: at.Add(-time.Hour), GenesisHash: hash,
			ProducerGeneration: 1, ProducerStartedAt: at.Add(-time.Minute)}, CapturedAt: at, ProducerState: "running",
		LastTerminal: &ProductionTerminalDetail{Slot: 42, ObservedAt: at.Add(-time.Second),
			Outcome: "broadcast", Reason: "complete", Terminal: "complete", Cause: "complete",
			Bank: &ProductionBankDetail{Leader: hash, ParentSlot: 41, ParentBankhash: hash},
			Costs: &ProductionCostDetail{BlockCost: 100, AllocatedDataSizeDeltaBytes: 2, SerializedEntryBytes: 300, Transactions: 1,
				Limits: ProductionCostLimits{BlockCost: 60000000, WritableAccountCost: 24000000,
					AllocatedDataSizeDeltaBytes: 100000000, MaxBatchBytes: 61632, MaxEntryBytes: 20000000}}}}
}

func TestProductionDetailStatesAndHistoricalDates(t *testing.T) {
	for _, state := range []string{"running", "stopped"} {
		for _, change := range []func(*ProductionDetail){
			func(d *ProductionDetail) { d.LastTerminal = nil },
			func(d *ProductionDetail) { *d.LastTerminal.Costs = ProductionCostDetail{} },
			func(d *ProductionDetail) {
				d.LastTerminal.Costs.BlockCost, d.LastTerminal.Costs.AllocatedDataSizeDeltaBytes = math.MaxUint64, math.MaxUint64
				d.LastTerminal.Costs.SerializedEntryBytes = math.MaxUint64
				d.LastTerminal.Costs.Limits = ProductionCostLimits{BlockCost: 1, AllocatedDataSizeDeltaBytes: 1, MaxEntryBytes: 1}
			},
			func(d *ProductionDetail) { d.LastTerminal.Costs = nil },
			func(d *ProductionDetail) { d.LastTerminal.Bank, d.LastTerminal.Costs = nil, nil },
			func(d *ProductionDetail) {
				d.LastTerminal.Outcome, d.LastTerminal.Reason = "missed", "replay_not_ready"
				d.LastTerminal.Terminal, d.LastTerminal.Cause = "deadline_elapsed", "replay_not_ready"
				d.LastTerminal.Bank, d.LastTerminal.Costs = nil, nil
			},
			func(d *ProductionDetail) {
				d.LastTerminal.Bank.Leader, d.LastTerminal.Bank.ParentBankhash = strings.Repeat("1", 32), strings.Repeat("1", 32)
				d.LastTerminal.Bank.ParentSlot = 0
			},
			func(d *ProductionDetail) {
				// The core does not infer monotonic order or health from wall dates.
				d.LastTerminal.ObservedAt = d.Binding.NodeStartedAt.Add(-24 * time.Hour)
				d.CapturedAt = d.LastTerminal.ObservedAt.Add(-time.Hour)
			},
		} {
			d := productionDetailFixture()
			d.ProducerState = state
			change(&d)
			body, err := json.Marshal(d)
			if err != nil {
				t.Fatal(err)
			}
			got, err := decodeProductionDetail(body)
			if err != nil || !reflect.DeepEqual(got, d) {
				t.Fatalf("valid %s document changed: %v", state, err)
			}
		}
	}
	// Decoder ownership is independent of the caller's input buffer.
	body, err := json.Marshal(productionDetailFixture())
	if err != nil {
		t.Fatal(err)
	}
	d, err := decodeProductionDetail(body)
	if err != nil {
		t.Fatal(err)
	}
	clear(body)
	if d.Binding.RunID != "production-run" || d.LastTerminal.Reason != "complete" {
		t.Fatal("decoded source aliases caller bytes")
	}
}

func TestProductionDetailCanonicalAndMaximumWidth(t *testing.T) {
	d := productionDetailFixture()
	d.Binding.RunID = strings.Repeat("<", 128)
	d.Binding.ProducerGeneration = math.MaxUint64 - 1
	at := time.Date(9999, 12, 31, 23, 59, 59, 123456789, time.UTC)
	d.Binding.NodeStartedAt, d.Binding.ProducerStartedAt, d.CapturedAt, d.LastTerminal.ObservedAt = at, at, at, at
	d.LastTerminal.Slot, d.LastTerminal.Bank.ParentSlot = math.MaxUint64, math.MaxUint64
	d.LastTerminal.Costs = &ProductionCostDetail{BlockCost: math.MaxUint64, AllocatedDataSizeDeltaBytes: math.MaxUint64,
		SerializedEntryBytes: math.MaxUint64, Transactions: math.MaxUint64, Limits: ProductionCostLimits{
			BlockCost: math.MaxUint64, WritableAccountCost: math.MaxUint64, AllocatedDataSizeDeltaBytes: math.MaxUint64,
			MaxBatchBytes: math.MaxUint64, MaxEntryBytes: math.MaxUint64}}
	for _, outcome := range []string{"broadcast", "missed"} {
		if outcome == "missed" {
			d.LastTerminal.Outcome, d.LastTerminal.Costs = "missed", nil
			d.LastTerminal.Reason, d.LastTerminal.Terminal, d.LastTerminal.Cause = "parent_changed_before_freeze", "parent_changed_before_freeze", "parent_changed_before_freeze"
		}
		body, err := json.Marshal(d)
		if err != nil || len(body) > MaxProductionDetailBytes {
			t.Fatalf("maximum-width %s: %d/%d bytes, %v", outcome, len(body), MaxProductionDetailBytes, err)
		}
		got, err := decodeProductionDetail(body)
		if err != nil || !reflect.DeepEqual(got, d) {
			t.Fatalf("maximum-width %s roundtrip changed: %v", outcome, err)
		}
		t.Logf("maximum-width private production %s: %d/%d bytes", outcome, len(body), MaxProductionDetailBytes)
	}
	base := productionDetailFixture()
	body, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2]string{
		{`"schema_version":1`, `"extra":0,"schema_version":1`},
		{`"schema_version":1`, `"schema_version":1,"schema_version":1`},
		{`"family":"production"`, `"family":"produc\u0074ion"`},
		{`"state":"fresh","reason":""`, `"reason":"","state":"fresh"`},
		{`"reason":"",`, ``},
		{`"reason":""`, `"reason":null`},
		{`"binding":{`, `"binding":{"unknown":0,`},
		{`"run_id":"production-run"`, `"run_id":"production-run","run_id":"production-run"`},
		{`"genesis_hash":"` + base.Binding.GenesisHash + `"`, `"genesis_hash":null`},
		{`"producer_generation":"1"`, `"producer_generation":"01"`},
		{`"producer_generation":"1"`, `"producer_generation":1`},
		{`"producer_generation":"1",`, ``},
		{`"last_terminal":{`, `"last_terminal":{"extra":false,`},
		{`"slot":"42"`, `"slot":null`},
		{`"slot":"42"`, `"slot":"-1"`},
		{`"slot":"42"`, `"slot":"18446744073709551616"`},
		{`"outcome":"broadcast"`, `"outcome":"broadcast","outcome":"broadcast"`},
		{`"cause":"complete",`, ``},
		{`"bank":{`, `"bank":{"extra":0,`},
		{`"parent_slot":"41"`, `"parent_slot":"41","parent_slot":"41"`},
		{`"parent_bankhash":"` + base.Binding.GenesisHash + `"`, `"parent_bankhash":null`},
		{`"costs":{`, `"costs":{"extra":0,`},
		{`"block_cost":"100"`, `"block_cost":"100","block_cost":"100"`},
		{`"block_cost":"100"`, `"block_cost":"+100"`},
		{`"transactions":"1",`, ``},
		{`"limits":{`, `"limits":{"extra":0,`},
		{`"max_batch_bytes":"61632"`, `"max_batch_bytes":null`},
		{`"max_entry_bytes":"20000000"`, `"max_entry_bytes":"20000000","max_entry_bytes":"20000000"`},
		{`"writable_account_cost":"24000000",`, ``},
		{`Z"`, `+00:00"`},
		{`.123456789Z"`, `.1234567890Z"`},
	} {
		bad := bytes.Replace(body, []byte(pair[0]), []byte(pair[1]), 1)
		if bytes.Equal(bad, body) {
			t.Fatalf("mutation did not reach its field: %s", pair[0])
		}
		if _, err := decodeProductionDetail(bad); err == nil || err.Error() != "source_invalid" {
			t.Fatalf("invalid canonical field %s returned %v", pair[0], err)
		}
	}
	for _, bad := range [][]byte{append([]byte(" "), body...), append(append([]byte(nil), body...), []byte("{}")...), bytes.Repeat([]byte(" "), MaxProductionDetailBytes+1)} {
		if _, err := decodeProductionDetail(bad); err == nil {
			t.Fatal("invalid body framing or size accepted")
		}
	}
	// Nullable members must still be explicitly present; null is not omission.
	for _, field := range []string{"last_terminal", "bank", "costs"} {
		d := productionDetailFixture()
		d.LastTerminal.Bank, d.LastTerminal.Costs = nil, nil
		if field == "last_terminal" {
			d.LastTerminal = nil
		}
		encoded, err := json.Marshal(d)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := decodeProductionDetail(encoded); err != nil {
			t.Fatal(err)
		}
		bad := bytes.Replace(encoded, []byte(`,"`+field+`":null`), nil, 1)
		if bytes.Equal(bad, encoded) {
			t.Fatalf("nullable mutation did not reach %s", field)
		}
		if _, err := decodeProductionDetail(bad); err == nil {
			t.Fatalf("missing nullable %s accepted", field)
		}
	}
}

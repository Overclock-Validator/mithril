package repairsim

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
)

// Logical-time experiment using production selection, authenticated packets and
// FEC recovery. A fixed request budget per 20ms round; this measures when the
// first data span is restored, not execution latency or production retry policy.
func TestStreamingPrefixRepairUnderLimitedBudget(t *testing.T) {
	ledger := testLedger(t, 1, 8)
	for _, budget := range []int{1, 2, 4, 16} {
		t.Run(fmt.Sprint(budget), func(t *testing.T) {
			type result struct {
				prefix, complete time.Duration
				requests         int
				block            *block.Block
			}
			run := func(stream bool) result {
				a := turbine.NewSlotAssembler()
				if stream {
					a.SubscribeStream(make(chan turbine.StreamEvent, 256))
				}
				slot := ledger.Slots[0]
				feed := func(p Packet) *block.Block {
					sh, err := parseAndVerify(p, ledger)
					if err != nil {
						t.Fatal(err)
					}
					b, err := a.AddShred(sh)
					if err != nil {
						t.Fatal(err)
					}
					return b
				}
				for j, f := range slot.FECs {
					for _, p := range f.Data[:29] {
						feed(p)
					}
					n := 2
					if j == 0 {
						n = 1
					}
					for _, p := range f.Coding[:n] {
						feed(p)
					}
				}
				a.PrioritizeRepairSlot(slot.Number)
				r := result{}
				for round := 1; round <= 20; round++ {
					req := a.RepairRequests(1, 256)
					if len(req) == 0 {
						t.Fatal("unfinished slot has no repair request")
					}
					indices := req[0].MissingDataShreds
					if len(indices) > budget {
						indices = indices[:budget]
					}
					if len(indices) == 0 {
						t.Fatal("no exact repair work")
					}
					for _, idx := range indices {
						r.requests++
						if b := feed(slot.Data[idx]); b != nil {
							r.block = b
							r.complete = time.Duration(round) * 20 * time.Millisecond
						}
					}
					remaining := a.RepairRequests(1, 256)
					hole := false
					for _, q := range remaining {
						for _, idx := range q.MissingDataShreds {
							if idx < 32 {
								hole = true
							}
						}
					}
					if !hole && r.prefix == 0 {
						r.prefix = time.Duration(round) * 20 * time.Millisecond
					}
					if r.block != nil {
						return r
					}
				}
				t.Fatal("did not complete")
				return r
			}
			before, after := run(false), run(true)
			t.Logf("first span %v -> %v; full block %v -> %v; requests %d -> %d", before.prefix, after.prefix, before.complete, after.complete, before.requests, after.requests)
			if after.prefix > before.prefix || (budget < 9 && after.prefix == before.prefix) {
				t.Fatal("prefix did not improve")
			}
			if after.requests != before.requests || after.complete > before.complete {
				t.Fatal("request count or completion regressed")
			}
			if !reflect.DeepEqual(before.block.Transactions, after.block.Transactions) || !reflect.DeepEqual(before.block.Entries, after.block.Entries) {
				t.Fatal("assembled payload mismatch")
			}
		})
	}
}

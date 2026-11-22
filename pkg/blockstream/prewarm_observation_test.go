package blockstream

import (
	"math"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	b "github.com/Overclock-Validator/mithril/pkg/block"
)

func observationPrewarm(floor uint64, capacity int, observe func(TurbinePrewarmHandoverObservation)) *TurbinePrewarm {
	done := make(chan struct{})
	return &TurbinePrewarm{
		cancel: func() { close(done) }, done: done, floor: floor, capacity: capacity,
		spool: make(map[uint64]*b.Block), observeHandover: observe,
	}
}

func TestPrewarmHandoverObservationPreservesSpool(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		name := "disabled"
		if enabled {
			name = "enabled"
		}
		t.Run(name, func(t *testing.T) {
			var events []TurbinePrewarmHandoverObservation
			pw := observationPrewarm(10, 3, nil)
			if enabled {
				pw.observeHandover = func(o TurbinePrewarmHandoverObservation) {
					select {
					case <-pw.done:
					default:
						t.Fatal("handover observed before drain joined")
					}
					if !pw.mu.TryLock() {
						t.Fatal("observer called under prewarm lock")
					}
					pw.mu.Unlock()
					pw.Stop() // The callback can inspect the stopped owner without deadlock.
					events = append(events, o)
				}
			}
			retained := &b.Block{Slot: 10}
			pw.add(nil)
			pw.add(&b.Block{Slot: 9})
			pw.add(&b.Block{Slot: 14})
			pw.add(retained)
			pw.add(&b.Block{Slot: 12})
			pw.add(&b.Block{Slot: 13})
			pw.add(&b.Block{Slot: 12}) // Duplicate does not evict.
			pw.add(&b.Block{Slot: 14}) // A second eviction of the same slot counts again.
			before := time.Now()
			blocks, dropped := pw.Handover()
			after := time.Now()
			var slots []uint64
			for _, blk := range blocks {
				slots = append(slots, blk.Slot)
			}
			if !reflect.DeepEqual(slots, []uint64{10, 12, 13}) || blocks[0] != retained || dropped != 2 || pw.spool != nil {
				t.Fatalf("handover changed: slots=%v dropped=%d retained=%v spool=%v", slots, dropped, blocks[0] == retained, pw.spool)
			}
			pw.add(&b.Block{Slot: 11}) // Retired collection cannot add another block.
			again, againDropped := pw.Handover()
			if len(again) != 0 || againDropped != dropped {
				t.Fatalf("repeated handover changed: %v, %d", again, againDropped)
			}
			if !enabled {
				if pw.handoverObserved || len(events) != 0 {
					t.Fatal("disabled observer consumed an observation")
				}
				return
			}
			if len(events) != 1 {
				t.Fatalf("events=%d, want 1", len(events))
			}
			o := events[0]
			if o.FloorSlot != 10 || o.Capacity != 3 || o.ReturnedBlocks != 3 || o.OverflowEvictions != 2 || o.FirstSlot != 10 || o.LastSlot != 13 || o.RepairKnown || o.Repair != (TurbinePrewarmRepairObservation{}) {
				t.Fatalf("wrong detached summary: %+v", o)
			}
			if o.HandedOverAt.Before(before) || o.HandedOverAt.After(after) {
				t.Fatal("handover date is not the actual handover observation")
			}
			blocks[0].Slot = 99
			if events[0].FirstSlot != 10 {
				t.Fatal("returned block mutation changed retained event")
			}
		})
	}
}

func TestPrewarmHandoverObservationEmptyBoundsAndRetirement(t *testing.T) {
	for _, slots := range [][]uint64{nil, {0}, {math.MaxUint64 - 2, math.MaxUint64}} {
		var events []TurbinePrewarmHandoverObservation
		pw := observationPrewarm(0, 3, func(o TurbinePrewarmHandoverObservation) { events = append(events, o) })
		for _, slot := range slots {
			pw.add(&b.Block{Slot: slot})
		}
		pw.Stop()
		if len(events) != 0 {
			t.Fatal("Stop alone emitted a handover")
		}
		blocks, _ := pw.Handover()
		if len(events) != 1 || len(blocks) != len(slots) || events[0].ReturnedBlocks != uint64(len(slots)) || events[0].RepairKnown {
			t.Fatalf("stopped owner lost actual spool: %+v, %v", events, slots)
		}
		o := events[0]
		if len(slots) == 0 {
			if o.FirstSlot != 0 || o.LastSlot != 0 {
				t.Fatal("empty spool fabricated bounds")
			}
		} else if o.FirstSlot != slots[0] || o.LastSlot != slots[len(slots)-1] {
			t.Fatalf("wrong bounds: %+v", o)
		}
	}
}

func TestPrewarmHandoverObservationInvalidCountsDoNotReappear(t *testing.T) {
	for _, counts := range [][2]int{{0, 0}, {-1, 0}, {1, -1}} {
		calls := 0
		pw := observationPrewarm(0, counts[0], func(TurbinePrewarmHandoverObservation) { calls++ })
		pw.dropped = counts[1]
		pw.Handover()
		pw.capacity, pw.dropped = 1, 0
		pw.Handover()
		if calls != 0 {
			t.Fatalf("invalid signed counts produced an event: %v", counts)
		}
	}
}

func TestPrewarmHandoverObservationConcurrentOnce(t *testing.T) {
	events := make(chan TurbinePrewarmHandoverObservation, 2)
	pw := observationPrewarm(1, 2, func(o TurbinePrewarmHandoverObservation) { events <- o })
	pw.add(&b.Block{Slot: 1})
	pw.add(&b.Block{Slot: 3})
	results := make(chan int, 2)
	var callers sync.WaitGroup
	for range 2 {
		callers.Go(func() { blocks, _ := pw.Handover(); results <- len(blocks) })
	}
	callers.Wait()
	if len(events) != 1 || <-results+<-results != 2 {
		t.Fatal("concurrent handover duplicated or lost spool/event")
	}
	if o := <-events; o.ReturnedBlocks != 2 || o.FirstSlot != 1 || o.LastSlot != 3 {
		t.Fatalf("empty second handover replaced first: %+v", o)
	}
}

func TestPrewarmHandoverObservationRealLifecycle(t *testing.T) {
	peer, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	for _, enabled := range []bool{false, true} {
		tvu, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			t.Fatal(err)
		}
		bind := tvu.LocalAddr().String()
		if err := tvu.Close(); err != nil {
			t.Fatal(err)
		}
		var events []TurbinePrewarmHandoverObservation
		cfg := TurbinePrewarmConfig{BindAddr: bind, GossipEntrypoint: peer.LocalAddr().String(), GossipBindAddr: "127.0.0.1:0", AdvertisedIP: "127.0.0.1", ShredVersion: 42}
		if enabled {
			cfg.ObserveHandover = func(o TurbinePrewarmHandoverObservation) { events = append(events, o) }
		}
		pw, err := StartTurbinePrewarm(cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pw.Stop)
		pw.add(&b.Block{Slot: 7})
		blocks, dropped := pw.Handover()
		if len(blocks) != 1 || blocks[0].Slot != 7 || dropped != 0 {
			t.Fatal("real receiver handover changed the spool")
		}
		pw.Handover()
		if !enabled {
			if pw.handoverObserved || len(events) != 0 {
				t.Fatal("disabled real receiver emitted observation")
			}
			continue
		}
		if len(events) != 1 || events[0].Capacity != 192 || !events[0].RepairKnown || events[0].Repair.SampledAt.IsZero() {
			t.Fatalf("missing original pre-stop repair sample: %+v", events)
		}
		r := events[0].Repair
		if r.Requests != 0 || r.Responses != 0 || r.LateResponses != 0 || r.Timeouts != 0 {
			t.Fatalf("no-peer repair totals are not known zero: %+v", r)
		}
		// Repair totals are non-resetting; taking the original log sample must
		// not clear them or manufacture a second observer sample on handover.
		stats := pw.receiver.Stats().Repair
		if stats.Requests != r.Requests || stats.Responses != r.Responses || stats.LateResponses != r.LateResponses || stats.Timeouts != r.Timeouts {
			t.Fatal("repair totals changed after joined empty-traffic handover")
		}
	}
}

func TestPrewarmHandoverObservationFailedConstructor(t *testing.T) {
	calls := 0
	pw, err := StartTurbinePrewarm(TurbinePrewarmConfig{ObserveHandover: func(TurbinePrewarmHandoverObservation) { calls++ }})
	if err == nil || pw != nil || calls != 0 {
		t.Fatal("failed constructor emitted a handover")
	}
}

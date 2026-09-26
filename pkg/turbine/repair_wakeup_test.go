package turbine

import (
	"context"
	"testing"
	"time"
)

func TestPriorityRepairWakeOnlyForNewPins(t *testing.T) {
	r := NewUDPReceiver("127.0.0.1:0")
	r.repairClient = &repairClient{priorityWake: make(chan struct{}, 1)}
	r.PrioritizeRepairSlot(10)
	if len(r.repairClient.priorityWake) != 1 {
		t.Fatal("new pin did not wake repair")
	}
	<-r.repairClient.priorityWake
	for i := 0; i < 100; i++ {
		r.PrioritizeRepairSlot(10)
	}
	if len(r.repairClient.priorityWake) != 0 {
		t.Fatal("duplicate pins caused wakeups")
	}
	r.PrioritizeRepairRange(10, 12)
	r.PrioritizeRepairSlot(13)
	if len(r.repairClient.priorityWake) != 1 {
		t.Fatal("new pins should coalesce")
	}
	<-r.repairClient.priorityWake
	r.assembler.completedSlots[14] = struct{}{}
	r.PrioritizeRepairSlot(14)
	r.PrioritizeRepairSlot(0)
	if len(r.repairClient.priorityWake) != 0 {
		t.Fatal("completed/invalid slot woke repair")
	}
}

func TestRepairScheduleWakeCoalescingAndCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wake := make(chan struct{}, 1)
	calls := make(chan time.Time, 4)
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		first := true
		runRepairSchedule(ctx, wake, time.Hour, 20*time.Millisecond, func() {
			calls <- time.Now()
			if first {
				first = false
				close(entered)
				<-release
			}
		})
	}()
	wake <- struct{}{}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("wake did not bypass periodic timer")
	}
	for i := 0; i < 100; i++ {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
	first := <-calls
	close(release)
	select {
	case second := <-calls:
		if second.Sub(first) < 20*time.Millisecond {
			t.Fatal("unbounded scan frequency")
		}
	case <-time.After(time.Second):
		t.Fatal("pending wake lost")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("scheduler did not stop")
	}
	if len(calls) != 0 {
		t.Fatal("wake burst caused extra scans")
	}
}

func TestRepairSchedulePeriodicWithoutWake(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		runRepairSchedule(ctx, nil, time.Millisecond, time.Millisecond, func() {
			select {
			case calls <- struct{}{}:
			default:
			}
		})
	}()
	select {
	case <-calls:
	case <-time.After(time.Second):
		t.Fatal("periodic repair stopped")
	}
	cancel()
	<-done
}

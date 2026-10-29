package sbpf

import (
	"errors"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
)

func TestCancelTokenPreservesExecution(t *testing.T) {
	rng := rand.New(rand.NewSource(88231))
	var token atomic.Bool
	for n := 0; n < 300; n++ {
		p := genProgram(rng, uint32(n%4))
		if p.Verify() != nil {
			continue
		}
		resolveCallTargetsIfSupported(p)
		for _, fast := range []bool{false, true} {
			for _, budget := range []uint64{0, 1, 1023, 1024, 1025, 1000000} {
				a := runWithCancelToken(t, p, fast, budget, 42, nil)
				b := runWithCancelToken(t, p, fast, budget, 42, &token)
				if a != b {
					t.Fatalf("n%d fast%v budget%d\n%s\n%s", n, fast, budget, a, b)
				}
			}
		}
	}
}
func TestCancelBeforeExecution(t *testing.T) {
	var token atomic.Bool
	token.Store(true)
	cm := cu.NewComputeMeter(10000)
	p := mkProgram([]Slot{slot(OpMov64Imm, 0, 0, 0, 3), slot(OpExit, 0, 0, 0, 0)}, 0)
	ip := NewInterpreter(p, &VMOpts{Cancel: &token, ComputeMeter: &cm})
	defer ip.Finish()
	_, used, err := ip.Run()
	if !errors.Is(err, ErrCancelled) || used != 0 || cm.Remaining() != 10000 {
		t.Fatalf("used%d remaining%d err%v", used, cm.Remaining(), err)
	}
}
func TestCancelRunningFastLoop(t *testing.T) {
	var token atomic.Bool
	cm := cu.NewComputeMeter(1000000000)
	p := mkProgram([]Slot{slot(OpAdd64Imm, 0, 0, 0, 1), slot(OpJa, 0, 0, -2, 0)}, 0)
	ip := NewInterpreter(p, &VMOpts{Cancel: &token, ComputeMeter: &cm})
	defer ip.Finish()
	done := make(chan error, 1)
	go func() { _, _, err := ip.Run(); done <- err }()
	time.Sleep(time.Millisecond)
	token.Store(true)
	select {
	case err := <-done:
		if !errors.Is(err, ErrCancelled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("fast loop did not observe cancellation")
	}
	if cm.Remaining() == 0 {
		t.Fatal("cancellation waited for CU exhaustion")
	}
}

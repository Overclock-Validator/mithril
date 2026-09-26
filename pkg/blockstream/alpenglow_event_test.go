package blockstream

import (
	"context"
	"testing"
	"time"

	b "github.com/Overclock-Validator/mithril/pkg/block"
)

func TestNextBlockOrAlpenglowEventDecisionChange(t *testing.T) {
	bs := &BlockSource{
		streamChan:              make(chan *b.Block),
		alpenglowParentSwitchCh: make(chan AlpenglowParentSwitch),
	}
	decisionChanges := make(chan struct{}, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() {
		decisionChanges <- struct{}{}
	}()
	block, parentSwitch, decisionChanged := bs.NextBlockOrAlpenglowEvent(ctx, decisionChanges)
	if block != nil || parentSwitch != nil || !decisionChanged {
		t.Fatalf("decision wakeup = (%v, %v, %v), want (nil, nil, true)", block, parentSwitch, decisionChanged)
	}
	if ctx.Err() != nil {
		t.Fatalf("decision wakeup waited for context cancellation: %v", ctx.Err())
	}
}

func TestNextBlockOrAlpenglowEventNilDecisionChannel(t *testing.T) {
	bs := &BlockSource{streamChan: make(chan *b.Block, 1)}
	want := &b.Block{Slot: 42}
	bs.streamChan <- want
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	block, parentSwitch, decisionChanged := bs.NextBlockOrAlpenglowEvent(ctx, nil)
	if block != want || parentSwitch != nil || decisionChanged {
		t.Fatalf("block delivery = (%v, %v, %v), want (%v, nil, false)", block, parentSwitch, decisionChanged, want)
	}
}

func TestNextBlockOrAlpenglowEventQueuedParentSwitchHasPriority(t *testing.T) {
	bs := &BlockSource{
		streamChan:              make(chan *b.Block, 1),
		alpenglowParentSwitchCh: make(chan AlpenglowParentSwitch, 1),
	}
	want := AlpenglowParentSwitch{SwitchSlot: 42, ParentSlot: 40, ChildSlot: 43}
	wantBlock := &b.Block{Slot: 42}
	bs.alpenglowParentSwitchCh <- want
	bs.streamChan <- wantBlock
	decisionChanges := make(chan struct{}, 1)
	decisionChanges <- struct{}{}

	// A queued parent switch must win even when every other input is ready.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	block, parentSwitch, decisionChanged := bs.NextBlockOrAlpenglowEvent(ctx, decisionChanges)
	if block != nil || parentSwitch == nil || *parentSwitch != want || decisionChanged {
		t.Fatalf("parent switch = (%v, %v, %v), want (nil, %v, false)", block, parentSwitch, decisionChanged, want)
	}
	if len(decisionChanges) != 1 {
		t.Fatal("parent switch consumed the pending decision notification")
	}
	select {
	case got := <-bs.streamChan:
		if got != wantBlock {
			t.Fatalf("buffered block = %v, want %v", got, wantBlock)
		}
	default:
		t.Fatal("parent switch consumed the pending block")
	}
}

func TestNextBlockOrAlpenglowEventClosedSource(t *testing.T) {
	bs := &BlockSource{streamChan: make(chan *b.Block)}
	close(bs.streamChan)
	decisionChanges := make(chan struct{}, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	block, parentSwitch, decisionChanged := bs.NextBlockOrAlpenglowEvent(ctx, decisionChanges)
	if block != nil || parentSwitch != nil || decisionChanged {
		t.Fatalf("closed source = (%v, %v, %v), want (nil, nil, false)", block, parentSwitch, decisionChanged)
	}
	if ctx.Err() != nil {
		t.Fatalf("closed source waited for context cancellation: %v", ctx.Err())
	}
}

func TestNextBlockOrAlpenglowEventCancellation(t *testing.T) {
	bs := &BlockSource{streamChan: make(chan *b.Block)}
	decisionChanges := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	block, parentSwitch, decisionChanged := bs.NextBlockOrAlpenglowEvent(ctx, decisionChanges)
	if block != nil || parentSwitch != nil || decisionChanged {
		t.Fatalf("cancellation = (%v, %v, %v), want (nil, nil, false)", block, parentSwitch, decisionChanged)
	}
}

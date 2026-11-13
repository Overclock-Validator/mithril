package replay

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

func localEpochReplayBlock(schedule sealevel.SysvarEpochSchedule, slot uint64, at time.Time, marker byte) CompletedBlock {
	epoch, index := schedule.GetEpochAndSlotIndex(slot)
	return CompletedBlock{At: at, Slot: slot, Bankhash: solana.Hash{marker}, Epoch: epoch, SlotIndex: index,
		SlotsInEpoch: schedule.SlotsInEpoch(epoch), EpochSchedule: schedule}
}

func localEpochReplayChild(block, parent CompletedBlock) CompletedBlock {
	block.hasExecutionParent = true
	block.executionParentSlot = parent.Slot
	block.executionParentHash = parent.Bankhash
	return block
}

func localEpochReplaySnapshot(t *testing.T, p *Progress) LocalEpochReplayBoundaries {
	t.Helper()
	s, ok := p.Snapshot()
	if !ok || s.LocalEpochReplay.Count > uint8(len(s.LocalEpochReplay.Rows)) {
		t.Fatalf("invalid progress snapshot: %+v, %t", s, ok)
	}
	return s.LocalEpochReplay
}

func TestLocalEpochReplayBoundaries(t *testing.T) {
	normal := sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32}
	at := time.Date(2026, 11, 2, 1, 2, 3, 4, time.FixedZone("offset", 19800))

	t.Run("normal trailing and leading skips", func(t *testing.T) {
		p := &Progress{}
		p.begin(30, nil)
		parent := localEpochReplayBlock(normal, 30, at, 1)
		p.completed(parent, 0, false)
		p.skipped(0, false)
		child := localEpochReplayChild(localEpochReplayBlock(normal, 33, at.Add(time.Second), 2), parent)
		p.completed(child, 0, false)
		got := localEpochReplaySnapshot(t, p)
		if got.Count != 2 || got.Rows[0].Epoch != 0 || got.Rows[0].FirstSlot != 0 || got.Rows[0].SlotCount != 32 ||
			!got.Rows[0].StartedAt.IsZero() || !got.Rows[0].EndedAt.Equal(at) || got.Rows[0].EndedAt.Location() != time.UTC ||
			got.Rows[1].Epoch != 1 || got.Rows[1].FirstSlot != 32 || got.Rows[1].SlotCount != 32 ||
			!got.Rows[1].StartedAt.Equal(at) || !got.Rows[1].EndedAt.IsZero() {
			t.Fatalf("wrong normal boundary: %+v", got)
		}
	})

	t.Run("warmup boundary", func(t *testing.T) {
		schedule := sealevel.SysvarEpochSchedule{SlotsPerEpoch: 128, Warmup: true, FirstNormalEpoch: 2, FirstNormalSlot: 96}
		p := &Progress{}
		p.begin(31, nil)
		parent := localEpochReplayBlock(schedule, 31, at, 1)
		child := localEpochReplayChild(localEpochReplayBlock(schedule, 32, at.Add(time.Second), 2), parent)
		p.completed(parent, 0, false)
		p.completed(child, 0, false)
		got := localEpochReplaySnapshot(t, p)
		if got.Count != 2 || got.Rows[0].SlotCount != 32 || got.Rows[1].FirstSlot != 32 || got.Rows[1].SlotCount != 64 ||
			!got.Rows[0].EndedAt.Equal(at) || !got.Rows[1].StartedAt.Equal(at) {
			t.Fatalf("wrong warmup boundary: %+v", got)
		}
	})

	t.Run("slot zero is a final slot without invented next start", func(t *testing.T) {
		p := &Progress{}
		p.begin(0, nil)
		p.completed(localEpochReplayBlock(sealevel.SysvarEpochSchedule{SlotsPerEpoch: 1}, 0, at, 1), 0, false)
		got := localEpochReplaySnapshot(t, p)
		if got.Count != 1 || got.Rows[0].Epoch != 0 || got.Rows[0].FirstSlot != 0 || got.Rows[0].SlotCount != 1 ||
			!got.Rows[0].StartedAt.IsZero() || !got.Rows[0].EndedAt.Equal(at) {
			t.Fatalf("slot zero became unknown or invented a start: %+v", got)
		}
	})

	t.Run("mid epoch and a wholly unobserved epoch stay unknown", func(t *testing.T) {
		p := &Progress{}
		p.begin(5, nil)
		parent := localEpochReplayBlock(normal, 5, at, 1)
		p.completed(parent, 0, false)
		child := localEpochReplayChild(localEpochReplayBlock(normal, 64, at.Add(time.Second), 2), parent)
		p.completed(child, 0, false)
		if got := localEpochReplaySnapshot(t, p); got.Count != 0 {
			t.Fatalf("unobserved epoch was interpolated: %+v", got)
		}
	})

	t.Run("parent and geometry mismatches do not bridge", func(t *testing.T) {
		warmup := sealevel.SysvarEpochSchedule{SlotsPerEpoch: 128, Warmup: true, FirstNormalEpoch: 2, FirstNormalSlot: 96}
		for _, mutate := range []func(*CompletedBlock){
			func(b *CompletedBlock) { b.hasExecutionParent = false },
			func(b *CompletedBlock) { b.executionParentSlot-- },
			func(b *CompletedBlock) { b.executionParentHash[0]++ },
			func(b *CompletedBlock) {
				changed := localEpochReplayBlock(warmup, b.Slot, b.At, b.Bankhash[0])
				changed.hasExecutionParent, changed.executionParentSlot, changed.executionParentHash = true, b.executionParentSlot, b.executionParentHash
				*b = changed
			},
		} {
			p := &Progress{}
			p.begin(30, nil)
			parent := localEpochReplayBlock(normal, 30, at, 1)
			child := localEpochReplayChild(localEpochReplayBlock(normal, 33, at.Add(time.Second), 2), parent)
			mutate(&child)
			p.completed(parent, 0, false)
			p.completed(child, 0, false)
			if got := localEpochReplaySnapshot(t, p); got.Count != 0 {
				t.Fatalf("mismatched ancestry or geometry bridged: %+v", got)
			}
		}
	})

	t.Run("duplicate keeps original time but conflicting config invalidates anchor", func(t *testing.T) {
		p := &Progress{}
		p.begin(31, nil)
		parent := localEpochReplayBlock(normal, 31, at, 1)
		p.completed(parent, 0, false)
		duplicate := parent
		duplicate.At = at.Add(time.Hour)
		p.completed(duplicate, 0, false)
		child := localEpochReplayChild(localEpochReplayBlock(normal, 32, at.Add(-time.Hour), 2), parent)
		p.completed(child, 0, false)
		got := localEpochReplaySnapshot(t, p)
		if got.Count != 2 || !got.Rows[0].EndedAt.Equal(at) || !got.Rows[1].StartedAt.Equal(at) {
			t.Fatalf("valid duplicate redated boundary: %+v", got)
		}

		p.begin(31, nil)
		p.completed(parent, 0, false)
		conflict := localEpochReplayBlock(sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32, LeaderScheduleSlotOffset: 1}, 31, at.Add(time.Minute), 1)
		p.completed(conflict, 0, false)
		p.completed(child, 0, false)
		got = localEpochReplaySnapshot(t, p)
		if got.Count != 1 || !got.Rows[0].EndedAt.Equal(at) || !got.Rows[1].StartedAt.IsZero() {
			t.Fatalf("conflicting duplicate retained a stale anchor: %+v", got)
		}
	})

	t.Run("invalid completion clears only the anchor", func(t *testing.T) {
		p := &Progress{}
		p.begin(30, nil)
		parent := localEpochReplayBlock(normal, 30, at, 1)
		p.completed(parent, 0, false)
		invalid := localEpochReplayBlock(normal, 31, time.Time{}, 2)
		p.completed(invalid, 0, false)
		p.completed(localEpochReplayChild(localEpochReplayBlock(normal, 33, at.Add(time.Second), 3), parent), 0, false)
		if got := localEpochReplaySnapshot(t, p); got.Count != 0 {
			t.Fatalf("invalid completion left a bridgeable anchor: %+v", got)
		}
	})

	t.Run("reverse wall clock keeps the parent instant", func(t *testing.T) {
		p := &Progress{}
		p.begin(31, nil)
		parent := localEpochReplayBlock(normal, 31, at.Add(time.Hour), 1)
		child := localEpochReplayChild(localEpochReplayBlock(normal, 32, at, 2), parent)
		p.completed(parent, 0, false)
		p.completed(child, 0, false)
		got := localEpochReplaySnapshot(t, p)
		if !got.Rows[0].EndedAt.Equal(parent.At) || !got.Rows[1].StartedAt.Equal(parent.At) {
			t.Fatalf("wall reversal changed the observed boundary: %+v", got)
		}
	})

	t.Run("max uint64 can be an observed final slot", func(t *testing.T) {
		p := &Progress{}
		p.begin(math.MaxUint64, nil)
		schedule := sealevel.SysvarEpochSchedule{SlotsPerEpoch: 1 << 63}
		p.completed(localEpochReplayBlock(schedule, math.MaxUint64, at, 1), 0, false)
		got := localEpochReplaySnapshot(t, p)
		if got.Count != 1 || got.Rows[0].Epoch != 1 || got.Rows[0].FirstSlot != 1<<63 || got.Rows[0].SlotCount != 1<<63 ||
			!got.Rows[0].EndedAt.Equal(at) {
			t.Fatalf("max slot geometry was lost: %+v", got)
		}
	})

	t.Run("attempt and rewind reset anchor and rows", func(t *testing.T) {
		for _, reset := range []func(*Progress){
			func(p *Progress) { p.begin(32, nil) },
			func(p *Progress) { p.rewound() },
		} {
			p := &Progress{}
			p.begin(31, nil)
			parent := localEpochReplayBlock(normal, 31, at, 1)
			p.completed(parent, 0, false)
			reset(p)
			p.completed(localEpochReplayChild(localEpochReplayBlock(normal, 32, at.Add(time.Second), 2), parent), 0, false)
			got := localEpochReplaySnapshot(t, p)
			if got.Count != 0 {
				t.Fatalf("reset retained epoch observation: %+v", got)
			}
		}
	})

	t.Run("only latest two rows are retained", func(t *testing.T) {
		p := &Progress{}
		p.begin(0, nil)
		schedule := sealevel.SysvarEpochSchedule{SlotsPerEpoch: 1}
		var parent CompletedBlock
		for slot := uint64(0); slot < 3; slot++ {
			block := localEpochReplayBlock(schedule, slot, at.Add(time.Duration(slot)*time.Second), byte(slot+1))
			if slot > 0 {
				block = localEpochReplayChild(block, parent)
			}
			p.completed(block, 0, false)
			parent = block
		}
		got := localEpochReplaySnapshot(t, p)
		if got.Count != 2 || got.Rows[0].Epoch != 1 || got.Rows[1].Epoch != 2 || got.Rows[0].StartedAt.IsZero() ||
			got.Rows[0].EndedAt.IsZero() || got.Rows[1].StartedAt.IsZero() || got.Rows[1].EndedAt.IsZero() {
			t.Fatalf("rows did not roll forward: %+v", got)
		}
	})
}

func TestLocalEpochReplaySnapshotDetachedAndConcurrent(t *testing.T) {
	var disabled *Progress
	disabled.begin(0, nil)
	disabled.completed(CompletedBlock{}, 0, false)
	disabled.rewound()
	if _, ok := disabled.Snapshot(); ok {
		t.Fatal("nil progress published an observation")
	}

	p := &Progress{}
	p.begin(0, nil)
	schedule := sealevel.SysvarEpochSchedule{SlotsPerEpoch: 1}
	done := make(chan struct{})
	var readers sync.WaitGroup
	for range 4 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-done:
					return
				default:
					s, ok := p.Snapshot()
					if !ok || s.LocalEpochReplay.Count > 2 || s.LocalEpochReplay.Count == 2 && s.LocalEpochReplay.Rows[0].Epoch >= s.LocalEpochReplay.Rows[1].Epoch {
						t.Errorf("torn epoch snapshot: %+v, %t", s.LocalEpochReplay, ok)
						return
					}
				}
			}
		}()
	}
	var parent CompletedBlock
	for slot := uint64(0); slot < 64; slot++ {
		block := localEpochReplayBlock(schedule, slot, time.Unix(int64(slot+1), 0), byte(slot+1))
		if slot > 0 {
			block = localEpochReplayChild(block, parent)
		}
		p.completed(block, 0, false)
		parent = block
	}
	close(done)
	readers.Wait()

	copy, _ := p.Snapshot()
	copy.LocalEpochReplay.Rows[0].Epoch = math.MaxUint64
	current, _ := p.Snapshot()
	if current.LocalEpochReplay.Rows[0].Epoch == math.MaxUint64 {
		t.Fatal("snapshot mutation changed the retained observation")
	}
}

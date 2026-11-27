package replay

import (
	"os"
	"sync"
	"sync/atomic"

	"github.com/Overclock-Validator/mithril/pkg/arena"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
)

// A private replay's own arenas. The node's replay serializes instruction
// parameters into one shared arena and takes borrowed-account records from
// one arena per worker; a private replay must not use those, and without any
// it put every such buffer on the heap, where each block left garbage for the
// collector. It takes a set from this pool for the length of one block
// instead. Sets are reused, so the memory is paid for once per concurrent
// private replay, not per block.
type privateArenas struct {
	params   *arena.Arena[byte]
	borrowed []*arena.Arena[sealevel.BorrowedAccount]
}

const (
	// Fallback sizes when the node has no arenas of its own to copy from.
	privateParameterArenaBytes = 256 << 20
	privateBorrowedAccounts    = 1024
)

// MITHRIL_PRIVATE_ARENAS=0 puts a private replay's buffers back on the heap
// (measurement).
var privateArenasOff = os.Getenv("MITHRIL_PRIVATE_ARENAS") == "0"

var (
	privateArenaPool sync.Pool
	// What private replays used, for sizing: the most parameter bytes one
	// block reserved, and how many bytes ever spilled to the heap.
	privateParameterPeak, privateParameterOverflow atomic.Uint64
)

func acquirePrivateArenas(workers int) *privateArenas {
	set, _ := privateArenaPool.Get().(*privateArenas)
	if set == nil {
		capacity := uint64(privateParameterArenaBytes)
		if SerializedParameterArena != nil {
			capacity = SerializedParameterArena.Capacity()
		}
		set = &privateArenas{params: arena.New[byte](capacity)}
	}
	borrowedCapacity := uint64(privateBorrowedAccounts)
	if len(sealevel.BorrowedAccountArenas) > 0 && sealevel.BorrowedAccountArenas[0] != nil {
		borrowedCapacity = sealevel.BorrowedAccountArenas[0].Capacity()
	}
	for len(set.borrowed) < workers {
		set.borrowed = append(set.borrowed, arena.New[sealevel.BorrowedAccount](borrowedCapacity))
	}
	return set
}

// release clears the set and returns it. Nothing from the block that used it
// may still point into it.
func (set *privateArenas) release() {
	used := set.params.Used()
	for {
		peak := privateParameterPeak.Load()
		if used <= peak || privateParameterPeak.CompareAndSwap(peak, used) {
			break
		}
	}
	if spilled, _ := set.params.Overflow(); spilled > 0 {
		privateParameterOverflow.Add(spilled)
	}
	set.params.Reset()
	for _, borrowed := range set.borrowed {
		borrowed.Reset()
	}
	privateArenaPool.Put(set)
}

func blockPrivateArenas(block *b.Block) *privateArenas {
	if block == nil || block.PrivateArenas == nil {
		return nil
	}
	set, _ := block.PrivateArenas.(*privateArenas)
	return set
}

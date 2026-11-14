package turbine

import (
	"sync/atomic"
	"unsafe"
)

// kernelShredDrops totals packets the kernel dropped on shred receive sockets
// because their buffers were full, across every socket this process opened.
var kernelShredDrops atomic.Uint64

// KernelShredDrops returns the process-wide total of kernel drops on shred
// receive sockets (Linux SO_RXQ_OVFL), zero where unsupported.
func KernelShredDrops() uint64 { return kernelShredDrops.Load() }

// socketDrops follows one socket's cumulative drop counter and adds its
// growth to the process total. Only that socket's reader goroutine calls it.
type socketDrops struct {
	seen bool
	last uint32
}

func (d *socketDrops) observe(cumulative uint32) {
	if d.seen && cumulative != d.last {
		// uint32 subtraction handles the counter wrapping.
		kernelShredDrops.Add(uint64(cumulative - d.last))
	} else if !d.seen && cumulative != 0 {
		// Drops before the first packet we read still happened on our socket.
		kernelShredDrops.Add(uint64(cumulative))
	}
	d.seen, d.last = true, cumulative
}

func unsafePointer(b *byte) unsafe.Pointer { return unsafe.Pointer(b) }

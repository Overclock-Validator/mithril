package spinlock

// pause executes one x86 PAUSE spin-wait hint.
//
//go:noescape
func pause()

//go:build !amd64

package spinlock

func pause() {}

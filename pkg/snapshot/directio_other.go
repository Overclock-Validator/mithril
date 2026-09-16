//go:build !linux

package snapshot

import "fmt"

func directIOFlag() (int, error) {
	return 0, fmt.Errorf("snapshot direct I/O is only supported on Linux; disable snapshot_directio")
}

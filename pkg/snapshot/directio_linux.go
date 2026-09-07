//go:build linux

package snapshot

import "syscall"

func directIOFlag() (int, error) { return syscall.O_DIRECT, nil }

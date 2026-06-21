//go:build !linux && !darwin

package config

// No portable statfs off Linux/Darwin; the false return makes callers skip the
// disk-space guard.
func freeBytesForFS(string) (uint64, bool) { return 0, false }

func writableFS(string) bool { return false }

func deviceID(string) (uint64, bool) { return 0, false }

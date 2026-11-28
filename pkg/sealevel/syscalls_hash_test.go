package sealevel

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/stretchr/testify/assert"
)

func TestSyscalls_Blake3RequiresFeature(t *testing.T) {
	ft := features.NewFeaturesDefault()

	_, ok := Syscalls(ft, false, sbpf.SymbolHash("sol_blake3"))
	assert.False(t, ok)

	ft.EnableFeature(features.Blake3SyscallEnabled, 0)

	_, ok = Syscalls(ft, false, sbpf.SymbolHash("sol_blake3"))
	assert.True(t, ok)
}

func TestSyscalls_Sha512RequiresFeature(t *testing.T) {
	ft := features.NewFeaturesDefault()

	_, ok := Syscalls(ft, false, sbpf.SymbolHash("sol_sha512"))
	assert.False(t, ok)

	ft.EnableFeature(features.EnableSha512Syscall, 0)

	_, ok = Syscalls(ft, false, sbpf.SymbolHash("sol_sha512"))
	assert.True(t, ok)
}

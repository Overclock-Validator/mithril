package sealevel

import (
	"testing"

	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/stretchr/testify/require"
)

// A native program with an implementation but no resolver case is invisible:
// resolveNativeProgramById returns InstrErrUnsupportedProgramId and the program
// simply never runs. That is how AddressLookupTableExecute sat unreachable while
// its own 31 unit tests failed, and it fails quietly rather than at build time
// because the implementation still compiles and is still exported.
//
// This pins the mapping so adding an implementation without wiring it, or
// dropping a case during a refactor, shows up here.
func TestEveryNativeProgramResolves(t *testing.T) {
	for _, tc := range []struct {
		name    string
		address [32]byte
		want    string
	}{
		{"system", a.SystemProgramAddr, a.SystemProgramAddrStr},
		{"vote", a.VoteProgramAddr, a.VoteProgramAddrStr},
		{"stake", a.StakeProgramAddr, a.StakeProgramAddrStr},
		{"config", a.ConfigProgramAddr, a.ConfigProgramAddrStr},
		{"address lookup table", a.AddressLookupTableAddr, a.AddressLookupTableProgramAddrStr},
		{"compute budget", a.ComputeBudgetProgramAddr, a.ComputeBudgetProgramAddrStr},
		{"bpf loader v2", a.BpfLoader2Addr, a.BpfLoader2AddrStr},
		{"bpf loader deprecated", a.BpfLoaderDeprecatedAddr, a.BpfLoaderDeprecatedAddrStr},
		{"bpf loader upgradeable", a.BpfLoaderUpgradeableAddr, a.BpfLoaderUpgradeableAddrStr},
		{"loader v4", a.LoaderV4Addr, a.LoaderV4AddrStr},
		{"zk elgamal proof", a.ZkElgamalProofProgramAddr, a.ZkElgamalProofProgramAddrStr},
		{"ed25519 precompile", a.Ed25519PrecompileAddr, a.Ed25519PrecompileAddrStr},
		{"secp256k1 precompile", a.Secp256kPrecompileAddr, a.Secp256kPrecompileAddrStr},
		{"secp256r1 precompile", a.Secp256r1PrecompileAddr, a.Secp256r1PrecompileAddrStr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			execute, name, err := resolveNativeProgramById(tc.address)
			require.NoError(t, err, "%s has an implementation but no resolver case", tc.name)
			require.NotNil(t, execute)
			require.Equal(t, tc.want, name)
		})
	}
}

// The resolver must not claim an address it has no implementation for, or a
// BPF-owned program would be shadowed by a nonexistent builtin.
func TestUnknownProgramIdDoesNotResolve(t *testing.T) {
	var unknown [32]byte
	unknown[0] = 0xAB

	_, _, err := resolveNativeProgramById(unknown)
	require.ErrorIs(t, err, InstrErrUnsupportedProgramId)
}

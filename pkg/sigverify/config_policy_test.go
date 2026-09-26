package sigverify

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveConfigPolicyDefaultsAndOverrides(t *testing.T) {
	zero, err := ResolveConfig(Config{})
	require.NoError(t, err)
	require.Equal(t, BackendAuto, zero.Backend)
	require.Equal(t, min(2, runtime.GOMAXPROCS(0)), zero.Workers)
	require.Equal(t, 8, zero.BatchTarget)
	require.False(t, zero.DisableShredOverlap)
	defaults, err := ResolveConfig(Defaults())
	require.NoError(t, err)
	require.Equal(t, zero, defaults)

	override := Config{Backend: BackendGeneric, Workers: 4, BatchTarget: 4, DisableShredOverlap: true}
	resolved, err := ResolveConfig(override)
	require.NoError(t, err)
	require.Equal(t, override, resolved)
}

func TestInvalidPolicyDoesNotLatchBackend(t *testing.T) {
	if !inChild() {
		out, err := runConfigureChild(t, t.Name())
		require.NoError(t, err, "child output:\n%s", out)
		require.Contains(t, out, "PASS")
		return
	}
	before := Cfg
	for _, cfg := range []Config{
		{Backend: BackendGeneric, Workers: -1},
		{Backend: BackendGeneric, BatchTarget: -1},
		{Backend: BackendGeneric, BatchTarget: 3},
		{Backend: BackendGeneric, BatchTarget: 16},
	} {
		_, err := Configure(cfg)
		require.Error(t, err)
		require.Equal(t, before, Cfg, "invalid policy must not become live")
		require.Empty(t, configuredBackend, "invalid policy must not latch the backend")
	}
	_, err := Configure(Config{Backend: BackendGeneric, Workers: 2, BatchTarget: 4})
	require.NoError(t, err, "valid configuration must remain possible after rejection")
	require.Equal(t, 2, TransactionWorkers())
	require.Equal(t, 4, TransactionBatchTarget())
}

func TestTransactionPolicyWithoutConfigure(t *testing.T) {
	if !inChild() {
		out, err := runConfigureChild(t, t.Name())
		require.NoError(t, err, "child output:\n%s", out)
		require.Contains(t, out, "PASS")
		return
	}
	Cfg = Config{}
	require.Equal(t, min(2, runtime.GOMAXPROCS(0)), TransactionWorkers())
	require.Equal(t, 8, TransactionBatchTarget())
	require.False(t, Cfg.DisableShredOverlap)
}

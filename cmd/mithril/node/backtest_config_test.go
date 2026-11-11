package node

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

// backtestTestFlags mirrors the backtest flags that a file can set.
func backtestTestFlags() *pflag.FlagSet {
	f := pflag.NewFlagSet("backtest", pflag.ContinueOnError)
	for _, name := range []string{"capture", "snapshot", "incremental", "accounts", "workdir", "cluster", "genesis-hash"} {
		f.String(name, "", "")
	}
	f.Int("txpar", 8, "")
	f.Int("warmup", 30, "")
	f.Uint64("end-slot", 0, "")
	f.Uint64("measure-from", 0, "")
	f.Bool("phase-timers", true, "")
	f.Bool("verify-shred-signatures", false, "")
	return f
}

func resetBacktestOptions(t *testing.T) {
	t.Helper()
	saved := bt
	t.Cleanup(func() { bt = saved })
	bt = saved
	bt.capture, bt.snapshot, bt.incremental, bt.accounts, bt.workdir = "", "", "", "", ""
	bt.cluster, bt.genesisHash = "mainnet", ""
	bt.txpar, bt.warmup, bt.endSlot, bt.measureFrom = 8, 30, 0, 0
	bt.phaseTimers, bt.verifySignatures = true, false
}

func writeBacktestFile(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "backtest.toml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o644))
	return path
}

func TestBacktestConfigFileSetsEverything(t *testing.T) {
	resetBacktestOptions(t)
	path := writeBacktestFile(t, `
[paths]
snapshot = "snap/snapshot-100-11111111111111111111111111111111.tar.zst"
incremental = "/abs/incremental-snapshot-100-200-11111111111111111111111111111111.tar.zst"
workdir = "work"
[network]
cluster = "devnet"
[layout]
exec_workers = 16
[ledger_input]
path = "cap.pcapng.zst"
end_slot = 500
[backtest]
warmup = 5
measure_from = 300
phase_timers = false
verify_shred_signatures = true
`)
	f, err := loadBacktestFile(path)
	require.NoError(t, err)
	applyBacktestFile(f, path, backtestTestFlags())
	dir := filepath.Dir(path)
	require.Equal(t, filepath.Join(dir, "cap.pcapng.zst"), bt.capture, "relative to the file")
	require.Equal(t, filepath.Join(dir, "snap/snapshot-100-11111111111111111111111111111111.tar.zst"), bt.snapshot)
	require.Equal(t, "/abs/incremental-snapshot-100-200-11111111111111111111111111111111.tar.zst", bt.incremental, "absolute paths stay")
	require.Equal(t, filepath.Join(dir, "work"), bt.workdir)
	require.Equal(t, "devnet", bt.cluster)
	require.Equal(t, 16, bt.txpar)
	require.Equal(t, uint64(500), bt.endSlot)
	require.Equal(t, 5, bt.warmup)
	require.Equal(t, uint64(300), bt.measureFrom)
	require.False(t, bt.phaseTimers)
	require.True(t, bt.verifySignatures)
	require.NoError(t, validateBacktestOptions())
}

func TestBacktestFlagsOverrideTheFile(t *testing.T) {
	resetBacktestOptions(t)
	path := writeBacktestFile(t, "[layout]\nexec_workers = 16\n[backtest]\nphase_timers = false\nwarmup = 5\n")
	f, err := loadBacktestFile(path)
	require.NoError(t, err)
	flags := backtestTestFlags()
	require.NoError(t, flags.Set("txpar", "4"))
	require.NoError(t, flags.Set("phase-timers", "true"))
	bt.txpar, bt.phaseTimers = 4, true // what cobra stores for the flags just set
	applyBacktestFile(f, path, flags)
	require.Equal(t, 4, bt.txpar, "flag wins")
	require.True(t, bt.phaseTimers, "flag wins")
	require.Equal(t, 5, bt.warmup, "file applies where no flag was given")
}

func TestBacktestConfigFileRefusesUnknownKeys(t *testing.T) {
	path := writeBacktestFile(t, "[backtest]\nphase_timer = false\n")
	_, err := loadBacktestFile(path)
	require.ErrorContains(t, err, "unknown setting")
	require.ErrorContains(t, err, "phase_timer")

	path = writeBacktestFile(t, "[network]\nrpc = [\"http://example\"]\n")
	_, err = loadBacktestFile(path)
	require.ErrorContains(t, err, "unknown setting", "the node configuration cannot be reached from here")
}

func TestValidateBacktestOptions(t *testing.T) {
	resetBacktestOptions(t)
	require.ErrorContains(t, validateBacktestOptions(), "no capture")
	bt.capture = "c.pcapng.zst"
	require.ErrorContains(t, validateBacktestOptions(), "exactly one of")
	bt.snapshot, bt.accounts = "s", "a"
	require.ErrorContains(t, validateBacktestOptions(), "exactly one of")
	bt.accounts = ""
	require.NoError(t, validateBacktestOptions())
	bt.snapshot, bt.accounts, bt.incremental = "", "a", "i"
	require.ErrorContains(t, validateBacktestOptions(), "incremental snapshot needs a full snapshot")
	bt.incremental, bt.txpar = "", 0
	require.ErrorContains(t, validateBacktestOptions(), "at least 1")
	bt.txpar, bt.warmup = 8, -1
	require.ErrorContains(t, validateBacktestOptions(), "cannot be negative")
}

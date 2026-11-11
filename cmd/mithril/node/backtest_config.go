package node

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pelletier/go-toml/v2"
	"github.com/spf13/pflag"
)

// backtestFile is a backtest configuration file, laid out like the TOML that
// firedancer-dev backtest runs from where the concepts match. It configures
// only the backtest; the node configuration the backtest runs with is
// generated and cannot be overridden here, so no file can enable an RPC
// endpoint, compaction or the verifier.
type backtestFile struct {
	Paths struct {
		Snapshot    string `toml:"snapshot"`    // Firedancer: [paths] snapshots
		Incremental string `toml:"incremental"` // incremental snapshot on top of snapshot
		Accounts    string `toml:"accounts"`    // existing AccountsDB instead of a snapshot
		Workdir     string `toml:"workdir"`
	} `toml:"paths"`
	Network struct {
		Cluster     string `toml:"cluster"`
		GenesisHash string `toml:"genesis_hash"` // custom clusters only
	} `toml:"network"`
	Layout struct {
		ExecWorkers *int `toml:"exec_workers"` // Firedancer: [layout] execrp_tile_count
	} `toml:"layout"`
	LedgerInput struct {
		Path    string  `toml:"path"`     // Firedancer: [development.ledger_input] path
		EndSlot *uint64 `toml:"end_slot"` // 0 = the capture's last block
	} `toml:"ledger_input"`
	Backtest struct {
		Warmup                *int    `toml:"warmup"`
		MeasureFrom           *uint64 `toml:"measure_from"`
		PhaseTimers           *bool   `toml:"phase_timers"`
		VerifyShredSignatures *bool   `toml:"verify_shred_signatures"`
	} `toml:"backtest"`
}

// loadBacktestFile reads path strictly: an unknown key is an error, so a
// misspelled setting never silently falls back to its default.
func loadBacktestFile(path string) (*backtestFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var f backtestFile
	dec := toml.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		var strict *toml.StrictMissingError
		if errors.As(err, &strict) {
			return nil, fmt.Errorf("%s: unknown setting:\n%s", path, strict.String())
		}
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &f, nil
}

// applyBacktestFile copies the file's settings into bt wherever the matching
// flag was not given on the command line: flags override the file. Relative
// paths in the file are relative to the file's own directory.
func applyBacktestFile(f *backtestFile, path string, flags *pflag.FlagSet) {
	dir := filepath.Dir(path)
	rel := func(p string) string {
		if p == "" || filepath.IsAbs(p) {
			return p
		}
		return filepath.Join(dir, p)
	}
	str := func(flag string, dst *string, v string) {
		if v != "" && !flags.Changed(flag) {
			*dst = v
		}
	}
	str("capture", &bt.capture, rel(f.LedgerInput.Path))
	str("snapshot", &bt.snapshot, rel(f.Paths.Snapshot))
	str("incremental", &bt.incremental, rel(f.Paths.Incremental))
	str("accounts", &bt.accounts, rel(f.Paths.Accounts))
	str("workdir", &bt.workdir, rel(f.Paths.Workdir))
	str("cluster", &bt.cluster, f.Network.Cluster)
	str("genesis-hash", &bt.genesisHash, f.Network.GenesisHash)
	if v := f.Layout.ExecWorkers; v != nil && !flags.Changed("txpar") {
		bt.txpar = *v
	}
	if v := f.LedgerInput.EndSlot; v != nil && !flags.Changed("end-slot") {
		bt.endSlot = *v
	}
	if v := f.Backtest.Warmup; v != nil && !flags.Changed("warmup") {
		bt.warmup = *v
	}
	if v := f.Backtest.MeasureFrom; v != nil && !flags.Changed("measure-from") {
		bt.measureFrom = *v
	}
	if v := f.Backtest.PhaseTimers; v != nil && !flags.Changed("phase-timers") {
		bt.phaseTimers = *v
	}
	if v := f.Backtest.VerifyShredSignatures; v != nil && !flags.Changed("verify-shred-signatures") {
		bt.verifySignatures = *v
	}
}

// validateBacktestOptions checks the merged flag and file settings.
func validateBacktestOptions() error {
	if bt.capture == "" {
		return errors.New("no capture: pass --capture or set [ledger_input] path")
	}
	if (bt.snapshot == "") == (bt.accounts == "") {
		return errors.New("set exactly one of a snapshot (--snapshot, [paths] snapshot) or an AccountsDB (--accounts, [paths] accounts)")
	}
	if bt.incremental != "" && bt.snapshot == "" {
		return errors.New("an incremental snapshot needs a full snapshot")
	}
	if bt.txpar < 1 {
		return fmt.Errorf("exec_workers / --txpar must be at least 1, got %d", bt.txpar)
	}
	return nil
}

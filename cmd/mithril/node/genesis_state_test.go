package node

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/state"
)

func TestNewReadyStateForBootstrapAlwaysBindsChain(t *testing.T) {
	for _, mode := range []string{"explicit", "snapshot", "new-snapshot", "new-incremental", "auto"} {
		t.Run(mode, func(t *testing.T) {
			got, err := newReadyStateForBootstrap(2233733, 41, mode, "alpenglow", testNodeGenesisHash)
			if err != nil {
				t.Fatalf("new ready state: %v", err)
			}
			if got.Cluster != "alpenglow" || got.GenesisHash != testNodeGenesisHash || got.BuildMode != mode {
				t.Fatalf("ready state lost bootstrap metadata: %+v", got)
			}
			if !got.IsReady() || got.SnapshotSlot != 2233733 || got.SnapshotEpoch != 41 {
				t.Fatalf("ready state has wrong snapshot metadata: %+v", got)
			}
		})
	}
}

func TestNewReadyStateForBootstrapRejectsUnboundBuild(t *testing.T) {
	if _, err := newReadyStateForBootstrap(1, 0, "new-snapshot", "alpenglow", ""); err == nil {
		t.Fatal("ready state accepted an empty genesis hash")
	}
	if _, err := newReadyStateForBootstrap(1, 0, "new-snapshot", "", testNodeGenesisHash); err == nil {
		t.Fatal("ready state accepted an empty cluster")
	}
}

func TestValidateAccountsGenesisForBootstrap(t *testing.T) {
	tests := []struct {
		name       string
		stored     string
		legacy     string
		mode       string
		wantPrior  string
		wantChange bool
		wantErr    bool
	}{
		{name: "same genesis resume", stored: testNodeGenesisHash, mode: "auto", wantPrior: testNodeGenesisHash},
		{name: "same genesis new snapshot", stored: testNodeGenesisHash, mode: "new-snapshot", wantPrior: testNodeGenesisHash},
		{name: "re-genesis requires new snapshot", stored: testNodeOldGenesisHash, mode: "auto", wantPrior: testNodeOldGenesisHash, wantChange: true, wantErr: true},
		{name: "re-genesis accepted for clean snapshot", stored: testNodeOldGenesisHash, mode: "new-snapshot", wantPrior: testNodeOldGenesisHash, wantChange: true},
		{name: "explicit legacy assertion proves re-genesis", legacy: testNodeOldGenesisHash, mode: "new-snapshot", wantPrior: testNodeOldGenesisHash, wantChange: true},
		{name: "unbound resume fails closed", mode: "auto", wantErr: true},
		{name: "unbound state may be replaced", mode: "new-snapshot"},
		{name: "stored state is not overridden by ledger assertion", stored: testNodeGenesisHash, legacy: testNodeOldGenesisHash, mode: "new-snapshot", wantPrior: testNodeGenesisHash},
		{name: "malformed stored hash fails closed", stored: "../../escape", mode: "new-snapshot", wantPrior: "../../escape", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prior, changed, err := validateAccountsGenesisForBootstrap(true, tt.stored, tt.legacy, testNodeGenesisHash, tt.mode)
			if prior != tt.wantPrior || changed != tt.wantChange || (err != nil) != tt.wantErr {
				t.Fatalf("decision = prior %q changed %t err %v; want prior %q changed %t error=%t",
					prior, changed, err, tt.wantPrior, tt.wantChange, tt.wantErr)
			}
		})
	}
}

func TestLoadBootstrapStateRejectsMalformedStateExceptReplacement(t *testing.T) {
	accountsPath := t.TempDir()
	statePath := filepath.Join(accountsPath, state.StateFileName)
	if err := os.WriteFile(statePath, []byte("{malformed"), 0o600); err != nil {
		t.Fatalf("write malformed state: %v", err)
	}

	for _, mode := range []string{"auto", "accountsdb", "snapshot", "new-incremental", "explicit"} {
		t.Run(mode, func(t *testing.T) {
			got, err := loadBootstrapState(accountsPath, mode)
			if err == nil || got != nil {
				t.Fatalf("state load = (%v, %v), want a fail-closed error", got, err)
			}
		})
	}

	got, err := loadBootstrapState(accountsPath, "new-snapshot")
	if err != nil || got != nil {
		t.Fatalf("new-snapshot state load = (%v, %v), want replacement to proceed", got, err)
	}
}

func TestLoadBootstrapStatePropagatesValidationErrors(t *testing.T) {
	wantErr := errors.New("state validation failed")
	checkCalled := false
	load := func(string) (*state.MithrilState, error) { return nil, nil }
	check := func(string) (*state.MithrilState, error) {
		checkCalled = true
		return nil, wantErr
	}

	got, err := loadBootstrapStateWith("unused", "auto", load, check)
	if !checkCalled {
		t.Fatal("state validity check was not called")
	}
	if got != nil || !errors.Is(err, wantErr) {
		t.Fatalf("state load = (%v, %v), want wrapped validation error", got, err)
	}

	got, err = loadBootstrapStateWith("unused", "new-snapshot", load, check)
	if got != nil || err != nil {
		t.Fatalf("new-snapshot state load = (%v, %v), want replacement to proceed", got, err)
	}
}

func TestLoadBootstrapStateKeepsMarkedCorruptionRebuildable(t *testing.T) {
	accountsPath := t.TempDir()
	corrupted := &state.MithrilState{
		StateSchemaVersion: state.CurrentStateSchemaVersion,
		Stage:              "corrupted",
		CorruptionReason:   "test corruption",
	}
	if err := corrupted.Save(accountsPath); err != nil {
		t.Fatalf("save corrupted state marker: %v", err)
	}

	got, err := loadBootstrapState(accountsPath, "auto")
	if err != nil || got != nil {
		t.Fatalf("marked-corrupt state load = (%v, %v), want ordinary auto rebuild", got, err)
	}
}

const testNodeGenesisHash = "87WDnn84R1RVXsaBC4JkugZtLaL2biu6e6kkwx6Z8ruJ"
const testNodeOldGenesisHash = "HtRW7y9hJZaEBgH8cvUomQQjaXY5vM8J54nqbZJz7MjW"

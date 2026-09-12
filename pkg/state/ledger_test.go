package state

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

const (
	testOldGenesis = "HtRW7y9hJZaEBgH8cvUomQQjaXY5vM8J54nqbZJz7MjW"
	testNewGenesis = "87WDnn84R1RVXsaBC4JkugZtLaL2biu6e6kkwx6Z8ruJ"
)

func TestEnsureLedgerChainBindingInitializesEmptyLedger(t *testing.T) {
	dir := t.TempDir()
	transition, err := EnsureLedgerChainBinding(dir, "alpenglow", testNewGenesis, "")
	if err != nil {
		t.Fatalf("ensure binding: %v", err)
	}
	if transition.QuarantineDir != "" || len(transition.MovedPaths) != 0 {
		t.Fatalf("empty ledger unexpectedly quarantined: %+v", transition)
	}
	binding, err := LoadLedgerChainBinding(dir)
	if err != nil {
		t.Fatalf("load binding: %v", err)
	}
	if binding == nil || binding.Cluster != "alpenglow" || binding.GenesisHash != testNewGenesis {
		t.Fatalf("binding = %+v", binding)
	}
}

func TestEnsureLedgerChainBindingFailsClosedForUnboundVotes(t *testing.T) {
	dir := t.TempDir()
	history := "vote_history-validator.mithril.json"
	writeLedgerFixture(t, filepath.Join(dir, history), "signed-old-votes")

	_, err := EnsureLedgerChainBinding(dir, "alpenglow", testNewGenesis, "")
	if !errors.Is(err, ErrUnboundLedgerArtifacts) {
		t.Fatalf("error = %v, want ErrUnboundLedgerArtifacts", err)
	}
	assertFileContents(t, filepath.Join(dir, history), "signed-old-votes")
	if binding, loadErr := LoadLedgerChainBinding(dir); loadErr != nil || binding != nil {
		t.Fatalf("binding after refusal = %+v, err=%v", binding, loadErr)
	}
}

func TestEnsureLedgerChainBindingSameGenesisPreservesArtifacts(t *testing.T) {
	dir := t.TempDir()
	writeLedgerFixture(t, filepath.Join(dir, "vote_history-validator.mithril.json"), "signed-votes")
	writeLedgerFixture(t, filepath.Join(dir, "catchup-spool", "s42.shreds"), "shreds")

	transition, err := EnsureLedgerChainBinding(dir, "alpenglow", testOldGenesis, testOldGenesis)
	if err != nil {
		t.Fatalf("ensure legacy binding: %v", err)
	}
	if transition.QuarantineDir != "" {
		t.Fatalf("same-genesis startup quarantined artifacts at %s", transition.QuarantineDir)
	}
	assertFileContents(t, filepath.Join(dir, "vote_history-validator.mithril.json"), "signed-votes")
	assertFileContents(t, filepath.Join(dir, "catchup-spool", "s42.shreds"), "shreds")

	// A subsequent same-genesis snapshot rebuild has a stored binding and must
	// retain the same voting record and prewarmed shreds.
	transition, err = EnsureLedgerChainBinding(dir, "alpenglow", testOldGenesis, "")
	if err != nil {
		t.Fatalf("ensure stored binding: %v", err)
	}
	if transition.QuarantineDir != "" {
		t.Fatalf("same-genesis restart quarantined artifacts at %s", transition.QuarantineDir)
	}
	assertFileContents(t, filepath.Join(dir, "vote_history-validator.mithril.json"), "signed-votes")
}

func TestEnsureLedgerChainBindingMismatchQuarantinesOnlyScopedArtifacts(t *testing.T) {
	dir := t.TempDir()
	writeLedgerFixture(t, filepath.Join(dir, "vote_history-validator.mithril.json"), "signed-old-votes")
	writeLedgerFixture(t, filepath.Join(dir, ".vote_history-validator.mithril.json.tmp-crash"), "partial")
	writeLedgerFixture(t, filepath.Join(dir, "catchup-spool", "s11643075.shreds"), "old-shreds")
	writeLedgerFixture(t, filepath.Join(dir, "operator-notes.txt"), "keep me")

	if _, err := EnsureLedgerChainBinding(dir, "alpenglow", testOldGenesis, testOldGenesis); err != nil {
		t.Fatalf("establish old binding: %v", err)
	}
	transition, err := EnsureLedgerChainBinding(dir, "alpenglow", testNewGenesis, "")
	if err != nil {
		t.Fatalf("transition binding: %v", err)
	}
	if transition.QuarantineDir == "" {
		t.Fatal("mismatch did not create quarantine")
	}
	if transition.PreviousGenesisHash != testOldGenesis || transition.CurrentGenesisHash != testNewGenesis {
		t.Fatalf("transition hashes = %+v", transition)
	}
	assertMissing(t, filepath.Join(dir, "vote_history-validator.mithril.json"))
	assertMissing(t, filepath.Join(dir, ".vote_history-validator.mithril.json.tmp-crash"))
	assertMissing(t, filepath.Join(dir, "catchup-spool"))
	assertFileContents(t, filepath.Join(dir, "operator-notes.txt"), "keep me")
	assertFileContents(t, filepath.Join(transition.QuarantineDir, "vote_history-validator.mithril.json"), "signed-old-votes")
	assertFileContents(t, filepath.Join(transition.QuarantineDir, "catchup-spool", "s11643075.shreds"), "old-shreds")

	data, err := os.ReadFile(filepath.Join(transition.QuarantineDir, "provenance.json"))
	if err != nil {
		t.Fatalf("read provenance: %v", err)
	}
	var provenance ledgerQuarantineProvenance
	if err := json.Unmarshal(data, &provenance); err != nil {
		t.Fatalf("parse provenance: %v", err)
	}
	if provenance.PreviousGenesisHash != testOldGenesis || provenance.CurrentGenesisHash != testNewGenesis {
		t.Fatalf("provenance = %+v", provenance)
	}
	if len(provenance.MovedPaths) != 4 { // spool, temp history, signed history, old binding
		t.Fatalf("moved paths = %#v", provenance.MovedPaths)
	}

	binding, err := LoadLedgerChainBinding(dir)
	if err != nil {
		t.Fatalf("load rebound ledger: %v", err)
	}
	if binding == nil || binding.GenesisHash != testNewGenesis {
		t.Fatalf("new binding = %+v", binding)
	}
}

func TestEnsureLedgerChainBindingResumesPartialQuarantine(t *testing.T) {
	dir := t.TempDir()
	writeLedgerFixture(t, filepath.Join(dir, "vote_history-validator.mithril.json"), "signed-old-votes")
	writeLedgerFixture(t, filepath.Join(dir, "catchup-spool", "s42.shreds"), "old-shreds")
	if _, err := EnsureLedgerChainBinding(dir, "alpenglow", testOldGenesis, testOldGenesis); err != nil {
		t.Fatalf("establish old binding: %v", err)
	}

	injected := errors.New("injected rename failure")
	renames := 0
	_, err := ensureLedgerChainBinding(dir, "alpenglow", testNewGenesis, "", ledgerTransitionOps{
		rename: func(oldPath, newPath string) error {
			renames++
			if renames == 2 {
				return injected
			}
			return os.Rename(oldPath, newPath)
		},
	})
	if !errors.Is(err, injected) {
		t.Fatalf("partial transition error = %v, want injected failure", err)
	}

	intent, err := loadLedgerTransitionIntent(dir)
	if err != nil || intent == nil {
		t.Fatalf("load durable transition intent: intent=%+v err=%v", intent, err)
	}
	if intent.Phase != ledgerTransitionPhaseMoving {
		t.Fatalf("intent phase = %q, want moving", intent.Phase)
	}
	quarantineDir := filepath.Join(dir, LedgerQuarantineDirName, intent.QuarantineName)
	assertFileContents(t, filepath.Join(quarantineDir, "catchup-spool", "s42.shreds"), "old-shreds")
	assertFileContents(t, filepath.Join(dir, "vote_history-validator.mithril.json"), "signed-old-votes")
	binding, loadErr := LoadLedgerChainBinding(dir)
	if loadErr != nil || binding == nil || binding.GenesisHash != testOldGenesis {
		t.Fatalf("old binding was not preserved at partial failure: binding=%+v err=%v", binding, loadErr)
	}

	transition, err := EnsureLedgerChainBinding(dir, "alpenglow", testNewGenesis, "")
	if err != nil {
		t.Fatalf("resume partial transition: %v", err)
	}
	if transition.QuarantineDir != quarantineDir {
		t.Fatalf("retry quarantine dir = %q, want original %q", transition.QuarantineDir, quarantineDir)
	}
	assertMissing(t, filepath.Join(dir, ledgerTransitionIntentFileName))
	assertMissing(t, filepath.Join(dir, "vote_history-validator.mithril.json"))
	assertFileContents(t, filepath.Join(quarantineDir, "vote_history-validator.mithril.json"), "signed-old-votes")
	binding, loadErr = LoadLedgerChainBinding(dir)
	if loadErr != nil || binding == nil || binding.GenesisHash != testNewGenesis {
		t.Fatalf("binding after resumed transition: binding=%+v err=%v", binding, loadErr)
	}
}

func TestEnsureLedgerChainBindingResumesBeforeBindingCommit(t *testing.T) {
	dir := t.TempDir()
	writeLedgerFixture(t, filepath.Join(dir, "vote_history-validator.mithril.json"), "signed-old-votes")
	if _, err := EnsureLedgerChainBinding(dir, "alpenglow", testOldGenesis, testOldGenesis); err != nil {
		t.Fatalf("establish old binding: %v", err)
	}

	injected := errors.New("injected binding commit failure")
	_, err := ensureLedgerChainBinding(dir, "alpenglow", testNewGenesis, "", ledgerTransitionOps{
		rename: func(oldPath, newPath string) error {
			if filepath.Base(oldPath) == ledgerTransitionIntentFileName && filepath.Base(newPath) == LedgerChainBindingFileName {
				return injected
			}
			return os.Rename(oldPath, newPath)
		},
	})
	if !errors.Is(err, injected) {
		t.Fatalf("pre-commit transition error = %v, want injected failure", err)
	}
	intent, err := loadLedgerTransitionIntent(dir)
	if err != nil || intent == nil {
		t.Fatalf("load durable transition intent: intent=%+v err=%v", intent, err)
	}
	if intent.Phase != ledgerTransitionPhaseQuarantined {
		t.Fatalf("intent phase = %q, want quarantined", intent.Phase)
	}
	assertMissing(t, filepath.Join(dir, LedgerChainBindingFileName))
	quarantineDir := filepath.Join(dir, LedgerQuarantineDirName, intent.QuarantineName)
	assertFileContents(t, filepath.Join(quarantineDir, "vote_history-validator.mithril.json"), "signed-old-votes")
	if _, err := os.Stat(filepath.Join(quarantineDir, "provenance.json")); err != nil {
		t.Fatalf("durable provenance missing before binding commit: %v", err)
	}

	transition, err := EnsureLedgerChainBinding(dir, "alpenglow", testNewGenesis, "")
	if err != nil {
		t.Fatalf("resume pre-commit transition: %v", err)
	}
	if transition.QuarantineDir != quarantineDir {
		t.Fatalf("retry quarantine dir = %q, want original %q", transition.QuarantineDir, quarantineDir)
	}
	assertMissing(t, filepath.Join(dir, ledgerTransitionIntentFileName))
	binding, loadErr := LoadLedgerChainBinding(dir)
	if loadErr != nil || binding == nil || binding.GenesisHash != testNewGenesis {
		t.Fatalf("binding after resumed commit: binding=%+v err=%v", binding, loadErr)
	}
}

func TestEnsureLedgerChainBindingPendingTransitionRejectsUnjournaledArtifact(t *testing.T) {
	dir := t.TempDir()
	writeLedgerFixture(t, filepath.Join(dir, "vote_history-validator.mithril.json"), "signed-old-votes")
	if _, err := EnsureLedgerChainBinding(dir, "alpenglow", testOldGenesis, testOldGenesis); err != nil {
		t.Fatalf("establish old binding: %v", err)
	}

	injected := errors.New("pause transition")
	_, err := ensureLedgerChainBinding(dir, "alpenglow", testNewGenesis, "", ledgerTransitionOps{
		rename: func(oldPath, newPath string) error {
			return injected
		},
	})
	if !errors.Is(err, injected) {
		t.Fatalf("paused transition error = %v, want injected failure", err)
	}
	writeLedgerFixture(t, filepath.Join(dir, "vote_history-unexpected.mithril.json"), "unjournaled-old-votes")

	_, err = EnsureLedgerChainBinding(dir, "alpenglow", testNewGenesis, "")
	if err == nil {
		t.Fatal("retry accepted an unjournaled live artifact")
	}
	assertFileContents(t, filepath.Join(dir, "vote_history-unexpected.mithril.json"), "unjournaled-old-votes")
	if intent, loadErr := loadLedgerTransitionIntent(dir); loadErr != nil || intent == nil {
		t.Fatalf("failed retry did not preserve intent: intent=%+v err=%v", intent, loadErr)
	}
	binding, loadErr := LoadLedgerChainBinding(dir)
	if loadErr != nil || binding == nil || binding.GenesisHash != testOldGenesis {
		t.Fatalf("failed retry changed binding: binding=%+v err=%v", binding, loadErr)
	}
}

func TestEnsureLedgerChainBindingSerializesOnLedgerLock(t *testing.T) {
	dir := t.TempDir()
	lock, err := acquireLedgerChainLock(dir)
	if err != nil {
		t.Fatalf("acquire test lock: %v", err)
	}
	released := false
	t.Cleanup(func() {
		if !released {
			_ = lock.release()
		}
	})

	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		_, err := EnsureLedgerChainBinding(dir, "alpenglow", testNewGenesis, "")
		done <- err
	}()
	<-started
	select {
	case err := <-done:
		t.Fatalf("Ensure completed while ledger lock was held: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := lock.release(); err != nil {
		t.Fatalf("release test lock: %v", err)
	}
	released = true
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Ensure after lock release: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Ensure remained blocked after ledger lock release")
	}
}

func TestEnsureLedgerChainBindingConcurrentSameTarget(t *testing.T) {
	dir := t.TempDir()
	writeLedgerFixture(t, filepath.Join(dir, "vote_history-validator.mithril.json"), "signed-old-votes")
	if _, err := EnsureLedgerChainBinding(dir, "alpenglow", testOldGenesis, testOldGenesis); err != nil {
		t.Fatalf("establish old binding: %v", err)
	}

	const callers = 8
	start := make(chan struct{})
	results := make(chan *LedgerGenesisTransition, callers)
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			transition, err := EnsureLedgerChainBinding(dir, "alpenglow", testNewGenesis, "")
			if err != nil {
				errs <- err
				return
			}
			results <- transition
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Errorf("concurrent Ensure: %v", err)
	}
	quarantines := 0
	for transition := range results {
		if transition.QuarantineDir != "" {
			quarantines++
		}
	}
	if quarantines != 1 {
		t.Fatalf("completed quarantine transitions = %d, want exactly 1", quarantines)
	}
	binding, err := LoadLedgerChainBinding(dir)
	if err != nil || binding == nil || binding.GenesisHash != testNewGenesis {
		t.Fatalf("final concurrent binding: binding=%+v err=%v", binding, err)
	}
}

func TestEnsureLedgerChainBindingRejectsConflictingLegacyAssertion(t *testing.T) {
	dir := t.TempDir()
	if _, err := EnsureLedgerChainBinding(dir, "alpenglow", testNewGenesis, ""); err != nil {
		t.Fatalf("establish binding: %v", err)
	}
	_, err := EnsureLedgerChainBinding(dir, "alpenglow", testNewGenesis, testOldGenesis)
	if err == nil {
		t.Fatal("conflicting explicit legacy genesis was accepted")
	}
	binding, loadErr := LoadLedgerChainBinding(dir)
	if loadErr != nil || binding == nil || binding.GenesisHash != testNewGenesis {
		t.Fatalf("binding changed after conflict: %+v, err=%v", binding, loadErr)
	}
}

func TestEnsureLedgerChainBindingRejectsMalformedGenesisBeforeFilesystemUse(t *testing.T) {
	t.Run("current", func(t *testing.T) {
		dir := t.TempDir()
		if _, err := EnsureLedgerChainBinding(dir, "alpenglow", "../../escape", ""); err == nil {
			t.Fatal("malformed current genesis was accepted")
		}
		if entries, err := os.ReadDir(dir); err != nil || len(entries) != 0 {
			t.Fatalf("malformed current genesis changed ledger: entries=%v err=%v", entries, err)
		}
	})

	t.Run("legacy assertion", func(t *testing.T) {
		dir := t.TempDir()
		writeLedgerFixture(t, filepath.Join(dir, "vote_history-validator.mithril.json"), "signed-votes")
		if _, err := EnsureLedgerChainBinding(dir, "alpenglow", testNewGenesis, "not-a-hash"); err == nil {
			t.Fatal("malformed legacy genesis was accepted")
		}
		assertFileContents(t, filepath.Join(dir, "vote_history-validator.mithril.json"), "signed-votes")
	})

	t.Run("stored binding", func(t *testing.T) {
		dir := t.TempDir()
		malformed := `{"version":1,"cluster":"alpenglow","genesis_hash":"../../escape"}`
		writeLedgerFixture(t, filepath.Join(dir, LedgerChainBindingFileName), malformed)
		writeLedgerFixture(t, filepath.Join(dir, "catchup-spool", "s42.shreds"), "shreds")
		if _, err := EnsureLedgerChainBinding(dir, "alpenglow", testNewGenesis, ""); err == nil {
			t.Fatal("malformed stored genesis was accepted")
		}
		assertFileContents(t, filepath.Join(dir, "catchup-spool", "s42.shreds"), "shreds")
		if _, err := os.Stat(filepath.Join(dir, LedgerQuarantineDirName)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("malformed stored genesis created quarantine (err=%v)", err)
		}
	})
}

func writeLedgerFixture(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir fixture: %v", err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
}

func assertFileContents(t *testing.T, path, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if string(got) != want {
		t.Fatalf("%s = %q, want %q", path, got, want)
	}
}

func assertMissing(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("%s still exists (err=%v)", path, err)
	}
}

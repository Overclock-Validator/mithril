package replay

import (
	"os"
	"testing"
)

var speculationEnvNames = []string{
	"MITHRIL_WORK_AHEAD",
	"MITHRIL_WORK_AHEAD_FAIL_DEPTH",
	"MITHRIL_WORK_AHEAD_EARLY_OFFER",
	"MITHRIL_WORK_AHEAD_PREDICT_PAYER",
	"MITHRIL_WORK_AHEAD_NONCE",
	"MITHRIL_WORK_AHEAD_UNCHANGED",
}

// resetSpeculation puts every switch a preset touches in its off state, clears
// the matching environment variables, and restores both when the test ends.
func resetSpeculation(t *testing.T) {
	t.Helper()
	enabled, depth, offer, payer, nonce, unchanged :=
		workAheadEnabled, workAheadFailDepth, workAheadEarlyOffer, workAheadPredictPayer, workAheadNonce, workAheadUnchanged
	t.Cleanup(func() {
		workAheadEnabled, workAheadFailDepth, workAheadEarlyOffer, workAheadPredictPayer, workAheadNonce, workAheadUnchanged =
			enabled, depth, offer, payer, nonce, unchanged
	})
	workAheadEnabled, workAheadFailDepth, workAheadEarlyOffer, workAheadPredictPayer, workAheadNonce, workAheadUnchanged =
		false, 0, false, false, false, false
	for _, name := range speculationEnvNames {
		if value, set := os.LookupEnv(name); set {
			os.Unsetenv(name)
			t.Cleanup(func() { os.Setenv(name, value) })
		}
	}
}

func TestConfigureSpeculationOffChangesNothing(t *testing.T) {
	for _, mode := range []string{"", "off", " OFF "} {
		resetSpeculation(t)
		if err := ConfigureSpeculation(mode); err != nil {
			t.Fatalf("mode %q: %v", mode, err)
		}
		if workAheadEnabled || workAheadFailDepth != 0 || workAheadEarlyOffer || workAheadPredictPayer || workAheadNonce || workAheadUnchanged {
			t.Fatalf("mode %q turned a switch on", mode)
		}
		if got := SpeculationSummary(); got != "off" {
			t.Fatalf("mode %q: summary %q", mode, got)
		}
	}
}

func TestConfigureSpeculationOn(t *testing.T) {
	for _, mode := range []string{"on", " ON "} {
		t.Run(mode, func(t *testing.T) {
			resetSpeculation(t)
			if err := ConfigureSpeculation(mode); err != nil {
				t.Fatal(err)
			}
			if !workAheadEnabled || workAheadFailDepth != 4 || !workAheadEarlyOffer || !workAheadPredictPayer || !workAheadNonce || !workAheadUnchanged {
				t.Fatalf("on left a switch off: %s", SpeculationSummary())
			}
		})
	}
}

func TestConfigureSpeculationUnchangedOptOut(t *testing.T) {
	resetSpeculation(t)
	t.Setenv("MITHRIL_WORK_AHEAD_UNCHANGED", "0")
	if err := ConfigureSpeculation("on"); err != nil {
		t.Fatal(err)
	}
	if !workAheadEnabled || workAheadUnchanged || !workAheadEarlyOffer || !workAheadPredictPayer || !workAheadNonce || workAheadFailDepth != 4 {
		t.Fatalf("unchanged opt-out changed the other settings: %s", SpeculationSummary())
	}
}

func TestConfigureSpeculationOnLeavesResearchSwitchesAlone(t *testing.T) {
	resetSpeculation(t)
	value, race, cancel, invalidate := workAheadValueCheck, workAheadRace, workAheadCancel, workAheadEarlyInvalidate
	t.Cleanup(func() {
		workAheadValueCheck, workAheadRace, workAheadCancel, workAheadEarlyInvalidate = value, race, cancel, invalidate
	})
	workAheadValueCheck, workAheadRace, workAheadCancel, workAheadEarlyInvalidate = false, false, false, false
	if err := ConfigureSpeculation("on"); err != nil {
		t.Fatal(err)
	}
	if workAheadValueCheck || workAheadRace || workAheadCancel || workAheadEarlyInvalidate {
		t.Fatal("on enabled an unrelated research switch")
	}
}

func TestConfigureSpeculationUnknownMode(t *testing.T) {
	for _, mode := range []string{"safe", "full", "fast"} {
		t.Run(mode, func(t *testing.T) {
			resetSpeculation(t)
			if err := ConfigureSpeculation(mode); err == nil {
				t.Fatal("an unknown mode was accepted")
			}
			if workAheadEnabled || workAheadUnchanged {
				t.Fatal("an unknown mode enabled FRODO")
			}
		})
	}
}

// An environment variable that is set keeps the value it gave at start-up,
// including an explicit off.
func TestConfigureSpeculationEnvironmentWins(t *testing.T) {
	resetSpeculation(t)
	t.Setenv("MITHRIL_WORK_AHEAD_NONCE", "0")
	t.Setenv("MITHRIL_WORK_AHEAD_FAIL_DEPTH", "2")
	t.Setenv("MITHRIL_WORK_AHEAD_UNCHANGED", "0")
	workAheadFailDepth = 2 // what the package read from the environment
	if err := ConfigureSpeculation("on"); err != nil {
		t.Fatal(err)
	}
	if workAheadNonce || workAheadUnchanged || workAheadFailDepth != 2 {
		t.Fatalf("a preset overrode the environment: %s", SpeculationSummary())
	}
	if !workAheadEnabled || !workAheadEarlyOffer || !workAheadPredictPayer {
		t.Fatalf("the unset switches did not take the preset: %s", SpeculationSummary())
	}
}

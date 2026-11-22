package replay

import (
	"fmt"
	"os"
	"strings"

	"github.com/Overclock-Validator/mithril/pkg/sealevel"
)

// FRODO is selected with tuning.speculation in the config file or
// --speculation on the command line: off (the default) or on.
//
// On permits early execution behind up to four prerequisites predicted to
// fail, early chains, payer prediction, durable nonces and unchanged-account
// prediction. An unchanged prediction widens only what may run early; it
// does not relax the read-set checks at adoption.
//
// A preset only chooses what is executed early. Every early result is still
// checked against the accounts as they are at the transaction's turn before
// it is adopted, and a footer bank hash mismatch in a block with early
// executions turns speculation off and replays the block without it.
const (
	SpeculationOff = "off"
	SpeculationOn  = "on"
)

const speculationFailDepth = 4

// ConfigureSpeculation applies a preset. It must be called before replay
// starts. A MITHRIL_WORK_AHEAD_* environment variable that is set keeps the
// value it gave, so a single switch can still be overridden for an experiment;
// with the preset off, the environment variables alone decide, as before.
func ConfigureSpeculation(mode string) error {
	mode = strings.ToLower(strings.TrimSpace(mode))
	switch mode {
	case "", SpeculationOff:
		return nil
	case SpeculationOn:
	default:
		return fmt.Errorf("unknown speculation mode %q (want %s or %s)", mode, SpeculationOff, SpeculationOn)
	}
	unset := func(name string) bool {
		_, set := os.LookupEnv(name)
		return !set
	}
	if unset("MITHRIL_WORK_AHEAD") {
		workAheadEnabled = true
	}
	if unset("MITHRIL_WORK_AHEAD_FAIL_DEPTH") {
		workAheadFailDepth = speculationFailDepth
	}
	if unset("MITHRIL_WORK_AHEAD_EARLY_OFFER") {
		workAheadEarlyOffer = true
	}
	if unset("MITHRIL_WORK_AHEAD_PREDICT_PAYER") {
		workAheadPredictPayer = true
	}
	if unset("MITHRIL_WORK_AHEAD_NONCE") {
		workAheadNonce = true
	}
	if unset("MITHRIL_WORK_AHEAD_UNCHANGED") {
		workAheadUnchanged = true
	}
	// The package initializer made this choice from the environment alone.
	if workAheadUnchanged && workAheadValueCheck {
		sealevel.RecordWriteOriginals = true
	}
	return nil
}

// SpeculationSummary describes the switches in effect, for the startup log.
func SpeculationSummary() string {
	if !workAheadEnabled {
		return "off"
	}
	onOff := func(v bool) string {
		if v {
			return "on"
		}
		return "off"
	}
	return fmt.Sprintf("on, fail depth %d, early offer %s, payer prediction %s, nonce %s, unchanged prediction %s",
		workAheadFailDepth, onOff(workAheadEarlyOffer), onOff(workAheadPredictPayer), onOff(workAheadNonce), onOff(workAheadUnchanged))
}

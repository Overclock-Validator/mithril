package replay

import (
	"fmt"
	"os"
	"strings"

	"github.com/Overclock-Validator/mithril/pkg/sealevel"
)

// Named speculation presets, selected with tuning.speculation in the config
// file or --speculation on the command line.
//
//   - off: nothing executes early. The default.
//   - safe: the set every correctness and timing study so far was run on.
//     A transaction may execute early behind up to four prerequisites that
//     are predicted to fail (by their fee payer's last outcome), chains may
//     continue early, a predicted-failure prerequisite may share its fee
//     payer with the transaction, and durable-nonce transactions take part.
//   - full: safe, plus executing early behind a prerequisite that is
//     predicted to leave the shared accounts unchanged. Faster on the
//     measured corpora, with less evidence behind it; treat as experimental.
//
// A preset only chooses what is executed early. Every early result is still
// checked against the accounts as they are at the transaction's turn before
// it is adopted, and a footer bank hash mismatch in a block with early
// executions turns speculation off and replays the block without it.
const (
	SpeculationOff  = "off"
	SpeculationSafe = "safe"
	SpeculationFull = "full"
)

const speculationSafeFailDepth = 4

// ConfigureSpeculation applies a preset. It must be called before replay
// starts. A MITHRIL_WORK_AHEAD_* environment variable that is set keeps the
// value it gave, so a single switch can still be overridden for an experiment;
// with the preset off, the environment variables alone decide, as before.
func ConfigureSpeculation(mode string) error {
	mode = strings.ToLower(strings.TrimSpace(mode))
	switch mode {
	case "", SpeculationOff:
		return nil
	case SpeculationSafe, SpeculationFull:
	default:
		return fmt.Errorf("unknown speculation mode %q (want %s, %s or %s)", mode, SpeculationOff, SpeculationSafe, SpeculationFull)
	}
	unset := func(name string) bool {
		_, set := os.LookupEnv(name)
		return !set
	}
	if unset("MITHRIL_WORK_AHEAD") {
		workAheadEnabled = true
	}
	if unset("MITHRIL_WORK_AHEAD_FAIL_DEPTH") {
		workAheadFailDepth = speculationSafeFailDepth
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
	if mode == SpeculationFull && unset("MITHRIL_WORK_AHEAD_UNCHANGED") {
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

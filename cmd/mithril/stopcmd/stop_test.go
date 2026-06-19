package stopcmd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// stop is a thin CLI wrapper around pkg/procctl. These tests cover only the
// CLI surface (flag defaults, registration); signal/wait paths live in procctl.

// Cobra flag defaults match the contract (60s wait, status off).
func TestStopCmd_DefaultFlags(t *testing.T) {
	// Reset in case other tests changed them.
	timeoutFlag = 0
	statusOnlyFlag = false
	accountsDirFlag = ""

	tf := StopCmd.Flags().Lookup("timeout")
	if assert.NotNil(t, tf) {
		assert.Equal(t, "1m0s", tf.DefValue, "default --timeout should be 60s")
	}
	sf := StopCmd.Flags().Lookup("status")
	if assert.NotNil(t, sf) {
		assert.Equal(t, "false", sf.DefValue)
	}
	af := StopCmd.Flags().Lookup("accounts")
	if assert.NotNil(t, af) {
		assert.Equal(t, "", af.DefValue)
	}
}

// Smoke check: Use, Short, and RunE are populated.
func TestStopCmd_Registered(t *testing.T) {
	assert.Equal(t, "stop", StopCmd.Use)
	assert.NotEmpty(t, StopCmd.Short)
	assert.NotNil(t, StopCmd.RunE, "stop must have a RunE")
}

// --timeout parses durations via cobra's binding. Drives the actual flag so a
// type change (breaking values like "5m") fails here.
func TestTimeoutDurationParses(t *testing.T) {
	cases := map[string]time.Duration{
		"30s":   30 * time.Second,
		"5m":    5 * time.Minute,
		"1h":    time.Hour,
		"2h30m": 2*time.Hour + 30*time.Minute,
	}
	for in, want := range cases {
		t.Run(in, func(t *testing.T) {
			// GetDuration only works if the flag is bound as a duration.
			if err := StopCmd.Flags().Set("timeout", in); err != nil {
				t.Fatalf("Set(--timeout %q): %v", in, err)
			}
			got, err := StopCmd.Flags().GetDuration("timeout")
			assert.NoError(t, err, "timeout must be a duration flag")
			assert.Equal(t, want, got)
		})
	}
	// Restore default so ordering can't leak a mutated global.
	_ = StopCmd.Flags().Set("timeout", "60s")
}

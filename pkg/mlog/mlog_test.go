package mlog

import (
	"sync/atomic"
	"testing"
)

// TestDebugEnabledMatchesDebugf pins DebugEnabled against the level rule Debugf
// applies. Callers use it to skip building expensive arguments (pkg/sbpf's
// Translate skips a stack unwind and a symbol lookup), so if the two ever
// disagreed those call sites would silently stop logging at debug level.
// Debugf is written in terms of DebugEnabled so they cannot drift.
func TestDebugEnabledMatchesDebugf(t *testing.T) {
	cases := []struct {
		level   LogLevel
		verbose bool
		want    bool
	}{
		{LevelDebug, false, true},
		{LevelInfo, false, false},
		{LevelWarn, false, false},
		{LevelError, false, false},
		// Verbose overrides the level, which is what makes this more than a
		// comparison against a constant.
		{LevelInfo, true, true},
		{LevelError, true, true},
		{LevelDebug, true, true},
	}

	for _, tc := range cases {
		l := &logger{level: tc.level, enableVerbose: &atomic.Bool{}}
		l.enableVerbose.Store(tc.verbose)

		if got := l.DebugEnabled(); got != tc.want {
			t.Errorf("level=%v verbose=%v: DebugEnabled()=%v want %v",
				tc.level, tc.verbose, got, tc.want)
		}
	}
}

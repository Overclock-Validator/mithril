package node

import "testing"

// A backtest or an offline replay is a benchmark that shares its host with
// other nodes, so neither may stop them; a normal run still clears stale
// processes. Only the decision is tested: the kill itself is never executed.
func TestShouldKillExistingMithril(t *testing.T) {
	for _, tc := range []struct {
		name              string
		backtest, offline bool
		want              bool
	}{
		{"normal run", false, false, true},
		{"offline replay", false, true, false},
		{"backtest", true, false, false},
		{"backtest (always offline)", true, true, false},
	} {
		if got := shouldKillExistingMithril(tc.backtest, tc.offline); got != tc.want {
			t.Errorf("%s: shouldKillExistingMithril(backtest=%v, offline=%v) = %v, want %v", tc.name, tc.backtest, tc.offline, got, tc.want)
		}
	}
}

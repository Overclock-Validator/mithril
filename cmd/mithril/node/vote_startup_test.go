package node

import (
	"math"
	"strings"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/config"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestConfiguredWaitToVoteSlot(t *testing.T) {
	for _, tc := range []struct {
		name, toml, cli string
		want            uint64
		invalid         bool
	}{
		{name: "default"},
		{name: "toml", toml: "wait_to_vote_slot = 1234", want: 1234},
		{name: "cli wins", toml: "wait_to_vote_slot = 1234", cli: "5678", want: 5678},
		{name: "explicit zero wins", toml: "wait_to_vote_slot = 1234", cli: "0"},
		{name: "maximum CLI", cli: "18446744073709551615", want: math.MaxUint64},
		{name: "negative TOML", toml: "wait_to_vote_slot = -1", invalid: true},
		{name: "fractional TOML", toml: "wait_to_vote_slot = 1.5", invalid: true},
		{name: "malformed TOML value", toml: `wait_to_vote_slot = "oops"`, invalid: true},
		{name: "empty TOML value", toml: `wait_to_vote_slot = ""`, invalid: true},
		{name: "overflow TOML value", toml: `wait_to_vote_slot = "18446744073709551616"`, invalid: true},
		{name: "negative CLI", cli: "-1", invalid: true},
		{name: "overflow CLI", cli: "18446744073709551616", invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			viper.Reset()
			t.Cleanup(viper.Reset)
			config.ApplyDefaults(viper.GetViper())
			viper.SetConfigType("toml")
			require.NoError(t, viper.ReadConfig(strings.NewReader("[validator]\n"+tc.toml)))
			cmd := &cobra.Command{}
			cmd.Flags().Uint64("wait-to-vote-slot", 0, "")
			var err error
			if tc.cli != "" {
				err = cmd.Flags().Set("wait-to-vote-slot", tc.cli)
			}
			var got uint64
			if err == nil {
				got, err = configuredWaitToVoteSlot(cmd)
			}
			if tc.invalid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
	require.NotNil(t, Run.Flags().Lookup("wait-to-vote-slot"))
}

func TestEffectiveWaitToVoteSlot(t *testing.T) {
	for _, tc := range []struct{ startup, configured, want uint64 }{
		{100, 0, 108},
		{103, 0, 108},
		{103, 104, 108},
		{103, 108, 108},
		{103, 123, 123}, // Operator cutoff need not align with a leader window.
		{103, math.MaxUint64, math.MaxUint64},
		{math.MaxUint64 - 7, 0, math.MaxUint64},
		{math.MaxUint64, 0, math.MaxUint64},
	} {
		require.Equal(t, tc.want, effectiveWaitToVoteSlot(tc.startup, tc.configured), "%+v", tc)
	}
}

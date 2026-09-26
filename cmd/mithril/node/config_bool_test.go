package node

import (
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestResolveBoolOptionPrecedence(t *testing.T) {
	for _, tc := range []struct {
		name, toml, cli    string
		defaultValue, want bool
	}{
		{"omitted true default", "", "", true, true},
		{"omitted false default", "", "", false, false},
		{"TOML false", "enabled=false", "", true, false},
		{"TOML true", "enabled=true", "", false, true},
		{"CLI false beats TOML true", "enabled=true", "false", true, false},
		{"CLI true beats TOML false", "enabled=false", "true", false, true},
		{"CLI false without TOML", "", "false", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := viper.New()
			v.SetConfigType("toml")
			require.NoError(t, v.ReadConfig(strings.NewReader(tc.toml)))
			flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
			flags.Bool("enabled", tc.defaultValue, "")
			if tc.cli != "" {
				require.NoError(t, flags.Set("enabled", tc.cli))
			}
			require.Equal(t, tc.want, resolveBoolOption(flags.Lookup("enabled"), v.IsSet("enabled"), v.GetBool("enabled")))
		})
	}
	require.False(t, resolveBoolOption(nil, false, false))
	require.True(t, resolveBoolOption(nil, true, true))
}

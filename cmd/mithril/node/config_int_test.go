package node

import (
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestTimingSampleShiftPrecedence(t *testing.T) {
	for _, tc := range []struct {
		name, toml, cli string
		want            int
	}{
		{"omitted", "", "", 0}, {"toml sampled", "shift=3", "", 3}, {"toml exact", "shift=0", "", 0}, {"cli zero wins", "shift=3", "0", 0}, {"cli sampled wins", "shift=0", "3", 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := viper.New()
			v.SetConfigType("toml")
			require.NoError(t, v.ReadConfig(strings.NewReader(tc.toml)))
			f := pflag.NewFlagSet("test", pflag.ContinueOnError)
			f.Int("shift", 0, "")
			if tc.cli != "" {
				require.NoError(t, f.Set("shift", tc.cli))
			}
			require.Equal(t, tc.want, resolveIntOption(f.Lookup("shift"), v.IsSet("shift"), v.GetInt("shift")))
		})
	}
}

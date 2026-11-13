package config

import (
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestMetricsBindAddressConfig(t *testing.T) {
	for _, tc := range []struct{ name, body, want string }{
		{"default", "", ":9090"},
		{"loopback", "[metrics]\nbind_addr = '127.0.0.1:19090'\n", "127.0.0.1:19090"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := viper.New()
			ApplyDefaults(v)
			v.SetConfigType("toml")
			require.NoError(t, v.ReadConfig(strings.NewReader(tc.body)))
			require.Equal(t, tc.want, v.GetString("metrics.bind_addr"))
			var cfg Config
			require.NoError(t, v.Unmarshal(&cfg))
			require.Equal(t, tc.want, cfg.Metrics.BindAddr)
		})
	}
}

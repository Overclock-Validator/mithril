package config

import (
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestVMMemoryPoolingDefaultAndExplicitOverride(t *testing.T) {
	for _, tc := range []struct {
		name, toml string
		want       bool
	}{
		{"omitted", "[tuning]\ntxpar = 8\n", true},
		{"enabled", "[tuning]\nuse_pool = true\n", true},
		{"disabled", "[tuning]\nuse_pool = false\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := viper.New()
			ApplyDefaults(v)
			v.SetConfigType("toml")
			require.NoError(t, v.ReadConfig(strings.NewReader(tc.toml)))
			require.Equal(t, tc.want, v.GetBool("tuning.use_pool"))
		})
	}
}

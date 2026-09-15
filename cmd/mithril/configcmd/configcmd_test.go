package configcmd

import (
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestStarterConfigSignatureVerification(t *testing.T) {
	for _, validator := range []bool{false, true} {
		v := viper.New()
		v.SetConfigType("toml")
		require.NoError(t, v.ReadConfig(strings.NewReader(generateStarterConfig(validator))))
		require.Equal(t, "auto", v.GetString("sigverify.backend"))
		require.True(t, v.IsSet("sigverify.workers"))
		require.Zero(t, v.GetInt("sigverify.workers"))
		require.Equal(t, 8, v.GetInt("sigverify.batch_target"))
		require.True(t, v.IsSet("sigverify.disable_shred_overlap"))
		require.False(t, v.GetBool("sigverify.disable_shred_overlap"))
		require.False(t, v.IsSet("tuning.sigverify_backend"))
	}
}

package node

import (
	"fmt"
	"maps"
	"strings"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/config"
	blockenginepb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/blockengine"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// loadHarmonicTOML loads a [harmonic] section of the given keys; values are
// TOML literals.
func loadHarmonicTOML(t *testing.T, values map[string]string) (harmonicSettings, error) {
	t.Helper()
	var toml strings.Builder
	toml.WriteString("[harmonic]\n")
	for key, value := range values {
		fmt.Fprintf(&toml, "%s = %s\n", key, value)
	}
	viper.Reset()
	t.Cleanup(viper.Reset)
	config.ApplyDefaults(viper.GetViper())
	viper.SetConfigType("toml")
	require.NoError(t, viper.ReadConfig(strings.NewReader(toml.String())))
	return loadHarmonicSettings()
}

// enabledHarmonic is a minimal enabled [harmonic] section with overrides.
func enabledHarmonic(overrides map[string]string) map[string]string {
	values := map[string]string{
		"enabled":                      "true",
		"block_engine_url":             `"https://be.example.com"`,
		"tip_payment_program":          `"T1pyyaTNZsKv2WcRAB8oVnk93mLJw2XzjtVYqCsaHqt"`,
		"tip_distribution_program":     `"4R3gSG8BpU4t19KYj8CfnbtRpnT8gtk4dvTHxVRwc2r7"`,
		"merkle_root_upload_authority": `"8F4jGUmxF36vQ6yabnsxX6AQVXdKBhs8kGSUuRKSg8Xt"`,
	}
	maps.Copy(values, overrides)
	return values
}

func TestHarmonicIsOffByDefault(t *testing.T) {
	s, err := loadHarmonicTOML(t, nil)
	require.NoError(t, err)
	assert.False(t, s.enabled)

	// Nothing else is read while disabled.
	s, err = loadHarmonicTOML(t, map[string]string{"block_engine_url": `"not a url"`})
	require.NoError(t, err)
	assert.False(t, s.enabled)
}

func TestHarmonicSettings(t *testing.T) {
	s, err := loadHarmonicTOML(t, enabledHarmonic(nil))
	require.NoError(t, err)
	assert.True(t, s.enabled)
	assert.Equal(t, "be.example.com:443", s.blockEngine.Target)
	assert.True(t, s.blockEngine.TLS)
	assert.Empty(t, s.relayer.Target, "the relayer is optional")
	assert.False(t, s.insecureSkipVerify, "certificates are verified by default")
	assert.Equal(t, blockenginepb.SchedulingStrategy_SCHEDULING_STRATEGY_FBA, s.strategy)
	assert.Equal(t, "8F4jGUmxF36vQ6yabnsxX6AQVXdKBhs8kGSUuRKSg8Xt", s.tips.MerkleRootUploadAuthority.String())
	assert.Zero(t, s.tips.CommissionBps)

	s, err = loadHarmonicTOML(t, enabledHarmonic(map[string]string{
		"relayer_url":     `"http://relayer.example.com:11226"`,
		"tls_cert_verify": "false",
		"strategy":        `"MREV"`,
		"commission_bps":  "800",
	}))
	require.NoError(t, err)
	assert.Equal(t, "relayer.example.com:11226", s.relayer.Target)
	assert.True(t, s.insecureSkipVerify)
	assert.Equal(t, blockenginepb.SchedulingStrategy_SCHEDULING_STRATEGY_MREV, s.strategy)
	assert.Equal(t, uint16(800), s.tips.CommissionBps)

	for name, bad := range map[string]map[string]string{
		"no block engine url":  {"block_engine_url": `""`},
		"bad block engine url": {"block_engine_url": `"be.example.com"`},
		"bad relayer url":      {"relayer_url": `"ftp://relayer.example.com"`},
		"bad strategy":         {"strategy": `"fastest"`},
		"no tip program":       {"tip_payment_program": `""`},
		"bad authority":        {"merkle_root_upload_authority": `"not base58"`},
		"commission over 100%": {"commission_bps": "10001"},
		"negative commission":  {"commission_bps": "-1"},
	} {
		_, err := loadHarmonicTOML(t, enabledHarmonic(bad))
		assert.Errorf(t, err, "%s", name)
	}
}

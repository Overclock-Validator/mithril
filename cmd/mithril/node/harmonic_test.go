package node

import (
	"fmt"
	"maps"
	"strconv"
	"strings"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/config"
	blockenginepb "github.com/Overclock-Validator/mithril/pkg/harmonic/proto/blockengine"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// flagsThenConfig resolves options as runLive does: a set flag wins over the
// config file.
func flagsThenConfig(flags *pflag.FlagSet) optionGetters {
	changed := func(cliKey string) *pflag.Flag {
		if f := flags.Lookup(cliKey); f != nil && f.Changed {
			return f
		}
		return nil
	}
	return optionGetters{
		getString: func(cliKey, tomlKey string) string {
			if f := changed(cliKey); f != nil {
				return f.Value.String()
			}
			return config.GetString(tomlKey)
		},
		getInt64: func(cliKey, tomlKey string) int64 {
			if f := changed(cliKey); f != nil {
				v, _ := strconv.ParseInt(f.Value.String(), 10, 64)
				return v
			}
			return config.GetInt64(tomlKey)
		},
		getBool: func(cliKey, tomlKey string) bool {
			return resolveBoolOption(flags.Lookup(cliKey), config.IsSet(tomlKey), config.GetBool(tomlKey))
		},
	}
}

// loadHarmonic loads a [harmonic] section of the given keys, whose values are
// TOML literals, under the given flag values.
func loadHarmonic(t *testing.T, values map[string]string, flagValues map[string]string) (harmonicSettings, error) {
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
	flags := pflag.NewFlagSet("run", pflag.ContinueOnError)
	registerHarmonicFlags(flags)
	for name, value := range flagValues {
		require.NoError(t, flags.Set(name, value))
	}
	return loadHarmonicSettings(flagsThenConfig(flags))
}

func loadHarmonicTOML(t *testing.T, values map[string]string) (harmonicSettings, error) {
	t.Helper()
	return loadHarmonic(t, values, nil)
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

func TestHarmonicFlagsOverrideTheConfigFile(t *testing.T) {
	require.NotNil(t, Run.Flags().Lookup("harmonic-enabled"))

	// Every key from flags alone.
	s, err := loadHarmonic(t, nil, map[string]string{
		"harmonic-enabled":                      "true",
		"harmonic-block-engine-url":             "https://be.example.com",
		"harmonic-block-engine-tls-domain":      "be.internal",
		"harmonic-relayer-url":                  "http://relayer.example.com:11226",
		"harmonic-relayer-tls-domain":           "relayer.internal",
		"harmonic-tls-cert-verify":              "false",
		"harmonic-strategy":                     "fifo",
		"harmonic-tip-payment-program":          "T1pyyaTNZsKv2WcRAB8oVnk93mLJw2XzjtVYqCsaHqt",
		"harmonic-tip-distribution-program":     "4R3gSG8BpU4t19KYj8CfnbtRpnT8gtk4dvTHxVRwc2r7",
		"harmonic-merkle-root-upload-authority": "8F4jGUmxF36vQ6yabnsxX6AQVXdKBhs8kGSUuRKSg8Xt",
		"harmonic-commission-bps":               "500",
	})
	require.NoError(t, err)
	assert.True(t, s.enabled)
	assert.Equal(t, "be.example.com:443", s.blockEngine.Target)
	assert.Equal(t, "be.internal", s.blockEngine.ServerName)
	assert.Equal(t, "relayer.example.com:11226", s.relayer.Target)
	assert.Equal(t, "relayer.internal", s.relayer.ServerName)
	assert.True(t, s.insecureSkipVerify)
	assert.Equal(t, blockenginepb.SchedulingStrategy_SCHEDULING_STRATEGY_FIFO, s.strategy)
	assert.Equal(t, "T1pyyaTNZsKv2WcRAB8oVnk93mLJw2XzjtVYqCsaHqt", s.tips.TipPaymentProgram.String())
	assert.Equal(t, "4R3gSG8BpU4t19KYj8CfnbtRpnT8gtk4dvTHxVRwc2r7", s.tips.TipDistributionProgram.String())
	assert.Equal(t, "8F4jGUmxF36vQ6yabnsxX6AQVXdKBhs8kGSUuRKSg8Xt", s.tips.MerkleRootUploadAuthority.String())
	assert.Equal(t, uint16(500), s.tips.CommissionBps)

	// A flag beats the config file, including an explicit false.
	s, err = loadHarmonic(t, enabledHarmonic(map[string]string{"strategy": `"mrev"`}), map[string]string{"harmonic-strategy": "fba"})
	require.NoError(t, err)
	assert.Equal(t, blockenginepb.SchedulingStrategy_SCHEDULING_STRATEGY_FBA, s.strategy)
	s, err = loadHarmonic(t, enabledHarmonic(nil), map[string]string{"harmonic-enabled": "false"})
	require.NoError(t, err)
	assert.False(t, s.enabled)
}

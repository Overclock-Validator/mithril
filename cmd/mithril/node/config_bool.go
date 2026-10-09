package node

import "github.com/spf13/pflag"

// resolveBoolOption preserves explicit false at either precedence level.
// Defaults use DefValue, not a flag value potentially left by a previous run.
func resolveBoolOption(flag *pflag.Flag, configured bool, configuredValue bool) bool {
	if flag != nil && flag.Changed {
		return flag.Value.String() == "true"
	}
	if configured {
		return configuredValue
	}
	return flag != nil && flag.DefValue == "true"
}

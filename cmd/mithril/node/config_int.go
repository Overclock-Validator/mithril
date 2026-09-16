package node

import (
	"github.com/spf13/pflag"
	"strconv"
)

// resolveIntOption preserves explicit CLI zero, then TOML, then the flag default.
func resolveIntOption(flag *pflag.Flag, configured bool, value int) int {
	if flag != nil && flag.Changed {
		if n, err := strconv.Atoi(flag.Value.String()); err == nil {
			return n
		}
	}
	if configured {
		return value
	}
	if flag != nil {
		if n, err := strconv.Atoi(flag.DefValue); err == nil {
			return n
		}
	}
	return 0
}

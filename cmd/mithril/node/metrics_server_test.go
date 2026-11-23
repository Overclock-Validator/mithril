package node

import (
	"testing"

	"github.com/spf13/viper"
)

func TestMetricsServerDisableOption(t *testing.T) {
	flag := Run.Flags().Lookup("no-metrics-server")
	oldValue, oldChanged := flag.Value.String(), flag.Changed
	t.Cleanup(func() {
		viper.Reset()
		if err := flag.Value.Set(oldValue); err != nil {
			t.Error(err)
		}
		flag.Changed = oldChanged
	})
	viper.Reset()
	if flag.DefValue != "false" {
		t.Fatal("metrics listener default changed")
	}
	if err := flag.Value.Set("false"); err != nil {
		t.Fatal(err)
	}
	flag.Changed = false
	resolve := func() bool {
		return resolveBoolOption(flag, viper.IsSet("reporting.no_metrics_server"), viper.GetBool("reporting.no_metrics_server"))
	}
	if resolve() {
		t.Fatal("default disabled the listener")
	}
	viper.Set("reporting.no_metrics_server", true)
	if !resolve() {
		t.Fatal("configured opt-out ignored")
	}
	if err := Run.Flags().Set("no-metrics-server", "false"); err != nil {
		t.Fatal(err)
	}
	if resolve() {
		t.Fatal("explicit flag did not override configuration")
	}
}

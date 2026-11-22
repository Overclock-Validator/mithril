package blockstream

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/turbine"
)

func TestSocketMemoryMonitoringRequiresSeparateOptIn(t *testing.T) {
	var source BlockSource
	if _, ok := source.observeTurbineSocketMemory(nil); ok {
		t.Fatal("disabled source observed a socket")
	}
	source.EnableMonitoring()
	// A nil receiver would panic if the getter were called. Ordinary dashboard
	// monitoring must stop before reaching that getter.
	if _, ok := source.observeTurbineSocketMemory(nil); ok {
		t.Fatal("ordinary monitoring observed a socket")
	}
	source.EnableSocketMemoryMonitoring()
	if _, ok := source.observeTurbineSocketMemory(&turbine.UDPReceiver{}); ok {
		t.Fatal("unattached receiver became known")
	}
	defer func() {
		if recover() == nil {
			t.Fatal("explicit opt-in did not delegate to the receiver")
		}
	}()
	source.observeTurbineSocketMemory(nil)
}

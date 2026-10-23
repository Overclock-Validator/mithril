package replay

import (
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"time"
)

// Called only after the preceding bank's users of parameter memory have finished.
func resetSerializedParameterArena() {
	if SerializedParameterArena == nil {
		return
	}
	used := SerializedParameterArena.Used()
	start := time.Now()
	SerializedParameterArena.Reset()
	metrics.GlobalBlockReplay.SerializedParameterArenaReset.AddTimingSince(start)
	metrics.GlobalBlockReplay.SerializedParameterArenaResetBytes += used
}

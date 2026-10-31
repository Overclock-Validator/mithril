package node

import (
	"fmt"
	"strings"

	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/state"
)

func replayShutdownReason(result *replay.ReplayResult) string {
	if result.WasCancelled {
		return state.ShutdownReasonNormal
	}
	if result.Error == nil {
		return state.ShutdownReasonCompleted
	}
	message := result.Error.Error()
	switch {
	case strings.HasPrefix(message, "block fetch stalled"):
		return state.ShutdownReasonStall
	case strings.Contains(message, "leader schedule"):
		return state.ShutdownReasonLeaderSchedule
	default:
		// In particular, "rooting stalled" is not a block-fetch failure.
		return fmt.Sprintf("%s: %v", state.ShutdownReasonError, result.Error)
	}
}

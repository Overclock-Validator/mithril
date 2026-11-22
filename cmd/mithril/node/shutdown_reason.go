package node

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/state"
)

// Return replay failures to Cobra only after runLive has saved state and closed
// AccountsDB. Cancellation remains successful unless shutdown itself failed.
func replayCommandError(result *replay.ReplayResult) error {
	if result.WasCancelled && errors.Is(result.Error, context.Canceled) {
		return nil
	}
	return result.Error
}

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

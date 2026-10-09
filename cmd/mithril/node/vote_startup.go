package node

import (
	"fmt"
	"math"
	"strconv"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/config"
	"github.com/spf13/cobra"
)

func configuredWaitToVoteSlot(cmd *cobra.Command) (uint64, error) {
	if flag := cmd.Flags().Lookup("wait-to-vote-slot"); flag != nil && flag.Changed {
		return cmd.Flags().GetUint64("wait-to-vote-slot")
	}
	const key = "validator.wait_to_vote_slot"
	if !config.IsSet(key) {
		return 0, nil
	}
	// Unlike GetUint64, parsing explicitly must not turn an invalid operator
	// cutoff into zero and silently remove the requested voting restriction.
	slot, err := strconv.ParseUint(config.GetString(key), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be an unsigned 64-bit slot: %w", key, err)
	}
	return slot, nil
}

// The operator cutoff can postpone voting but cannot weaken the existing
// startup guard. Equality permits voting, subject to all other Votor checks.
func effectiveWaitToVoteSlot(startupWallSlot, configured uint64) uint64 {
	automatic := startupWallSlot - startupWallSlot%alpenglow.LeaderWindowSlots
	if automatic <= math.MaxUint64-2*alpenglow.LeaderWindowSlots {
		automatic += 2 * alpenglow.LeaderWindowSlots
	} else {
		automatic = math.MaxUint64
	}
	return max(automatic, configured)
}

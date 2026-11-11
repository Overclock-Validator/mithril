package blockstream

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHealthSlotRequiresFreshNetworkObservation(t *testing.T) {
	bs := &BlockSource{}
	_, fresh := bs.HealthSlot()
	require.False(t, fresh)
	bs.confirmedTip.Store(2000)
	bs.lastTipUpdate.Store(time.Now().Unix())
	_, fresh = bs.HealthSlot()
	require.False(t, fresh, "a locally advanced tip is not a network observation")
	bs.updateTipSnapshot(1234)
	slot, fresh := bs.HealthSlot()
	require.Equal(t, uint64(1234), slot)
	require.True(t, fresh)
	bs.lastObservedTipUpdate.Store(time.Now().Add(-31 * time.Second).Unix())
	_, fresh = bs.HealthSlot()
	require.False(t, fresh)
	bs.tipPollInterval = 20 * time.Second
	_, fresh = bs.HealthSlot()
	require.True(t, fresh, "a longer configured polling interval needs a longer freshness window")
	bs.lastObservedTipUpdate.Store(time.Now().Add(-41 * time.Second).Unix())
	_, fresh = bs.HealthSlot()
	require.False(t, fresh)
}

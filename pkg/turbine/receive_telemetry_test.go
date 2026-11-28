package turbine

import (
	"context"
	"testing"

	"github.com/Overclock-Validator/mithril/fixtures"
	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/stretchr/testify/require"
)

// The block carries the kernel receive time of the shred whose arrival made
// it full, not the first or an earlier one.
func TestBlockCarriesReceiveTimeOfCompletingShred(t *testing.T) {
	packets := fixtures.DataShreds(t, "mainnet", 102815960)
	receiver := NewUDPReceiver("127.0.0.1:0")
	receiver.assembler.verifyTransactions = func(context.Context, *block.Block) error { return nil }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := newSlotCompletionPool(receiver.assembler, &receiver.slotResetMu, receiver.startPendingBlock, 1, 2)
	receiver.completionPool = pool
	go receiver.consumeCompletionResults(ctx, pool.results)

	const base = int64(1_700_000_000_000_000_000)
	for idx, packet := range packets {
		require.True(t, receiver.processPacketAt(ctx, nil, packet, nil, false, base+int64(idx)))
	}
	blk := <-receiver.Blocks()
	require.Equal(t, base+int64(len(packets)-1), blk.ShredFullReceivedNanos)
	require.Positive(t, blk.ShredFullNanos)
}

func TestSocketDropsAddsGrowthOnce(t *testing.T) {
	start := KernelShredDrops()
	var d socketDrops
	d.observe(0)
	d.observe(3)
	d.observe(3)
	d.observe(5)
	require.Equal(t, uint64(5), KernelShredDrops()-start)

	var late socketDrops
	late.observe(2) // drops before our first read still count
	require.Equal(t, uint64(7), KernelShredDrops()-start)

	var wrap socketDrops
	wrap.observe(0)
	wrap.observe(^uint32(0))
	wrap.observe(1) // wrapped past zero: two more
	require.Equal(t, uint64(7)+uint64(^uint32(0))+2, KernelShredDrops()-start)
}

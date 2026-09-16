package rpcserver

import (
	"net/http/httptest"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	solanarpc "github.com/gagliardetto/solana-go/rpc"
	"github.com/stretchr/testify/require"
)

func TestGetEpochInfoSchedule(t *testing.T) {
	savedSlot, savedEpoch := global.Slot(), global.Epoch()
	t.Cleanup(func() {
		global.SetSlot(savedSlot)
		global.SetEpoch(savedEpoch)
	})

	for _, schedule := range []struct {
		name  string
		value sealevel.SysvarEpochSchedule
		cases []struct{ slot, epoch, index, length uint64 }
	}{
		{
			name:  "short epochs",
			value: sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32, LeaderScheduleSlotOffset: 32},
			cases: []struct{ slot, epoch, index, length uint64 }{
				{0, 0, 0, 32}, {31, 0, 31, 32}, {32, 1, 0, 32}, {79, 2, 15, 32},
			},
		},
		{
			name:  "normal epochs",
			value: sealevel.SysvarEpochSchedule{SlotsPerEpoch: 432000, LeaderScheduleSlotOffset: 432000},
			cases: []struct{ slot, epoch, index, length uint64 }{
				{431999, 0, 431999, 432000}, {432000, 1, 0, 432000},
			},
		},
		{
			name: "warmup epochs",
			value: sealevel.SysvarEpochSchedule{
				SlotsPerEpoch: 432000, LeaderScheduleSlotOffset: 432000,
				Warmup: true, FirstNormalEpoch: 14, FirstNormalSlot: 524256,
			},
			cases: []struct{ slot, epoch, index, length uint64 }{
				{0, 0, 0, 32}, {32, 1, 0, 64}, {95, 1, 63, 64},
				{524255, 13, 262143, 262144}, {524256, 14, 0, 432000},
			},
		},
	} {
		t.Run(schedule.name, func(t *testing.T) {
			server := NewRpcServer(nil, 0, &schedule.value, solana.Hash{1})
			require.NoError(t, server.listener.Close())
			endpoint := httptest.NewServer(server)
			t.Cleanup(endpoint.Close)
			client := solanarpc.New(endpoint.URL)
			for _, test := range schedule.cases {
				global.SetSlot(test.slot)
				global.SetEpoch(test.epoch)
				got, err := client.GetEpochInfo(t.Context(), solanarpc.CommitmentProcessed)
				require.NoError(t, err, "slot %d", test.slot)
				require.Equal(t, test.slot, got.AbsoluteSlot)
				require.Equal(t, test.epoch, got.Epoch, "slot %d", test.slot)
				require.Equal(t, test.index, got.SlotIndex, "slot %d", test.slot)
				require.Equal(t, test.length, got.SlotsInEpoch, "slot %d", test.slot)
				require.Equal(t, global.BlockHeight(), got.BlockHeight)
				require.NotNil(t, got.TransactionCount)
				require.Equal(t, global.TransactionCount(), *got.TransactionCount)

				// Replay publishes slot and epoch separately; keep the response tied to its slot.
				global.SetEpoch(test.epoch + 1)
				repeated, err := client.GetEpochInfo(t.Context(), solanarpc.CommitmentProcessed)
				require.NoError(t, err)
				require.Equal(t, got, repeated)
			}
		})
	}
}

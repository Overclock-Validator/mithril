package rpcserver

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	solanarpc "github.com/gagliardetto/solana-go/rpc"
	"github.com/stretchr/testify/require"
)

func TestGetEpochInfoSchedule(t *testing.T) {
	for _, schedule := range []struct {
		name  string
		value sealevel.SysvarEpochSchedule
		cases []struct{ slot, epoch, index, length uint64 }
	}{
		{
			name:  "short epochs",
			value: sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32, LeaderScheduleSlotOffset: 32},
			cases: []struct{ slot, epoch, index, length uint64 }{
				{1, 0, 1, 32}, {31, 0, 31, 32}, {32, 1, 0, 32}, {79, 2, 15, 32},
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
				{1, 0, 1, 32}, {32, 1, 0, 64}, {95, 1, 63, 64},
				{524255, 13, 262143, 262144}, {524256, 14, 0, 432000},
			},
		},
	} {
		t.Run(schedule.name, func(t *testing.T) {
			server, client, _ := newViewServer(t, schedule.value)
			for _, test := range schedule.cases {
				server.BankViews().PublishProcessed(testView(t, test.slot, test.slot/2))
				got, err := client.GetEpochInfo(t.Context(), solanarpc.CommitmentProcessed)
				require.NoError(t, err, "slot %d", test.slot)
				require.Equal(t, test.slot, got.AbsoluteSlot)
				require.Equal(t, test.epoch, got.Epoch, "slot %d", test.slot)
				require.Equal(t, test.index, got.SlotIndex, "slot %d", test.slot)
				require.Equal(t, test.length, got.SlotsInEpoch, "slot %d", test.slot)
				require.Equal(t, test.slot/2, got.BlockHeight)
				require.NotNil(t, got.TransactionCount)
				require.Equal(t, test.slot*10, *got.TransactionCount)
			}
		})
	}
}

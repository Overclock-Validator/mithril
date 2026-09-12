package fees

import "testing"

func TestLeaderReward(t *testing.T) {
	tests := []struct {
		name string
		info TxFeeInfo
		want uint64
	}{
		{
			name: "sig fee only",
			info: TxFeeInfo{ExecutionFee: 5000, PriorityFee: 0, TotalFee: 5000},
			want: 2500,
		},
		{
			name: "odd sig fee rounds down burn half",
			info: TxFeeInfo{ExecutionFee: 5001, PriorityFee: 0, TotalFee: 5001},
			want: 2501, // 5001 - 5001/2
		},
		{
			name: "priority plus unburned sig",
			info: TxFeeInfo{ExecutionFee: 5000, PriorityFee: 12_000, TotalFee: 17_000},
			want: 14_500,
		},
		{
			name: "nil",
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var info *TxFeeInfo
			if tt.name != "nil" {
				info = &tt.info
			}
			if got := LeaderReward(info); got != tt.want {
				t.Fatalf("LeaderReward() = %d, want %d", got, tt.want)
			}
		})
	}
}

package node

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
)

func TestActivityHistoryMeasurement(t *testing.T) {
	binding := dashboard.ReplayDetailBinding{RunID: "test", Attempt: 3, Rewinds: 2}
	rows := []replay.ChainActivitySummary{{Available: true, AnchorSlot: 20, LastSlot: 30, StartTime: 100, EndTime: 110,
		Blocks: 8, Transactions: sealevel.BankTransactionCounts{Total: 20, NonVote: 12, Failed: 4, NonVoteFailed: 3}}}
	h, err := activityHistoryMeasurement(rows, binding, replayDetailAt, replayDetailAt)
	if err != nil || len(h.Rows) != 1 || h.Binding != binding || h.Rows[0].Transactions != 20 ||
		h.Rows[0].NonVote != 12 || h.Rows[0].Failed != 4 || h.Rows[0].FailedNonVote != 3 || h.Rows[0].Blocks != 8 {
		t.Fatalf("activity history mapping: %+v, %v", h, err)
	}
	rows[0].Transactions.Total = 0
	if h.Rows[0].Transactions != 20 {
		t.Fatal("history aliases replay data")
	}
	if _, err := activityHistoryMeasurement(nil, binding, replayDetailAt, replayDetailAt); err == nil {
		t.Fatal("missing activity returned a zero-valued observation")
	}
}

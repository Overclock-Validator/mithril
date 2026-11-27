package replay

import (
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func BenchmarkProcessTransactionDefault(b *testing.B) {
	ctx, cleanup := newCommitTestSlotCtx()
	defer cleanup()
	tx, err := solana.TransactionFromBytes(txfixture.MustSignedTransferWire(0))
	require.NoError(b, err)
	var wg sync.WaitGroup
	b.ReportAllocs()
	for b.Loop() {
		fee, _, err := ProcessTransaction(ctx, &wg, tx, nil, nil, nil, false)
		if err != nil || fee == nil {
			b.Fatalf("execution: %v", err)
		}
	}
}

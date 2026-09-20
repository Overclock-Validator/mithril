package replay

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEpochVerifierBoundary(t *testing.T) {
	src := &fakeVerificationSource{blocks: map[uint64]*verifiedBlock{}}
	v := newTrailingVerifier(src, TrailingVerifierDefaults())
	for slot := uint64(501); slot <= 511; slot++ {
		d, block := digestFor(slot, vhash(byte(slot)), vsig(byte(slot)), 5000, false, []uint64{10}, []uint64{5}, nil)
		v.Record(d)
		src.blocks[slot] = block
	}
	v.verifyNext()
	require.Equal(t, 0, src.calls, "normal lag still applies before the boundary")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); v.Run(ctx) }()
	err := v.WaitThrough(ctx, 511)
	cancel()
	<-done
	require.NoError(t, err)
	t.Logf("executed=%d verified=%d oracle_requests=%d", v.executedTip, v.VerifiedWatermark(), src.calls)
	require.Equal(t, uint64(511), v.VerifiedWatermark(), "epoch transition requires its executed parent to be verifiable without executing the next epoch")
	require.Equal(t, uint64(511), v.executedTip, "verification must not invent execution progress")
	d, block := digestFor(512, vhash(1), vsig(1), 5000, false, []uint64{10}, []uint64{5}, nil)
	v.Record(d)
	src.blocks[512] = block
	v.verifyNext()
	require.Equal(t, uint64(511), v.VerifiedWatermark(), "later slots retain the normal lag")
}

func TestEpochVerifierWaitFailsClosed(t *testing.T) {
	for _, scenario := range []string{"divergence", "unavailable", "sibling", "unexecuted", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			d, block := digestFor(511, vhash(1), vsig(1), 5000, false, []uint64{10}, []uint64{5}, nil)
			src := &fakeVerificationSource{blocks: map[uint64]*verifiedBlock{511: block}}
			v := newTestVerifier(src)
			v.Record(d)
			target := uint64(511)
			switch scenario {
			case "divergence":
				block.Txs[0].Fee++
			case "unavailable":
				delete(src.blocks, 511)
			case "sibling":
				block.Blockhash = vhash(2)
			case "unexecuted":
				target++
			}
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			if scenario == "canceled" {
				cancel()
			}
			done := make(chan struct{})
			go func() { defer close(done); v.Run(ctx) }()
			err := v.WaitThrough(ctx, target)
			cancel()
			<-done
			require.Error(t, err)
			require.Equal(t, uint64(510), v.VerifiedWatermark())
			if scenario == "divergence" {
				var div *ReplayDivergence
				require.True(t, errors.As(err, &div))
			} else {
				require.Nil(t, v.Failure())
			}
		})
	}
}

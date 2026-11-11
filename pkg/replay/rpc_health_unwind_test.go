package replay

import (
	"encoding/base64"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/require"
)

type unwindHealthCheck struct {
	onInvalidate func()
}

func (h *unwindHealthCheck) InvalidateHealth() { h.onInvalidate() }

func TestTryInLoopUnwindInvalidatesHealthBeforeMutation(t *testing.T) {
	resetVoteStakeDirty()
	t.Cleanup(resetVoteStakeDirty)
	for _, missingSysvars := range []bool{false, true} {
		t.Run(map[bool]string{false: "retained parent", true: "checkpoint fallback"}[missingSysvars], func(t *testing.T) {
			tail := newUnrootedTail(&fakeDurable{}, &fakeCommitter{durable: accounts.NewMemAccounts()}, 512, 1, "")
			parent := &state.ResumeContext{
				Slot: 7, Bankhash: base58.Encode(make([]byte, 32)),
				AcctsLtHash: base64.StdEncoding.EncodeToString(make([]byte, 2048)),
			}
			account := testAccount(1, 71)
			tail.Add(7, []*accounts.Account{account}, testHashBytes(7))
			var sysvars *sealevel.BankSysvars
			if !missingSysvars {
				sysvars = testUnwindBankSysvars(t, 7, 700)
			}
			tail.SetContext(7, parent, sysvars)
			tail.Add(8, []*accounts.Account{testAccount(1, 81)}, testHashBytes(8))
			invalidated := false
			health := &unwindHealthCheck{onInvalidate: func() {
				before, err := tail.GetAccount(0, account.Key)
				require.NoError(t, err)
				require.Equal(t, uint64(81), before.Lamports, "health must be invalidated before discarded state is evicted")
				invalidated = true
			}}
			schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 432000}
			resumed, _, reason := tryInLoopUnwind(&CertifiedSwitch{Slot: 8}, tail, &state.MithrilState{}, schedule, 0, nil, health)
			after, err := tail.GetAccount(0, account.Key)
			require.NoError(t, err)
			require.Equal(t, uint64(71), after.Lamports, "the real unwind restores the retained parent's account")
			if missingSysvars {
				require.Nil(t, resumed)
				require.Equal(t, unwindFallbackMissingSysvars, reason)
			} else {
				require.NotNil(t, resumed)
				require.Equal(t, uint64(7), resumed.ParentSlot)
				require.Empty(t, reason)
			}
			require.True(t, invalidated, "discarding the RPC bank must invalidate health, including failed reconstruction")
		})
	}
}

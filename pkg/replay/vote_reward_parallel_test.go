package replay

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type voteUpdateFixture struct {
	keys    []solana.PublicKey
	data    map[solana.PublicKey][]byte
	update  voteAccountUpdate
	corrupt map[solana.PublicKey]bool
}

// A mainnet-sized block: about 660 rewarded validators, most of them also final
// signers, some final-only signers, and a few accounts whose data does not decode.
func newVoteUpdateFixture(t testing.TB, seed int64) *voteUpdateFixture {
	rng := rand.New(rand.NewSource(seed))
	start := make([]byte, sealevel.VoteStateV4Size)
	require.NoError(t, sealevel.WriteVersionedVoteStateInPlace(start, combinedVoteFixture(64)))
	f := &voteUpdateFixture{data: map[solana.PublicKey][]byte{}, corrupt: map[solana.PublicKey]bool{}}
	u := &f.update
	u.slot, u.rewardSlot, u.finalSlot = 108, 100, 99
	u.rewardSlotTimestampNs, u.finalSlotTimestampNs = 100e9, 99e9
	u.currentEpoch, u.migrationEpoch = 64, 60
	u.inflation = EpochInflationState{MaxPossibleValidatorReward: 1_000_000, SlotsPerEpoch: 100}
	u.rewardValidators = map[solana.PublicKey]struct{}{}
	u.finalSigners = map[solana.PublicKey]struct{}{}
	u.stakes = map[solana.PublicKey]uint64{}
	for i := 0; i < 703; i++ {
		var k solana.PublicKey
		binary.LittleEndian.PutUint64(k[:], uint64(i)+1)
		k[31] = 0x56
		f.keys = append(f.keys, k)
		f.data[k] = bytes.Clone(start)
		switch {
		case i < 433:
			u.rewardValidators[k] = struct{}{}
			u.finalSigners[k] = struct{}{}
		case i < 660:
			u.rewardValidators[k] = struct{}{}
		default:
			u.finalSigners[k] = struct{}{}
		}
		if i%250 == 7 {
			f.data[k] = []byte{99, 0, 0, 0}
			f.corrupt[k] = true
		}
		stake := uint64(rng.Intn(1_000_000)) + 1
		u.stakes[k] = stake
		u.totalStake += stake
	}
	u.leaderVote = f.keys[17]
	return f
}

func (f *voteUpdateFixture) slotCtx() *sealevel.SlotCtx {
	parent := accounts.NewMemAccounts()
	for _, k := range f.keys {
		k := k
		if err := parent.SetAccount((*[32]byte)(&k), &accounts.Account{Key: k, Lamports: 1_000_000, Data: bytes.Clone(f.data[k])}); err != nil {
			panic(err)
		}
	}
	return &sealevel.SlotCtx{
		Accounts:      accounts.NewOverlayAccountsWithLen(parent, len(f.keys)),
		AcctMapsMu:    &sync.Mutex{},
		ModifiedAccts: map[solana.PublicKey]bool{},
		WritableAccts: map[solana.PublicKey]bool{},
	}
}

type voteUpdateOutcome struct {
	leaderReward uint64
	updated      int
	leaderSeen   bool
	accounts     map[solana.PublicKey]string
	modified     map[solana.PublicKey]bool
	writable     map[solana.PublicKey]bool
}

func runVoteUpdates(t testing.TB, f *voteUpdateFixture, workers int) voteUpdateOutcome {
	old := maxVoteAccountWorkers
	maxVoteAccountWorkers = workers
	defer func() { maxVoteAccountWorkers = old }()
	slotCtx := f.slotCtx()
	leader, updated, seen, err := applyVoteAccountUpdates(slotCtx, &f.update)
	require.NoError(t, err)
	out := voteUpdateOutcome{leaderReward: leader, updated: updated, leaderSeen: seen,
		accounts: map[solana.PublicKey]string{}, modified: slotCtx.ModifiedAccts, writable: slotCtx.WritableAccts}
	for _, k := range f.keys {
		acct, err := slotCtx.GetAccount(k)
		require.NoError(t, err)
		out.accounts[k] = fmt.Sprintf("%d:%x", acct.Lamports, acct.Data)
	}
	return out
}

// The parallel update must produce exactly the serial result: the same bytes
// and lamports in every vote account, the same leader reward sum, and the same
// modified/writable sets and counts.
func TestApplyVoteAccountUpdatesParallelMatchesSerial(t *testing.T) {
	for seed := int64(1); seed <= 3; seed++ {
		f := newVoteUpdateFixture(t, seed)
		serial := runVoteUpdates(t, f, 1)
		require.Greater(t, serial.leaderReward, uint64(0))
		require.True(t, serial.leaderSeen)
		require.Equal(t, len(f.keys)-len(f.corrupt), serial.updated)
		for k := range f.corrupt {
			require.False(t, serial.modified[k], "corrupt account %s must be skipped", k)
		}
		for _, workers := range []int{2, 3, 8} {
			got := runVoteUpdates(t, f, workers)
			require.Equal(t, serial, got, "seed %d, %d workers", seed, workers)
		}
	}
}

func BenchmarkApplyVoteAccountUpdates(b *testing.B) {
	f := newVoteUpdateFixture(b, 1)
	for _, workers := range []int{1, 8} {
		b.Run(fmt.Sprintf("workers%d", workers), func(b *testing.B) {
			old := maxVoteAccountWorkers
			maxVoteAccountWorkers = workers
			defer func() { maxVoteAccountWorkers = old }()
			ctxs := make([]*sealevel.SlotCtx, b.N)
			for i := range ctxs {
				ctxs[i] = f.slotCtx()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, _, err := applyVoteAccountUpdates(ctxs[i], &f.update); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

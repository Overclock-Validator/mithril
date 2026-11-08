package replay

import (
	"bytes"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/bankhash"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// countingAccountSource serves parent values as fresh copies, as the unrooted
// tail does, and counts the reads.
type countingAccountSource struct {
	mu    sync.Mutex
	accts map[solana.PublicKey]*accounts.Account
	reads int
}

func (src *countingAccountSource) GetAccount(_ uint64, pubkey solana.PublicKey) (*accounts.Account, error) {
	src.mu.Lock()
	src.reads++
	src.mu.Unlock()
	acct, ok := src.accts[pubkey]
	if !ok {
		return nil, accountsdb.ErrNoAccount
	}
	return acct.Clone(), nil
}

func (src *countingAccountSource) readCount() int {
	src.mu.Lock()
	defer src.mu.Unlock()
	return src.reads
}

type voteRewardFixture struct {
	parents  map[solana.PublicKey]*accounts.Account
	live     map[solana.PublicKey]*accounts.Account
	reward   map[solana.PublicKey]struct{}
	final    map[solana.PublicKey]struct{}
	stakes   map[solana.PublicKey]uint64
	total    uint64
	leader   solana.PublicKey
	accounts []solana.PublicKey
}

func testVoteAccount(t *testing.T, rng *rand.Rand, key solana.PublicKey, version uint32) *accounts.Account {
	t.Helper()
	var voters sealevel.AuthorizedVoters
	voters.AuthorizedVoters.Set(1, key)
	state := sealevel.VoteState4{
		NodePubkey:                key,
		AuthorizedWithdrawer:      key,
		AuthorizedVoters:          voters,
		InflationRewardsCollector: key,
		BlockRevenueCollector:     key,
		BlockRevenueCommissionBps: 10000,
		LastTimestamp:             sealevel.BlockTimestamp{Slot: 40, Timestamp: int64(rng.Intn(3))},
	}
	if rng.Intn(2) == 0 {
		root := uint64(rng.Intn(120))
		state.RootSlot = &root
	}
	for i := range rng.Intn(4) {
		state.Votes.PushBack(sealevel.LandedVote{Lockout: sealevel.VoteLockout{Slot: uint64(50 + 10*i + rng.Intn(10)), ConfirmationCount: 1}})
	}
	epoch := uint64(2 + rng.Intn(3))
	for i := range rng.Intn(3) {
		credits := uint64(1000 * (i + 1))
		state.EpochCredits = append(state.EpochCredits, sealevel.EpochCredits{Epoch: epoch + uint64(i), Credits: credits, PrevCredits: credits / 2})
	}
	versioned := &sealevel.VoteStateVersions{Type: sealevel.VoteStateVersionV4, V4: state}
	marshaled, err := sealevel.MarshalVersionedVoteState(versioned)
	require.NoError(t, err)
	data := make([]byte, sealevel.VoteStateV4Size)
	copy(data, marshaled)
	if version != sealevel.VoteStateVersionV4 {
		data[0] = byte(version) // an unsupported vote state version is skipped
	}
	return &accounts.Account{Key: key, Lamports: 1_000_000_000 + uint64(rng.Intn(1000)), Owner: a.VoteProgramAddr, Data: data}
}

// newVoteRewardFixture builds n vote accounts covering every path: reward only,
// final only, both, written earlier in the slot and, with skips, absent from
// the source or undecodable.
func newVoteRewardFixture(t *testing.T, n int, seed int64, skips bool) *voteRewardFixture {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	fx := &voteRewardFixture{
		parents: map[solana.PublicKey]*accounts.Account{},
		live:    map[solana.PublicKey]*accounts.Account{},
		reward:  map[solana.PublicKey]struct{}{},
		final:   map[solana.PublicKey]struct{}{},
		stakes:  map[solana.PublicKey]uint64{},
	}
	for i := range n {
		var key solana.PublicKey
		key[0], key[1], key[2] = byte(i), byte(i>>8), 0xa7
		fx.accounts = append(fx.accounts, key)
		version := uint32(sealevel.VoteStateVersionV4)
		if skips && i%37 == 5 {
			version = sealevel.VoteStateVersionCurrent
		}
		if !skips || i%29 != 3 {
			fx.parents[key] = testVoteAccount(t, rng, key, version)
		}
		if i%11 == 7 && fx.parents[key] != nil {
			live := testVoteAccount(t, rng, key, version)
			live.Lamports = fx.parents[key].Lamports + 5
			fx.live[key] = live
		}
		switch i % 4 {
		case 0:
			fx.reward[key] = struct{}{}
		case 1:
			fx.final[key] = struct{}{}
		default:
			fx.reward[key] = struct{}{}
			fx.final[key] = struct{}{}
		}
		if _, ok := fx.reward[key]; ok && i%13 != 2 {
			stake := uint64(1 + rng.Intn(1_000_000))
			fx.stakes[key] = stake
			fx.total += stake
		}
	}
	fx.leader = fx.accounts[2]
	return fx
}

func (fx *voteRewardFixture) bank(t *testing.T) (*sealevel.SlotCtx, *countingAccountSource) {
	t.Helper()
	feats := features.NewFeaturesDefault()
	feats.EnableFeature(features.AccountsLtHash, 0)
	feats.EnableFeature(features.RemoveAccountsDeltaHash, 0)
	src := &countingAccountSource{accts: fx.parents}
	parent := accounts.NewMemAccounts()
	slotCtx := &sealevel.SlotCtx{
		// One shard, as a streamed bank's overlay has: its shell block carries
		// no transactions to size it from.
		Accounts:      accounts.NewOverlayAccounts(parent),
		ParentAccts:   parent,
		UnrootedRead:  src,
		Slot:          100,
		Features:      feats,
		AcctMapsMu:    &sync.Mutex{},
		ModifiedAccts: map[solana.PublicKey]bool{},
		WritableAccts: map[solana.PublicKey]bool{},
		AcctsLtHash:   &lthash.LtHash{},
	}
	// An account a transaction wrote earlier in the slot: its parent image was
	// loaded with the transaction, its live value sits in the overlay.
	for key, live := range fx.live {
		pk := [32]byte(key)
		require.NoError(t, parent.SetAccount(&pk, fx.parents[key].Clone()))
		require.NoError(t, slotCtx.SetAccount(key, live.Clone()))
		slotCtx.RecordModifiedAcct(key)
	}
	return slotCtx, src
}

func (fx *voteRewardFixture) update(slotCtx *sealevel.SlotCtx) *voteAccountUpdate {
	return &voteAccountUpdate{
		slot:                  slotCtx.Slot,
		rewardValidators:      fx.reward,
		finalSigners:          fx.final,
		rewardSlot:            92,
		rewardSlotTimestampNs: 7_000_000_000,
		finalSlot:             90,
		finalSlotTimestampNs:  6_000_000_000,
		currentEpoch:          5,
		migrationEpoch:        3,
		inflation:             EpochInflationState{MaxPossibleValidatorReward: 1_000_000_000_000, SlotsPerEpoch: 432_000, Epoch: 5},
		totalStake:            fx.total,
		stakes:                fx.stakes,
		leaderVote:            fx.leader,
	}
}

// voteRewardTally is what crediting a union reports.
type voteRewardTally struct {
	leaderReward  uint64
	updated       int
	leaderUpdated bool
}

func runVoteAccountUpdates(slotCtx *sealevel.SlotCtx, u *voteAccountUpdate) (voteRewardTally, error) {
	leaderReward, updated, leaderUpdated, err := applyVoteAccountUpdates(slotCtx, u)
	return voteRewardTally{leaderReward: leaderReward, updated: updated, leaderUpdated: leaderUpdated}, err
}

// referenceCreditVoteAccounts is the serial loop ApplyAlpenglowVoteRewards
// ran before crediting was split across workers, with separate reward and
// final-certificate decodes and its original loader (no parent image recorded).
func referenceCreditVoteAccounts(slotCtx *sealevel.SlotCtx, pass *voteAccountUpdate, union map[solana.PublicKey]struct{}) (voteRewardTally, error) {
	load := func(pubkey solana.PublicKey) (*accounts.Account, error) {
		if acct, err := slotCtx.GetAccount(pubkey); err == nil && acct != nil {
			return acct, nil
		}
		acct, err := slotCtx.GetAccountFromAccountsDb(pubkey)
		if err != nil {
			return nil, err
		}
		return acct.Clone(), nil
	}
	var tally voteRewardTally
	for votePubkey := range union {
		acct, err := load(votePubkey)
		if err != nil {
			continue
		}
		_, inReward := pass.rewardValidators[votePubkey]
		_, inFinal := pass.finalSigners[votePubkey]
		var applied bool
		if inReward {
			applied, err = applyVoteRewardToAccount(acct, pass.rewardSlot, pass.rewardSlotTimestampNs, pass.currentEpoch,
				pass.migrationEpoch, pass.inflation, pass.totalStake, pass.stakes[votePubkey], &tally.leaderReward)
			if err != nil {
				continue
			}
		}
		if inFinal {
			if err := applyFinalCertToAccount(acct, pass.finalSlot, pass.finalSlotTimestampNs); err != nil {
				continue
			}
			applied = true
		}
		if !applied {
			continue
		}
		if err := slotCtx.SetAccount(votePubkey, acct); err != nil {
			return voteRewardTally{}, err
		}
		slotCtx.RecordModifiedAcct(votePubkey)
		tally.updated++
		if votePubkey == pass.leaderVote {
			tally.leaderUpdated = true
		}
	}
	return tally, nil
}

func modifiedAccountsOf(t *testing.T, slotCtx *sealevel.SlotCtx) []*accounts.Account {
	t.Helper()
	var modified []*accounts.Account
	for key := range slotCtx.ModifiedAccts {
		acct, err := slotCtx.GetAccount(key)
		require.NoError(t, err)
		modified = append(modified, acct)
	}
	return modified
}

func requireSameAccount(t *testing.T, want, got *accounts.Account, key solana.PublicKey) {
	t.Helper()
	require.Equal(t, want.Lamports, got.Lamports, key)
	require.Equal(t, want.Owner, got.Owner, key)
	require.Equal(t, want.Executable, got.Executable, key)
	require.True(t, bytes.Equal(want.Data, got.Data), "data differs for %s", key)
}

func TestVoteAccountUpdatesMatchSerialReference(t *testing.T) {
	for _, tc := range []struct {
		name string
		n    int
	}{
		{"serial", 20},    // one worker
		{"parallel", 700}, // a mainnet-sized union across workers
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newVoteRewardFixture(t, tc.n, int64(tc.n), true)
			union := unionVotePubkeys(fx.reward, fx.final)

			refCtx, refSrc := fx.bank(t)
			want, err := referenceCreditVoteAccounts(refCtx, fx.update(refCtx), union)
			require.NoError(t, err)

			gotCtx, gotSrc := fx.bank(t)
			got, err := runVoteAccountUpdates(gotCtx, fx.update(gotCtx))
			require.NoError(t, err)

			require.Equal(t, want, got)
			require.NotZero(t, got.leaderReward)
			require.True(t, got.leaderUpdated)
			require.Less(t, got.updated, len(union)) // the skip paths ran
			require.Greater(t, got.updated, len(union)/2)
			require.Equal(t, refCtx.ModifiedAccts, gotCtx.ModifiedAccts)
			for _, key := range fx.accounts {
				wantAcct, wantErr := refCtx.GetAccount(key)
				gotAcct, gotErr := gotCtx.GetAccount(key)
				if wantErr != nil {
					// Never read into the bank before; now only its parent
					// image may be present, and only when the source has it.
					if gotErr == nil {
						requireSameAccount(t, fx.parents[key], gotAcct, key)
						require.False(t, gotCtx.ModifiedAccts[key])
					}
					continue
				}
				require.NoError(t, gotErr, key)
				requireSameAccount(t, wantAcct, gotAcct, key)
			}

			// Each parent value is read once, and ensureParentAccountsForModified
			// then finds every image it used to read and copy again.
			readsBeforeEnsure := gotSrc.readCount()
			require.Equal(t, refSrc.readCount(), readsBeforeEnsure)
			refModified := modifiedAccountsOf(t, refCtx)
			gotModified := modifiedAccountsOf(t, gotCtx)
			require.NoError(t, ensureParentAccountsForModified(refCtx, refModified))
			require.NoError(t, ensureParentAccountsForModified(gotCtx, gotModified))
			require.Equal(t, readsBeforeEnsure, gotSrc.readCount())
			require.Greater(t, refSrc.readCount(), readsBeforeEnsure)
			for key := range refCtx.ModifiedAccts {
				wantParent, err := refCtx.GetParentAccount(key)
				require.NoError(t, err)
				gotParent, err := gotCtx.GetParentAccount(key)
				require.NoError(t, err)
				requireSameAccount(t, wantParent, gotParent, key)
			}

			wantHash := bankhash.CalculateBankHash(refCtx, nil, refModified, [32]byte{1}, 3, [32]byte{2})
			gotHash := bankhash.CalculateBankHash(gotCtx, nil, gotModified, [32]byte{1}, 3, [32]byte{2})
			require.Equal(t, wantHash, gotHash)
		})
	}
}

// The parent image is the source's value, untouched by the copy the reward
// modifies.
func TestLoadAccountLiveOrParentRecordsUnmodifiedParentImage(t *testing.T) {
	fx := newVoteRewardFixture(t, 4, 1, true)
	slotCtx, _ := fx.bank(t)
	key := fx.accounts[0]
	acct, parent, err := loadAccountLiveOrParentForReplay(slotCtx, key)
	require.NoError(t, err)
	require.NotSame(t, acct, parent)
	acct.Data[len(acct.Data)-1] ^= 0xff
	acct.Lamports++
	requireSameAccount(t, fx.parents[key], parent, key)
	recordParentImage(slotCtx, key, parent)
	image, err := slotCtx.GetParentAccount(key)
	require.NoError(t, err)
	require.Same(t, parent, image)

	// An image already present (loaded with a transaction) is kept.
	other := parent.Clone()
	recordParentImage(slotCtx, key, other)
	image, err = slotCtx.GetParentAccount(key)
	require.NoError(t, err)
	require.Same(t, parent, image)

	// A live value has no parent read to record.
	for key := range fx.live {
		_, parent, err := loadAccountLiveOrParentForReplay(slotCtx, key)
		require.NoError(t, err)
		require.Nil(t, parent)
	}

	// Without the LtHash nothing reads parent images of untouched accounts.
	slotCtx, _ = fx.bank(t)
	slotCtx.Features.DisableFeature(features.AccountsLtHash)
	recordParentImage(slotCtx, key, parent)
	_, err = slotCtx.GetParentAccount(key)
	require.Error(t, err)
}

// A mainnet-sized union: crediting the accounts, then the parent images the
// LtHash needs (ensureParentAccountsForModified), timed separately.
func BenchmarkVoteRewardCredit664(b *testing.B) {
	t := &testing.T{}
	fx := newVoteRewardFixture(t, 664, 664, false)
	union := unionVotePubkeys(fx.reward, fx.final)
	// A live replay heap is tens of GB: allocations reuse swept spans instead
	// of faulting in fresh pages, which a small benchmark heap would measure.
	ballast := make([]byte, 1<<30)
	defer runtime.KeepAlive(ballast)
	for _, mode := range []string{"reference", "pass"} {
		b.Run(mode, func(b *testing.B) {
			var credit, parents time.Duration
			for b.Loop() {
				b.StopTimer()
				slotCtx, _ := fx.bank(t)
				b.StartTimer()
				start := time.Now()
				var err error
				if mode == "reference" {
					_, err = referenceCreditVoteAccounts(slotCtx, fx.update(slotCtx), union)
				} else {
					_, err = runVoteAccountUpdates(slotCtx, fx.update(slotCtx))
				}
				if err != nil {
					b.Fatal(err)
				}
				credit += time.Since(start)
				var modified []*accounts.Account
				for key := range slotCtx.ModifiedAccts {
					acct, _ := slotCtx.GetAccountShared(key)
					modified = append(modified, acct)
				}
				start = time.Now()
				if err := ensureParentAccountsForModified(slotCtx, modified); err != nil {
					b.Fatal(err)
				}
				parents += time.Since(start)
			}
			b.ReportMetric(float64(credit.Microseconds())/float64(b.N), "credit-us/op")
			b.ReportMetric(float64(parents.Microseconds())/float64(b.N), "parents-us/op")
		})
	}
}

// nilParentReader reports every account as absent without an error.
type nilParentReader struct{}

func (nilParentReader) GetAccount(uint64, solana.PublicKey) (*accounts.Account, error) {
	return nil, nil
}

// A source that reports absence as (nil, nil) must yield a missing-account
// error, which the reward pass skips, not a nil dereference.
func TestLoadAccountLiveOrParentNilParentIsMissing(t *testing.T) {
	slotCtx := &sealevel.SlotCtx{Accounts: accounts.NewMemAccounts(), UnrootedRead: nilParentReader{}}
	acct, parent, err := loadAccountLiveOrParentForReplay(slotCtx, solana.PublicKey{0xfe})
	require.ErrorIs(t, err, accountsdb.ErrNoAccount)
	require.Nil(t, acct)
	require.Nil(t, parent)
}

package replay

import (
	"bytes"
	"math/big"
	"sync"
	"testing"
	"time"

	bls12381 "github.com/Overclock-Validator/gnark-crypto/ecc/bls12-381"
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/bankhash"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/rewardcerts"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type leaderRewardTestBuilder struct {
	certs rewardcerts.RewardCertificates
}

func (b *leaderRewardTestBuilder) BuildForLeaderSlot(uint64) rewardcerts.RewardCertificates {
	return b.certs
}

func setupLeaderRewardPreparation(t testing.TB, epoch uint64) (*sealevel.SysvarEpochSchedule, *voteRewardVerifierMaterial) {
	t.Helper()
	clearVoteRewardVerifierCacheForTest()
	installTestVoteRewardValidator(epoch, 3)
	t.Cleanup(func() {
		global.ClearEpochStakes(epoch)
		clearVoteRewardVerifierCacheForTest()
	})
	material, err := loadVoteRewardVerifierMaterial(epoch, 42, nil)
	require.NoError(t, err)
	return &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32}, material
}

func testLeaderNotarCertificate(t testing.TB, rewardSlot uint64) []byte {
	t.Helper()
	blockID := solana.Hash{7}
	payload, err := alpenglow.EncodeVotePayloadToSign(alpenglow.NewNotarizationVote(rewardSlot, blockID), 42)
	require.NoError(t, err)
	point, err := bls12381.HashToG2(payload, []byte(alpenglow.DefaultHashToPointDST))
	require.NoError(t, err)
	var signature bls12381.G2Affine
	signature.ScalarMultiplication(&point, big.NewInt(3))
	bitmap, err := alpenglow.EncodeSignerStoreBitmap(alpenglow.SignerBitmap{
		Encoding: alpenglow.SignerBitmapBase2, Length: 1, Base: []bool{true},
	})
	require.NoError(t, err)
	raw, err := rewardcerts.EncodeNotarRewardCertificate(rewardcerts.NotarRewardCertificate{
		Slot: rewardSlot, BlockID: blockID, Signature: signature.Bytes(), Bitmap: bitmap,
	})
	require.NoError(t, err)
	return raw
}

func TestLeaderRewardPipelinePreparesNotarValidationBeforeFooter(t *testing.T) {
	const epoch = uint64(9_000_000_414)
	schedule, material := setupLeaderRewardPreparation(t, epoch)
	rewardSlot := epoch*32 + 8
	slot := rewardSlot + rewardcerts.SlotsForReward
	notar := testLeaderNotarCertificate(t, rewardSlot)
	p := NewLeaderRewardCertPreparer(&leaderRewardTestBuilder{rewardcerts.RewardCertificates{Notar: notar}}, schedule, 42)
	p.cache = &leaderRewardValidationCache{}
	pipeline := rewardcerts.NewPipeline(p)
	defer pipeline.Close()
	pipeline.PrepareForLeaderSlot(slot)
	// Observe background publication without prematurely consuming/retiring an
	// unfinished Pipeline result. Full validation is part of the builder call.
	require.Eventually(t, func() bool {
		p.cache.mu.Lock()
		defer p.cache.mu.Unlock()
		return len(p.cache.entries) == 1
	}, time.Second, time.Millisecond)
	// No footer has run. The ordinary pipeline worker has already performed the
	// full BLS validation, and its immutable result is available independently.
	got, hit := p.cache.take(slot, nil, notar, material)
	require.True(t, hit)
	require.Equal(t, rewardSlot, got.RewardSlot)
	require.Len(t, got.Validators, 1)
}

func BenchmarkLeaderRewardValidation(b *testing.B) {
	const epoch = uint64(9_000_000_415)
	_, material := setupLeaderRewardPreparation(b, epoch)
	rewardSlot := epoch*32 + 8
	slot := rewardSlot + rewardcerts.SlotsForReward
	notar := testLeaderNotarCertificate(b, rewardSlot)
	validated, _, err := rewardcerts.ValidateRewardCertificatesWithVerifier(slot, nil, notar, epoch, material.verifier, false)
	require.NoError(b, err)
	b.Run("synchronous_full_verification", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _, err := rewardcerts.ValidateRewardCertificatesWithVerifier(slot, nil, notar, epoch, material.verifier, false)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("prepared_publish_and_take", func(b *testing.B) {
		cache := leaderRewardValidationCache{entries: make(map[uint64]preparedLeaderRewardValidation)}
		entry := preparedLeaderRewardValidation{notar: bytes.Clone(notar), material: material, validated: cloneValidatedRewardCert(validated)}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			// Include map publication cost; cryptography and ownership copies were
			// performed by the background worker. The consumer never mutates this
			// benchmark's immutable result, allowing it to be reused each iteration.
			cache.mu.Lock()
			cache.entries[slot] = entry
			cache.mu.Unlock()
			if _, hit := cache.take(slot, nil, notar, material); !hit {
				b.Fatal("prepared result missing")
			}
		}
	})
}

func TestLeaderRewardPreparationRequiresExactBytesSlotAndDomain(t *testing.T) {
	const epoch = uint64(9_000_000_411)
	schedule, material := setupLeaderRewardPreparation(t, epoch)
	// Preparation must use the reward epoch even when the leader is in the
	// next epoch and only the previous epoch's stake material is installed.
	rewardSlot := epoch*32 + 31
	slot := rewardSlot + rewardcerts.SlotsForReward
	raw := testPreflightSkipCertificate(t, rewardSlot, 42)
	builder := &leaderRewardTestBuilder{rewardcerts.RewardCertificates{Skip: raw}}
	p := NewLeaderRewardCertPreparer(builder, schedule, 42)
	p.cache = &leaderRewardValidationCache{}
	certs := p.BuildForLeaderSlot(slot)
	want, _, err := rewardcerts.ValidateRewardCertificatesWithVerifier(slot, raw, nil, epoch, material.verifier, false)
	require.NoError(t, err)
	bad := bytes.Clone(certs.Skip)
	bad[8] ^= 1
	_, hit := p.cache.take(slot, bad, nil, material)
	require.False(t, hit)
	_, hit = p.cache.take(slot+1, certs.Skip, nil, material)
	require.False(t, hit)
	_, hit = p.cache.take(slot, certs.Skip, []byte{1}, material)
	require.False(t, hit)
	other, err := loadVoteRewardVerifierMaterial(epoch, 43, nil)
	require.NoError(t, err)
	_, hit = p.cache.take(slot, certs.Skip, nil, other)
	require.False(t, hit)
	// Source buffer mutation cannot change either returned bytes or the cache.
	raw[8] ^= 1
	require.NotEqual(t, raw, certs.Skip)
	got, hit := p.cache.take(slot, certs.Skip, nil, material)
	require.True(t, hit)
	require.Equal(t, want, got)
	_, hit = p.cache.take(slot, certs.Skip, nil, material)
	require.False(t, hit, "successful preparation transfers ownership once")

	builder.certs = certs
	p.BuildForLeaderSlot(slot)
	installTestVoteRewardValidator(epoch, 3)
	reloaded, err := loadVoteRewardVerifierMaterial(epoch, 42, nil)
	require.NoError(t, err)
	require.NotSame(t, material, reloaded)
	_, hit = p.cache.take(slot, certs.Skip, nil, reloaded)
	require.False(t, hit, "identical keys with a new stake generation must revalidate")
}

func TestLeaderRewardPreparationErrorsPreserveBytesAndReplayCannotEvict(t *testing.T) {
	const epoch = uint64(9_000_000_412)
	schedule, material := setupLeaderRewardPreparation(t, epoch)
	rewardSlot := epoch*32 + 8
	slot := rewardSlot + rewardcerts.SlotsForReward
	raw := testPreflightSkipCertificate(t, rewardSlot, 42)
	builder := &leaderRewardTestBuilder{rewardcerts.RewardCertificates{Skip: raw}}
	p := NewLeaderRewardCertPreparer(builder, schedule, 42)
	p.cache = &leaderRewardValidationCache{}
	p.BuildForLeaderSlot(slot)
	// Replay's latest-certificate cache advances independently of the leader.
	next := testPreflightSkipCertificate(t, rewardSlot+1, 42)
	_, _, err := material.validateRewardCertificates(slot+1, next, nil, false)
	require.NoError(t, err)
	_, hit := p.cache.take(slot, raw, nil, material)
	require.True(t, hit)

	for _, invalid := range []rewardcerts.RewardCertificates{
		{Skip: []byte{1}},
		{Skip: testPreflightSkipCertificate(t, rewardSlot, 43)},
		{Skip: testPreflightSkipCertificate(t, rewardSlot+1, 42)},
		{Skip: raw, Notar: []byte{1}},
	} {
		builder.certs = invalid
		got := p.BuildForLeaderSlot(slot)
		require.Equal(t, invalid, got, "preparation errors must not silently omit a certificate")
		_, hit = p.cache.take(slot, got.Skip, got.Notar, material)
		require.False(t, hit)
		_, _, err = material.validateRewardCertificates(slot, got.Skip, got.Notar, false)
		require.Error(t, err, "ordinary finalization still rejects invalid bytes")
	}
}

func TestLeaderRewardPreparationBoundedOwnedAndConcurrent(t *testing.T) {
	var cache leaderRewardValidationCache
	material := &voteRewardVerifierMaterial{}
	raw := []byte{1, 2, 3}
	validated := &rewardcerts.ValidatedRewardCert{RewardSlot: 1, Validators: map[solana.PublicKey]struct{}{{1}: {}}}
	cache.put(100, raw, nil, material, validated)
	raw[0] = 9
	delete(validated.Validators, solana.PublicKey{1})
	got, hit := cache.take(100, []byte{1, 2, 3}, nil, material)
	require.True(t, hit)
	require.Len(t, got.Validators, 1)
	for slot := uint64(101); slot < 10_000; slot++ {
		cache.put(slot, raw, nil, material, validated)
		require.LessOrEqual(t, len(cache.entries), leaderRewardPreparationSlots)
	}
	cache.put(1, raw, nil, material, validated)
	require.NotContains(t, cache.entries, uint64(1), "late background results cannot revive retired entries")
	cache.advance(20_000)
	require.Empty(t, cache.entries, "empty/invalid later preparations still expire abandoned results")

	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				slot := uint64(20_001 + worker*200 + i)
				cache.put(slot, raw, nil, material, validated)
				result, ok := cache.take(slot, raw, nil, material)
				if ok {
					result.Validators[solana.PublicKey{2}] = struct{}{}
				}
			}
		}(worker)
	}
	wg.Wait()
	require.Empty(t, validated.Validators)
}

func TestPreparedLeaderVoteRewardsMatchSynchronousStateAndHash(t *testing.T) {
	const epoch = uint64(9_000_000_413)
	schedule, material := setupLeaderRewardPreparation(t, epoch)
	rewardSlot := epoch*32 + 8
	slot := rewardSlot + rewardcerts.SlotsForReward
	raw := testPreflightSkipCertificate(t, rewardSlot, 42)
	var vote, node solana.PublicKey
	for key, account := range material.snapshot.VoteAccounts {
		vote, node = key, account.NodePubkey
	}
	active := features.NewFeaturesDefault()
	active.EnableFeature(features.Alpenglow, 0)
	active.EnableFeature(features.AccountsLtHash, 0)
	active.EnableFeature(features.RemoveAccountsDeltaHash, 0)
	voteState := combinedVoteFixture(1)
	voteState.V4.NodePubkey = node
	voteData := make([]byte, sealevel.VoteStateV4Size)
	require.NoError(t, sealevel.WriteVersionedVoteStateInPlace(voteData, voteState))
	parent := &accounts.Account{Key: vote, Data: voteData, Lamports: 1_000_000}
	newBank := func(replay bool) *sealevel.SlotCtx {
		ctx := &sealevel.SlotCtx{Slot: slot, Epoch: epoch, ParentSlot: slot - 1,
			Features: active, Replay: replay, Accounts: accounts.NewMemAccounts(), ParentAccts: accounts.NewMemAccounts(),
			ModifiedAccts: make(map[solana.PublicKey]bool), WritableAccts: make(map[solana.PublicKey]bool),
			AcctMapsMu: &sync.Mutex{}, AcctsLtHash: &lthash.LtHash{},
		}
		// Same-slot changes must survive reward application in both paths.
		live := parent.Clone()
		live.Lamports += 123
		require.NoError(t, ctx.SetAccount(vote, live))
		require.NoError(t, ctx.ParentAccts.SetAccountWithoutLock(vote, parent.Clone()))
		require.NoError(t, ctx.SetAccount(VoteRewardAccountAddr(), &accounts.Account{Key: VoteRewardAccountAddr(),
			Data: encodeEpochInflationAccountState(EpochInflationAccountState{Current: EpochInflationState{
				Epoch: epoch, SlotsPerEpoch: 32, MaxPossibleValidatorReward: 1_000_000,
			}}),
		}))
		return ctx
	}
	blk := &b.Block{Slot: slot, Epoch: epoch, Leader: node, Features: active, FooterProducerTimeNanos: 100_000_000_000, HasAlpenglowFooter: true}
	builder := &leaderRewardTestBuilder{rewardcerts.RewardCertificates{Skip: raw}}
	p := NewLeaderRewardCertPreparer(builder, schedule, 42)
	p.BuildForLeaderSlot(slot)
	want := newBank(true)
	var ignored LeaderRewardValidationStatus
	require.NoError(t, ApplyAlpenglowVoteRewards(want, blk, schedule, raw, nil, nil, 42, &ignored))
	require.False(t, ignored.Prepared, "network replay must not consume leader preparation")
	got := newBank(false)
	var status LeaderRewardValidationStatus
	require.NoError(t, ApplyAlpenglowVoteRewards(got, blk, schedule, raw, nil, nil, 42, &status))
	require.True(t, status.Prepared)
	require.Positive(t, status.LookupDuration)
	wantVote, err := want.GetAccount(vote)
	require.NoError(t, err)
	gotVote, err := got.GetAccount(vote)
	require.NoError(t, err)
	require.Equal(t, wantVote, gotVote)
	require.NotEqual(t, parent.Data, gotVote.Data, "certificate actually updates vote rewards")
	require.Equal(t, parent.Lamports+123, gotVote.Lamports)
	require.Equal(t, want.ModifiedAccts, got.ModifiedAccts)
	wantHash := bankhash.CalculateBankHash(want, nil, []*accounts.Account{wantVote}, [32]byte{1}, 0, [32]byte{2})
	gotHash := bankhash.CalculateBankHash(got, nil, []*accounts.Account{gotVote}, [32]byte{1}, 0, [32]byte{2})
	require.Equal(t, wantHash, gotHash)

	// A miss is equivalent, and reports the fallback instead of pretending that
	// preparation succeeded. Successful entries are consumed once.
	fallback := newBank(false)
	status = LeaderRewardValidationStatus{}
	require.NoError(t, ApplyAlpenglowVoteRewards(fallback, blk, schedule, raw, nil, nil, 42, &status))
	require.False(t, status.Prepared)
	fallbackVote, err := fallback.GetAccount(vote)
	require.NoError(t, err)
	require.Equal(t, wantVote, fallbackVote)
}

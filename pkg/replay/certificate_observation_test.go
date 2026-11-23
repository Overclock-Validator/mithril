package replay

import (
	"encoding/binary"
	"math/big"
	"math/rand"
	"sync"
	"testing"
	"time"

	bls12381 "github.com/Overclock-Validator/gnark-crypto/ecc/bls12-381"
	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestCertificateObservationEmptyFooterUsesOnlyExactWarmedMaterial(t *testing.T) {
	const epoch, version = uint64(9_000_000_017), uint16(42)
	clearVoteRewardVerifierCacheForTest()
	t.Cleanup(func() { global.ClearEpochStakes(epoch); clearVoteRewardVerifierCacheForTest() })
	installTestVoteRewardValidator(epoch, 3)
	subject := solana.PublicKey{byte(epoch & 255)}
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 16}
	feats := features.NewFeaturesDefault()
	feats.EnableFeature(features.Alpenglow, 0)
	ctx := &sealevel.SlotCtx{Features: feats}
	block := &b.Block{Slot: (epoch + 1) * 16, Epoch: epoch + 1, HasAlpenglowFooter: true}
	apply := func() b.CertificateObservation {
		block.EnableCertificateObservation(subject)
		require.NoError(t, ApplyAlpenglowVoteRewards(ctx, block, schedule, nil, nil, nil, version))
		got, ok := block.CertificateObservation()
		require.True(t, ok)
		require.Equal(t, epoch, got.Reward.TargetEpoch, "target epoch, not carrier epoch")
		return got
	}
	cold := apply()
	require.False(t, cold.Reward.AdmissionKnown)
	require.Empty(t, cachedVoteRewardVerifiers.entries, "observing an empty footer must not build a verifier")
	material, err := loadVoteRewardVerifierMaterial(epoch, version, nil)
	require.NoError(t, err)
	warm := apply()
	require.True(t, warm.Reward.AdmissionKnown)
	require.True(t, warm.Reward.Eligible)
	require.False(t, warm.Reward.Present)
	require.False(t, warm.Reward.Included)
	require.Equal(t, material.snapshot.Generation, warm.Reward.MaterialGeneration)
	require.Equal(t, byte(3), warm.Reward.EpochIdentity[1])
	block.EnableCertificateObservation(solana.PublicKey{0xff, 1})
	require.NoError(t, ApplyAlpenglowVoteRewards(ctx, block, schedule, nil, nil, nil, version))
	ineligible, _ := block.CertificateObservation()
	require.True(t, ineligible.Reward.AdmissionKnown)
	require.False(t, ineligible.Reward.Eligible)
	installTestVoteRewardValidator(epoch, 3) // Identical data, new publication.
	require.False(t, apply().Reward.AdmissionKnown, "do not borrow a stale generation")
	require.Len(t, cachedVoteRewardVerifiers.entries, 1)
	block.HasAlpenglowFooter = false
	block.EnableCertificateObservation(subject)
	require.NoError(t, ApplyAlpenglowVoteRewards(ctx, block, schedule, nil, nil, nil, version))
	_, ok := block.CertificateObservation()
	require.False(t, ok, "absence of a complete footer is not exclusion")
	require.Zero(t, testing.AllocsPerRun(20, func() {
		if err := ApplyAlpenglowVoteRewards(nil, nil, nil, nil, nil, nil, version); err != nil {
			panic(err)
		}
	}))
}

func TestCertificateObservationApplyOnOffParityAndPublication(t *testing.T) {
	const epoch, version = uint64(9_000_000_019), uint16(42)
	clearVoteRewardVerifierCacheForTest()
	t.Cleanup(func() { global.ClearEpochStakes(epoch); clearVoteRewardVerifierCacheForTest() })
	installTestVoteRewardValidator(epoch, 3)
	subject := solana.PublicKey{byte(epoch & 255)}
	target := epoch*16 + 15
	blockID := solana.Hash{9}
	raw := observationFastFinalCertificate(t, target, blockID, version)
	fixture := newVoteRewardFixture(t, 4, 7, false)
	fixture.parents[subject] = testVoteAccount(t, rand.New(rand.NewSource(9)), subject, sealevel.VoteStateVersionV4)
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 16}
	var results [2]*sealevel.SlotCtx
	var reads [2]int
	var captured b.CertificateObservation
	for i := range results {
		ctx, source := fixture.bank(t)
		ctx.Features.EnableFeature(features.Alpenglow, 0)
		block := &b.Block{Slot: target + 4, Epoch: epoch + 1, Leader: solana.PublicKey{200}, HasAlpenglowFooter: true, FooterProducerTimeNanos: 1_000_000_000_000_000_000}
		if i == 1 {
			block.EnableCertificateObservation(subject)
		}
		require.NoError(t, ApplyAlpenglowVoteRewards(ctx, block, schedule, nil, nil, raw, version))
		results[i], reads[i] = ctx, source.readCount()
		got, ok := block.CertificateObservation()
		require.Equal(t, i == 1, ok)
		if ok {
			captured = got
		}
	}
	offAccount, err := results[0].GetAccount(subject)
	require.NoError(t, err)
	onAccount, err := results[1].GetAccount(subject)
	require.NoError(t, err)
	require.Equal(t, offAccount, onAccount, "monitoring must not change account application")
	require.Equal(t, results[0].ModifiedAccts, results[1].ModifiedAccts)
	require.Equal(t, results[0].AcctsLtHash, results[1].AcctsLtHash)
	require.Equal(t, reads[0], reads[1], "no additional account reads")
	require.True(t, captured.Final.AdmissionKnown && captured.Final.Eligible && captured.Final.Present && captured.Final.Included)
	require.Equal(t, epoch, captured.Final.TargetEpoch)
	require.Equal(t, target, captured.Final.TargetSlot)
	require.Equal(t, byte(3), captured.Final.EpochIdentity[1], "identity comes from target epoch, not current block leader")
	require.Equal(t, blockID, captured.FinalBlockID)
	require.True(t, captured.FinalFast)
	for _, invalid := range [][]byte{[]byte{0xff}, observationFastFinalCertificate(t, target, blockID, version+1)} {
		var errors [2]string
		for i := range errors {
			ctx, _ := fixture.bank(t)
			ctx.Features.EnableFeature(features.Alpenglow, 0)
			block := &b.Block{Slot: target + 4, HasAlpenglowFooter: true}
			if i == 1 {
				block.EnableCertificateObservation(subject)
			}
			err := ApplyAlpenglowVoteRewards(ctx, block, schedule, nil, nil, invalid, version)
			require.Error(t, err)
			errors[i] = err.Error()
			_, ok := block.CertificateObservation()
			require.False(t, ok)
		}
		require.Equal(t, errors[0], errors[1], "invalid certificate rejection is unchanged")
	}
	reused := &b.Block{Slot: target + 4, HasAlpenglowFooter: true}
	reused.EnableCertificateObservation(subject)
	require.NoError(t, ApplyAlpenglowVoteRewards(results[1], reused, schedule, nil, nil, nil, version))
	_, ok := reused.CertificateObservation()
	require.True(t, ok)
	require.Error(t, ApplyAlpenglowVoteRewards(results[1], reused, schedule, nil, nil, []byte{0xff}, version))
	_, ok = reused.CertificateObservation()
	require.False(t, ok, "a repeated failed apply must not expose the previous successful result")
	retainedSubject, enabled := reused.CertificateObservationSubject()
	require.True(t, enabled)
	require.Equal(t, subject, retainedSubject)
	// Apply and speculative production do not publish. Only the shared successful
	// replay completion (after execution and callback acceptance) copies a value.
	var progress Progress
	progress.begin(target+4, nil)
	before, _ := progress.Snapshot()
	require.False(t, before.HasCompleted)
	progress.phase("halted") // Failed execution/rejected callback never calls completed.
	failed, _ := progress.Snapshot()
	require.False(t, failed.HasCompleted)
	progress.completed(CompletedBlock{At: time.Now(), Certificates: captured, HasCertificates: true}, 0, false)
	published, _ := progress.Snapshot()
	captured.Final.Included = false
	require.True(t, published.Completed.Certificates.Final.Included)
}

func observationFastFinalCertificate(t *testing.T, target uint64, blockID solana.Hash, version uint16) []byte {
	t.Helper()
	payload, err := alpenglow.EncodeVotePayloadToSign(alpenglow.NewNotarizationVote(target, blockID), version)
	require.NoError(t, err)
	point, err := bls12381.HashToG2(payload, []byte(alpenglow.DefaultHashToPointDST))
	require.NoError(t, err)
	var signature bls12381.G2Affine
	signature.ScalarMultiplication(&point, big.NewInt(3))
	compressed := signature.Bytes()
	bitmap, err := alpenglow.EncodeSignerStoreBitmap(alpenglow.SignerBitmap{Encoding: alpenglow.SignerBitmapBase2, Length: 1, Base: []bool{true}})
	require.NoError(t, err)
	raw := make([]byte, 8+32+96+2+len(bitmap)+1)
	binary.LittleEndian.PutUint64(raw, target)
	copy(raw[8:40], blockID[:])
	copy(raw[40:136], compressed[:])
	binary.LittleEndian.PutUint16(raw[136:138], uint16(len(bitmap)))
	copy(raw[138:], bitmap)
	return raw
}

func TestCertificateObservationAdmissionConcurrent(t *testing.T) {
	const epoch = uint64(9_000_000_020)
	clearVoteRewardVerifierCacheForTest()
	t.Cleanup(func() { global.ClearEpochStakes(epoch); clearVoteRewardVerifierCacheForTest() })
	installTestVoteRewardValidator(epoch, 3)
	material, err := loadVoteRewardVerifierMaterial(epoch, 42, nil)
	require.NoError(t, err)
	var readers sync.WaitGroup
	for i := range 4 {
		readers.Go(func() {
			subject := solana.PublicKey{byte(epoch & 255), byte(i % 2)}
			for target := range uint64(100) {
				got := material.observationMembership(subject, target)
				if !got.AdmissionKnown || got.Eligible != (i%2 == 0) || got.TargetSlot != target || got.TargetEpoch != epoch {
					t.Errorf("incorrect concurrent admission: %+v", got)
					return
				}
			}
		})
	}
	readers.Wait()
}

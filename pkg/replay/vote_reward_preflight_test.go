package replay

import (
	"bytes"
	"fmt"
	"math/big"
	"sync"
	"testing"

	bls12381 "github.com/Overclock-Validator/gnark-crypto/ecc/bls12-381"
	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/rewardcerts"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/stretchr/testify/require"
)

func TestRewardCertificatePreflightUsesExpectedEpochAndClassifiesErrors(t *testing.T) {
	const epoch = uint64(9_000_000_042)
	clearVoteRewardVerifierCacheForTest()
	global.ClearEpochStakes(epoch)
	t.Cleanup(func() {
		global.ClearEpochStakes(epoch)
		clearVoteRewardVerifierCacheForTest()
	})
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32}
	rewardSlot := epoch*32 + 31
	blk := &block.Block{Slot: rewardSlot + rewardcerts.SlotsForReward, SkipRewardCert: []byte{1}}

	// Missing local keys do not justify quarantining even malformed bytes.
	err := validatePreConsensusRewardCertificates(blk, schedule, 42)
	require.ErrorContains(t, err, "missing epoch stakes")
	require.False(t, IsInvalidRewardCertificateError(err))
	installTestVoteRewardValidator(epoch, 3)
	err = validatePreConsensusRewardCertificates(blk, schedule, 42)
	require.True(t, IsInvalidRewardCertificateError(err))

	// The containing block is in the next epoch. Only the reward epoch is
	// installed; a valid certificate must still pass across that boundary.
	blk.SkipRewardCert = testPreflightSkipCertificate(t, rewardSlot, 42)
	require.NoError(t, validatePreConsensusRewardCertificates(blk, schedule, 42))

	// A claimed slot outside the expected epoch is invalid content, not a
	// request to load another epoch's validator material.
	blk.SkipRewardCert = testPreflightSkipCertificate(t, rewardSlot+32, 42)
	err = validatePreConsensusRewardCertificates(blk, schedule, 42)
	require.ErrorContains(t, err, "invalid reward slot")
	require.True(t, IsInvalidRewardCertificateError(err))

	installTestVoteRewardValidator(epoch, 7)
	err = validatePreConsensusRewardCertificates(blk, schedule, 42)
	require.ErrorContains(t, err, "epoch material is immutable")
	require.False(t, IsInvalidRewardCertificateError(err))
}

func TestRewardCertificateValidationCacheRequiresExactContents(t *testing.T) {
	const (
		epoch       = uint64(9_000_000_043)
		rewardSlot  = uint64(123)
		currentSlot = rewardSlot + rewardcerts.SlotsForReward
	)
	clearVoteRewardVerifierCacheForTest()
	t.Cleanup(func() {
		global.ClearEpochStakes(epoch)
		clearVoteRewardVerifierCacheForTest()
	})
	installTestVoteRewardValidator(epoch, 3)
	material, err := loadVoteRewardVerifierMaterial(epoch, 42, nil)
	require.NoError(t, err)
	raw := testPreflightSkipCertificate(t, rewardSlot, 42)
	original := bytes.Clone(raw)
	first, timings, err := material.validateRewardCertificates(currentSlot, raw, nil, true)
	require.NoError(t, err)
	require.Positive(t, timings.Skip)
	require.Len(t, first.Validators, 1)
	for validator := range first.Validators {
		delete(first.Validators, validator)
	}
	second, timings, err := material.validateRewardCertificates(currentSlot, raw, nil, true)
	require.NoError(t, err)
	require.Zero(t, timings.Skip)
	require.Len(t, second.Validators, 1, "caller mutation must not change cached signers")

	// In-place mutation must not change the cache key or reuse successful
	// validation for a different signature.
	raw[8] ^= 1
	_, _, err = material.validateRewardCertificates(currentSlot, raw, nil, true)
	require.Error(t, err)
	_, _, err = material.validateRewardCertificates(currentSlot, original, []byte{1}, true)
	require.Error(t, err, "a newly supplied notar cert must be checked")
	_, _, err = material.validateRewardCertificates(currentSlot+1, original, nil, true)
	require.Error(t, err, "a different containing slot must be checked")
	_, timings, err = material.validateRewardCertificates(currentSlot, original, nil, true)
	require.NoError(t, err)
	require.Zero(t, timings.Skip, "invalid candidates must not replace the successful entry")

	// A different shred version owns different immutable verifier material.
	otherMaterial, err := loadVoteRewardVerifierMaterial(epoch, 43, nil)
	require.NoError(t, err)
	_, _, err = otherMaterial.validateRewardCertificates(currentSlot, original, nil, true)
	require.Error(t, err)

	// Only one successful pair is retained per material.
	next := testPreflightSkipCertificate(t, rewardSlot+1, 42)
	_, _, err = material.validateRewardCertificates(currentSlot+1, next, nil, true)
	require.NoError(t, err)
	_, timings, err = material.validateRewardCertificates(currentSlot, original, nil, true)
	require.NoError(t, err)
	require.Positive(t, timings.Skip)
}

func TestRewardCertificateValidationCacheConcurrentCandidates(t *testing.T) {
	const epoch = uint64(9_000_000_044)
	clearVoteRewardVerifierCacheForTest()
	t.Cleanup(func() {
		global.ClearEpochStakes(epoch)
		clearVoteRewardVerifierCacheForTest()
	})
	installTestVoteRewardValidator(epoch, 3)
	material, err := loadVoteRewardVerifierMaterial(epoch, 42, nil)
	require.NoError(t, err)
	raw := [][]byte{
		testPreflightSkipCertificate(t, 200, 42),
		testPreflightSkipCertificate(t, 201, 42),
	}
	const workers = 4
	start := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			for iteration := 0; iteration < 12; iteration++ {
				candidate := (worker + iteration/2) % len(raw)
				wantSlot := uint64(200 + candidate)
				validated, _, err := material.validateRewardCertificates(wantSlot+rewardcerts.SlotsForReward, raw[candidate], nil, true)
				if err != nil {
					errs <- err
					return
				}
				if validated == nil || validated.RewardSlot != wantSlot || len(validated.Validators) != 1 {
					errs <- fmt.Errorf("candidate %d got inconsistent reward validation: %+v", candidate, validated)
					return
				}
				// Replay and local production must each own their result even
				// when they concurrently validate the same cached footer.
				validated.RewardSlot = 0
				for validator := range validated.Validators {
					delete(validated.Validators, validator)
				}
			}
		}(worker)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

func testPreflightSkipCertificate(t *testing.T, rewardSlot uint64, shredVersion uint16) []byte {
	t.Helper()
	payload, err := alpenglow.EncodeVotePayloadToSign(alpenglow.NewSkipVote(rewardSlot), shredVersion)
	require.NoError(t, err)
	point, err := bls12381.HashToG2(payload, []byte(alpenglow.DefaultHashToPointDST))
	require.NoError(t, err)
	var signature bls12381.G2Affine
	signature.ScalarMultiplication(&point, big.NewInt(3))
	bitmap, err := alpenglow.EncodeSignerStoreBitmap(alpenglow.SignerBitmap{
		Encoding: alpenglow.SignerBitmapBase2, Length: 1, Base: []bool{true},
	})
	require.NoError(t, err)
	raw, err := rewardcerts.EncodeSkipRewardCertificate(rewardcerts.SkipRewardCertificate{
		Slot: rewardSlot, Signature: signature.Bytes(), Bitmap: bitmap,
	})
	require.NoError(t, err)
	return raw
}

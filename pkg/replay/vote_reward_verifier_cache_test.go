package replay

import (
	"math/big"
	"sync"
	"testing"

	bls12381 "github.com/Overclock-Validator/gnark-crypto/ecc/bls12-381"
	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/rewardcerts"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloneValidatedRewardCertPreservesMetadataAndCopiesValidators(t *testing.T) {
	var vote solana.PublicKey
	vote[0] = 1
	original := &rewardcerts.ValidatedRewardCert{
		RewardSlot: 123,
		Epoch:      4,
		Validators: map[solana.PublicKey]struct{}{vote: {}},
	}
	clone := cloneValidatedRewardCert(original)
	require.Equal(t, original, clone)
	delete(clone.Validators, vote)
	assert.Contains(t, original.Validators, vote)
	assert.Nil(t, cloneValidatedRewardCert(nil))
}

func TestVoteRewardVerifierCacheReusesIdenticalAndRejectsChangedEpochMaterial(t *testing.T) {
	const epoch = uint64(9_000_000_017)
	clearVoteRewardVerifierCacheForTest()
	global.ClearEpochStakes(epoch)
	t.Cleanup(func() {
		global.ClearEpochStakes(epoch)
		clearVoteRewardVerifierCacheForTest()
	})

	installTestVoteRewardValidator(epoch, 3)
	var details metrics.VoteRewardDetails

	first, err := loadVoteRewardVerifierMaterial(epoch, 42, &details)
	require.NoError(t, err)
	require.NotNil(t, first)
	assert.Equal(t, uint64(1), details.ValidatorCacheMisses)
	assert.Zero(t, details.ValidatorCacheHits)

	second, err := loadVoteRewardVerifierMaterial(epoch, 42, &details)
	require.NoError(t, err)
	assert.Same(t, first, second)
	assert.Equal(t, uint64(1), details.ValidatorCacheMisses)
	assert.Equal(t, uint64(1), details.ValidatorCacheHits)
	assert.Equal(t, uint64(2), details.ValidatorPreparation.Count)

	otherVersion, err := loadVoteRewardVerifierMaterial(epoch, 43, &details)
	require.NoError(t, err)
	assert.NotSame(t, first.verifier, otherVersion.verifier)
	assert.Equal(t, uint16(43), otherVersion.verifier.ShredVersion())

	oldGeneration := first.snapshot.Generation
	installTestVoteRewardValidator(epoch, 3)
	reloaded, err := loadVoteRewardVerifierMaterial(epoch, 42, &details)
	require.NoError(t, err)
	assert.NotSame(t, first, reloaded)
	assert.Same(t, first.verifier, reloaded.verifier)
	assert.NotEqual(t, oldGeneration, reloaded.snapshot.Generation)
	assert.Equal(t, first.snapshot.Stakes, reloaded.snapshot.Stakes)

	installTestVoteRewardValidator(epoch, 7)
	changed, err := loadVoteRewardVerifierMaterial(epoch, 42, &details)
	require.Error(t, err)
	assert.Nil(t, changed)
	assert.Contains(t, err.Error(), "epoch material is immutable")

	firstSet, firstSetOK := first.verifier.ValidatorSetForEpoch(epoch)
	require.True(t, firstSetOK)
	require.Len(t, firstSet.Validators, 1)
	assert.Equal(t, byte(3), firstSet.Validators[0].NodePubkey[1])
}

func TestVoteRewardVerifierCacheConcurrentIdenticalReloads(t *testing.T) {
	const (
		epoch      = uint64(9_000_000_018)
		readers    = 4
		iterations = 300
	)
	clearVoteRewardVerifierCacheForTest()
	global.ClearEpochStakes(epoch)
	t.Cleanup(func() {
		global.ClearEpochStakes(epoch)
		clearVoteRewardVerifierCacheForTest()
	})

	installTestVoteRewardValidator(epoch, 5)
	first, _, err := cachedVoteRewardVerifiers.get(epoch, 77)
	require.NoError(t, err)

	start := make(chan struct{})
	errs := make(chan error, readers)
	var wg sync.WaitGroup
	wg.Add(readers + 1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < iterations; i++ {
			installTestVoteRewardValidator(epoch, 5)
		}
	}()
	for i := 0; i < readers; i++ {
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < iterations; j++ {
				material, _, getErr := cachedVoteRewardVerifiers.get(epoch, 77)
				if getErr != nil {
					errs <- getErr
					return
				}
				if material.verifier != first.verifier {
					errs <- assert.AnError
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

func TestVoteRewardVerifierCacheLRUEvictionIsBounded(t *testing.T) {
	const baseEpoch = uint64(9_000_000_100)
	clearVoteRewardVerifierCacheForTest()
	t.Cleanup(func() {
		for i := 0; i <= voteRewardVerifierCacheCapacity; i++ {
			global.ClearEpochStakes(baseEpoch + uint64(i))
		}
		clearVoteRewardVerifierCacheForTest()
	})

	for i := 0; i <= voteRewardVerifierCacheCapacity; i++ {
		epoch := baseEpoch + uint64(i)
		installTestVoteRewardValidator(epoch, int64(i+2))
		_, hit, err := cachedVoteRewardVerifiers.get(epoch, 88)
		require.NoError(t, err)
		assert.False(t, hit)
	}

	cachedVoteRewardVerifiers.mu.Lock()
	entryCount := len(cachedVoteRewardVerifiers.entries)
	identityCount := len(cachedVoteRewardVerifiers.epochIdentity)
	cachedVoteRewardVerifiers.mu.Unlock()
	assert.Equal(t, voteRewardVerifierCacheCapacity, entryCount)
	assert.Equal(t, voteRewardVerifierCacheCapacity+1, identityCount)

	// The evicted epoch can be rebuilt only from the exact immutable material.
	_, hit, err := cachedVoteRewardVerifiers.get(baseEpoch, 88)
	require.NoError(t, err)
	assert.False(t, hit)
}

func installTestVoteRewardValidator(epoch uint64, scalar int64) {
	stakes, voteAccounts := testVoteRewardValidatorMaterial(epoch, scalar)
	global.PutEpochStakes(epoch, stakes, voteAccounts, 100)
}

func testVoteRewardValidatorMaterial(
	epoch uint64,
	scalar int64,
) (map[solana.PublicKey]uint64, map[solana.PublicKey]*epochstakes.VoteAccount) {
	var voteAccount solana.PublicKey
	voteAccount[0] = byte(epoch)
	var nodePubkey solana.PublicKey
	nodePubkey[0] = byte(epoch >> 8)
	nodePubkey[1] = byte(scalar)

	var blsPubkey bls12381.G1Affine
	blsPubkey.ScalarMultiplicationBase(big.NewInt(scalar))
	compressed := blsPubkey.Bytes()
	return map[solana.PublicKey]uint64{
			voteAccount: 100,
		}, map[solana.PublicKey]*epochstakes.VoteAccount{
			voteAccount: {
				NodePubkey:          nodePubkey,
				BlsPubkeyCompressed: &compressed,
			},
		}
}

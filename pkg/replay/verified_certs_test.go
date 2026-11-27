package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/rewardcerts"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestVerifiedCertsAnswerOnlyForIdenticalInput(t *testing.T) {
	store := &verifiedCertStore{entries: make(map[verifiedCertKey]any)}
	skip, notar := []byte{1, 2, 3}, []byte{4, 5}
	key := rewardCertKey(100, 7, 9, skip, notar)
	validator := solana.PublicKey{42}
	store.putReward(key, &rewardcerts.ValidatedRewardCert{RewardSlot: 92, Validators: map[solana.PublicKey]struct{}{validator: {}}})

	got, ok := store.reward(rewardCertKey(100, 7, 9, []byte{1, 2, 3}, []byte{4, 5}))
	require.True(t, ok)
	require.Equal(t, uint64(92), got.RewardSlot)
	delete(got.Validators, validator)
	again, _ := store.reward(key)
	require.Contains(t, again.Validators, validator, "a caller's changes never reach the stored result")

	for name, other := range map[string]verifiedCertKey{
		"another slot":          rewardCertKey(101, 7, 9, skip, notar),
		"another epoch":         rewardCertKey(100, 8, 9, skip, notar),
		"another shred version": rewardCertKey(100, 7, 10, skip, notar),
		"other bytes":           rewardCertKey(100, 7, 9, []byte{1, 2}, notar),
		"bytes moved between the two certificates": rewardCertKey(100, 7, 9, []byte{1, 2, 3, 4}, []byte{5}),
		"a final certificate with the same bytes":  finalCertKey(100, 7, 9, append(append([]byte{}, skip...), notar...)),
	} {
		_, ok := store.reward(other)
		require.False(t, ok, name)
		_, ok = store.final(other)
		require.False(t, ok, name)
	}

	finalKey := finalCertKey(100, 7, 9, []byte{9})
	store.putFinal(finalKey, &rewardcerts.ValidatedFinalCert{FinalSlot: 99, Signers: map[solana.PublicKey]struct{}{validator: {}}})
	final, ok := store.final(finalKey)
	require.True(t, ok)
	require.Equal(t, uint64(99), final.FinalSlot)
	_, ok = store.reward(finalKey)
	require.False(t, ok)

	store.putReward(rewardCertKey(1, 1, 1, nil, nil), nil)
	_, ok = store.reward(rewardCertKey(1, 1, 1, nil, nil))
	require.False(t, ok, "a nil result is not remembered")

	for slot := uint64(1000); slot < 1000+3*verifiedCertSlots; slot++ {
		store.putFinal(finalCertKey(slot, 7, 9, []byte{1}), &rewardcerts.ValidatedFinalCert{FinalSlot: slot})
	}
	require.LessOrEqual(t, len(store.entries), 2*verifiedCertSlots)
	_, ok = store.reward(key)
	require.False(t, ok, "the oldest entries are dropped first")
}

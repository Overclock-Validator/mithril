package accountsdb

import (
	"encoding/binary"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCommonCacheAdmissionDuplicatesDoNotFakeReuse(t *testing.T) {
	admission := newCommonCacheAdmission()
	count := commonAccountImmediateAdmissionLimit + 1
	pks := make([]solana.PublicKey, count)
	cold := make([]int, count)
	for i := range cold {
		pks[i] = solana.PublicKey{1}
		cold[i] = i
	}

	first := admission.classifyAndObserve(pks, cold)
	assert.Zero(t, countTrue(first), "duplicates in one large request must not manufacture a second observation")
	second := admission.classifyAndObserve(pks, cold)
	assert.Equal(t, count, countTrue(second), "a later request is a genuine second observation")
}

func TestCommonCacheAdmissionIsBoundedAndRotates(t *testing.T) {
	admission := newCommonCacheAdmission()
	count := commonAccountMaxAdmissionsPerBatch * 2
	pks := make([]solana.PublicKey, count)
	cold := make([]int, count)
	for i := range pks {
		binary.LittleEndian.PutUint64(pks[i][:8], uint64(i+1))
		cold[i] = i
	}

	assert.Zero(t, countTrue(admission.classifyAndObserve(pks, cold)))
	admitted := countTrue(admission.classifyAndObserve(pks, cold))
	assert.Positive(t, admitted)
	assert.LessOrEqual(t, admitted, commonAccountMaxAdmissionsPerBatch)
	assert.Len(t, admission.current, commonAdmissionBloomWords)
	assert.Len(t, admission.previous, commonAdmissionBloomWords)

	old := solana.PublicKey{0xf0}
	newer := solana.PublicKey{0xf1}
	admission = newCommonCacheAdmission()
	admission.insert(old)
	admission.observed = commonAdmissionRotateAfterKeys
	admission.rotateBeforeInsert(1)
	assert.True(t, admission.contains(old), "one previous generation remains reusable")
	admission.insert(newer)
	admission.observed = commonAdmissionRotateAfterKeys
	admission.rotateBeforeInsert(1)
	assert.False(t, admission.contains(old), "keys expire after two rotations")
	assert.True(t, admission.contains(newer))
}

func TestCommonAccountCacheUsesByteBudget(t *testing.T) {
	previous := CommonAccountCacheMaxMB
	CommonAccountCacheMaxMB = 1
	defer func() { CommonAccountCacheMaxMB = previous }()

	db := &AccountsDb{}
	db.InitCaches()
	small := &accounts.Account{Key: solana.PublicKey{1}, Data: make([]byte, 1024)}
	require.True(t, db.CommonAcctsCache.Set(small.Key, small))
	assert.Equal(t, uint32(commonAccountCacheEntryOverheadBytes+1024), commonAccountCacheCost(small))

	// Otter rejects a single value above its 10% small-queue budget instead
	// of evicting the entire retained working set for one giant account.
	oversized := &accounts.Account{Key: solana.PublicKey{2}, Data: make([]byte, 128<<10)}
	assert.False(t, db.CommonAcctsCache.Set(oversized.Key, oversized))

	grown := &accounts.Account{Key: small.Key, Lamports: 2, Data: make([]byte, 128<<10)}
	db.refreshReadCaches([]*accounts.Account{grown})
	assert.False(t, db.CommonAcctsCache.Has(small.Key), "a rejected weighted refresh must evict the stale prior value")
}

func countTrue(values []bool) int {
	count := 0
	for _, value := range values {
		if value {
			count++
		}
	}
	return count
}

// refreshReadCaches publishes a cache epoch and refreshes entries, as a fold
// does, so cache tests can exercise coherence without a full CommitBatch.
func (accountsDb *AccountsDb) refreshReadCaches(accts []*accounts.Account) {
	accountsDb.readCacheEpochMu.Lock()
	defer accountsDb.readCacheEpochMu.Unlock()
	accountsDb.readCacheEpoch++
	accountsDb.refreshReadCacheEntries(accts)
}

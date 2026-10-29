package bankhash

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/zeebo/blake3"
)

const ltHashBytes = 2048

func ltHashFixtureKey(index int) solana.PublicKey {
	var key solana.PublicKey
	binary.LittleEndian.PutUint64(key[:8], uint64(index+1))
	binary.LittleEndian.PutUint64(key[8:16], uint64(index+1)*0x9e3779b97f4a7c15)
	key[31] = byte(index*37 + 11)
	return key
}

func ltHashFixtureAccount(key solana.PublicKey, seed int, lamports uint64, dataLen int) *accounts.Account {
	data := make([]byte, dataLen)
	for i := range data {
		data[i] = byte(seed*17 + i*29)
	}
	var owner [32]byte
	for i := range owner {
		owner[i] = byte(seed*13 + i*7)
	}
	return &accounts.Account{
		Key:        key,
		Lamports:   lamports,
		Data:       data,
		Owner:      owner,
		Executable: seed%3 == 0,
		RentEpoch:  uint64(seed * 5),
	}
}

func buildLtHashDifferentialFixture(t testing.TB, uniqueAccounts int) (*sealevel.SlotCtx, []*accounts.Account) {
	t.Helper()
	parent := accounts.NewMemAccounts()
	modified := make([]*accounts.Account, 0, uniqueAccounts+uniqueAccounts/17+uniqueAccounts/29+2)

	for i := range uniqueAccounts {
		key := ltHashFixtureKey(i)
		oldAcct := ltHashFixtureAccount(key, i+1, uint64(i+101), i%257)
		newAcct := oldAcct.Clone()

		switch i % 7 {
		case 0: // unchanged
		case 1: // RentEpoch is not part of the LtHash input.
			newAcct.RentEpoch += 991
		case 2: // create
			oldAcct.Lamports = 0
			newAcct.Lamports += 10_000
		case 3: // delete
			newAcct.Lamports = 0
			newAcct.Data = []byte{0xde, 0xad}
		case 4: // data and lamports update
			newAcct.Lamports += 77
			newAcct.Data = append(newAcct.Data, byte(i), 0xa5)
		case 5: // owner/executable update
			newAcct.Owner[0] ^= 0xff
			newAcct.Executable = !newAcct.Executable
		case 6: // both zero; other fields cannot contribute
			oldAcct.Lamports = 0
			newAcct.Lamports = 0
			newAcct.Data = append(newAcct.Data, 0x42)
		}

		if err := parent.SetAccountWithoutLock(key, oldAcct); err != nil {
			t.Fatalf("seed parent account %d: %v", i, err)
		}

		if i%17 == 0 {
			stale := newAcct.Clone()
			stale.Lamports += 1234
			stale.Data = append(stale.Data, 0x17)
			modified = append(modified, stale)
		}
		modified = append(modified, newAcct)
		if i%29 == 0 {
			modified = append(modified, nil)
		}
	}

	return &sealevel.SlotCtx{Slot: 4242, ParentAccts: parent, AcctsLtHash: &lthash.LtHash{}}, modified
}

func legacyDedupeModifiedAccts(modifiedAccts []*accounts.Account) []*accounts.Account {
	unique := make([]*accounts.Account, 0, len(modifiedAccts))
	seen := make(map[[32]byte]int, len(modifiedAccts))
	for _, acct := range modifiedAccts {
		if acct == nil {
			continue
		}
		key := [32]byte(acct.Key)
		if index, ok := seen[key]; ok {
			unique[index] = acct
			continue
		}
		seen[key] = len(unique)
		unique = append(unique, acct)
	}
	return unique
}

// legacyAccountHashBytes is an independent copy of the pre-optimization
// account hash: it materializes the 2 KiB BLAKE3 XOF before LtHash reduction.
func legacyAccountHashBytes(acct *accounts.Account) []byte {
	if acct.Lamports == 0 {
		return nil
	}
	hasher := blake3.New()
	var lamportBytes [8]byte
	binary.LittleEndian.PutUint64(lamportBytes[:], acct.Lamports)
	_, _ = hasher.Write(lamportBytes[:])
	_, _ = hasher.Write(acct.Data)
	if acct.Executable {
		_, _ = hasher.Write([]byte{1})
	} else {
		_, _ = hasher.Write([]byte{0})
	}
	_, _ = hasher.Write(acct.Owner[:])
	_, _ = hasher.Write(acct.Key[:])
	output := make([]byte, ltHashBytes)
	_, _ = hasher.Digest().Read(output)
	return output
}

func legacyAccountsEqual(a, b *accounts.Account) bool {
	return a.Lamports == b.Lamports &&
		a.Executable == b.Executable &&
		a.RentEpoch == b.RentEpoch &&
		a.Owner == b.Owner &&
		bytes.Equal(a.Data, b.Data)
}

func legacySingleDeltaLtHash(slotCtx *sealevel.SlotCtx, modifiedAcct *accounts.Account) *lthash.LtHash {
	previousAcct, err := slotCtx.GetParentAccount(modifiedAcct.Key)
	if err != nil {
		panic(fmt.Sprintf("couldn't find parent acct for %s for slot %d", modifiedAcct.Key, slotCtx.Slot))
	}

	var delta lthash.LtHash
	if previousAcct.Lamports != 0 {
		if legacyAccountsEqual(modifiedAcct, previousAcct) {
			return &delta
		}
		var oldHash lthash.LtHash
		oldHash.InitWithHash(legacyAccountHashBytes(previousAcct))
		delta.Sub(&oldHash)
	}

	if modifiedAcct.Lamports != 0 {
		var newHash lthash.LtHash
		newHash.InitWithHash(legacyAccountHashBytes(modifiedAcct))
		delta.Add(&newHash)
	}
	return &delta
}

func legacyCalculateDeltaLtHash(slotCtx *sealevel.SlotCtx, modifiedAccts []*accounts.Account) *lthash.LtHash {
	modifiedAccts = legacyDedupeModifiedAccts(modifiedAccts)
	if len(modifiedAccts) == 0 {
		return &lthash.LtHash{}
	}

	numWorkers := min(32, len(modifiedAccts))
	chunkSize := (len(modifiedAccts) + numWorkers - 1) / numWorkers
	perAccount := make([]*lthash.LtHash, len(modifiedAccts))
	var wg sync.WaitGroup
	for workerID := range numWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := workerID * chunkSize
			end := min(start+chunkSize, len(modifiedAccts))
			for i := start; i < end; i++ {
				perAccount[i] = legacySingleDeltaLtHash(slotCtx, modifiedAccts[i])
			}
		}()
	}
	wg.Wait()

	var result lthash.LtHash
	for _, accountDelta := range perAccount {
		result.Add(accountDelta)
	}
	return &result
}

func TestCalculateDeltaLtHashMatchesLegacyReference(t *testing.T) {
	for _, size := range []int{0, 1, 2, 31, 32, 33, 257, 1025} {
		t.Run(fmt.Sprintf("accounts_%d", size), func(t *testing.T) {
			ctx, modified := buildLtHashDifferentialFixture(t, size)
			want := legacyCalculateDeltaLtHash(ctx, modified)
			got := calculateDeltaLtHash(ctx, modified)
			if !bytes.Equal(got.Hash(), want.Hash()) {
				t.Fatalf("optimized delta differs byte-for-byte from legacy reference for %d accounts", size)
			}
		})
	}
}

func TestCalculateDeltaLtHashFastPathMetrics(t *testing.T) {
	parent := accounts.NewMemAccounts()
	modified := make([]*accounts.Account, 0, 8)
	add := func(index, oldLen, newLen int, oldLamports, newLamports uint64) (*accounts.Account, *accounts.Account) {
		key := ltHashFixtureKey(index)
		oldAcct := ltHashFixtureAccount(key, index+31, oldLamports, oldLen)
		newAcct := ltHashFixtureAccount(key, index+71, newLamports, newLen)
		if err := parent.SetAccountWithoutLock(key, oldAcct); err != nil {
			t.Fatalf("seed account %d: %v", index, err)
		}
		return oldAcct, newAcct
	}

	oldUnchanged, _ := add(0, 2, 2, 10, 10)
	modified = append(modified, oldUnchanged.Clone())
	oldRent, _ := add(1, 3, 3, 20, 20)
	rentOnly := oldRent.Clone()
	rentOnly.RentEpoch++
	modified = append(modified, rentOnly)
	_, created := add(2, 4, 5, 0, 30)
	modified = append(modified, created)
	oldDeleted, _ := add(3, 7, 1, 40, 0)
	deleted := oldDeleted.Clone()
	deleted.Lamports = 0
	modified = append(modified, deleted)
	oldUpdated, newest := add(4, 11, 13, 50, 60)
	stale := newest.Clone()
	stale.Lamports++
	modified = append(modified, stale, nil, newest)
	oldZero, _ := add(5, 17, 19, 0, 0)
	zeroToZero := oldZero.Clone()
	zeroToZero.Data = append(zeroToZero.Data, 0xff)
	modified = append(modified, zeroToZero)

	metrics.GlobalBlockReplay = metrics.BlockReplay{}
	t.Cleanup(func() { metrics.GlobalBlockReplay = metrics.BlockReplay{} })
	ctx := &sealevel.SlotCtx{Slot: 55, ParentAccts: parent, AcctsLtHash: &lthash.LtHash{}, Replay: true}
	got := calculateDeltaLtHash(ctx, modified)
	want := legacyCalculateDeltaLtHash(ctx, modified)
	if !got.Equals(want) {
		t.Fatal("fast-path fixture differs from the legacy delta")
	}

	replayMetrics := metrics.GlobalBlockReplay
	if replayMetrics.LtHashInputAccounts != 8 || replayMetrics.LtHashUniqueAccounts != 6 {
		t.Fatalf("input/unique metrics = %d/%d, want 8/6", replayMetrics.LtHashInputAccounts, replayMetrics.LtHashUniqueAccounts)
	}
	if replayMetrics.LtHashUnchangedAccounts != 3 || replayMetrics.LtHashCreatedAccounts != 1 || replayMetrics.LtHashDeletedAccounts != 1 {
		t.Fatalf("unchanged/created/deleted metrics = %d/%d/%d, want 3/1/1",
			replayMetrics.LtHashUnchangedAccounts, replayMetrics.LtHashCreatedAccounts, replayMetrics.LtHashDeletedAccounts)
	}
	if replayMetrics.LtHashOldDataBytes != uint64(len(oldDeleted.Data)+len(oldUpdated.Data)) {
		t.Fatalf("old hashed data bytes = %d, want %d", replayMetrics.LtHashOldDataBytes, len(oldDeleted.Data)+len(oldUpdated.Data))
	}
	if replayMetrics.LtHashNewDataBytes != uint64(len(created.Data)+len(newest.Data)) {
		t.Fatalf("new hashed data bytes = %d, want %d", replayMetrics.LtHashNewDataBytes, len(created.Data)+len(newest.Data))
	}
	// Zero-to-zero accounts carry no hashed data, so only the two live ones count.
	if replayMetrics.LtHashUnchangedDataBytes != uint64(len(oldUnchanged.Data)+len(oldRent.Data)) {
		t.Fatalf("unchanged data bytes = %d, want %d", replayMetrics.LtHashUnchangedDataBytes, len(oldUnchanged.Data)+len(oldRent.Data))
	}
	if replayMetrics.LtHashDedupe.Count != 1 || replayMetrics.LtHashWorkerCompute.Count != 1 || replayMetrics.LtHashPartialReduce.Count != 1 {
		t.Fatalf("phase timing counts = %d/%d/%d, want 1/1/1",
			replayMetrics.LtHashDedupe.Count, replayMetrics.LtHashWorkerCompute.Count, replayMetrics.LtHashPartialReduce.Count)
	}
}

func TestCalculateDeltaLtHashProducerDoesNotWriteReplayMetrics(t *testing.T) {
	ctx, modified := buildLtHashDifferentialFixture(t, 64)
	ctx.Replay = false
	metrics.GlobalBlockReplay = metrics.BlockReplay{}
	t.Cleanup(func() { metrics.GlobalBlockReplay = metrics.BlockReplay{} })

	calculateDeltaLtHash(ctx, modified)
	if metrics.GlobalBlockReplay.LtHashDedupe.Count != 0 ||
		metrics.GlobalBlockReplay.LtHashWorkerCompute.Count != 0 ||
		metrics.GlobalBlockReplay.LtHashPartialReduce.Count != 0 ||
		metrics.GlobalBlockReplay.LtHashInputAccounts != 0 {
		t.Fatal("leader-production LtHash calculation wrote replay metrics")
	}
}

func TestBankHashPhaseMetricsAreReplayOnly(t *testing.T) {
	featureSet := features.NewFeaturesDefault()
	ctx := &sealevel.SlotCtx{Features: featureSet}
	metrics.GlobalBlockReplay = metrics.BlockReplay{}
	t.Cleanup(func() { metrics.GlobalBlockReplay = metrics.BlockReplay{} })

	CalculateBankHash(ctx, nil, nil, [32]byte{}, 0, [32]byte{})
	if metrics.GlobalBlockReplay.BankHashFinalize.Count != 0 {
		t.Fatal("leader-production finalization wrote replay metrics")
	}
	if metrics.GlobalBlockReplay.AccountsDeltaHash.Count != 0 {
		t.Fatal("leader-production accounts delta hash wrote replay metrics")
	}

	ctx.Replay = true
	CalculateBankHash(ctx, nil, nil, [32]byte{}, 0, [32]byte{})
	if metrics.GlobalBlockReplay.BankHashFinalize.Count != 1 {
		t.Fatalf("replay finalization timing count = %d, want 1", metrics.GlobalBlockReplay.BankHashFinalize.Count)
	}
	if metrics.GlobalBlockReplay.AccountsDeltaHash.Count != 1 {
		t.Fatalf("replay accounts delta hash timing count = %d, want 1", metrics.GlobalBlockReplay.AccountsDeltaHash.Count)
	}
}

func TestAccumulateSingleDeltaLtHashMissingParentPanics(t *testing.T) {
	ctx := &sealevel.SlotCtx{Slot: 77, ParentAccts: accounts.NewMemAccounts()}
	modified := ltHashFixtureAccount(ltHashFixtureKey(9001), 1, 10, 4)
	var delta, scratch lthash.LtHash
	var hasher lthash.AccountHasher
	var stats ltHashWorkerStats

	defer func() {
		if recover() == nil {
			t.Fatal("missing parent account did not retain the legacy panic behavior")
		}
	}()
	accumulateSingleDeltaLtHash(ctx, modified, &delta, &scratch, &hasher, &stats)
}

func TestDedupeModifiedAcctsDropsSingleNil(t *testing.T) {
	if got := dedupeModifiedAccts([]*accounts.Account{nil}); got != nil {
		t.Fatalf("single nil account was not dropped: %#v", got)
	}
}

func buildLtHashBenchmarkFixture(b *testing.B, accountCount, dataLen int) (*sealevel.SlotCtx, []*accounts.Account) {
	b.Helper()
	parent := accounts.NewMemAccounts()
	modified := make([]*accounts.Account, accountCount)
	for i := range accountCount {
		key := ltHashFixtureKey(i)
		oldAcct := ltHashFixtureAccount(key, i+101, uint64(i+1), dataLen)
		newAcct := oldAcct.Clone()
		newAcct.Lamports += 1_000_000
		newAcct.Data[len(newAcct.Data)-1] ^= 0xff
		if err := parent.SetAccountWithoutLock(key, oldAcct); err != nil {
			b.Fatalf("seed benchmark account %d: %v", i, err)
		}
		modified[i] = newAcct
	}
	return &sealevel.SlotCtx{Slot: 99, ParentAccts: parent, AcctsLtHash: &lthash.LtHash{}}, modified
}

var benchmarkDeltaLtHashByte byte

func benchmarkCalculateDeltaLtHash(b *testing.B, accountCount int) {
	ctx, modified := buildLtHashBenchmarkFixture(b, accountCount, 128)
	b.Run("worker_partials", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			result := calculateDeltaLtHash(ctx, modified)
			benchmarkDeltaLtHashByte = result.Hash()[0]
		}
	})
	b.Run("legacy_per_account", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			result := legacyCalculateDeltaLtHash(ctx, modified)
			benchmarkDeltaLtHashByte = result.Hash()[0]
		}
	})
}

func BenchmarkCalculateDeltaLtHash8192(b *testing.B) {
	benchmarkCalculateDeltaLtHash(b, 8192)
}

func BenchmarkCalculateDeltaLtHash30000(b *testing.B) {
	benchmarkCalculateDeltaLtHash(b, 30_000)
}

package alpenglow

import (
	"crypto/sha256"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	bls12381 "github.com/Overclock-Validator/gnark-crypto/ecc/bls12-381"
	"github.com/gagliardetto/solana-go"
)

// Benchmark the full pending-vote fold, including verification, aggregation and
// accounting. Keys and signatures are prepared outside the timed region.
func BenchmarkCertPoolFoldVerifiedBatch(b *testing.B) {
	for _, size := range []int{1, 8, 32, 64} {
		b.Run(fmt.Sprintf("votes=%d", size), func(b *testing.B) {
			verifier, installed, vote, batch := certPoolBenchmarkFixture(b, size)
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				pool := NewCertPool(DefaultCertPoolConfig(), verifier, nil)
				pool.SetEpochLookup(func(uint64) uint64 { return installed.Epoch })
				tl := newTally()
				ps := &poolSlot{verifiedHash: make(map[voteDedupKey][]solana.Hash), pendingByRank: make(map[uint16]int)}
				pool.slots[vote.Slot] = ps
				for _, msg := range batch {
					tl.pending[msg.Rank] = map[[sha256.Size]byte]VoteMessage{sha256.Sum256(msg.Signature): msg}
					ps.pendingByRank[msg.Rank]++
					ps.pendingCount++
					pool.totalPending++
				}
				pool.mu.Lock()
				pool.foldTallyLocked(vote.Slot, ps, tl, &installed)
				pool.mu.Unlock()
				if len(tl.verified) != size || tl.stake != uint64(size) || pool.totalPending != 0 {
					b.Fatal("incomplete verified fold")
				}
			}
		})
	}
}

func certPoolBenchmarkFixture(b testing.TB, size int) (*CertificateVerifier, ValidatorSet, Vote, []VoteMessage) {
	stakes := make([]uint64, size)
	for i := range stakes {
		stakes[i] = 1
	}
	set, keys := testBLSValidatorSet(uint64(size), stakes...)
	verifier := NewCertificateVerifier()
	if err := verifier.SetValidatorSet(set); err != nil {
		b.Fatal(err)
	}
	// Exercise the same cached public-key representation used in production.
	installed, ok := verifier.ValidatorSetForEpoch(set.Epoch)
	if !ok {
		b.Fatal("missing installed validator set")
	}
	vote := NewSkipVote(500)
	payload, err := EncodeVotePayloadToSign(vote, verifier.ShredVersion())
	if err != nil {
		b.Fatal(err)
	}
	point, err := bls12381.HashToG2(payload, []byte(blsHashToPointDST))
	if err != nil {
		b.Fatal(err)
	}
	batch := make([]VoteMessage, size)
	for i := range batch {
		var sig bls12381.G2Affine
		sig.ScalarMultiplication(&point, keys[i])
		raw := sig.RawBytes()
		batch[i] = VoteMessage{Vote: vote, Rank: uint16(i), Signature: raw[:]}
	}
	return verifier, installed, vote, batch
}

// Observe short pool reads while eight peer-like producers submit one slot.
// The latency metrics, not ns/op (which includes deliberate sampling delays),
// measure whether cryptographic work blocks unrelated pool readers.
func BenchmarkCertPoolConcurrentVotes(b *testing.B) {
	verifier, installed, vote, batch := certPoolBenchmarkFixture(b, 64)
	waits := make([]int64, 0, 16*b.N)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		pool := NewCertPool(DefaultCertPoolConfig(), verifier, nil)
		pool.SetEpochLookup(func(uint64) uint64 { return installed.Epoch })
		pool.NoteLiveSlot(vote.Slot)
		var verified atomic.Int64
		pool.SetVerifiedVoteSink(func(VerifiedVote) { verified.Add(1) })
		start := make(chan struct{})
		var producers sync.WaitGroup
		for worker := 0; worker < 8; worker++ {
			producers.Add(1)
			go func(worker int) {
				defer producers.Done()
				<-start
				for i := worker; i < len(batch); i += 8 {
					pool.AddVote(batch[i])
				}
			}(worker)
		}
		close(start)
		for sample := 0; sample < 16; sample++ {
			time.Sleep(100 * time.Microsecond)
			t := time.Now()
			pool.Snapshot()
			waits = append(waits, time.Since(t).Nanoseconds())
		}
		producers.Wait()
		pool.FlushRewardVotes(vote.Slot)
		if verified.Load() != 64 || pool.Snapshot().PendingTotal != 0 {
			b.Fatal("lost or duplicated votes")
		}
	}
	b.StopTimer()
	slices.Sort(waits)
	b.ReportMetric(float64(waits[len(waits)*95/100]), "snapshot-p95-ns")
	b.ReportMetric(float64(waits[len(waits)-1]), "snapshot-max-ns")
}

func BenchmarkCertPoolWeightedPairing(b *testing.B) {
	for _, size := range []int{2, 4, 8, 16, 32, 64, 128} {
		verifier, set, vote, batch := certPoolBenchmarkFixture(b, size)
		payload, err := EncodeVotePayloadToSign(vote, verifier.ShredVersion())
		if err != nil {
			b.Fatal(err)
		}
		members := make([]parsedBatchVote, size)
		for i, msg := range batch {
			pub, err := validatorBLSPubkey(set, int(msg.Rank))
			if err != nil {
				b.Fatal(err)
			}
			members[i] = parsedBatchVote{message: msg, pubkey: pub}
			if _, err := members[i].sig.SetBytes(msg.Signature); err != nil {
				b.Fatal(err)
			}
		}
		for _, impl := range []struct {
			name   string
			verify func([]parsedBatchVote, []byte) (bool, error)
		}{
			{"Scalar", randomizedAggregatePairingScalarOK},
			{"MultiExp", randomizedAggregatePairingMultiExpOK},
		} {
			b.Run(fmt.Sprintf("votes=%d/%s", size, impl.name), func(b *testing.B) {
				b.ReportAllocs()
				for n := 0; n < b.N; n++ {
					if ok, err := impl.verify(members, payload); err != nil || !ok {
						b.Fatalf("verification: %t %v", ok, err)
					}
				}
			})
		}
	}
}

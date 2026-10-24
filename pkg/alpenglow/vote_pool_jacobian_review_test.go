package alpenglow

import (
	"bytes"
	"fmt"
	"math/big"
	"testing"

	bls12381 "github.com/Overclock-Validator/gnark-crypto/ecc/bls12-381"
	"github.com/gagliardetto/solana-go"
)

// Exercise the production assembler, not a copy of its arithmetic. The
// reference uses the former affine accumulation of actual signed votes.
func TestVotePoolJacobianProductionAssemblyReview(t *testing.T) {
	for _, typ := range []CertificateType{CertificateSkip, CertificateNotarizeFallback} {
		for split := 0; split <= 5; split++ {
			t.Run(fmt.Sprintf("%s/split%d", typ, split), func(t *testing.T) {
				pool, set, keys, _ := newTestPool(t)
				hash := solana.Hash{9}
				baseVote, fallbackVote := NewSkipVote(500), NewSkipFallbackVote(500)
				if typ == CertificateNotarizeFallback {
					baseVote = NewNotarizationVote(500, hash)
					fallbackVote = NewNotarizationFallbackVote(500, hash)
				}
				base, fb := newTally(), newTally()
				var expected bls12381.G2Affine
				expected.SetInfinity()
				for rank, key := range keys {
					vote, tl := baseVote, base
					if rank >= split {
						vote, tl = fallbackVote, fb
					}
					sig, err := decodeBLSSignature(signTestVote(t, vote, key))
					if err != nil {
						t.Fatal(err)
					}
					tl.aggSig.AddMixed(&sig)
					tl.verified[uint16(rank)] = struct{}{}
					tl.stake += set.Validators[rank].Stake
					expected.Add(&expected, &sig)
				}
				baseBefore, fbBefore := base.aggSig, fb.aggSig
				target := certTarget{certType: typ, base: tallyKey{}}
				target.base.Hash = baseVote.BlockHash
				want := expected.RawBytes()
				for repeat := 0; repeat < 3; repeat++ {
					pool.mu.Lock()
					cert, err := pool.assembleLocked(500, target, base, fb, &set, 100)
					pool.mu.Unlock()
					if err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(cert.Signature, want[:]) {
						t.Fatal("signature differs from affine reference")
					}
					if base.aggSig != baseBefore || fb.aggSig != fbBefore {
						t.Fatal("assembly mutated a retained tally")
					}
					if _, _, err := verifyCertificateWithSet(set, cert, true); err != nil {
						t.Fatal(err)
					}
					bitmap, err := DecodeSignerStoreBitmap(cert.Bitmap, 5)
					if err != nil {
						t.Fatal(err)
					}
					for rank := range keys {
						fallback := len(bitmap.Fallback) > rank && bitmap.Fallback[rank]
						if bitmap.Base[rank] != (rank < split) || fallback != (rank >= split) {
							t.Fatal("wrong signer groups")
						}
					}
				}
				// Empty and absent base tallies must produce the same fallback-only proof.
				if split == 0 {
					cert, err := pool.assembleLocked(500, target, nil, fb, &set, 100)
					if err != nil || !bytes.Equal(cert.Signature, want[:]) {
						t.Fatalf("nil base: %v", err)
					}
				}
				// Preserve the existing fail-closed guard even with a valid group sum.
				base.verified[0] = struct{}{}
				fb.verified[0] = struct{}{}
				if _, err := pool.assembleLocked(500, target, base, fb, &set, 100); err == nil {
					t.Fatal("overlapping signer groups accepted")
				}
			})
		}
	}
}

func TestVotePoolJacobianScalarVerificationReview(t *testing.T) {
	for _, size := range []int{2, 8, 16, 64} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			verifier, set, vote, batch := certPoolBenchmarkFixture(t, size)
			pool := NewCertPool(DefaultCertPoolConfig(), verifier, nil)
			members := pool.verifyBatch(batch, &set)
			if len(members) != size {
				t.Fatal("valid fixture rejected")
			}
			payload, err := EncodeVotePayloadToSign(vote, verifier.ShredVersion())
			if err != nil {
				t.Fatal(err)
			}
			before := append([]parsedBatchVote(nil), members...)
			if ok, err := randomizedAggregatePairingScalarOK(members, payload); err != nil || !ok {
				t.Fatalf("valid scalar batch: %t %v", ok, err)
			}
			for i := range members {
				if members[i].sig != before[i].sig || members[i].pubkey != before[i].pubkey {
					t.Fatal("scalar verification mutated parsed members")
				}
			}
			// Force the changed scalar path even for batches normally using MultiExp.
			var tweak bls12381.G2Affine
			tweak.ScalarMultiplicationBase(big.NewInt(1234567))
			members[0].sig.Add(&members[0].sig, &tweak)
			members[len(members)-1].sig.Sub(&members[len(members)-1].sig, &tweak)
			if ok, err := randomizedAggregatePairingScalarOK(members, payload); err != nil || ok {
				t.Fatalf("cancelling invalid shares: %t %v", ok, err)
			}
		})
	}
}

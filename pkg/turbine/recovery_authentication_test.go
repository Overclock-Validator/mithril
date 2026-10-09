package turbine

import (
	"crypto/ed25519"
	"encoding/binary"
	"fmt"
	"sort"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/klauspost/reedsolomon"
	"github.com/stretchr/testify/require"
)

func resignRecoveryFixture(t *testing.T, packets [][]byte) {
	t.Helper()
	nodes, err := buildMerkleTree(packets)
	require.NoError(t, err)
	root := nodes[len(nodes)-1]
	sig := ed25519.Sign(ed25519.PrivateKey(benchmarkLeaderKey()), root[:])
	for i, p := range packets {
		s, err := ParseShred(p)
		require.NoError(t, err)
		shard, err := s.erasureShard()
		require.NoError(t, err)
		start := shredSignatureSize
		if s.Type == ShredTypeCode {
			start = codingHeaderSize
		}
		_, chained, _, _ := merkleVariantInfo(s.Variant)
		offset := start + len(shard)
		if chained {
			offset += merkleRootSize
		}
		copy(p[:shredSignatureSize], sig)
		writeMerkleProof(p[offset:], nodes, i, len(packets))
	}
}

func recoveryFixtureState(t testing.TB, packets [][]byte, missing int) (*SlotAssembler, *slotState) {
	t.Helper()
	state := &slotState{slot: 10, shreds: make(map[uint32]*Shred), fecSets: make(map[uint32]*fecState), lastIndex: ^uint32(0)}
	// Exactly 32 received shreds: also force reconstruction of missing parity.
	for i, p := range packets {
		if i < missing || i >= 32+missing {
			continue
		}
		s, err := ParseShred(p)
		require.NoError(t, err)
		state.slot = s.Slot
		state.shredVer = s.Version
		if s.Type == ShredTypeData {
			require.NoError(t, state.addDataShred(s))
		} else {
			require.NoError(t, state.addCodingShred(s))
		}
	}
	return NewSlotAssembler(), state
}

func TestRecoveredFECAuthentication(t *testing.T) {
	for _, resigned := range []bool{false, true} {
		for _, missing := range []int{1, 3, 32} {
			for _, attack := range []string{"valid", "received_parity", "missing_parity", "recovered_bytes"} {
				t.Run(fmt.Sprintf("resigned=%t/missing=%d/%s", resigned, missing, attack), func(t *testing.T) {
					gen := ShredGenerator{Slot: 10, ParentSlot: 9, Version: 1}
					packets, _, _, _, err := gen.MakeShredsFromData(benchmarkLeaderKey(), benchmarkPayload(128), resigned, solana.Hash{9}, 0, 0)
					require.NoError(t, err)
					require.Len(t, packets, 64)
					if attack == "received_parity" {
						packets[32][codingHeaderSize+128] ^= 1
						resignRecoveryFixture(t, packets)
					}
					if attack == "missing_parity" {
						if missing == 32 {
							t.Skip("no missing coding shard")
						}
						packets[63][codingHeaderSize+128] ^= 1
						resignRecoveryFixture(t, packets)
					}
					// The adversarial fixtures have valid leader signatures, not corrupted
					// network packets: only RS/tree consistency is wrong.
					for _, p := range packets {
						s, e := ParseShred(p)
						require.NoError(t, e)
						require.NoError(t, s.VerifySignature(benchmarkLeaderKey().PublicKey()))
					}
					a, state := recoveryFixtureState(t, packets, missing)
					recovered, err := a.recoverFEC(state, 0)
					if attack == "received_parity" || attack == "missing_parity" {
						require.ErrorIs(t, err, ErrInvalidSignature)
						require.Empty(t, recovered)
						return
					}
					require.NoError(t, err)
					require.Len(t, recovered, missing)
					for _, s := range recovered {
						require.Equal(t, packets[s.Index], s.Payload)
						require.NoError(t, s.VerifySignature(benchmarkLeaderKey().PublicKey()))
					}
					if attack == "recovered_bytes" {
						f := state.fecSets[0]
						shards := make([][]byte, 64)
						for i, s := range f.data {
							shards[i], err = s.erasureShard()
							require.NoError(t, err)
						}
						for i, s := range f.coding {
							shards[32+int(i)], err = s.erasureShard()
							require.NoError(t, err)
						}
						for _, s := range recovered {
							shards[s.Index], err = s.erasureShard()
							require.NoError(t, err)
						}
						recovered[0].Payload[dataHeaderSize+1] ^= 1
						require.ErrorIs(t, a.authenticateRecoveredFEC(f, shards, recovered), ErrInvalidSignature)
					}
				})
			}
		}
	}
}

// Recreate coding packets from immutable Agave data packets, without signing a
// new root. Equality to the root committed by Agave checks erasure layout,
// parity coefficients, coding headers, chain roots and Merkle construction.
func TestRecoveredFECAgaveSignedCapture(t *testing.T) {
	all := agavePaddedSlot1752420Packets(t)
	sort.Slice(all, func(i, j int) bool {
		return binary.LittleEndian.Uint32(all[i][shredIndexOffset:]) < binary.LittleEndian.Uint32(all[j][shredIndexOffset:])
	})
	for group := 0; group < 4; group++ {
		packets := make([][]byte, 64)
		shards := make([][]byte, 64)
		var first *Shred
		for i := 0; i < 32; i++ {
			// Captured repair packets may append a four-byte nonce, which is
			// transport metadata rather than part of the authenticated shred.
			packets[i] = all[group*32+i][:dataPayloadSize]
			s, err := ParseShred(packets[i])
			require.NoError(t, err)
			if i == 0 {
				first = s
			}
			shards[i], err = s.erasureShard()
			require.NoError(t, err)
		}
		codeVariant, ok := merkleCounterpartVariant(first.Variant, ShredTypeCode)
		require.True(t, ok)
		for i := 0; i < 32; i++ {
			p := make([]byte, codingPayloadSize)
			copy(p[:codingNumDataOffset], first.Payload[:codingNumDataOffset])
			p[shredVariantOffset] = codeVariant
			binary.LittleEndian.PutUint32(p[shredIndexOffset:], first.FECSetIndex+uint32(i))
			binary.LittleEndian.PutUint16(p[codingNumDataOffset:], 32)
			binary.LittleEndian.PutUint16(p[codingNumCodingOffset:], 32)
			binary.LittleEndian.PutUint16(p[codingPositionOffset:], uint16(i))
			copy(p[codingHeaderSize+len(shards[0]):], first.Payload[shredSignatureSize+len(shards[0]):])
			packets[32+i] = p
			shards[32+i] = p[codingHeaderSize : codingHeaderSize+len(shards[0])]
		}
		enc, err := reedsolomon.New(32, 32)
		require.NoError(t, err)
		require.NoError(t, enc.Encode(shards))
		nodes, err := buildMerkleTree(packets)
		require.NoError(t, err)
		root, err := first.MerkleRoot()
		require.NoError(t, err)
		require.Equal(t, root, nodes[len(nodes)-1])
		proofSize, chained, _, _ := merkleVariantInfo(codeVariant)
		offset := codingHeaderSize + len(shards[0])
		if chained {
			offset += merkleRootSize
		}
		for i := 32; i < 64; i++ {
			require.Equal(t, int(proofSize), writeMerkleProof(packets[i][offset:], nodes, i, 64))
		}
		for _, missing := range []int{1, 3, 32} {
			a, state := recoveryFixtureState(t, packets, missing)
			got, err := a.recoverFEC(state, first.FECSetIndex)
			require.NoError(t, err)
			require.Len(t, got, missing)
			for _, s := range got {
				end := dataPayloadSize
				_, _, resigned, _ := merkleVariantInfo(s.Variant)
				if resigned {
					// Hop signatures can differ by relay; Agave copies the
					// received coding template's signature, not a lost one.
					end -= shredSignatureSize
					want, err := first.RetransmitterSignature()
					require.NoError(t, err)
					got, err := s.RetransmitterSignature()
					require.NoError(t, err)
					require.Equal(t, want, got)
				}
				require.Equal(t, packets[s.Index-first.FECSetIndex][:end], s.Payload[:end])
				actual, err := s.MerkleRoot()
				require.NoError(t, err)
				require.Equal(t, root, actual)
			}
		}
	}
}

func BenchmarkAuthenticatedFECRecovery(b *testing.B) {
	for _, missing := range []int{1, 3, 32} {
		b.Run(fmt.Sprintf("missing_data=%d", missing), func(b *testing.B) {
			gen := ShredGenerator{Slot: 10, ParentSlot: 9, Version: 1}
			packets, _, _, _, err := gen.MakeShredsFromData(benchmarkLeaderKey(), benchmarkPayload(128), false, solana.Hash{9}, 0, 0)
			require.NoError(b, err)
			a, state := recoveryFixtureState(b, packets, missing)
			_, err = a.recoverFEC(state, 0)
			require.NoError(b, err)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				recovered, err := a.recoverFEC(state, 0)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkRecoveredShredsSink = recovered
			}
		})
	}
}

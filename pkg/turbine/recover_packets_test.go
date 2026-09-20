package turbine

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"fmt"
	"os"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestRecoverMerkleDataPackets(t *testing.T) {
	for _, resigned := range []bool{false, true} {
		name := "chained"
		if resigned {
			name = "resigned"
		}
		t.Run(name, func(t *testing.T) {
			leader := testShredLeader(t)
			g := ShredGenerator{Slot: 100, ParentSlot: 99, Version: 7}
			packets, _, err := g.makeFECBatch(leader, []byte("repair packet proof"), dataCapacity(6, resigned), 6, resigned, 1, 0, resigned, solana.Hash{9}, 32, 64)
			require.NoError(t, err)
			for _, missing := range []int{1, 16, 32} {
				input := append([][]byte(nil), packets[missing:32]...)
				input = append(input, packets[32:32+missing]...)
				original := make([][]byte, len(input))
				for i := range input {
					original[i] = bytes.Clone(input[i])
				}
				recovered, err := recoverMerkleDataPackets(input)
				require.NoError(t, err)
				require.Len(t, recovered, missing)
				for i, packet := range recovered {
					require.Equal(t, packets[i], packet, "recovered wire packet differs")
					shred, err := ParseShred(packet)
					require.NoError(t, err)
					require.NoError(t, shred.VerifySignature(leader.PublicKey()))
				}
				require.Equal(t, original, input, "recovery modified inputs")
			}
		})
	}
}

func TestRecoverMerkleDataPacketsRejectsInvalidBatch(t *testing.T) {
	leader := testShredLeader(t)
	g := ShredGenerator{Slot: 100, ParentSlot: 99, Version: 7}
	packets, _, err := g.makeFECBatch(leader, []byte("repair"), dataCapacity(6, true), 6, true, 1, 0, true, solana.Hash{9}, 32, 64)
	require.NoError(t, err)
	for _, name := range []string{"short", "too few", "slot", "signature", "proof", "layout", "coding index", "duplicate"} {
		t.Run(name, func(t *testing.T) {
			input := make([][]byte, 32)
			for i := range input {
				input[i] = bytes.Clone(packets[i+1])
			}
			switch name {
			case "short":
				input[0] = input[0][:10]
			case "too few":
				input = input[1:]
			case "slot":
				input[0][shredSlotOffset]++
			case "signature":
				input[0][0] ^= 1
			case "proof":
				input[0][1100] ^= 1
			case "layout":
				binary.LittleEndian.PutUint16(input[31][codingNumDataOffset:], 65535)
			case "coding index":
				binary.LittleEndian.PutUint16(input[31][codingPositionOffset:], 200)
			case "duplicate":
				input = append(input[:31], input[0])
			}
			out, err := recoverMerkleDataPackets(input)
			require.Error(t, err)
			require.Empty(t, out)
		})
	}
}

func TestSpoolRecoversMissingDataPacket(t *testing.T) {
	g := ShredGenerator{Slot: 100, ParentSlot: 99, Version: 7}
	packets, _, err := g.makeFECBatch(testShredLeader(t), []byte("repair"), dataCapacity(6, true), 6, true, 1, 0, true, solana.Hash{9}, 32, 64)
	require.NoError(t, err)
	spool, err := OpenShredSpool(t.TempDir(), 1<<20)
	require.NoError(t, err)
	defer spool.Close()
	for _, packet := range packets[1:33] {
		shred, err := ParseShred(packet)
		require.NoError(t, err)
		require.True(t, spool.AppendShred(shred, packet))
	}
	got, ok, err := spool.RecoverDataShred(100, 32)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, packets[0], got)
	stored, err := spool.ReadSlot(100)
	require.NoError(t, err)
	require.Len(t, stored, 33)
	got, ok, err = spool.RecoverDataShred(100, 32)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, packets[0], got)
	spool.SetFloor(101)
	require.Nil(t, spool.recovery[100], "retention must remove the record index")
	_, ok, err = spool.RecoverDataShred(100, 32)
	require.NoError(t, err)
	require.False(t, ok, "recovery must not resurrect retired slots")
	spool.Close()
	_, _, err = spool.RecoverDataShred(100, 32)
	require.Error(t, err)
}

func TestRecoveryRejectsSignedInvalidParity(t *testing.T) {
	leader := testShredLeader(t)
	g := ShredGenerator{Slot: 100, ParentSlot: 99, Version: 7}
	packets, _, err := g.makeFECBatch(leader, []byte("repair"), dataCapacity(6, false), 6, false, 1, 0, false, solana.Hash{9}, 0, 0)
	require.NoError(t, err)
	// A leader can sign a consistent Merkle tree containing invalid parity.
	packets[40][codingHeaderSize+100] ^= 1
	tree, err := buildMerkleTree(packets)
	require.NoError(t, err)
	root := tree[len(tree)-1]
	signature := ed25519.Sign(ed25519.PrivateKey(leader), root[:])
	for i, packet := range packets {
		copy(packet[:shredSignatureSize], signature)
		proof := makeMerkleProof(tree, i, len(packets))
		offset := len(packet) - len(proof)*merkleProofEntrySize
		for j, entry := range proof {
			copy(packet[offset+j*merkleProofEntrySize:], entry[:])
		}
		shred, err := ParseShred(packet)
		require.NoError(t, err)
		require.NoError(t, shred.VerifySignature(leader.PublicKey()))
	}
	out, err := recoverMerkleDataPackets(packets[1:33])
	require.EqualError(t, err, "recovered Merkle root mismatch")
	require.Empty(t, out)
}

func TestSpoolRecoveryIndexLifecycle(t *testing.T) {
	spool, err := OpenShredSpool(t.TempDir(), 1<<20)
	require.NoError(t, err)
	defer spool.Close()
	for version := uint16(1); version <= 2; version++ {
		spool.DiscardSlot(100)
		require.Nil(t, spool.recovery[100])
		g := ShredGenerator{Slot: 100, ParentSlot: 99, Version: version}
		packets, _, err := g.makeFECBatch(testShredLeader(t), []byte("repair"), dataCapacity(6, false), 6, false, 1, 0, false, solana.Hash{9}, 0, 0)
		require.NoError(t, err)
		for _, packet := range packets[1:33] {
			shred, err := ParseShred(packet)
			require.NoError(t, err)
			require.True(t, spool.AppendShred(shred, packet))
		}
		got, ok, err := spool.RecoverDataShred(100, 0)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, packets[0], got, "a replacement slot must not use old record offsets")
		_, err = spool.ReadSlot(100)
		require.NoError(t, err)
		data, err := os.ReadFile(spool.pathFor(100))
		require.NoError(t, err)
		corrupt := bytes.Clone(data)
		corrupt[len(spoolFileMagic)+spoolRecordHeaderSize+100] ^= 1
		require.NoError(t, os.WriteFile(spool.pathFor(100), corrupt, 0600))
		got, ok, err = spool.RecoverDataShred(100, 1)
		require.Error(t, err)
		require.False(t, ok)
		require.Empty(t, got)
		require.Nil(t, spool.recovery[100], "a stale index must be rebuilt after a record read fails")
		require.NoError(t, os.WriteFile(spool.pathFor(100), data, 0600))
		got, ok, err = spool.RecoverDataShred(100, 1)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, packets[1], got)
	}
}

func TestSpoolIncompleteFECIsNotFound(t *testing.T) {
	g := ShredGenerator{Slot: 100, ParentSlot: 99, Version: 7}
	packets, _, err := g.makeFECBatch(testShredLeader(t), []byte("repair"), dataCapacity(6, false), 6, false, 1, 0, false, solana.Hash{9}, 0, 0)
	require.NoError(t, err)
	spool, err := OpenShredSpool(t.TempDir(), 1<<20)
	require.NoError(t, err)
	defer spool.Close()
	for _, packet := range packets[2:33] {
		shred, err := ParseShred(packet)
		require.NoError(t, err)
		require.True(t, spool.AppendShred(shred, packet))
	}
	_, ok, err := spool.RecoverDataShred(100, 0)
	require.NoError(t, err, "ordinary partial reception is not a storage failure")
	require.False(t, ok)
	indexed := spool.recovery[100][0]
	require.NotNil(t, indexed)
	shred, err := ParseShred(packets[1])
	require.NoError(t, err)
	require.True(t, spool.AppendShred(shred, packets[1]))
	require.Same(t, indexed, spool.recovery[100][0], "an append must update rather than discard the recovery index")
	got, ok, err := spool.RecoverDataShred(100, 0)
	require.NoError(t, err)
	require.True(t, ok, "new packets must allow a previously incomplete batch to recover")
	require.Equal(t, packets[0], got)
}

func BenchmarkSpoolMissingPacketRecovery(b *testing.B) {
	for _, warm := range []bool{false, true} {
		name := "cold"
		if warm {
			name = "indexed"
		}
		b.Run(name, func(b *testing.B) { benchmarkSpoolRecovery(b, warm) })
	}
}

func benchmarkSpoolRecovery(b *testing.B, warm bool) {
	for _, sets := range []int{1, 32, 128, 512, 1024} {
		b.Run(fmt.Sprint(sets), func(b *testing.B) {
			leader := solana.NewWallet().PrivateKey
			g := ShredGenerator{Slot: 100, ParentSlot: 99, Version: 7}
			var packets [][]byte
			for i := 0; i < sets; i++ {
				batch, _, err := g.makeFECBatch(leader, []byte("repair"), dataCapacity(6, false), 6, false, 1, 0, false, solana.Hash{9}, uint32(i*32), uint32(i*32))
				if err != nil {
					b.Fatal(err)
				}
				packets = append(packets, batch...)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				spool, err := OpenShredSpool(b.TempDir(), 128<<20)
				if err != nil {
					b.Fatal(err)
				}
				for _, packet := range packets {
					shred, err := ParseShred(packet)
					if err != nil {
						b.Fatal(err)
					}
					if shred.Type == ShredTypeData && shred.Index == 7 {
						continue
					}
					if !spool.AppendShred(shred, packet) {
						b.Fatal("packet not stored")
					}
				}
				if warm {
					if _, _, err := spool.RecoverDataShred(100, maxDataShredsPerSlot-1); err != nil {
						b.Fatal(err)
					}
				}
				b.StartTimer()
				_, ok, err := spool.RecoverDataShred(100, 7)
				b.StopTimer()
				spool.Close()
				if err != nil || !ok {
					b.Fatalf("recovery: present=%t err=%v", ok, err)
				}
				b.StartTimer()
			}
		})
	}
}

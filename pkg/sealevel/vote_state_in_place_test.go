package sealevel

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/stretchr/testify/require"
)

func TestWriteVoteStateInPlaceOwnership(t *testing.T) {
	old := sbpf.UsePool
	t.Cleanup(func() { sbpf.UsePool = old })
	for _, pooled := range []bool{false, true} {
		sbpf.UsePool = pooled
		for _, version := range []uint32{VoteStateVersionV0_23_5, VoteStateVersionV1_14_11, VoteStateVersionCurrent, VoteStateVersionV4} {
			t.Run(fmt.Sprintf("pool=%v/version=%d", pooled, version), func(t *testing.T) {
				state := &VoteStateVersions{Type: version}
				owned, err := MarshalVersionedVoteState(state)
				require.NoError(t, err)
				want := bytes.Clone(owned)
				data := bytes.Repeat([]byte{0x42}, len(want)+64)
				require.NoError(t, WriteVersionedVoteStateInPlace(data, state))
				require.Equal(t, append(bytes.Clone(want), make([]byte, 64)...), data)
				retained := bytes.Clone(data)
				short := bytes.Clone(want[:len(want)-1])
				before := bytes.Clone(short)
				require.Error(t, WriteVersionedVoteStateInPlace(short, state))
				require.Equal(t, before, short, "failed write must be atomic")

				// Exercise scratch reuse and growth, retaining both an account copy and
				// public Marshal output. Neither may alias recycled scratch memory.
				for i := 0; i < 64; i++ {
					next := &VoteStateVersions{Type: VoteStateVersionV4, V4: VoteState4{
						PendingDelegatorRewards: uint64(i + 1),
						EpochCredits:            make([]EpochCredits, 256),
					}}
					require.NoError(t, WriteVersionedVoteStateInPlace(make([]byte, 8192), next))
				}
				require.Equal(t, retained, data)
				require.Equal(t, want, owned)
			})
		}
	}
}

func TestWriteVoteStateInPlaceConcurrent(t *testing.T) {
	old := sbpf.UsePool
	sbpf.UsePool = true
	t.Cleanup(func() { sbpf.UsePool = old })
	var wg sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			state := &VoteStateVersions{Type: VoteStateVersionV4, V4: VoteState4{
				PendingDelegatorRewards: uint64(worker + 1),
			}}
			owned, err := MarshalVersionedVoteState(state)
			if err != nil {
				t.Error(err)
				return
			}
			want := make([]byte, VoteStateV4Size)
			copy(want, owned)
			for i := 0; i < 100; i++ {
				data := make([]byte, VoteStateV4Size)
				if err := WriteVersionedVoteStateInPlace(data, state); err != nil {
					t.Error(err)
					return
				}
				if !bytes.Equal(data, want) || !bytes.Equal(owned, want[:len(owned)]) {
					t.Error("concurrent scratch reuse changed owned bytes")
					return
				}
			}
		}()
	}
	wg.Wait()
}

func BenchmarkWriteVoteStateInPlace(b *testing.B) {
	old := sbpf.UsePool
	b.Cleanup(func() { sbpf.UsePool = old })
	for _, pooled := range []bool{true, false} {
		b.Run(fmt.Sprintf("pool=%v", pooled), func(b *testing.B) {
			sbpf.UsePool = pooled
			state := &VoteStateVersions{Type: VoteStateVersionV4, V4: VoteState4{
				EpochCredits: make([]EpochCredits, MaxEpochCreditsHistory),
			}}
			want, err := MarshalVersionedVoteState(state)
			if err != nil {
				b.Fatal(err)
			}
			data := make([]byte, VoteStateV4Size)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := WriteVersionedVoteStateInPlace(data, state); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if !bytes.Equal(data[:len(want)], want) || !bytes.Equal(data[len(want):], make([]byte, len(data)-len(want))) {
				b.Fatal("serialized output changed")
			}
		})
	}
}

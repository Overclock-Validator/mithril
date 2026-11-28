package sealevel

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/sbpf"
)

func TestVoteStateScratchConcurrentOwnership(t *testing.T) {
	old := sbpf.UsePool
	sbpf.UsePool = true
	t.Cleanup(func() { sbpf.UsePool = old })
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for worker := 0; worker < 16; worker++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			state := &VoteStateVersions{Type: VoteStateVersionV4, V4: VoteState4{PendingDelegatorRewards: uint64(id + 1), EpochCredits: make([]EpochCredits, 256)}}
			want, err := MarshalVersionedVoteState(state)
			if err != nil {
				errs <- err
				return
			}
			padded := append(bytes.Clone(want), make([]byte, 64)...)
			for i := 0; i < 64; i++ {
				short := bytes.Repeat([]byte{0xa5}, len(want)-1)
				original := bytes.Clone(short)
				if err := WriteVersionedVoteStateInPlace(short, state); err == nil || !bytes.Equal(short, original) {
					errs <- fmt.Errorf("failed write changed data")
					return
				}
				data := bytes.Repeat([]byte{0xa5}, len(padded))
				if err := WriteVersionedVoteStateInPlace(data, state); err != nil {
					errs <- err
					return
				}
				if !bytes.Equal(data, padded) {
					errs <- fmt.Errorf("concurrent output changed")
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

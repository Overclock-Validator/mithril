package turbine

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestCompletedShredOrderPreservesSparseAndExtraTails(t *testing.T) {
	for _, indexes := range [][]uint32{{0, 1, 2}, {0, 2, 3}, {0, 1, 2, 9}, {1, 3, 7}} {
		s := &slotState{haveLast: true, lastIndex: 2, shreds: make(map[uint32]*Shred)}
		for _, idx := range indexes {
			s.shreds[idx] = &Shred{Index: idx, Type: ShredTypeData}
		}
		got := s.orderedShreds()
		require.Len(t, got, len(indexes))
		for i, idx := range indexes {
			require.Same(t, s.shreds[idx], got[i])
		}
	}
}

func TestOrderedEntryDecodeMatchesPublicUnorderedDecode(t *testing.T) {
	packets := agavePaddedSlot1752420Packets(t)
	s := &slotState{shreds: make(map[uint32]*Shred)}
	var shuffled []*Shred
	for _, packet := range packets {
		shred, err := ParseShred(packet)
		require.NoError(t, err)
		s.shreds[shred.Index] = shred
		shuffled = append(shuffled, shred)
	}
	for i, j := 0, len(shuffled)-1; i < j; i, j = i+1, j-1 {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	}
	want, parent, footer, err := DecodeEntriesAndAlpenglowMarkersFromDataShreds(shuffled)
	require.NoError(t, err)
	got, gotParent, gotFooter, err := decodeEntriesFromOrderedDataShreds(s.orderedShreds(), nil)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Equal(t, parent, gotParent)
	require.Equal(t, footer, gotFooter)
}

func TestAuthenticatedFECRootRejectsChangedInputAndReplacement(t *testing.T) {
	shred, leader := buildSignedTestShred(t, 100, 42)
	var verifier shredSigCache
	root, err := verifier.verifyShredRoot(shred, leader)
	require.NoError(t, err)
	f := &fecState{data: map[uint32]*Shred{0: shred}}
	f.rememberAuthenticatedRoot(shred, root)
	cached := f.rootCache
	require.True(t, cached.matches(shred))
	for i := range shred.Payload {
		shred.Payload[i] ^= 1
		require.False(t, cached.matches(shred), "payload byte %d", i)
		shred.Payload[i] ^= 1
	}
	for _, mutate := range []func(*Shred){
		func(s *Shred) { s.Variant ^= 1 }, func(s *Shred) { s.Type = ShredTypeCode },
		func(s *Shred) { s.Index++ }, func(s *Shred) { s.FECSetIndex++ },
		func(s *Shred) { s.NumDataShreds++ }, func(s *Shred) { s.Position++ },
	} {
		original := *shred
		mutate(shred)
		require.False(t, cached.matches(shred))
		*shred = original
	}
	shred.Payload[dataHeaderSize] ^= 1
	want, err := shred.MerkleRoot()
	require.NoError(t, err)
	got, ok, err := f.merkleRoot()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, want, got)
	require.NotEqual(t, root, got)
	_, err = verifier.verifyShredRoot(shred, leader)
	require.ErrorIs(t, err, ErrInvalidSignature)
	shred.Payload[dataHeaderSize] ^= 1
	replacement := *shred
	require.False(t, cached.matches(&replacement))
	f.data[0] = &replacement
	got, ok, err = f.merkleRoot()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, root, got)
}

func TestAuthenticatedFECRootAdmissionAndReset(t *testing.T) {
	shred, leader := buildSignedTestShred(t, 100, 42)
	var verifier shredSigCache
	root, err := verifier.verifyShredRoot(shred, leader)
	require.NoError(t, err)
	a := NewSlotAssembler()
	work, err := a.addShredFromWithRoot(shred, false, &root)
	require.NoError(t, err)
	require.NotNil(t, work)
	f := work.state.fecSets[0]
	require.True(t, f.rootCache.matches(shred))
	// A duplicate cannot overwrite the admitted root, even while completion is
	// retried after cancellation. Reset starts a separate FEC generation.
	a.abortCompletion(work)
	wrong := solana.Hash{99}
	_, err = a.addShredFromWithRoot(shred, true, &wrong)
	require.NoError(t, err)
	require.Equal(t, root, f.rootCache.root)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.True(t, a.processCompletion(ctx, work).canceled)
	a.ResetSlot(100)
	work, err = a.addShredFrom(shred, false)
	require.NoError(t, err)
	require.NotNil(t, work)
	require.Nil(t, work.state.fecSets[0].rootCache)
}

func referenceFECRoot(f *fecState) (solana.Hash, bool, error) {
	for _, idx := range sortedUint32Keys(f.data) {
		s := f.data[idx]
		if s == nil || s.Recovered {
			continue
		}
		root, err := s.MerkleRoot()
		if errors.Is(err, ErrUnsupportedShred) {
			continue
		}
		return root, err == nil, err
	}
	for _, idx := range sortedUint16Keys(f.coding) {
		s := f.coding[idx]
		if s == nil {
			continue
		}
		root, err := s.MerkleRoot()
		if errors.Is(err, ErrUnsupportedShred) {
			continue
		}
		return root, err == nil, err
	}
	return solana.Hash{}, false, nil
}

func FuzzFECRootSelectionMatchesSortedReference(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7})
	f.Add([]byte{9, 0, 0, 0, 2, 1, 3, 7})
	f.Fuzz(func(t *testing.T, choices []byte) {
		if len(choices) > 512 {
			t.Skip()
		}
		state := &fecState{data: make(map[uint32]*Shred), coding: make(map[uint16]*Shred)}
		for i, choice := range choices {
			s := &Shred{Variant: merkleDataVariant, Type: ShredType(choice % 3), Payload: bytes.Repeat([]byte{choice}, dataPayloadSize)}
			switch choice % 5 {
			case 0:
				s = nil
			case 1:
				s.Recovered = true
			case 2:
				s.Variant = legacyDataVariant
			case 3:
				s.Payload = s.Payload[:20]
			}
			if i%2 == 0 {
				state.data[uint32(choice)] = s
			} else {
				state.coding[uint16(choice)] = s
			}
		}
		want, wantOK, wantErr := referenceFECRoot(state)
		got, gotOK, gotErr := state.merkleRoot()
		require.Equal(t, want, got)
		require.Equal(t, wantOK, gotOK)
		if wantErr == nil {
			require.NoError(t, gotErr)
		} else {
			require.EqualError(t, gotErr, wantErr.Error())
		}
	})
}

func TestFECRootCacheKeepsDeterministicDataPrecedence(t *testing.T) {
	packets := append(localnetMerkleShreds(t, "d"), localnetMerkleShreds(t, "c")...)
	f := &fecState{data: make(map[uint32]*Shred), coding: make(map[uint16]*Shred)}
	var shreds []*Shred
	for _, packet := range packets {
		s, err := ParseShred(packet)
		require.NoError(t, err)
		if s.FECSetIndex != 0 {
			continue
		}
		shreds = append(shreds, s)
	}
	require.NotEmpty(t, shreds)
	sort.Slice(shreds, func(i, j int) bool {
		if shreds[i].Type != shreds[j].Type {
			return shreds[i].Type == ShredTypeCode
		}
		return shreds[i].Index > shreds[j].Index
	})
	for _, s := range shreds {
		root, err := s.MerkleRoot()
		require.NoError(t, err)
		if s.Type == ShredTypeData {
			f.data[s.Index-s.FECSetIndex] = s
		} else {
			f.coding[s.Position] = s
		}
		f.rememberAuthenticatedRoot(s, root)
		want, wantOK, wantErr := referenceFECRoot(f)
		got, gotOK, gotErr := f.merkleRoot()
		require.Equal(t, wantErr, gotErr)
		require.Equal(t, wantOK, gotOK)
		require.Equal(t, want, got)
	}
	for _, s := range f.data {
		s.Recovered = true
	}
	want, wantOK, wantErr := referenceFECRoot(f)
	got, gotOK, gotErr := f.merkleRoot()
	require.Equal(t, wantErr, gotErr)
	require.Equal(t, wantOK, gotOK)
	require.Equal(t, want, got)
}

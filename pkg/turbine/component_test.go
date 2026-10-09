package turbine_test

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/rewardcerts"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func testLeader(t *testing.T) solana.PrivateKey {
	t.Helper()
	var seed [32]byte
	for i := range seed {
		seed[i] = byte(i + 1)
	}
	return solana.PrivateKey(ed25519.NewKeyFromSeed(seed[:]))
}

func TestBlockComponentEntryBatchRoundTrip(t *testing.T) {
	entry := turbine.Entry{
		NumHashes: 1,
		Hash:      solana.Hash{9},
	}
	component, err := turbine.NewEntryBatch([]turbine.Entry{entry})
	require.NoError(t, err)

	raw, err := turbine.MarshalBlockComponent(component)
	require.NoError(t, err)
	decoded, err := turbine.UnmarshalBlockComponent(raw)
	require.NoError(t, err)
	require.True(t, decoded.IsEntryBatch())
	require.Len(t, decoded.EntryBatch, 1)
	require.Equal(t, entry.NumHashes, decoded.EntryBatch[0].NumHashes)
	require.Equal(t, entry.Hash, decoded.EntryBatch[0].Hash)
}

func TestBlockComponentMarkerRoundTrip(t *testing.T) {
	parentID := solana.Hash{7}
	component := turbine.NewBlockHeader(99, parentID)

	raw, err := turbine.MarshalBlockComponent(component)
	require.NoError(t, err)
	require.True(t, turbine.InferIsBlockMarker(raw))

	decoded, err := turbine.UnmarshalBlockComponent(raw)
	require.NoError(t, err)
	require.True(t, decoded.IsMarker())
	require.Equal(t, turbine.MarkerBlockHeader, decoded.Marker.Kind)
	require.Equal(t, uint64(99), decoded.Marker.Header.ParentSlot)
	require.Equal(t, parentID, decoded.Marker.Header.ParentBlockID)
}

func TestBlockComponentRejectsImpossibleEntryCount(t *testing.T) {
	raw := make([]byte, 8)
	binary.LittleEndian.PutUint64(raw, ^uint64(0))

	_, err := turbine.UnmarshalBlockComponent(raw)
	require.Error(t, err)
	require.True(t, errors.Is(err, turbine.ErrInvalidBlockComponent))
}

func TestBlockComponentRejectsTrailingBytes(t *testing.T) {
	entryComponent, err := turbine.NewEntryBatch([]turbine.Entry{{NumHashes: 1, Hash: solana.Hash{9}}})
	require.NoError(t, err)
	entryRaw, err := turbine.MarshalBlockComponent(entryComponent)
	require.NoError(t, err)
	_, err = turbine.UnmarshalBlockComponent(append(entryRaw, 0))
	require.Error(t, err)
	require.True(t, errors.Is(err, turbine.ErrInvalidBlockComponent))

	markerRaw, err := turbine.MarshalBlockComponent(turbine.NewBlockHeader(99, solana.Hash{7}))
	require.NoError(t, err)
	_, err = turbine.UnmarshalBlockComponent(append(markerRaw, 0))
	require.Error(t, err)
	require.True(t, errors.Is(err, turbine.ErrInvalidBlockComponent))
}

func TestBlockComponentRejectsOversizedGenesisBitmap(t *testing.T) {
	inner := make([]byte, 240)
	binary.LittleEndian.PutUint64(inner[232:240], ^uint64(0))
	raw := make([]byte, 0, 8+2+3+len(inner))
	raw = append(raw, make([]byte, 8)...)
	raw = append(raw, 1, 0) // versioned marker v1
	raw = append(raw, byte(turbine.MarkerGenesisCertificate), byte(len(inner)), byte(len(inner)>>8))
	raw = append(raw, inner...)

	_, err := turbine.UnmarshalBlockComponent(raw)
	require.Error(t, err)
	require.True(t, errors.Is(err, turbine.ErrInvalidBlockComponent))
}

func TestShredEntryBatchRoundTrip(t *testing.T) {
	leader := testLeader(t)
	entry := turbine.Entry{NumHashes: 1, Hash: solana.Hash{3}}
	component, err := turbine.NewEntryBatch([]turbine.Entry{entry})
	require.NoError(t, err)

	shredder := turbine.Shredder{
		Slot:          100,
		ParentSlot:    95,
		Version:       42,
		ReferenceTick: 5,
	}
	batch, _, _, err := shredder.MakeMerkleShredsFromComponent(
		leader,
		component,
		true,
		solana.Hash{1},
		0,
		0,
	)
	require.NoError(t, err)
	require.NotEmpty(t, batch.DataShreds)
	require.NotEmpty(t, batch.CodeShreds)

	leaderPub := leader.PublicKey()
	for _, shred := range append(batch.DataShreds, batch.CodeShreds...) {
		require.NoError(t, shred.VerifySignature(leaderPub))
	}

	components, err := turbine.DecodeComponentsFromDataShreds(batch.DataShreds)
	require.NoError(t, err)
	require.Len(t, components, 1)
	require.True(t, components[0].IsEntryBatch())
	require.Equal(t, entry.NumHashes, components[0].EntryBatch[0].NumHashes)
}

func TestShredMultiFECEntryBatchRoundTrip(t *testing.T) {
	leader := testLeader(t)
	entries := make([]turbine.Entry, 1300)
	for i := range entries {
		entries[i] = turbine.Entry{NumHashes: 1, Hash: solana.Hash{byte(i), byte(i >> 8)}}
	}
	component, err := turbine.NewEntryBatch(entries)
	require.NoError(t, err)

	shredder := turbine.Shredder{Slot: 100, ParentSlot: 99, Version: 42, ReferenceTick: 63}
	batch, _, _, err := shredder.MakeMerkleShredsFromComponent(
		leader, component, true, solana.Hash{}, 0, 0,
	)
	require.NoError(t, err)
	require.Greater(t, len(batch.DataShreds), 32)
	for i, shred := range batch.DataShreds[:len(batch.DataShreds)-1] {
		require.False(t, shred.DataComplete(), "intermediate data shred %d ended the component", i)
	}
	require.True(t, batch.DataShreds[len(batch.DataShreds)-1].DataComplete())
	require.True(t, batch.DataShreds[len(batch.DataShreds)-1].LastInSlot())

	components, err := turbine.DecodeComponentsFromDataShreds(batch.DataShreds)
	require.NoError(t, err)
	require.Len(t, components, 1)
	require.Len(t, components[0].EntryBatch, len(entries))
	for i := range entries {
		require.Equal(t, entries[i].Hash, components[0].EntryBatch[i].Hash)
	}
}

func TestShredBlockHeaderMarkerRoundTrip(t *testing.T) {
	leader := testLeader(t)
	parentID := solana.Hash{8}
	component := turbine.NewBlockHeader(95, parentID)

	shredder := turbine.Shredder{Slot: 100, ParentSlot: 95, Version: 1}
	batch, _, _, err := shredder.MakeMerkleShredsFromComponent(
		leader,
		component,
		false,
		solana.Hash{2},
		0,
		0,
	)
	require.NoError(t, err)

	components, err := turbine.DecodeComponentsFromDataShreds(batch.DataShreds)
	require.NoError(t, err)
	require.Len(t, components, 1)
	require.True(t, components[0].IsMarker())
	require.Equal(t, parentID, components[0].Marker.Header.ParentBlockID)
}

func TestShredBlockFooterMarkerRoundTrip(t *testing.T) {
	leader := testLeader(t)
	footer := turbine.BlockFooter{
		BankHash:               solana.Hash{4},
		BlockProducerTimeNanos: 123,
		BlockUserAgent:         []byte("mithril"),
	}
	component := turbine.NewBlockFooter(footer)

	shredder := turbine.Shredder{Slot: 200, ParentSlot: 199, Version: 1, ReferenceTick: 63}
	batch, _, _, err := shredder.MakeMerkleShredsFromComponent(
		leader,
		component,
		true,
		solana.Hash{3},
		10,
		10,
	)
	require.NoError(t, err)

	components, err := turbine.DecodeComponentsFromDataShreds(batch.DataShreds)
	require.NoError(t, err)
	require.Len(t, components, 1)
	require.Equal(t, footer.BankHash, components[0].Marker.Footer.BankHash)
	require.Equal(t, footer.BlockUserAgent, components[0].Marker.Footer.BlockUserAgent)
}

func TestShredBlockFooterRewardCertsRoundTrip(t *testing.T) {
	leader := testLeader(t)
	skipRaw, err := rewardcerts.EncodeSkipRewardCertificate(rewardcerts.SkipRewardCertificate{
		Slot:      99,
		Signature: [96]byte{1, 2, 3},
		Bitmap:    []byte{4, 5},
	})
	require.NoError(t, err)
	notarRaw, err := rewardcerts.EncodeNotarRewardCertificate(rewardcerts.NotarRewardCertificate{
		Slot:      99,
		BlockID:   solana.Hash{6},
		Signature: [96]byte{7, 8, 9},
		Bitmap:    []byte{10},
	})
	require.NoError(t, err)
	footer := turbine.BlockFooter{
		BankHash:               solana.Hash{4},
		BlockProducerTimeNanos: 123,
		BlockUserAgent:         []byte("mithril"),
		SkipRewardCert:         skipRaw,
		NotarRewardCert:        notarRaw,
	}
	component := turbine.NewBlockFooter(footer)

	shredder := turbine.Shredder{Slot: 200, ParentSlot: 199, Version: 1, ReferenceTick: 63}
	batch, _, _, err := shredder.MakeMerkleShredsFromComponent(
		leader,
		component,
		true,
		solana.Hash{3},
		10,
		10,
	)
	require.NoError(t, err)

	components, err := turbine.DecodeComponentsFromDataShreds(batch.DataShreds)
	require.NoError(t, err)
	require.Len(t, components, 1)
	require.Equal(t, footer.SkipRewardCert, components[0].Marker.Footer.SkipRewardCert)
	require.Equal(t, footer.NotarRewardCert, components[0].Marker.Footer.NotarRewardCert)
}

func TestShredBlockFooterWithFinalCertAndRewardCertsRoundTrip(t *testing.T) {
	leader := testLeader(t)
	finalCert := buildAgaveFinalCertWire(t)
	skipRaw, err := rewardcerts.EncodeSkipRewardCertificate(rewardcerts.SkipRewardCertificate{
		Slot:      99,
		Signature: [96]byte{1, 2, 3},
		Bitmap:    []byte{4, 5},
	})
	require.NoError(t, err)
	notarRaw, err := rewardcerts.EncodeNotarRewardCertificate(rewardcerts.NotarRewardCertificate{
		Slot:      99,
		BlockID:   solana.Hash{6},
		Signature: [96]byte{7, 8, 9},
		Bitmap:    []byte{10},
	})
	require.NoError(t, err)
	footer := turbine.BlockFooter{
		BankHash:               solana.Hash{4},
		BlockProducerTimeNanos: 123,
		BlockUserAgent:         []byte("mithril"),
		BlockFinalCert:         finalCert,
		SkipRewardCert:         skipRaw,
		NotarRewardCert:        notarRaw,
	}
	component := turbine.NewBlockFooter(footer)

	shredder := turbine.Shredder{Slot: 200, ParentSlot: 199, Version: 1, ReferenceTick: 63}
	batch, _, _, err := shredder.MakeMerkleShredsFromComponent(
		leader,
		component,
		true,
		solana.Hash{3},
		10,
		10,
	)
	require.NoError(t, err)

	components, err := turbine.DecodeComponentsFromDataShreds(batch.DataShreds)
	require.NoError(t, err)
	require.Len(t, components, 1)
	got := components[0].Marker.Footer
	require.Equal(t, footer.BlockFinalCert, got.BlockFinalCert)
	require.Equal(t, footer.SkipRewardCert, got.SkipRewardCert)
	require.Equal(t, footer.NotarRewardCert, got.NotarRewardCert)
}

func buildAgaveFinalCertWire(t *testing.T) []byte {
	t.Helper()
	var out []byte
	var slotBuf [8]byte
	binary.LittleEndian.PutUint64(slotBuf[:], 1234567890)
	out = append(out, slotBuf[:]...)
	out = append(out, bytes.Repeat([]byte{1}, 32)...)
	out = append(out, make([]byte, 96)...)
	bitmap := bytes.Repeat([]byte{42}, 64)
	var bitmapLen [2]byte
	binary.LittleEndian.PutUint16(bitmapLen[:], uint16(len(bitmap)))
	out = append(out, bitmapLen[:]...)
	out = append(out, bitmap...)
	out = append(out, 0) // notar_aggregate None
	return out
}

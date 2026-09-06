package turbine

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/Overclock-Validator/mithril/pkg/txverify"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func testShredLeader(t *testing.T) solana.PrivateKey {
	t.Helper()
	var seed [32]byte
	for i := range seed {
		seed[i] = byte(i + 11)
	}
	return solana.PrivateKey(ed25519.NewKeyFromSeed(seed[:]))
}

func mustParseTransferTx(t *testing.T, seq uint64) solana.Transaction {
	t.Helper()
	wire := txfixture.MustSignedTransferWire(seq)
	var tx solana.Transaction
	dec := bin.NewBinDecoder(wire)
	require.NoError(t, dec.Decode(&tx))
	return tx
}

// buildAlpenglowTxEntryBatch returns a realistic mid-slot entry batch:
// two PoH entries carrying signed system transfers, matching Alpenglow's
// low-power tick model (num_hashes == 1 on every entry).
func buildAlpenglowTxEntryBatch(t *testing.T) []byte {
	t.Helper()
	var hash solana.Hash
	copy(hash[:], bytes.Repeat([]byte{0xab}, 32))

	entries := []Entry{
		{
			NumHashes: 1,
			Hash:      hash,
			Txns: []solana.Transaction{
				mustParseTransferTx(t, 0),
				mustParseTransferTx(t, 1),
				mustParseTransferTx(t, 2),
			},
		},
		{
			NumHashes: 1,
			Hash:      solana.Hash{0xcd},
			Txns: []solana.Transaction{
				mustParseTransferTx(t, 3),
				mustParseTransferTx(t, 4),
			},
		},
	}
	payload, err := marshalEntryBatch(entries)
	require.NoError(t, err)
	return payload
}

// buildAlpenglowEndingTick returns the single empty tick entry broadcast at slot end.
func buildAlpenglowEndingTick(t *testing.T) []byte {
	t.Helper()
	payload, err := marshalEntryBatch([]Entry{{
		NumHashes: 1,
		Hash:      solana.Hash{0xfe},
	}})
	require.NoError(t, err)
	return payload
}

// buildAlpenglowBlockHeaderMarker is the wincode BlockComponent payload for a header marker.
func buildAlpenglowBlockHeaderMarker(t *testing.T, parentSlot uint64, parentBlockID solana.Hash) []byte {
	t.Helper()
	body := append([]byte{1}, appendUint64LE(nil, parentSlot)...)
	body = append(body, parentBlockID[:]...)
	tlv := append([]byte{1}, appendUint16LE(nil, uint16(len(body)))...)
	tlv = append(tlv, body...)

	out := appendUint64LE(nil, 0)
	out = appendUint16LE(out, 1)
	out = append(out, tlv...)
	return out
}

func buildAlpenglowBlockFooterMarker(t *testing.T, bankHash solana.Hash) []byte {
	t.Helper()
	// BlockMarkerV1::BlockFooter = tag 0
	body := append([]byte{1}, bankHash[:]...)
	body = appendUint64LE(body, 42)
	body = append(body, byte(len("mithril")))
	body = append(body, "mithril"...)
	body = append(body, 0, 0, 0) // no final/skip/notar certs

	tlv := append([]byte{0}, appendUint16LE(nil, uint16(len(body)))...)
	tlv = append(tlv, body...)

	out := appendUint64LE(nil, 0)
	out = appendUint16LE(out, 1)
	out = append(out, tlv...)
	return out
}

func appendUint64LE(buf []byte, v uint64) []byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	return append(buf, b[:]...)
}

func appendUint16LE(buf []byte, v uint16) []byte {
	var b [2]byte
	binary.LittleEndian.PutUint16(b[:], v)
	return append(buf, b[:]...)
}

type alpenglowComponent struct {
	name         string
	payload      []byte
	isLastInSlot bool
}

func buildAlpenglowSlot(t *testing.T) []alpenglowComponent {
	t.Helper()
	parentBlockID := solana.Hash{0x11}
	bankHash := solana.Hash{0x22}
	return []alpenglowComponent{
		{name: "header", payload: buildAlpenglowBlockHeaderMarker(t, 99, parentBlockID), isLastInSlot: false},
		{name: "tx-batch", payload: buildAlpenglowTxEntryBatch(t), isLastInSlot: false},
		{name: "footer", payload: buildAlpenglowBlockFooterMarker(t, bankHash), isLastInSlot: false},
		{name: "ending-tick", payload: buildAlpenglowEndingTick(t), isLastInSlot: true},
	}
}

func TestMakeShredsFromDataRoundTrip(t *testing.T) {
	leader := testShredLeader(t)
	payload := buildAlpenglowTxEntryBatch(t)

	gen := ShredGenerator{
		Slot:          100,
		ParentSlot:    99,
		Version:       7,
		ReferenceTick: 63,
	}
	packets, root, _, _, err := gen.MakeShredsFromData(leader, payload, false, solana.Hash{5}, 0, 0)
	require.NoError(t, err)
	require.NotEmpty(t, packets)
	require.NotEqual(t, solana.Hash{}, root)

	var dataShreds []*Shred
	for _, packet := range packets {
		shred, err := ParseShred(packet)
		require.NoError(t, err)
		require.NoError(t, shred.VerifySignature(leader.PublicKey()))
		if shred.Type == ShredTypeData {
			dataShreds = append(dataShreds, shred)
		}
	}

	entries, err := decodeEntryBatchFromDataShreds(dataShreds)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Len(t, entries[0].Txns, 3)
	require.Len(t, entries[1].Txns, 2)
	for _, entry := range entries {
		require.Equal(t, uint64(1), entry.NumHashes)
		for i := range entry.Txns {
			require.NoError(t, txverify.VerifyTransaction(&entry.Txns[i]))
		}
	}
}

func TestGeneratedFECRootsMatchPacketProofs(t *testing.T) {
	unsignedBatch := dataShredsPerFECBlock * dataCapacity(proofEntriesFor32x32, false)
	signedBatch := dataShredsPerFECBlock * dataCapacity(proofEntriesFor32x32, true)
	for _, last := range []bool{false, true} {
		for _, size := range []int{0, 1, signedBatch, signedBatch + 1, unsignedBatch, unsignedBatch + 1, 2 * unsignedBatch, 2*unsignedBatch + signedBatch} {
			t.Run(fmt.Sprintf("last=%t/bytes=%d", last, size), func(t *testing.T) {
				gen := ShredGenerator{Slot: 100, ParentSlot: 99, Version: 7, ReferenceTick: 17}
				parentRoot := solana.Hash{5}
				leader := testShredLeader(t)
				batch, nextData, nextCode, err := gen.makeShredsFromData(
					leader, benchmarkPayload(size), last, parentRoot, 7, 11,
				)
				require.NoError(t, err)
				require.NotEmpty(t, batch.fecSetRoots)
				require.Len(t, batch.packets, len(batch.fecSetRoots)*(dataShredsPerFECBlock+codingShredsPerFECBlock))
				require.Equal(t, uint32(7+len(batch.fecSetRoots)*dataShredsPerFECBlock), nextData)
				require.Equal(t, uint32(11+len(batch.fecSetRoots)*codingShredsPerFECBlock), nextCode)
				require.Equal(t, batch.fecSetRoots[len(batch.fecSetRoots)-1], batch.chainedMerkleRoot)
				for i, packet := range batch.packets {
					fec := i / (dataShredsPerFECBlock + codingShredsPerFECBlock)
					shred, err := ParseShred(packet)
					require.NoError(t, err)
					root, err := shred.MerkleRoot()
					require.NoError(t, err)
					require.Equal(t, batch.fecSetRoots[fec], root)
					require.Equal(t, uint32(7+fec*dataShredsPerFECBlock), shred.FECSetIndex)
					previousRoot := parentRoot
					if fec > 0 {
						previousRoot = batch.fecSetRoots[fec-1]
					}
					embeddedRoot, err := shred.EmbeddedChainedMerkleRoot()
					require.NoError(t, err)
					require.Equal(t, previousRoot, embeddedRoot)
					require.NoError(t, shred.VerifySignature(leader.PublicKey()))
				}
			})
		}
	}
}

func TestMakeShredsFromAlpenglowBlock(t *testing.T) {
	leader := testShredLeader(t)
	gen := ShredGenerator{
		Slot:          100,
		ParentSlot:    99,
		Version:       7,
		ReferenceTick: 63,
	}

	var (
		chainedRoot          = solana.Hash{5}
		nextData      uint32 = 0
		nextCode      uint32 = 0
		allDataShreds []*Shred
	)
	for _, component := range buildAlpenglowSlot(t) {
		packets, root, newData, newCode, err := gen.MakeShredsFromData(
			leader,
			component.payload,
			component.isLastInSlot,
			chainedRoot,
			nextData,
			nextCode,
		)
		require.NoError(t, err, "component %s", component.name)
		chainedRoot = root
		nextData = newData
		nextCode = newCode

		for _, packet := range packets {
			shred, err := ParseShred(packet)
			require.NoError(t, err)
			require.NoError(t, shred.VerifySignature(leader.PublicKey()))
			if shred.Type == ShredTypeData {
				allDataShreds = append(allDataShreds, shred)
			}
		}
	}

	components, err := decodeBlockComponentsFromDataShreds(allDataShreds)
	require.NoError(t, err)
	require.Len(t, components, 4)

	require.True(t, inferIsBlockMarker(components[0]))
	require.True(t, inferIsEntryBatch(components[1]))
	require.True(t, inferIsBlockMarker(components[2]))
	require.True(t, inferIsEntryBatch(components[3]))

	txEntries, err := decodeEntryBatchFromDataShreds(filterDataShredsUpTo(allDataShreds, components[1]))
	require.NoError(t, err)
	require.Len(t, txEntries, 2)
	require.Len(t, txEntries[0].Txns, 3)

	tickEntries, err := decodeEntryBatchFromDataShreds(filterDataShredsFrom(allDataShreds, components[3]))
	require.NoError(t, err)
	require.Len(t, tickEntries, 1)
	require.Empty(t, tickEntries[0].Txns)
	require.Equal(t, uint64(1), tickEntries[0].NumHashes)

	for _, entry := range txEntries {
		for i := range entry.Txns {
			require.NoError(t, txverify.VerifyTransaction(&entry.Txns[i]))
		}
	}

	last := allDataShreds[len(allDataShreds)-1]
	require.True(t, last.LastInSlot())
}

func TestDecodeEntriesUsesUpdateParentReplayBoundary(t *testing.T) {
	leader := testShredLeader(t)
	headerParentID := solana.Hash{0x11}
	updateParentID := solana.Hash{0x22}
	bankHash := solana.Hash{0x33}
	sharedTx := mustParseTransferTx(t, 100)
	otherTx := mustParseTransferTx(t, 101)
	preComponent, err := NewEntryBatch([]Entry{{
		NumHashes: 1,
		Hash:      solana.Hash{0xa1},
		Txns:      []solana.Transaction{sharedTx},
	}})
	require.NoError(t, err)
	postComponent, err := NewEntryBatch([]Entry{{
		NumHashes: 1,
		Hash:      solana.Hash{0xa2},
		Txns:      []solana.Transaction{sharedTx, otherTx},
	}})
	require.NoError(t, err)
	tickComponent, err := NewEntryBatch([]Entry{{
		NumHashes: 1,
		Hash:      solana.Hash{0xfe},
	}})
	require.NoError(t, err)

	components := []struct {
		name         string
		component    BlockComponent
		isLastInSlot bool
	}{
		{name: "header", component: NewBlockHeader(99, headerParentID)},
		{name: "optimistic-prefix", component: preComponent},
		{name: "update-parent", component: NewUpdateParent(98, updateParentID)},
		{name: "replayed-suffix", component: postComponent},
		{name: "footer", component: NewBlockFooter(BlockFooter{BankHash: bankHash})},
		{name: "ending-tick", component: tickComponent, isLastInSlot: true},
	}

	makeDataShreds := func(slot uint64) ([]*Shred, uint32) {
		gen := ShredGenerator{
			Slot:          slot,
			ParentSlot:    99,
			Version:       7,
			ReferenceTick: 63,
		}
		var (
			chainedRoot        = solana.Hash{5}
			nextData    uint32 = 0
			nextCode    uint32 = 0
			updateStart uint32
			dataShreds  []*Shred
		)
		for _, component := range components {
			payload, err := MarshalBlockComponent(component.component)
			require.NoError(t, err, "component %s", component.name)
			if component.name == "update-parent" {
				updateStart = nextData
			}
			packets, root, newData, newCode, err := gen.MakeShredsFromData(
				leader,
				payload,
				component.isLastInSlot,
				chainedRoot,
				nextData,
				nextCode,
			)
			require.NoError(t, err, "component %s", component.name)
			chainedRoot = root
			nextData = newData
			nextCode = newCode
			for _, packet := range packets {
				shred, err := ParseShred(packet)
				require.NoError(t, err)
				if shred.Type == ShredTypeData {
					dataShreds = append(dataShreds, shred)
				}
			}
		}
		return dataShreds, updateStart
	}

	allDataShreds, updateStart := makeDataShreds(100)
	require.NotZero(t, updateStart)
	require.Zero(t, updateStart%dataShredsPerFECBlock)

	entries, parentInfo, footer, err := DecodeEntriesAndAlpenglowMarkersFromDataShreds(allDataShreds)
	require.NoError(t, err)
	require.NotNil(t, parentInfo)
	require.True(t, parentInfo.FromUpdateParent)
	require.Equal(t, uint64(98), parentInfo.ParentSlot)
	require.Equal(t, updateParentID, parentInfo.ParentBlockID)
	require.Equal(t, updateStart, parentInfo.ReplayFECSetIndex)
	require.NotNil(t, footer)
	require.Equal(t, bankHash, footer.BankHash)

	var replayTxs []solana.Transaction
	for _, entry := range entries {
		replayTxs = append(replayTxs, entry.Txns...)
	}
	require.Len(t, replayTxs, 2)
	require.Equal(t, sharedTx.Signatures[0], replayTxs[0].Signatures[0])
	require.Equal(t, otherTx.Signatures[0], replayTxs[1].Signatures[0])
	require.NotEqual(t, replayTxs[0].Signatures[0], replayTxs[1].Signatures[0])

	nonFirstShreds, _ := makeDataShreds(101)
	state := &slotState{
		slot:       101,
		parentSlot: 99,
		shredVer:   7,
		shreds:     make(map[uint32]*Shred, len(nonFirstShreds)),
	}
	for _, shred := range nonFirstShreds {
		state.shreds[shred.Index] = shred
	}
	_, err = state.block(headerParentID, true)
	require.ErrorContains(t, err, "update-parent marker is not in the first slot of its leader window")
}

func decodeEntryBatchFromDataShreds(shreds []*Shred) ([]Entry, error) {
	return DecodeEntriesFromDataShreds(shreds)
}

func decodeBlockComponentsFromDataShreds(shreds []*Shred) ([][]byte, error) {
	var components [][]byte
	var batchBytes []byte
	for _, shred := range shreds {
		if shred == nil || shred.Type != ShredTypeData {
			continue
		}
		batchBytes = append(batchBytes, shred.Data...)
		if !shred.DataComplete() {
			continue
		}
		components = append(components, append([]byte(nil), batchBytes...))
		batchBytes = nil
	}
	if len(batchBytes) != 0 {
		return nil, ErrSlotIncomplete
	}
	return components, nil
}

func inferIsEntryBatch(data []byte) bool {
	return len(data) >= 8 && binary.LittleEndian.Uint64(data[:8]) != 0
}

func inferIsBlockMarker(data []byte) bool {
	return len(data) >= 8 && binary.LittleEndian.Uint64(data[:8]) == 0 && len(data) > 8
}

func filterDataShredsUpTo(shreds []*Shred, component []byte) []*Shred {
	var out []*Shred
	var batch []byte
	for _, shred := range shreds {
		if shred.Type != ShredTypeData {
			continue
		}
		batch = append(batch, shred.Data...)
		out = append(out, shred)
		if shred.DataComplete() {
			if bytes.Equal(batch, component) {
				return out
			}
			out = nil
			batch = nil
		}
	}
	return out
}

func filterDataShredsFrom(shreds []*Shred, component []byte) []*Shred {
	var out []*Shred
	var batch []byte
	for _, shred := range shreds {
		if shred.Type != ShredTypeData {
			continue
		}
		batch = append(batch, shred.Data...)
		out = append(out, shred)
		if shred.DataComplete() {
			if bytes.Equal(batch, component) {
				return out
			}
			out = nil
			batch = nil
		}
	}
	return out
}

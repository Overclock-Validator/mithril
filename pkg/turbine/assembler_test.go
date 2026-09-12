package turbine

import (
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/fixtures"
	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/txverify"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

func TestParseMerkleDataShred(t *testing.T) {
	raw := fixtures.DataShreds(t, "mainnet", 102815960)[0]

	shred, err := ParseShred(raw)
	if err != nil {
		t.Fatalf("ParseShred returned error: %v", err)
	}

	if shred.Slot != 102815960 {
		t.Fatalf("slot = %d, want 102815960", shred.Slot)
	}
	if shred.Index != 0 {
		t.Fatalf("index = %d, want 0", shred.Index)
	}
	if shred.ParentSlot() != 102815959 {
		t.Fatalf("parent slot = %d, want 102815959", shred.ParentSlot())
	}
	if len(shred.Data) == 0 {
		t.Fatalf("expected parsed data payload")
	}
	if &shred.Data[0] != &shred.Payload[dataHeaderSize] {
		t.Fatalf("data and payload should share one owned packet copy")
	}
	originalDataByte := shred.Data[0]
	raw[dataHeaderSize] ^= 0xff
	if shred.Data[0] != originalDataByte {
		t.Fatalf("parsed shred retained the caller's reusable packet buffer")
	}
}

func TestClassifyCurrentMerkleVariants(t *testing.T) {
	for _, variant := range []byte{0x80, 0x8f, 0x90, 0x9f, 0xb0, 0xbf} {
		shredType, err := classifyVariant(variant)
		if err != nil {
			t.Fatalf("classifyVariant(0x%02x) returned error: %v", variant, err)
		}
		if shredType != ShredTypeData {
			t.Fatalf("classifyVariant(0x%02x) = %v, want data", variant, shredType)
		}
	}
	for _, variant := range []byte{0x40, 0x4f, 0x60, 0x6f, 0x70, 0x7f} {
		shredType, err := classifyVariant(variant)
		if err != nil {
			t.Fatalf("classifyVariant(0x%02x) returned error: %v", variant, err)
		}
		if shredType != ShredTypeCode {
			t.Fatalf("classifyVariant(0x%02x) = %v, want code", variant, shredType)
		}
	}
	for _, variant := range []byte{0x00, 0x50, 0xa0, 0xc0} {
		if _, err := classifyVariant(variant); !errors.Is(err, ErrUnsupportedShred) {
			t.Fatalf("classifyVariant(0x%02x) error = %v, want ErrUnsupportedShred", variant, err)
		}
	}
}

func TestMerkleShredSignatureVerification(t *testing.T) {
	packet := make([]byte, dataPayloadSize)
	packet[shredVariantOffset] = merkleDataVariant
	packet[dataFlagsOffset] = shredFlagLastShredInSlot
	binary.LittleEndian.PutUint64(packet[shredSlotOffset:shredSlotOffset+8], 10)
	binary.LittleEndian.PutUint32(packet[shredIndexOffset:shredIndexOffset+4], 0)
	binary.LittleEndian.PutUint16(packet[shredVersionOffset:shredVersionOffset+2], 1)
	binary.LittleEndian.PutUint32(packet[shredFECSetIndexOffset:shredFECSetIndexOffset+4], 0)
	binary.LittleEndian.PutUint16(packet[dataParentOffsetOffset:dataParentOffsetOffset+2], 1)
	binary.LittleEndian.PutUint16(packet[dataSizeOffset:dataSizeOffset+2], dataHeaderSize+4)
	copy(packet[dataHeaderSize:], []byte("test"))

	shred, err := ParseShred(packet)
	if err != nil {
		t.Fatalf("ParseShred returned error: %v", err)
	}
	root, err := shred.MerkleRoot()
	if err != nil {
		t.Fatalf("MerkleRoot returned error: %v", err)
	}

	var seed [32]byte
	for i := range seed {
		seed[i] = byte(i + 1)
	}
	privateKey := ed25519.NewKeyFromSeed(seed[:])
	signature := ed25519.Sign(privateKey, root[:])
	copy(packet[shredSignatureOffset:shredSignatureSize], signature)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	var leader solana.PublicKey
	copy(leader[:], publicKey)

	shred, err = ParseShred(packet)
	if err != nil {
		t.Fatalf("ParseShred signed packet returned error: %v", err)
	}
	if err := shred.VerifySignature(leader); err != nil {
		t.Fatalf("VerifySignature returned error: %v", err)
	}

	packet[dataHeaderSize] ^= 0xff
	corrupt, err := ParseShred(packet)
	if err != nil {
		t.Fatalf("ParseShred corrupt packet returned error: %v", err)
	}
	if err := corrupt.VerifySignature(leader); !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("VerifySignature corrupt error = %v, want ErrInvalidSignature", err)
	}
}

func TestSlotAssemblerBuildsBlockFromCompleteDataShreds(t *testing.T) {
	rawShreds := fixtures.DataShreds(t, "mainnet", 102815960)
	fecRoots := fecMerkleRootsFromPackets(t, rawShreds)
	var parentBlockID solana.Hash
	parentBlockID[0] = 7
	var expectedBlockID solana.Hash
	assembler := NewSlotAssembler()
	if len(fecRoots) > 0 {
		expectedBlockID = doubleMerkleBlockID(fecRoots, 102815959, parentBlockID)
		assembler.SetKnownAlpenglowBlockID(102815959, parentBlockID)
	}

	var blockSeen bool
	for idx, raw := range rawShreds {
		blk, err := assembler.AddPacket(raw)
		if err != nil {
			t.Fatalf("AddPacket(%d) returned error: %v", idx, err)
		}
		if blk == nil {
			continue
		}
		blockSeen = true
		if idx != len(rawShreds)-1 {
			t.Fatalf("assembled block before final shred: idx=%d total=%d", idx, len(rawShreds))
		}
		if blk.Slot != 102815960 {
			t.Fatalf("block slot = %d, want 102815960", blk.Slot)
		}
		if blk.SourceParentSlot != 102815959 {
			t.Fatalf("source parent slot = %d, want 102815959", blk.SourceParentSlot)
		}
		if !blk.FromLiveStream {
			t.Fatalf("expected block to be marked as live shred-stream sourced")
		}
		if !blk.TransactionSignaturesVerified() {
			t.Fatalf("expected assembler-verified block to carry the in-memory verification marker")
		}
		if len(fecRoots) == 0 && blk.HasAlpenglowBlockID {
			t.Fatalf("legacy fixture unexpectedly carried an Alpenglow block id")
		}
		if len(fecRoots) > 0 && !blk.HasAlpenglowBlockID {
			t.Fatalf("expected block to carry an Alpenglow block id")
		}
		if len(fecRoots) > 0 && solana.Hash(blk.AlpenglowBlockID) != expectedBlockID {
			t.Fatalf("alpenglow block id = %s, want %s", solana.Hash(blk.AlpenglowBlockID), expectedBlockID)
		}
		if len(blk.Transactions) != 3177 {
			t.Fatalf("transactions = %d, want 3177", len(blk.Transactions))
		}
		if len(blk.Transactions[0].Signatures) == 0 {
			t.Fatalf("first transaction has no signatures")
		}
	}

	if !blockSeen {
		t.Fatalf("assembler did not emit a completed block")
	}
}

func TestSlotAssemblerRejectsBlockThatContradictsKnownAlpenglowBlockID(t *testing.T) {
	rawShreds := fixtures.DataShreds(t, "mainnet", 102815960)
	fecRoots := fecMerkleRootsFromPackets(t, rawShreds)
	if len(fecRoots) == 0 {
		t.Skip("fixture does not carry Merkle roots")
	}

	var parentBlockID solana.Hash
	parentBlockID[0] = 7
	expectedBlockID := doubleMerkleBlockID(fecRoots, 102815959, parentBlockID)
	wrongBlockID := expectedBlockID
	wrongBlockID[0] ^= 0xff

	assembler := NewSlotAssembler()
	assembler.SetKnownAlpenglowBlockID(102815959, parentBlockID)
	assembler.SetKnownAlpenglowBlockID(102815960, wrongBlockID)

	for idx, raw := range rawShreds {
		blk, err := assembler.AddPacket(raw)
		if err != nil {
			t.Fatalf("AddPacket(%d) returned error: %v", idx, err)
		}
		if blk != nil {
			t.Fatalf("assembler emitted non-canonical block id %s, want suppressed", solana.Hash(blk.AlpenglowBlockID))
		}
	}

	assembler.mu.Lock()
	_, completed := assembler.completedSlots[102815960]
	_, active := assembler.slots[102815960]
	known := assembler.knownBlockIDs[102815960]
	assembler.mu.Unlock()
	if completed {
		t.Fatalf("non-canonical slot was marked completed")
	}
	if active {
		t.Fatalf("non-canonical slot state was retained")
	}
	if known != wrongBlockID {
		t.Fatalf("known block id = %s, want %s", known, wrongBlockID)
	}
}

func TestSlotAssemblerResetSlotClearsCompletedSlotForRepair(t *testing.T) {
	rawShreds := fixtures.DataShreds(t, "mainnet", 102815960)
	assembler := NewSlotAssembler()

	var emitted bool
	for idx, raw := range rawShreds {
		blk, err := assembler.AddPacket(raw)
		if err != nil {
			t.Fatalf("AddPacket(%d) returned error: %v", idx, err)
		}
		if blk != nil {
			emitted = true
		}
	}
	if !emitted {
		t.Fatalf("assembler did not emit completed fixture block")
	}

	assembler.ResetSlot(102815960)

	assembler.mu.Lock()
	_, completed := assembler.completedSlots[102815960]
	_, active := assembler.slots[102815960]
	assembler.mu.Unlock()
	if completed {
		t.Fatalf("slot remained completed after reset")
	}
	if active {
		t.Fatalf("slot state remained active after reset")
	}
}

func TestSlotAssemblerRecordsCompleteSlotDecodeFailure(t *testing.T) {
	rawShreds := fixtures.DataShreds(t, "mainnet", 102815960)
	if len(rawShreds) == 0 {
		t.Fatal("fixture has no shreds")
	}
	// Keep the shred structurally complete but make its transaction bytes
	// invalid. The receiver has already authenticated live packets before the
	// assembler sees them; this test exercises the post-completion decode path.
	corrupt := make([][]byte, len(rawShreds))
	for i, raw := range rawShreds {
		corrupt[i] = append([]byte(nil), raw...)
	}
	corrupt[len(corrupt)/2][dataHeaderSize+8] ^= 0xff

	assembler := NewSlotAssembler()
	var decodeErr error
	for _, raw := range corrupt {
		_, err := assembler.AddPacket(raw)
		if err != nil {
			decodeErr = err
		}
	}
	if decodeErr == nil {
		t.Fatal("corrupt complete slot unexpectedly decoded")
	}
	count, latest := assembler.SlotAssemblyErrors(102815960)
	if count == 0 || latest == "" {
		t.Fatalf("complete-slot decode failure was not retained: count=%d latest=%q", count, latest)
	}
}

func TestValidateBlockTransactionsRejectsInvalidSignature(t *testing.T) {
	rawShreds := fixtures.DataShreds(t, "mainnet", 102815960)
	assembler := NewSlotAssembler()

	var blk *block.Block
	for _, raw := range rawShreds {
		var err error
		blk, err = assembler.AddPacket(raw)
		if err != nil {
			t.Fatalf("AddPacket returned error: %v", err)
		}
	}
	if blk == nil || len(blk.Transactions) == 0 {
		t.Fatalf("assembler did not emit a block with transactions")
	}

	blk.Transactions[0].Message.RecentBlockhash[0] ^= 0xff
	if err := validateBlockTransactions(blk); err == nil {
		t.Fatalf("validateBlockTransactions accepted a mutated transaction")
	}
}

func TestFixtureRawTransactionSignatureBytes(t *testing.T) {
	rawShreds := fixtures.DataShreds(t, "mainnet", 102815960)
	var batchBytes []byte
	var tx solana.Transaction
	var rawTx []byte
	var txIndex uint64
	for _, raw := range rawShreds {
		shred, err := ParseShred(raw)
		if err != nil {
			t.Fatalf("ParseShred returned error: %v", err)
		}
		batchBytes = append(batchBytes, shred.Data...)
		if !shred.DataComplete() {
			continue
		}

		var decoder bin.Decoder
		decoder.SetEncoding(bin.EncodingBin)
		decoder.Reset(batchBytes)
		numEntries, err := decoder.ReadUint64(bin.LE)
		if err != nil {
			t.Fatalf("read entry count: %v", err)
		}
		for entryIdx := uint64(0); entryIdx < numEntries; entryIdx++ {
			if _, err := decoder.ReadUint64(bin.LE); err != nil {
				t.Fatalf("read num hashes: %v", err)
			}
			if _, err := decoder.ReadBytes(32); err != nil {
				t.Fatalf("read hash: %v", err)
			}
			numTxns, err := decoder.ReadUint64(bin.LE)
			if err != nil {
				t.Fatalf("read tx count: %v", err)
			}
			for i := uint64(0); i < numTxns; i++ {
				start := decoder.Position()
				var current solana.Transaction
				if err := current.UnmarshalWithDecoder(&decoder); err != nil {
					t.Fatalf("read tx: %v", err)
				}
				if txIndex == 1 {
					tx = current
					rawTx = append([]byte(nil), batchBytes[start:decoder.Position()]...)
					break
				}
				txIndex++
			}
			if rawTx != nil {
				break
			}
		}
		if rawTx != nil {
			break
		}
		batchBytes = batchBytes[:0]
	}
	if rawTx == nil {
		t.Fatalf("did not find transaction 1")
	}

	rawDecoder := bin.NewBinDecoder(rawTx)
	numSigs, err := rawDecoder.ReadCompactU16()
	if err != nil {
		t.Fatalf("read raw signature count: %v", err)
	}
	if numSigs != len(tx.Signatures) {
		t.Fatalf("raw signature count = %d, decoded signatures = %d", numSigs, len(tx.Signatures))
	}
	for i := 0; i < numSigs; i++ {
		if _, err := rawDecoder.ReadBytes(64); err != nil {
			t.Fatalf("read raw signature %d: %v", i, err)
		}
	}
	rawMessage := rawTx[rawDecoder.Position():]
	remarshaled, err := tx.Message.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary returned error: %v", err)
	}
	if string(remarshaled) != string(rawMessage) {
		limit := len(remarshaled)
		if len(rawMessage) < limit {
			limit = len(rawMessage)
		}
		diff := -1
		for i := 0; i < limit; i++ {
			if remarshaled[i] != rawMessage[i] {
				diff = i
				break
			}
		}
		t.Fatalf("remarshaled message differs from raw at %d raw_len=%d remarshaled_len=%d raw_prefix=%x remarshaled_prefix=%x", diff, len(rawMessage), len(remarshaled), rawMessage[:min(24, len(rawMessage))], remarshaled[:min(24, len(remarshaled))])
	}
	signers := tx.Message.Signers()
	if len(signers) != len(tx.Signatures) {
		t.Fatalf("signers=%d signatures=%d", len(signers), len(tx.Signatures))
	}
	for i, sig := range tx.Signatures {
		if !sig.Verify(signers[i], rawMessage) {
			t.Fatalf("raw message signature %d failed for %s", i, signers[i])
		}
	}
	if err := txverify.VerifyTransaction(&tx); err != nil {
		t.Fatalf("txverify failed for raw transaction: %v", err)
	}

	var parsed []*Shred
	for _, raw := range rawShreds {
		shred, err := ParseShred(raw)
		if err != nil {
			t.Fatalf("ParseShred for block decode returned error: %v", err)
		}
		parsed = append(parsed, shred)
	}
	entries, err := DecodeEntriesFromDataShreds(parsed)
	if err != nil {
		t.Fatalf("DecodeEntriesFromDataShreds returned error: %v", err)
	}
	var entryTx *solana.Transaction
	var entryTxIndex uint64
	for entryIdx := range entries {
		for txIdx := range entries[entryIdx].Txns {
			if entryTxIndex == 1 {
				entryTx = &entries[entryIdx].Txns[txIdx]
				break
			}
			entryTxIndex++
		}
		if entryTx != nil {
			break
		}
	}
	if entryTx == nil {
		t.Fatalf("did not find decoded entry transaction 1")
	}
	entryMsg, _ := txverify.MessageBytes(entryTx)
	if string(entryMsg) != string(rawMessage) {
		t.Fatalf("decoded transaction message does not match raw message")
	}
	if err := txverify.VerifyTransaction(entryTx); err != nil {
		t.Fatalf("txverify failed for decoded entry transaction 1: %v", err)
	}
	blk := BlockFromEntries(102815960, 102815959, entries)
	if len(blk.Transactions) < 2 {
		t.Fatalf("block transactions = %d", len(blk.Transactions))
	}
	if err := txverify.VerifyTransaction(blk.Transactions[1]); err != nil {
		t.Fatalf("txverify failed for block transaction 1: %v", err)
	}
}

func TestSlotAssemblerIgnoresCodingShreds(t *testing.T) {
	raw := fixtures.CodeShreds(t, "mainnet", 102815960)[0]

	blk, err := NewSlotAssembler().AddPacket(raw)
	if err != nil {
		t.Fatalf("AddPacket returned error: %v", err)
	}
	if blk != nil {
		t.Fatalf("expected coding shred to be ignored")
	}

	if _, err := ParseShred(raw); !errors.Is(err, ErrCodingShredIgnored) {
		t.Fatalf("ParseShred error = %v, want ErrCodingShredIgnored", err)
	}
}

func TestParseMerkleCodingShred(t *testing.T) {
	raw := localnetMerkleShreds(t, "c")[0]

	shred, err := ParseShred(raw)
	if err != nil {
		t.Fatalf("ParseShred returned error: %v", err)
	}
	if shred.Type != ShredTypeCode {
		t.Fatalf("type = %v, want code", shred.Type)
	}
	if shred.NumDataShreds == 0 || shred.NumCodingShreds == 0 {
		t.Fatalf("invalid FEC layout: data=%d coding=%d", shred.NumDataShreds, shred.NumCodingShreds)
	}
	if _, err := shred.erasureShard(); err != nil {
		t.Fatalf("erasureShard returned error: %v", err)
	}
}

func TestSlotStateDerivesAlpenglowBlockIDFromDoubleMerkleTree(t *testing.T) {
	dataShreds := localnetMerkleShreds(t, "d")
	codeShreds := localnetMerkleShreds(t, "c")
	var parentBlockID solana.Hash
	parentBlockID[0] = 11

	var state *slotState
	add := func(raw []byte) {
		t.Helper()
		shred, err := ParseShred(raw)
		if err != nil {
			t.Fatalf("ParseShred returned error: %v", err)
		}
		if state == nil {
			state = &slotState{
				slot:      shred.Slot,
				shreds:    make(map[uint32]*Shred),
				fecSets:   make(map[uint32]*fecState),
				shredVer:  shred.Version,
				lastIndex: ^uint32(0),
			}
		}
		switch shred.Type {
		case ShredTypeData:
			if err := state.addDataShred(shred); err != nil {
				t.Fatalf("addDataShred returned error: %v", err)
			}
		case ShredTypeCode:
			if err := state.addCodingShred(shred); err != nil {
				t.Fatalf("addCodingShred returned error: %v", err)
			}
		}
	}
	for _, raw := range dataShreds {
		add(raw)
	}
	for _, raw := range codeShreds {
		add(raw)
	}

	fecRoots, err := state.fecSetMerkleRoots()
	if err != nil {
		t.Fatalf("fecSetMerkleRoots returned error: %v", err)
	}
	if len(fecRoots) == 0 {
		t.Fatalf("fixture did not expose FEC Merkle roots")
	}
	expectedBlockID := doubleMerkleBlockID(fecRoots, state.parentSlot, parentBlockID)
	lastFECBlockID := fecRoots[len(fecRoots)-1]

	blockID, ok, err := state.alpenglowBlockID(state.parentSlot, parentBlockID, true)
	if err != nil {
		t.Fatalf("alpenglowBlockID returned error: %v", err)
	}
	if !ok {
		t.Fatalf("alpenglowBlockID did not find a block id")
	}
	if blockID != expectedBlockID {
		t.Fatalf("alpenglow block id = %s, want %s (fec_roots=%d last_index=%d fec_set_count=%d)",
			blockID, expectedBlockID, len(fecRoots), state.lastIndex, state.lastIndex/dataShredsPerFECBlock+1)
	}
	if blockID == lastFECBlockID {
		t.Fatalf("alpenglow block id unexpectedly used the last FEC root")
	}
}

func TestDecodeAlpenglowParentMarkers(t *testing.T) {
	var headerParentID solana.Hash
	headerParentID[0] = 1
	header, ok, err := decodeAlpenglowParentMarker(testAlpenglowParentMarkerBytes(blockMarkerVariantHeader, 42, headerParentID), 0)
	if err != nil {
		t.Fatalf("decode header marker returned error: %v", err)
	}
	if !ok || header == nil {
		t.Fatalf("expected header marker to decode")
	}
	if header.ParentSlot != 42 || header.ParentBlockID != headerParentID || header.FromUpdateParent {
		t.Fatalf("decoded header marker = %+v", header)
	}

	var updateParentID solana.Hash
	updateParentID[0] = 2
	update, ok, err := decodeAlpenglowParentMarker(testAlpenglowParentMarkerBytes(blockMarkerVariantUpdateParent, 40, updateParentID), dataShredsPerFECBlock)
	if err != nil {
		t.Fatalf("decode update-parent marker returned error: %v", err)
	}
	if !ok || update == nil {
		t.Fatalf("expected update-parent marker to decode")
	}
	if update.ParentSlot != 40 || update.ParentBlockID != updateParentID || !update.FromUpdateParent || update.ReplayFECSetIndex != dataShredsPerFECBlock {
		t.Fatalf("decoded update-parent marker = %+v", update)
	}

	if marker, ok, err := decodeAlpenglowParentMarker(testAlpenglowParentMarkerBytes(blockMarkerVariantUpdateParent, 40, updateParentID), 0); err != nil || ok || marker != nil {
		t.Fatalf("update-parent at batch start 0 = marker=%+v ok=%t err=%v, want ignored", marker, ok, err)
	}

	markerWithTrailingByte := append(testAlpenglowParentMarkerBytes(blockMarkerVariantUpdateParent, 40, updateParentID), 0)
	if _, _, err := decodeAlpenglowParentMarker(markerWithTrailingByte, dataShredsPerFECBlock); err == nil {
		t.Fatal("update-parent marker with trailing wrapper byte unexpectedly decoded")
	}
	parentWithTrailingByte := testAlpenglowParentMarkerBytes(blockMarkerVariantUpdateParent, 40, updateParentID)
	innerLen := binary.LittleEndian.Uint16(parentWithTrailingByte[11:13])
	binary.LittleEndian.PutUint16(parentWithTrailingByte[11:13], innerLen+1)
	parentWithTrailingByte = append(parentWithTrailingByte, 0)
	if _, _, err := decodeAlpenglowParentMarker(parentWithTrailingByte, dataShredsPerFECBlock); err == nil {
		t.Fatal("update-parent V1 payload with trailing byte unexpectedly decoded")
	}

	merged, err := mergeAlpenglowParentInfo(header, update)
	if err != nil {
		t.Fatalf("mergeAlpenglowParentInfo returned error: %v", err)
	}
	if merged == nil || !merged.FromUpdateParent || merged.ParentSlot != update.ParentSlot || merged.ParentBlockID != update.ParentBlockID {
		t.Fatalf("merged parent marker = %+v, want update-parent", merged)
	}
	laterBoundary := *update
	laterBoundary.ReplayFECSetIndex += dataShredsPerFECBlock
	if _, err := mergeAlpenglowParentInfo(update, &laterBoundary); err == nil {
		t.Fatal("same update-parent at a different FEC boundary unexpectedly merged")
	}
}

func TestSlotAssemblerRecoversMissingMerkleDataShredFromCodingShreds(t *testing.T) {
	dataShreds := localnetMerkleShreds(t, "d")
	codeShreds := localnetMerkleShreds(t, "c")
	if len(dataShreds) < 2 || len(codeShreds) == 0 {
		t.Fatalf("fixture needs data and coding shreds")
	}

	missing, err := ParseShred(dataShreds[1])
	if err != nil {
		t.Fatalf("ParseShred missing data returned error: %v", err)
	}
	assembler := NewSlotAssembler()
	for idx, raw := range dataShreds {
		if idx == 1 {
			continue
		}
		if _, err := assembler.AddPacket(raw); err != nil {
			t.Fatalf("AddPacket data %d returned error: %v", idx, err)
		}
	}
	var blockSeen bool
	for idx, raw := range codeShreds {
		blk, err := assembler.AddPacket(raw)
		if err != nil {
			t.Fatalf("AddPacket code %d returned error: %v", idx, err)
		}
		if blk != nil {
			blockSeen = true
		}
	}
	if blockSeen {
		return
	}

	assembler.mu.Lock()
	defer assembler.mu.Unlock()
	state := assembler.slots[missing.Slot]
	if state == nil {
		t.Fatalf("slot state missing for slot %d", missing.Slot)
	}
	recovered := state.shreds[missing.Index]
	if recovered == nil {
		t.Fatalf("missing data shred index %d was not recovered", missing.Index)
	}
	if !recovered.Recovered {
		t.Fatalf("data shred index %d present but not marked recovered", missing.Index)
	}
	if string(recovered.Data) != string(missing.Data) {
		t.Fatalf("recovered data does not match original data")
	}
}

func TestSlotAssemblerRejectsOversizedShredIndex(t *testing.T) {
	raw := fixtures.DataShreds(t, "mainnet", 102815960)[0]
	shred, err := ParseShred(raw)
	if err != nil {
		t.Fatalf("ParseShred returned error: %v", err)
	}
	shred.Index = maxDataShredsPerSlot

	if _, err := NewSlotAssembler().AddShred(shred); !errors.Is(err, ErrSlotOverflow) {
		t.Fatalf("AddShred error = %v, want ErrSlotOverflow", err)
	}
}

func TestSlotAssemblerPrunesOldIncompleteSlots(t *testing.T) {
	assembler := NewSlotAssembler()
	maxSlot := maxRetainedIncompleteSlotLag + 20
	for slot := uint64(1); slot <= maxSlot; slot++ {
		_, err := assembler.AddShred(&Shred{
			Variant:      legacyDataVariant,
			Type:         ShredTypeData,
			Slot:         slot,
			Index:        0,
			Version:      1,
			ParentOffset: 1,
			Data:         []byte{byte(slot)},
		})
		if err != nil {
			t.Fatalf("AddShred(%d) returned error: %v", slot, err)
		}
	}

	if got := assembler.ActiveSlots(); got > int(maxRetainedIncompleteSlotLag)+1 {
		t.Fatalf("active slots = %d, want at most %d", got, maxRetainedIncompleteSlotLag+1)
	}
	if got := assembler.EvictedSlots(); got == 0 {
		t.Fatalf("expected old incomplete slots to be evicted")
	}
	assembler.mu.Lock()
	_, oldExists := assembler.slots[1]
	assembler.mu.Unlock()
	if oldExists {
		t.Fatalf("expected oldest incomplete slot to be evicted")
	}

	_, err := assembler.AddShred(&Shred{
		Variant:      legacyDataVariant,
		Type:         ShredTypeData,
		Slot:         1,
		Index:        1,
		Version:      1,
		ParentOffset: 1,
		Data:         []byte{1},
	})
	if err != nil {
		t.Fatalf("AddShred for stale slot returned error: %v", err)
	}
	assembler.mu.Lock()
	_, oldExists = assembler.slots[1]
	assembler.mu.Unlock()
	if oldExists {
		t.Fatalf("expected stale shred not to recreate evicted slot")
	}
	if got := assembler.IgnoredOldShreds(); got == 0 {
		t.Fatalf("expected stale shred counter to increment")
	}
}

func TestSlotAssemblerRepairRequestsIncludeAbsentAndIncompleteSlots(t *testing.T) {
	assembler := NewSlotAssembler()
	for _, shred := range []*Shred{
		{
			Variant:      legacyDataVariant,
			Type:         ShredTypeData,
			Slot:         10,
			Index:        1,
			Version:      1,
			ParentOffset: 1,
			Data:         []byte{1},
		},
		{
			Variant:      legacyDataVariant,
			Type:         ShredTypeData,
			Slot:         14,
			Index:        0,
			Version:      1,
			ParentOffset: 1,
			Data:         []byte{1},
		},
	} {
		if _, err := assembler.AddShred(shred); err != nil {
			t.Fatalf("AddShred returned error: %v", err)
		}
	}

	requests := assembler.RepairRequests(32, 8)
	bySlot := make(map[uint64]SlotRepairRequest, len(requests))
	for _, req := range requests {
		bySlot[req.Slot] = req
	}

	if req, ok := bySlot[9]; !ok || !req.NeedHighestDataShred || req.HighestDataShredIndex != 0 {
		t.Fatalf("slot 9 absent repair request = %+v, ok=%v", req, ok)
	}
	req, ok := bySlot[10]
	if !ok {
		t.Fatalf("expected repair request for incomplete slot 10")
	}
	if !req.NeedHighestDataShred || req.HighestDataShredIndex != 2 {
		t.Fatalf("slot 10 highest request = need %v index %d, want true index 2", req.NeedHighestDataShred, req.HighestDataShredIndex)
	}
	if len(req.MissingDataShreds) != 1 || req.MissingDataShreds[0] != 0 {
		t.Fatalf("slot 10 missing shreds = %v, want [0]", req.MissingDataShreds)
	}
}

func TestSlotAssemblerRepairRequestsPrioritizeRequestedSlot(t *testing.T) {
	assembler := NewSlotAssembler()
	assembler.maxObservedSlot = 220
	assembler.PrioritizeRepairSlot(180)

	requests := assembler.RepairRequests(4, 8)
	if len(requests) == 0 {
		t.Fatalf("expected repair requests")
	}
	if requests[0].Slot != 180 {
		t.Fatalf("first repair request slot = %d, want priority slot 180; requests=%+v", requests[0].Slot, requests)
	}
	if !requests[0].NeedHighestDataShred || requests[0].HighestDataShredIndex != 0 {
		t.Fatalf("priority absent-slot request = %+v, want highest shred from index 0", requests[0])
	}
}

func TestSlotAssemblerRepairRequestsPrioritizeRequestedRange(t *testing.T) {
	assembler := NewSlotAssembler()
	assembler.maxObservedSlot = 220
	assembler.PrioritizeRepairRange(180, 183)

	requests := assembler.RepairRequests(8, 8)
	if len(requests) < 4 {
		t.Fatalf("expected at least 4 repair requests, got %d: %+v", len(requests), requests)
	}
	for i, wantSlot := range []uint64{180, 181, 182, 183} {
		if requests[i].Slot != wantSlot {
			t.Fatalf("request %d slot = %d, want %d; requests=%+v", i, requests[i].Slot, wantSlot, requests)
		}
	}
}

func TestSlotAssemblerPriorityRepairBypassesLiveEdgeGrace(t *testing.T) {
	assembler := NewSlotAssembler()
	assembler.maxObservedSlot = 220
	assembler.slots[220] = &slotState{
		slot:    220,
		shreds:  map[uint32]*Shred{7: {Slot: 220, Type: ShredTypeData, Index: 7}},
		fecSets: make(map[uint32]*fecState),
	}
	assembler.PrioritizeRepairSlot(220)

	priority, edge := assembler.RepairRequestsTiered(32, 8)
	if len(priority) != 1 || priority[0].Slot != 220 {
		t.Fatalf("priority = %+v, want newest observed slot 220", priority)
	}
	if !priority[0].NeedHighestDataShred || priority[0].HighestDataShredIndex != 8 {
		t.Fatalf("priority newest-slot request = %+v, want highest probe from index 8", priority[0])
	}
	for _, req := range edge {
		if req.Slot == 220 {
			t.Fatalf("ordinary freshness tier repaired newest slot despite edge grace: %+v", edge)
		}
	}
}

func TestSlotAssemblerPriorityRepairCanDiscoverAbsentSlotBeyondObservedEdge(t *testing.T) {
	assembler := NewSlotAssembler()
	assembler.maxObservedSlot = 220
	assembler.PrioritizeRepairSlot(221)

	priority, _ := assembler.RepairRequestsTiered(32, 8)
	if len(priority) != 1 || priority[0].Slot != 221 {
		t.Fatalf("priority = %+v, want absent pinned slot 221", priority)
	}
	if !priority[0].NeedHighestDataShred || priority[0].HighestDataShredIndex != 0 {
		t.Fatalf("absent priority request = %+v, want highest probe from index 0", priority[0])
	}
}

func TestSlotAssemblerPriorityRepairDoesNotNeedObservedEdge(t *testing.T) {
	assembler := NewSlotAssembler()
	assembler.PrioritizeRepairSlot(100)

	priority, edge := assembler.RepairRequestsTiered(32, 8)
	if len(priority) != 1 || priority[0].Slot != 100 || !priority[0].NeedHighestDataShred {
		t.Fatalf("priority = %+v, want absent pinned slot 100", priority)
	}
	if len(edge) != 0 {
		t.Fatalf("edge = %+v, want no freshness scan without an observed edge", edge)
	}
}

func TestUDPReceiverSignalsReadyAfterBind(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	receiver := NewUDPReceiver("127.0.0.1:0")
	done := make(chan error, 1)
	go func() {
		done <- receiver.Run(ctx)
	}()

	select {
	case err := <-receiver.Ready():
		if err != nil {
			t.Fatalf("Ready returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for receiver readiness")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error after cancel: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for receiver shutdown")
	}
}

func TestUDPReceiverRecordsBlockActivity(t *testing.T) {
	receiver := NewUDPReceiver("127.0.0.1:0")
	before := time.Now().Unix()
	receiver.recordBlockEmitted(123)
	receiver.recordBlockEmitted(124)
	after := time.Now().Unix()

	stats := receiver.Stats()
	if stats.BlocksEmitted != 2 || stats.LastBlockSlot != 124 {
		t.Fatalf("block stats = count %d slot %d, want 2 and 124", stats.BlocksEmitted, stats.LastBlockSlot)
	}
	if stats.LastBlockUnix < before || stats.LastBlockUnix > after {
		t.Fatalf("last block timestamp = %d, want [%d,%d]", stats.LastBlockUnix, before, after)
	}
}

func localnetMerkleShreds(t testing.TB, prefix string) [][]byte {
	t.Helper()
	dir := fixtures.Path(t, "shreds", "localnet", "merkle")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("cannot read localnet merkle shreds: %v", err)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})
	var shreds [][]byte
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), prefix) {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatalf("cannot read shred %s: %v", entry.Name(), err)
		}
		shreds = append(shreds, raw)
	}
	return shreds
}

func fecMerkleRootsFromPackets(t testing.TB, rawShreds [][]byte) []solana.Hash {
	t.Helper()

	rootsByFECSet := make(map[uint32]solana.Hash)
	for idx, raw := range rawShreds {
		shred, err := ParseShred(raw)
		if errors.Is(err, ErrCodingShredIgnored) {
			continue
		}
		if err != nil {
			t.Fatalf("ParseShred(%d) returned error: %v", idx, err)
		}
		if shred.Type != ShredTypeData || shred.Recovered {
			continue
		}
		current, err := shred.MerkleRoot()
		if errors.Is(err, ErrUnsupportedShred) {
			continue
		}
		if err != nil {
			t.Fatalf("MerkleRoot(%d) returned error: %v", idx, err)
		}
		rootsByFECSet[shred.FECSetIndex] = current
	}

	fecSetIndexes := sortedUint32Keys(rootsByFECSet)
	roots := make([]solana.Hash, 0, len(fecSetIndexes))
	for _, fecSetIndex := range fecSetIndexes {
		roots = append(roots, rootsByFECSet[fecSetIndex])
	}
	return roots
}

func doubleMerkleBlockID(fecRoots []solana.Hash, parentSlot uint64, parentBlockID solana.Hash) solana.Hash {
	var parentSlotBytes [8]byte
	binary.LittleEndian.PutUint64(parentSlotBytes[:], parentSlot)
	var fecSetCountBytes [4]byte
	binary.LittleEndian.PutUint32(fecSetCountBytes[:], uint32(len(fecRoots)))
	leaves := append([]solana.Hash(nil), fecRoots...)
	leaves = append(leaves, hashv([][]byte{parentSlotBytes[:], parentBlockID[:], fecSetCountBytes[:]}))
	return merkleTreeRoot(leaves)
}

func testAlpenglowParentMarkerBytes(variant byte, parentSlot uint64, parentBlockID solana.Hash) []byte {
	var payload []byte
	payload = binary.LittleEndian.AppendUint64(payload, 0)
	payload = binary.LittleEndian.AppendUint16(payload, blockComponentMarkerVersionV1)
	payload = append(payload, variant)
	payload = binary.LittleEndian.AppendUint16(payload, 1+8+32)
	payload = append(payload, versionedParentInfoV1)
	payload = binary.LittleEndian.AppendUint64(payload, parentSlot)
	payload = append(payload, parentBlockID[:]...)
	return payload
}

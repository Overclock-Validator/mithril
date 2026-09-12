package turbine

import (
	"encoding/binary"
	"fmt"
	"sort"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/block"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

const (
	dataShredsPerFECBlock = 32

	blockComponentMarkerVersionV1  = 1
	blockMarkerVariantFooter       = 0
	blockMarkerVariantHeader       = 1
	blockMarkerVariantUpdateParent = 2
	versionedParentInfoV1          = 1
	// A minimally encoded transaction still has compact counts, the message
	// header, and a recent blockhash.
	minimumTransactionWireSize = 1 + 3 + 1 + 32 + 1
	legacyTransactionWireLimit = 1232
)

type Entry struct {
	NumHashes uint64
	Hash      solana.Hash
	Txns      []solana.Transaction
}

type AlpenglowParentInfo struct {
	ParentSlot        uint64
	ParentBlockID     solana.Hash
	ReplayFECSetIndex uint32
	FromUpdateParent  bool
}

type entryDecodeTimings struct {
	transactionParse time.Duration
}

func (e *Entry) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error
	if e.NumHashes, err = decoder.ReadUint64(bin.LE); err != nil {
		return fmt.Errorf("read num_hashes: %w", err)
	}
	if _, err = decoder.Read(e.Hash[:]); err != nil {
		return fmt.Errorf("read hash: %w", err)
	}
	numTxns, err := decoder.ReadUint64(bin.LE)
	if err != nil {
		return fmt.Errorf("read transaction count: %w", err)
	}
	if numTxns > uint64(decoder.Remaining()/minimumTransactionWireSize) {
		return fmt.Errorf("transaction count %d exceeds remaining bytes %d", numTxns, decoder.Remaining())
	}
	e.Txns = make([]solana.Transaction, numTxns)
	for i := uint64(0); i < numTxns; i++ {
		transactionStart := decoder.Position()
		if err = e.Txns[i].UnmarshalWithDecoder(decoder); err != nil {
			return fmt.Errorf("read transaction %d: %w", i, err)
		}
		transactionSize := decoder.Position() - transactionStart
		var transactionLimit uint
		switch version := e.Txns[i].Message.GetVersion(); version {
		case solana.MessageVersionLegacy, solana.MessageVersionV0:
			transactionLimit = legacyTransactionWireLimit
		case solana.MessageVersionV1:
			transactionLimit = solana.MaxTransactionSizeV1
		default:
			return fmt.Errorf("read transaction %d: unsupported message version %d", i, version)
		}
		if transactionSize > transactionLimit {
			return fmt.Errorf("read transaction %d: wire size %d exceeds version %d limit %d", i, transactionSize, e.Txns[i].Message.GetVersion(), transactionLimit)
		}
	}
	return nil
}

type entryBatch struct {
	Entries []Entry
}

func (b *entryBatch) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	numEntries, err := decoder.ReadUint64(bin.LE)
	if err != nil {
		return fmt.Errorf("read entry count: %w", err)
	}
	if numEntries > uint64(decoder.Remaining()/minimumEntryWireSize) {
		return fmt.Errorf("entry count %d exceeds remaining bytes %d", numEntries, decoder.Remaining())
	}
	b.Entries = make([]Entry, numEntries)
	for i := uint64(0); i < numEntries; i++ {
		if err = b.Entries[i].UnmarshalWithDecoder(decoder); err != nil {
			return fmt.Errorf("read entry %d: %w", i, err)
		}
	}
	return nil
}

func DecodeEntriesFromDataShreds(shreds []*Shred) ([]Entry, error) {
	entries, _, _, err := DecodeEntriesAndAlpenglowMarkersFromDataShreds(shreds)
	return entries, err
}

func DecodeEntriesAndAlpenglowParentInfoFromDataShreds(shreds []*Shred) ([]Entry, *AlpenglowParentInfo, error) {
	entries, parentInfo, _, err := DecodeEntriesAndAlpenglowMarkersFromDataShreds(shreds)
	return entries, parentInfo, err
}

// DecodeEntriesAndAlpenglowMarkersFromDataShreds decodes both transaction
// entry batches and Alpenglow header/update/footer marker batches.
func DecodeEntriesAndAlpenglowMarkersFromDataShreds(shreds []*Shred) ([]Entry, *AlpenglowParentInfo, *BlockFooter, error) {
	return decodeEntriesAndAlpenglowMarkersFromDataShreds(shreds, nil)
}

func decodeEntriesAndAlpenglowMarkersFromDataShreds(shreds []*Shred, timings *entryDecodeTimings) ([]Entry, *AlpenglowParentInfo, *BlockFooter, error) {
	if len(shreds) == 0 {
		return nil, nil, nil, nil
	}
	sort.Slice(shreds, func(i, j int) bool {
		return shreds[i].Index < shreds[j].Index
	})

	type decodedEntryBatch struct {
		start   uint32
		entries []Entry
	}
	var entryBatches []decodedEntryBatch
	var parentInfo *AlpenglowParentInfo
	var blockFooter *BlockFooter
	var batchBytes []byte
	var batchStart uint32
	var haveBatch bool
	for _, shred := range shreds {
		if shred == nil || shred.Type != ShredTypeData {
			continue
		}
		if !haveBatch {
			batchStart = shred.Index
			haveBatch = true
		}
		batchBytes = append(batchBytes, shred.Data...)
		if !shred.DataComplete() {
			continue
		}
		if parent, footer, ok, err := decodeAlpenglowMarkerFromShredBatch(batchBytes, batchStart); err != nil {
			return nil, nil, nil, fmt.Errorf("decode alpenglow block marker ending at shred %d: %w", shred.Index, err)
		} else if ok {
			if parent != nil {
				parentInfo, err = mergeAlpenglowParentInfo(parentInfo, parent)
				if err != nil {
					return nil, nil, nil, fmt.Errorf("merge alpenglow parent marker ending at shred %d: %w", shred.Index, err)
				}
			}
			if footer != nil {
				blockFooter = footer
			}
			batchBytes = nil
			haveBatch = false
			continue
		}
		parseStart := time.Now()
		batchEntries, consumed, err := decodeEntryBatchPrefix(batchBytes)
		if timings != nil {
			timings.transactionParse += time.Since(parseStart)
		}
		// A zero entry count with more bytes denotes a marker. Preserve the
		// rejection of unrecognized or misplaced markers in this fallback path.
		if err == nil && len(batchEntries) == 0 && consumed != len(batchBytes) {
			err = fmt.Errorf("entry batch has %d trailing bytes", len(batchBytes)-consumed)
		}
		if err != nil {
			return nil, nil, nil, fmt.Errorf("decode entry batch ending at shred %d: %w", shred.Index, err)
		}
		entryBatches = append(entryBatches, decodedEntryBatch{
			start:   batchStart,
			entries: batchEntries,
		})
		// Decoded transactions retain slices into the batch buffer for instruction data.
		// Keep the backing array alive instead of reusing and overwriting it.
		batchBytes = nil
		haveBatch = false
	}
	if len(batchBytes) != 0 {
		return nil, nil, nil, fmt.Errorf("slot ended with %d undecoded entry bytes", len(batchBytes))
	}
	var entries []Entry
	for _, batch := range entryBatches {
		// An Alpenglow UpdateParent abandons the optimistic prefix. The producer
		// re-injects that prefix after the marker against the selected parent, so
		// replaying both sides would manufacture same-bank AlreadyProcessed
		// duplicates. Keep all shreds for block-id verification, but expose only
		// entry batches at or after the selected replay boundary.
		if parentInfo != nil && parentInfo.FromUpdateParent && batch.start < parentInfo.ReplayFECSetIndex {
			continue
		}
		entries = append(entries, batch.entries...)
	}
	return entries, parentInfo, blockFooter, nil
}

func decodeEntryBatch(data []byte) ([]Entry, error) {
	entries, consumed, err := decodeEntryBatchPrefix(data)
	if err != nil {
		return nil, err
	}
	if consumed != len(data) {
		return nil, fmt.Errorf("entry batch has %d trailing bytes", len(data)-consumed)
	}
	return entries, nil
}

// decodeEntryBatchPrefix reads one entry batch and reports its encoded size.
// Agave's entry/src/block_component_parser.rs ignores trailing bytes after one
// component in a DATA_COMPLETE batch. Standalone decoding still requires an
// exact envelope.
func decodeEntryBatchPrefix(data []byte) ([]Entry, int, error) {
	var decoder bin.Decoder
	decoder.SetEncoding(bin.EncodingBin)
	decoder.Reset(data)
	var batch entryBatch
	if err := batch.UnmarshalWithDecoder(&decoder); err != nil {
		return nil, 0, err
	}
	return batch.Entries, len(data) - decoder.Remaining(), nil
}

func decodeAlpenglowParentMarker(data []byte, batchStart uint32) (*AlpenglowParentInfo, bool, error) {
	parent, _, ok, err := decodeAlpenglowMarker(data, batchStart)
	return parent, ok && parent != nil, err
}

func decodeAlpenglowMarkerFromShredBatch(data []byte, batchStart uint32) (*AlpenglowParentInfo, *BlockFooter, bool, error) {
	if len(data) >= 13 && binary.LittleEndian.Uint64(data[:8]) == 0 && binary.LittleEndian.Uint16(data[8:10]) == blockComponentMarkerVersionV1 {
		markerSize, err := blockMarkerPrefixSize(data)
		if err != nil {
			return nil, nil, false, err
		}
		data = data[:markerSize]
	}
	return decodeAlpenglowMarker(data, batchStart)
}

func decodeAlpenglowMarker(data []byte, batchStart uint32) (*AlpenglowParentInfo, *BlockFooter, bool, error) {
	if len(data) < 8 {
		return nil, nil, false, nil
	}
	if binary.LittleEndian.Uint64(data[:8]) != 0 {
		return nil, nil, false, nil
	}
	if len(data) < 13 {
		return nil, nil, false, nil
	}
	markerVersion := binary.LittleEndian.Uint16(data[8:10])
	if markerVersion != blockComponentMarkerVersionV1 {
		return nil, nil, false, nil
	}
	variant := data[10]
	innerLen := int(binary.LittleEndian.Uint16(data[11:13]))
	if len(data) != 13+innerLen {
		return nil, nil, false, fmt.Errorf("marker payload length %d does not match remaining %d", innerLen, len(data)-13)
	}
	inner := data[13 : 13+innerLen]
	if len(inner) < 1 {
		return nil, nil, false, fmt.Errorf("empty marker payload")
	}

	switch variant {
	case blockMarkerVariantHeader:
		if batchStart != 0 {
			return nil, nil, false, nil
		}
		parentSlot, parentBlockID, err := decodeVersionedParentInfo(inner)
		if err != nil {
			return nil, nil, false, fmt.Errorf("block header: %w", err)
		}
		return &AlpenglowParentInfo{
			ParentSlot:        parentSlot,
			ParentBlockID:     parentBlockID,
			ReplayFECSetIndex: 0,
		}, nil, true, nil
	case blockMarkerVariantUpdateParent:
		if batchStart == 0 || batchStart%dataShredsPerFECBlock != 0 {
			return nil, nil, false, nil
		}
		parentSlot, parentBlockID, err := decodeVersionedParentInfo(inner)
		if err != nil {
			return nil, nil, false, fmt.Errorf("update parent: %w", err)
		}
		return &AlpenglowParentInfo{
			ParentSlot:        parentSlot,
			ParentBlockID:     parentBlockID,
			ReplayFECSetIndex: batchStart,
			FromUpdateParent:  true,
		}, nil, true, nil
	case blockMarkerVariantFooter:
		footer, err := unmarshalVersionedBlockFooter(inner)
		if err != nil {
			return nil, nil, false, fmt.Errorf("block footer: %w", err)
		}
		return nil, footer, true, nil
	default:
		return nil, nil, false, nil
	}
}

func decodeVersionedParentInfo(data []byte) (uint64, solana.Hash, error) {
	if len(data) != 41 {
		return 0, solana.Hash{}, fmt.Errorf("payload length %d, want 41", len(data))
	}
	if data[0] != versionedParentInfoV1 {
		return 0, solana.Hash{}, fmt.Errorf("unsupported version tag %d", data[0])
	}
	parentSlot := binary.LittleEndian.Uint64(data[1:9])
	var parentBlockID solana.Hash
	copy(parentBlockID[:], data[9:41])
	return parentSlot, parentBlockID, nil
}

func mergeAlpenglowParentInfo(current *AlpenglowParentInfo, next *AlpenglowParentInfo) (*AlpenglowParentInfo, error) {
	if next == nil {
		return current, nil
	}
	if current == nil {
		copied := *next
		return &copied, nil
	}
	if current.ParentSlot == next.ParentSlot && current.ParentBlockID == next.ParentBlockID && current.ReplayFECSetIndex == next.ReplayFECSetIndex && current.FromUpdateParent == next.FromUpdateParent {
		return current, nil
	}
	switch {
	case current.FromUpdateParent && next.FromUpdateParent:
		return current, fmt.Errorf("multiple update-parent markers disagree: %d:%s and %d:%s",
			current.ParentSlot, current.ParentBlockID, next.ParentSlot, next.ParentBlockID)
	case current.FromUpdateParent && !next.FromUpdateParent:
		if current.ParentSlot == next.ParentSlot && current.ParentBlockID == next.ParentBlockID {
			return current, fmt.Errorf("update-parent marker matches block header: %d:%s", current.ParentSlot, current.ParentBlockID)
		}
		if current.ParentSlot > next.ParentSlot {
			return current, fmt.Errorf("update-parent slot %d is newer than block-header parent slot %d", current.ParentSlot, next.ParentSlot)
		}
		return current, nil
	case !current.FromUpdateParent && next.FromUpdateParent:
		if current.ParentSlot == next.ParentSlot && current.ParentBlockID == next.ParentBlockID {
			return current, fmt.Errorf("update-parent marker matches block header: %d:%s", current.ParentSlot, current.ParentBlockID)
		}
		if next.ParentSlot > current.ParentSlot {
			return current, fmt.Errorf("update-parent slot %d is newer than block-header parent slot %d", next.ParentSlot, current.ParentSlot)
		}
		copied := *next
		return &copied, nil
	default:
		return current, fmt.Errorf("multiple block headers disagree: %d:%s and %d:%s",
			current.ParentSlot, current.ParentBlockID, next.ParentSlot, next.ParentBlockID)
	}
}

func BlockFromEntries(slot uint64, parentSlot uint64, entries []Entry) *block.Block {
	blk := &block.Block{
		Slot:             slot,
		SourceParentSlot: parentSlot,
		Transactions:     make([]*solana.Transaction, 0, len(entries)*4),
		Entries:          make([]*block.TxEntry, len(entries)),
		FromLiveStream:   true,
	}

	var txOffset uint64
	for entryIdx, entry := range entries {
		txEntry := &block.TxEntry{
			NumHashes: entry.NumHashes,
			Hash:      append([]byte(nil), entry.Hash[:]...),
			Indices:   make([]uint64, len(entry.Txns)),
		}
		for txIdx := range entry.Txns {
			tx := &entry.Txns[txIdx]
			blk.Transactions = append(blk.Transactions, tx)
			blk.NumSignatures += uint64(tx.Message.Header.NumRequiredSignatures)
			blk.Versions = append(blk.Versions, uint8(tx.Message.GetVersion()))
			txEntry.Indices[txIdx] = txOffset + uint64(txIdx)
		}
		blk.Entries[entryIdx] = txEntry
		txOffset += uint64(len(entry.Txns))
	}
	if len(entries) > 0 {
		blk.Blockhash = entries[len(entries)-1].Hash
	}
	return blk
}

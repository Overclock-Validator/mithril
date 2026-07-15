package turbine

import (
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/txverify"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

const (
	blockComponentMarkerVersionV1  = 1
	blockMarkerVariantFooter       = 0
	blockMarkerVariantHeader       = 1
	blockMarkerVariantUpdateParent = 2
	versionedParentInfoV1          = 1
	// A minimally encoded legacy transaction has a compact signature count,
	// three header bytes, compact account/instruction counts, and a blockhash.
	minimumTransactionWireSize = 1 + 3 + 1 + 32 + 1
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
		if err = e.Txns[i].UnmarshalWithDecoder(decoder); err != nil {
			return fmt.Errorf("read transaction %d: %w", i, err)
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

func DecodeEntriesAndAlpenglowMarkersFromDataShreds(shreds []*Shred) ([]Entry, *AlpenglowParentInfo, *BlockFooter, error) {
	if len(shreds) == 0 {
		return nil, nil, nil, nil
	}
	sort.Slice(shreds, func(i, j int) bool {
		return shreds[i].Index < shreds[j].Index
	})

	var decoded []decodedSlotComponent
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
		component, err := UnmarshalBlockComponent(batchBytes)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("decode block component ending at shred %d: %w", shred.Index, err)
		}
		decoded = append(decoded, decodedSlotComponent{component: component, batchStart: batchStart})
		batchBytes = nil
		haveBatch = false
	}
	if len(batchBytes) != 0 {
		return nil, nil, nil, fmt.Errorf("slot ended with %d undecoded entry bytes", len(batchBytes))
	}

	var slot, parentSlot uint64
	for _, shred := range shreds {
		if shred == nil || shred.Type != ShredTypeData {
			continue
		}
		slot = shred.Slot
		parentSlot = shred.ParentSlot()
		break
	}
	processor := componentStreamProcessor{slot: slot, shredParentSlot: parentSlot}
	for index, component := range decoded {
		if err := processor.consume(component, index == len(decoded)-1); err != nil {
			return nil, nil, nil, fmt.Errorf("slot %d component %d: %w", slot, index, err)
		}
	}
	if err := processor.finish(); err != nil {
		return nil, nil, nil, fmt.Errorf("slot %d component stream: %w", slot, err)
	}
	return processor.entries, processor.parent, processor.footer, nil
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
	if len(data) < 13+innerLen {
		return nil, nil, false, fmt.Errorf("marker payload length %d exceeds remaining %d", innerLen, len(data)-13)
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
	if len(data) < 41 {
		return 0, solana.Hash{}, fmt.Errorf("payload length %d too short", len(data))
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
	if current.ParentSlot == next.ParentSlot && current.ParentBlockID == next.ParentBlockID && current.FromUpdateParent == next.FromUpdateParent {
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
		FromLightbringer: true,
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

func validateBlockTransactions(blk *block.Block) error {
	if blk == nil {
		return nil
	}
	for txIdx, tx := range blk.Transactions {
		if tx == nil {
			return fmt.Errorf("slot %d transaction %d is nil", blk.Slot, txIdx)
		}
		if err := txverify.VerifyTransaction(tx); err != nil {
			txSig := "<missing>"
			if len(tx.Signatures) > 0 {
				txSig = tx.Signatures[0].String()
			}
			return fmt.Errorf("slot %d transaction %d %s version=%d failed signature verification: %w", blk.Slot, txIdx, txSig, tx.Message.GetVersion(), err)
		}
	}
	return nil
}

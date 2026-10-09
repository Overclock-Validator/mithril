package turbine

import (
	"fmt"

	"github.com/gagliardetto/solana-go"
)

// Shredder builds merkle FEC shreds from Alpenglow block components.
type Shredder struct {
	Slot          uint64
	ParentSlot    uint64
	Version       uint16
	ReferenceTick uint8
}

// ShredBatch contains the FEC batches emitted for a single block component.
type ShredBatch struct {
	Slot              uint64
	Component         BlockComponent
	DataShreds        []*Shred
	CodeShreds        []*Shred
	Packets           [][]byte
	ChainedMerkleRoot solana.Hash
	IsLastInSlot      bool
}

// MakeMerkleShredsFromComponent serializes and shreds one block component.
func (s *Shredder) MakeMerkleShredsFromComponent(
	leader solana.PrivateKey,
	component BlockComponent,
	isLastInSlot bool,
	chainedMerkleRoot solana.Hash,
	nextShredIndex uint32,
	nextCodeIndex uint32,
) (ShredBatch, uint32, uint32, error) {
	generated, nextData, nextCode, err := s.makeMerklePacketsFromComponent(
		leader, component, isLastInSlot, chainedMerkleRoot, nextShredIndex, nextCodeIndex,
	)
	if err != nil {
		return ShredBatch{}, nextShredIndex, nextCodeIndex, err
	}
	batch := ShredBatch{
		Slot:              s.Slot,
		Component:         component,
		Packets:           generated.packets,
		ChainedMerkleRoot: generated.chainedMerkleRoot,
		IsLastInSlot:      isLastInSlot,
	}
	for _, packet := range batch.Packets {
		shred, err := ParseShred(packet)
		if err != nil {
			return ShredBatch{}, nextShredIndex, nextCodeIndex, fmt.Errorf("parse generated shred: %w", err)
		}
		if shred.Type == ShredTypeData {
			batch.DataShreds = append(batch.DataShreds, shred)
		} else {
			batch.CodeShreds = append(batch.CodeShreds, shred)
		}
	}
	return batch, nextData, nextCode, nil
}

// makeMerklePacketsFromComponent serves the producer, which needs the wire
// packets and FEC roots but not the owning Shred objects exposed by the public API.
func (s *Shredder) makeMerklePacketsFromComponent(
	leader solana.PrivateKey,
	component BlockComponent,
	isLastInSlot bool,
	chainedMerkleRoot solana.Hash,
	nextShredIndex uint32,
	nextCodeIndex uint32,
) (shredPackets, uint32, uint32, error) {
	bytes, err := MarshalBlockComponent(component)
	if err != nil {
		return shredPackets{}, nextShredIndex, nextCodeIndex, err
	}
	gen := ShredGenerator{
		Slot:          s.Slot,
		ParentSlot:    s.ParentSlot,
		Version:       s.Version,
		ReferenceTick: s.ReferenceTick,
	}
	batch, nextData, nextCode, err := gen.makeShredsFromData(
		leader,
		bytes,
		isLastInSlot,
		chainedMerkleRoot,
		nextShredIndex,
		nextCodeIndex,
	)
	if err != nil {
		return shredPackets{}, nextShredIndex, nextCodeIndex, err
	}
	return batch, nextData, nextCode, nil
}

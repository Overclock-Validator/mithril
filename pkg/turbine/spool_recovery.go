package turbine

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"

	"github.com/klauspost/reedsolomon"
)

type spoolRecoveryRecord struct {
	offset int64
	size   uint32
	crc    uint32
}

type spoolRecoverySet struct {
	dataShreds uint16
	records    map[spoolShredKey]spoolRecoveryRecord
}

type spoolRecoveryIndex map[uint32]*spoolRecoverySet

func (idx spoolRecoveryIndex) add(slot uint64, packet []byte, offset int64) error {
	if len(packet) < commonShredHeaderSize || len(packet) > packetDataSize+4 {
		return fmt.Errorf("invalid recovery record size %d", len(packet))
	}
	if binary.LittleEndian.Uint64(packet[shredSlotOffset:]) != slot {
		return fmt.Errorf("spool slot mismatch")
	}
	kind, err := classifyVariant(packet[shredVariantOffset])
	if err != nil {
		return err
	}
	key := spoolShredKey{type_: kind, index: binary.LittleEndian.Uint32(packet[shredIndexOffset:]), fecSetIndex: binary.LittleEndian.Uint32(packet[shredFECSetIndexOffset:])}
	if key.fecSetIndex >= maxDataShredsPerSlot {
		return fmt.Errorf("invalid recovery FEC index")
	}
	var dataShreds uint16
	if kind == ShredTypeCode {
		if len(packet) < codingHeaderSize {
			return ErrShortShred
		}
		key.position = binary.LittleEndian.Uint16(packet[codingPositionOffset:])
		dataShreds = binary.LittleEndian.Uint16(packet[codingNumDataOffset:])
	}
	set := idx[key.fecSetIndex]
	if set == nil {
		set = &spoolRecoverySet{records: make(map[spoolShredKey]spoolRecoveryRecord)}
		idx[key.fecSetIndex] = set
	}
	if kind == ShredTypeCode {
		if set.dataShreds != 0 && set.dataShreds != dataShreds {
			return fmt.Errorf("mixed recovery coding layout")
		}
		set.dataShreds = dataShreds
	}
	if _, exists := set.records[key]; exists {
		return nil
	}
	if len(set.records) >= 256 {
		return fmt.Errorf("too many recovery shreds")
	}
	set.records[key] = spoolRecoveryRecord{offset: offset, size: uint32(len(packet)), crc: crc32.ChecksumIEEE(packet)}
	return nil
}

func (s *ShredSpool) recoveryIndexLocked(slot uint64) (spoolRecoveryIndex, error) {
	if idx := s.recovery[slot]; idx != nil {
		return idx, nil
	}
	packets, err := s.readSlotLocked(slot)
	if err != nil {
		return nil, err
	}
	idx := make(spoolRecoveryIndex)
	offset := int64(len(spoolFileMagic))
	for _, packet := range packets {
		if err := idx.add(slot, packet, offset); err != nil {
			return nil, err
		}
		offset += int64(spoolRecordHeaderSize + len(packet))
	}
	if s.recovery == nil {
		s.recovery = make(map[uint64]spoolRecoveryIndex)
	}
	s.recovery[slot] = idx
	return idx, nil
}

func (s *ShredSpool) readRecoveryRecordLocked(slot uint64, f *os.File, ref spoolRecoveryRecord) (packet []byte, err error) {
	defer func() {
		if err != nil {
			delete(s.recovery, slot)
		}
	}()
	record := make([]byte, spoolRecordHeaderSize+int(ref.size))
	if _, err := f.ReadAt(record, ref.offset); err != nil {
		return nil, err
	}
	packet = record[spoolRecordHeaderSize:]
	if binary.LittleEndian.Uint32(record) != ref.size || binary.LittleEndian.Uint32(record[4:]) != ref.crc || crc32.ChecksumIEEE(packet) != ref.crc {
		return nil, fmt.Errorf("recovery record checksum mismatch")
	}
	return packet, nil
}

// RecoverDataShred reconstructs a missing packet from retained verified shreds,
// preserving the leader's signed Merkle root. Call after an indexed lookup
// misses. Recovery also attempts to cache the reconstructed data packets.
func (s *ShredSpool) RecoverDataShred(slot uint64, index uint64) ([]byte, bool, error) {
	if index >= maxDataShredsPerSlot {
		return nil, false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, false, fmt.Errorf("shred spool is closed")
	}
	if slot < s.floor || s.sizes[slot] <= int64(len(spoolFileMagic)) {
		return nil, false, nil
	}
	const maxRecoveryBytes = int64(maxDataShredsPerSlot * 2 * (packetDataSize + spoolRecordHeaderSize))
	if s.sizes[slot] > maxRecoveryBytes {
		return nil, false, fmt.Errorf("slot %d exceeds recovery byte limit", slot)
	}
	// The first lookup indexes one bounded slot under the spool lock.
	idx, err := s.recoveryIndexLocked(slot)
	if err != nil {
		return nil, false, err
	}
	var selected *spoolRecoverySet
	var original spoolRecoveryRecord
	var haveOriginal bool
	for first, set := range idx {
		if ref, ok := set.records[spoolShredKey{type_: ShredTypeData, index: uint32(index), fecSetIndex: first}]; ok {
			original, haveOriginal = ref, true
			break
		}
		if uint64(first) <= index && index < uint64(first)+uint64(set.dataShreds) {
			if selected != nil {
				return nil, false, fmt.Errorf("overlapping recovery FEC sets")
			}
			selected = set
		}
	}
	if !haveOriginal && selected == nil {
		return nil, false, nil
	}
	s.closeSlotLocked(slot)
	f, err := os.Open(s.pathFor(slot))
	if err != nil {
		return nil, false, err
	}
	defer f.Close()
	if haveOriginal {
		packet, err := s.readRecoveryRecordLocked(slot, f, original)
		if err != nil {
			return nil, false, err
		}
		shred, err := ParseShred(packet)
		if err != nil {
			return nil, false, err
		}
		return shred.Payload, true, nil
	}
	batch := make([][]byte, 0, len(selected.records))
	for _, ref := range selected.records {
		packet, err := s.readRecoveryRecordLocked(slot, f, ref)
		if err != nil {
			return nil, false, err
		}
		batch = append(batch, packet)
	}
	recovered, err := recoverMerkleDataPackets(batch)
	if errors.Is(err, reedsolomon.ErrTooFewShards) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var target []byte
	for _, packet := range recovered {
		shred, err := ParseShred(packet)
		if err != nil {
			return nil, false, err
		}
		s.appendShredLocked(shred, packet)
		if uint64(shred.Index) == index {
			target = packet
		}
	}
	return target, target != nil, nil
}

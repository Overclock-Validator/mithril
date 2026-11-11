// Package shredcap reads and writes Firedancer's shredcap capture format
// (v0.1), the input of `firedancer-dev backtest`: a pcapng file, optionally
// zstd-compressed, whose network interfaces carry one shred per IPv4/UDP
// packet and whose "shredcap0" interface carries bank hash and root slot
// records. The format is specified in Firedancer's
// src/discof/backtest/shredcap.md and fd_shredcap.h.
package shredcap

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

// Kind says what a Record holds.
type Kind uint8

const (
	KindShred    Kind = iota + 1 // Shred holds one shred's bytes (a UDP payload)
	KindBankHash                 // Slot, BankHash and DataShreds are set
	KindRootSlot                 // Slot is set
)

// Record is one shred or metadata record, in file order.
type Record struct {
	Kind Kind
	// Shred aliases the reader's buffer and is valid until the next call to
	// Next.
	Shred []byte
	Slot  uint64
	// BankHash is the hash observed after replaying Slot; DataShreds is the
	// number of data shreds that produced it.
	BankHash   [32]byte
	DataShreds uint64
}

const (
	blockSectionHeader = 0x0A0D0D0A
	blockInterface     = 1
	blockSimplePacket  = 3
	blockEnhanced      = 6

	byteOrderMagic = 0x1A2B3C4D

	linkEthernet = 1
	linkRaw      = 101
	linkUser0    = 147
	linkIPv4     = 228

	optEnd    = 0
	optIfName = 2

	// MetadataInterfaceName names the interface that carries records.
	MetadataInterfaceName = "shredcap0"

	recordBankHashV0 = 0x1
	recordRootSlotV0 = 0x2
	bankHashV0Size   = 8 + 32 + 8
	rootSlotV0Size   = 8

	// maxBlockSize bounds one pcapng block. A shred packet is under 1.3 KiB;
	// the bound only stops a corrupt length from allocating gigabytes.
	maxBlockSize = 16 << 20
)

var zstdMagic = []byte{0x28, 0xB5, 0x2F, 0xFD}

type ifaceKind uint8

const (
	ifaceIgnored ifaceKind = iota
	ifaceEthernet
	ifaceIP
	ifaceMetadata
)

// Reader returns a capture's records in file order.
type Reader struct {
	in        *bufio.Reader
	zstd      *zstd.Decoder
	order     binary.ByteOrder
	ifaces    []ifaceKind
	inSection bool
	buf       []byte
	offset    int64
}

// NewReader reads a shredcap capture from r, decompressing it when it starts
// with a zstd frame. Legacy pcap files are not supported.
func NewReader(r io.Reader) (*Reader, error) {
	in := bufio.NewReaderSize(r, 1<<20)
	rd := &Reader{in: in}
	if magic, err := in.Peek(len(zstdMagic)); err == nil && bytes.Equal(magic, zstdMagic) {
		dec, err := zstd.NewReader(in, zstd.WithDecoderConcurrency(1))
		if err != nil {
			return nil, fmt.Errorf("shredcap: zstd: %w", err)
		}
		rd.zstd = dec
		rd.in = bufio.NewReaderSize(dec, 1<<20)
	}
	first, err := rd.in.Peek(4)
	if err != nil {
		rd.Close()
		return nil, fmt.Errorf("shredcap: reading the first block: %w", err)
	}
	if binary.LittleEndian.Uint32(first) != blockSectionHeader {
		rd.Close()
		return nil, errors.New("shredcap: not a pcapng file (no section header block)")
	}
	return rd, nil
}

// Close releases the zstd decoder, if any. It does not close the underlying
// reader.
func (r *Reader) Close() {
	if r.zstd != nil {
		r.zstd.Close()
		r.zstd = nil
	}
}

// Next returns the next shred or metadata record, or io.EOF after the last.
// Blocks and packets that are not shredcap data (other link types, non-UDP
// packets, unknown record types, Simple Packet Blocks) are skipped.
func (r *Reader) Next() (Record, error) {
	for {
		blockType, body, err := r.readBlock()
		if err != nil {
			return Record{}, err
		}
		switch blockType {
		case blockSectionHeader:
			// A new section resets interface numbering.
			r.ifaces = r.ifaces[:0]
		case blockInterface:
			kind, err := r.parseInterface(body)
			if err != nil {
				return Record{}, err
			}
			r.ifaces = append(r.ifaces, kind)
		case blockEnhanced:
			rec, ok, err := r.parseEnhanced(body)
			if err != nil {
				return Record{}, err
			}
			if ok {
				return rec, nil
			}
		}
	}
}

// readBlock returns a block's type and its body, without the type, length
// and trailing length fields.
func (r *Reader) readBlock() (uint32, []byte, error) {
	var head [8]byte
	if _, err := io.ReadFull(r.in, head[:]); err != nil {
		if err == io.EOF {
			return 0, nil, io.EOF
		}
		return 0, nil, r.errorf("block header: %v", err)
	}
	blockType := binary.LittleEndian.Uint32(head[:]) // palindromic for a section header
	if blockType == blockSectionHeader {
		var magic [4]byte
		if _, err := io.ReadFull(r.in, magic[:]); err != nil {
			return 0, nil, r.errorf("section header: %v", err)
		}
		switch {
		case binary.LittleEndian.Uint32(magic[:]) == byteOrderMagic:
			r.order = binary.LittleEndian
		case binary.BigEndian.Uint32(magic[:]) == byteOrderMagic:
			r.order = binary.BigEndian
		default:
			return 0, nil, r.errorf("section header has no byte-order magic")
		}
		r.inSection = true
		total := r.order.Uint32(head[4:])
		body, err := r.readBody(total, 12)
		if err != nil {
			return 0, nil, err
		}
		r.offset += int64(total)
		return blockType, body, nil
	}
	if !r.inSection {
		return 0, nil, r.errorf("block before the first section header")
	}
	blockType = r.order.Uint32(head[:])
	total := r.order.Uint32(head[4:])
	body, err := r.readBody(total, 8)
	if err != nil {
		return 0, nil, err
	}
	r.offset += int64(total)
	return blockType, body, nil
}

// readBody reads the rest of a block of total bytes, consumed bytes of which
// have been read, and checks the trailing length.
func (r *Reader) readBody(total uint32, consumed int) ([]byte, error) {
	if total%4 != 0 || int(total) < consumed+4 || total > maxBlockSize {
		return nil, r.errorf("invalid block length %d", total)
	}
	n := int(total) - consumed
	if cap(r.buf) < n {
		r.buf = make([]byte, n)
	}
	buf := r.buf[:n]
	if _, err := io.ReadFull(r.in, buf); err != nil {
		return nil, r.errorf("block of %d bytes: %v", total, err)
	}
	if r.order.Uint32(buf[n-4:]) != total {
		return nil, r.errorf("block trailing length does not match %d", total)
	}
	return buf[:n-4], nil
}

func (r *Reader) parseInterface(body []byte) (ifaceKind, error) {
	if len(body) < 8 {
		return 0, r.errorf("interface description block too short")
	}
	link := r.order.Uint16(body)
	name := ""
	for opts := body[8:]; len(opts) >= 4; {
		code, size := r.order.Uint16(opts), int(r.order.Uint16(opts[2:]))
		if code == optEnd {
			break
		}
		if 4+size > len(opts) {
			return 0, r.errorf("interface option %d overruns its block", code)
		}
		if code == optIfName {
			name = string(bytes.TrimRight(opts[4:4+size], "\x00"))
		}
		opts = opts[min(len(opts), 4+(size+3)&^3):]
	}
	switch {
	case name == MetadataInterfaceName || link == linkUser0:
		return ifaceMetadata, nil
	case link == linkEthernet:
		return ifaceEthernet, nil
	case link == linkRaw || link == linkIPv4:
		return ifaceIP, nil
	default:
		return ifaceIgnored, nil
	}
}

func (r *Reader) parseEnhanced(body []byte) (Record, bool, error) {
	if len(body) < 20 {
		return Record{}, false, r.errorf("enhanced packet block too short")
	}
	id := r.order.Uint32(body)
	captured := int(r.order.Uint32(body[12:]))
	if captured > len(body)-20 {
		return Record{}, false, r.errorf("packet of %d bytes overruns its block", captured)
	}
	if int(id) >= len(r.ifaces) {
		return Record{}, false, r.errorf("packet on undeclared interface %d", id)
	}
	data := body[20 : 20+captured]
	switch r.ifaces[id] {
	case ifaceMetadata:
		return parseMetadata(data)
	case ifaceEthernet:
		// EtherType IPv4 only, like Firedancer's reader.
		if len(data) < 14 || data[12] != 0x08 || data[13] != 0x00 {
			return Record{}, false, nil
		}
		data = data[14:]
		fallthrough
	case ifaceIP:
		payload, ok := udpPayload(data)
		if !ok {
			return Record{}, false, nil
		}
		return Record{Kind: KindShred, Shred: payload}, true, nil
	}
	return Record{}, false, nil
}

// udpPayload returns what follows the IPv4 and UDP headers. Like Firedancer's
// reader it takes the captured remainder as the payload rather than trusting
// the IP or UDP length fields.
func udpPayload(ip []byte) ([]byte, bool) {
	if len(ip) < 20+8 || ip[0]>>4 != 4 {
		return nil, false
	}
	headerLen := int(ip[0]&0x0F) * 4
	if headerLen < 20 || len(ip) < headerLen+8 || ip[9] != 17 {
		return nil, false
	}
	return ip[headerLen+8:], true
}

// parseMetadata decodes a metadata record. Integers are little endian, as
// the specification states.
func parseMetadata(data []byte) (Record, bool, error) {
	if len(data) < 4 {
		return Record{}, false, nil
	}
	body := data[4:]
	switch binary.LittleEndian.Uint32(data) {
	case recordBankHashV0:
		if len(body) < bankHashV0Size {
			return Record{}, false, nil
		}
		rec := Record{Kind: KindBankHash, Slot: binary.LittleEndian.Uint64(body)}
		copy(rec.BankHash[:], body[8:40])
		rec.DataShreds = binary.LittleEndian.Uint64(body[40:])
		return rec, true, nil
	case recordRootSlotV0:
		if len(body) < rootSlotV0Size {
			return Record{}, false, nil
		}
		return Record{Kind: KindRootSlot, Slot: binary.LittleEndian.Uint64(body)}, true, nil
	}
	return Record{}, false, nil
}

func (r *Reader) errorf(format string, args ...any) error {
	return fmt.Errorf("shredcap: at byte %d: %s", r.offset, fmt.Sprintf(format, args...))
}

package shredcap

import (
	"bufio"
	"encoding/binary"
	"io"
)

// Writer produces a capture in the layout of Firedancer's
// blockstore2shredcap: one section, interface 0 an IPv4 link named "lo" for
// shreds (wrapped in IPv4/UDP headers from 127.0.0.1:42424 to port 8003) and
// interface 1 the "shredcap0" metadata link. Timestamps are zero. Firedancer's
// backtest can replay a file written this way.
type Writer struct {
	out *bufio.Writer
	err error
	buf []byte
}

// NewWriter writes the section header and both interface descriptions.
// Wrap w in a zstd encoder to produce a .pcapng.zst file.
func NewWriter(w io.Writer, application string) (*Writer, error) {
	wr := &Writer{out: bufio.NewWriterSize(w, 1<<20)}
	var shb []byte
	shb = binary.LittleEndian.AppendUint32(shb, byteOrderMagic)
	shb = binary.LittleEndian.AppendUint16(shb, 1) // major version
	shb = binary.LittleEndian.AppendUint16(shb, 0) // minor version
	shb = binary.LittleEndian.AppendUint64(shb, ^uint64(0))
	shb = appendOption(shb, 4, []byte(application)) // shb_userappl
	shb = appendOption(shb, optEnd, nil)
	wr.block(blockSectionHeader, shb)
	wr.block(blockInterface, interfaceBody(linkIPv4, "lo"))
	wr.block(blockInterface, interfaceBody(linkUser0, MetadataInterfaceName))
	return wr, wr.err
}

func interfaceBody(link uint16, name string) []byte {
	var b []byte
	b = binary.LittleEndian.AppendUint16(b, link)
	b = binary.LittleEndian.AppendUint16(b, 0) // reserved
	b = binary.LittleEndian.AppendUint32(b, 0) // snaplen: unlimited
	b = appendOption(b, optIfName, []byte(name))
	b = appendOption(b, 9, []byte{9}) // if_tsresol: nanoseconds
	return appendOption(b, optEnd, nil)
}

func appendOption(b []byte, code uint16, value []byte) []byte {
	b = binary.LittleEndian.AppendUint16(b, code)
	b = binary.LittleEndian.AppendUint16(b, uint16(len(value)))
	b = append(b, value...)
	for i := len(value); i%4 != 0; i++ {
		b = append(b, 0)
	}
	return b
}

// WriteShred writes one shred as an IPv4/UDP packet on interface 0.
func (w *Writer) WriteShred(shred []byte) error {
	const ipHeader, udpHeader = 20, 8
	packet := make([]byte, ipHeader+udpHeader, ipHeader+udpHeader+len(shred))
	total := ipHeader + udpHeader + len(shred)
	packet[0] = 0x45 // IPv4, 20-byte header
	binary.BigEndian.PutUint16(packet[2:], uint16(total))
	packet[6] = 0x40 // don't fragment
	packet[8] = 64   // TTL
	packet[9] = 17   // UDP
	copy(packet[12:], []byte{127, 0, 0, 1})
	copy(packet[16:], []byte{127, 0, 0, 1})
	binary.BigEndian.PutUint16(packet[10:], ipChecksum(packet[:ipHeader]))
	binary.BigEndian.PutUint16(packet[20:], 42424)
	binary.BigEndian.PutUint16(packet[22:], 8003)
	binary.BigEndian.PutUint16(packet[24:], uint16(udpHeader+len(shred)))
	return w.packet(0, append(packet, shred...))
}

// WriteBankHash records the bank hash observed after replaying slot from
// dataShreds data shreds.
func (w *Writer) WriteBankHash(slot uint64, hash [32]byte, dataShreds uint64) error {
	rec := binary.LittleEndian.AppendUint32(nil, recordBankHashV0)
	rec = binary.LittleEndian.AppendUint64(rec, slot)
	rec = append(rec, hash[:]...)
	rec = binary.LittleEndian.AppendUint64(rec, dataShreds)
	return w.packet(1, rec)
}

// WriteRootSlot records that slot was rooted.
func (w *Writer) WriteRootSlot(slot uint64) error {
	rec := binary.LittleEndian.AppendUint32(nil, recordRootSlotV0)
	return w.packet(1, binary.LittleEndian.AppendUint64(rec, slot))
}

// Flush writes buffered blocks to the underlying writer.
func (w *Writer) Flush() error {
	if w.err != nil {
		return w.err
	}
	return w.out.Flush()
}

func (w *Writer) packet(iface uint32, data []byte) error {
	b := w.buf[:0]
	b = binary.LittleEndian.AppendUint32(b, iface)
	b = binary.LittleEndian.AppendUint64(b, 0) // timestamp
	b = binary.LittleEndian.AppendUint32(b, uint32(len(data)))
	b = binary.LittleEndian.AppendUint32(b, uint32(len(data)))
	b = append(b, data...)
	for len(b)%4 != 0 {
		b = append(b, 0)
	}
	w.buf = b
	w.block(blockEnhanced, b)
	return w.err
}

func (w *Writer) block(blockType uint32, body []byte) {
	if w.err != nil {
		return
	}
	total := uint32(12 + len(body))
	var head [8]byte
	binary.LittleEndian.PutUint32(head[:], blockType)
	binary.LittleEndian.PutUint32(head[4:], total)
	if _, w.err = w.out.Write(head[:]); w.err != nil {
		return
	}
	if _, w.err = w.out.Write(body); w.err != nil {
		return
	}
	_, w.err = w.out.Write(head[4:])
}

func ipChecksum(header []byte) uint16 {
	var sum uint32
	for i := 0; i+1 < len(header); i += 2 {
		sum += uint32(binary.BigEndian.Uint16(header[i:]))
	}
	for sum > 0xFFFF {
		sum = sum&0xFFFF + sum>>16
	}
	return ^uint16(sum)
}

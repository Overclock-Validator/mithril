package loader

import (
	"bufio"
	"bytes"
	"debug/elf"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/bits"
	"strings"

	"github.com/Overclock-Validator/mithril/pkg/sbpf"
)

// parse checks ELF file for validity and loads metadata with minimal allocations.
func (l *Loader) parse() error {
	if err := l.readHeader(); err != nil {
		return err
	}
	if l.enableStricterElfHeaders() {
		return l.parseStrict()
	}
	if err := l.validateElfHeader(); err != nil {
		return err
	}
	if err := l.loadProgramHeaderTable(); err != nil {
		return err
	}
	if err := l.readSectionHeaderTable(); err != nil {
		return err
	}
	if err := l.parseSections(); err != nil {
		return err
	}
	if err := l.parseDynamic(); err != nil {
		return err
	}
	if err := l.validate(); err != nil {
		return err
	}

	return nil
}

const (
	ehLen    = 0x40 // sizeof(elf.Header64)
	phEntLen = 0x38 // sizeof(elf.Prog64)
	shEntLen = 0x40 // sizeof(elf.Section64)
	dynLen   = 0x10 // sizeof(elf.Dyn64)
	relLen   = 0x10 // sizeof(elf.Rel64)
	symLen   = 0x18 // sizeof(elf.Sym64)
)

const (
	maxSectionNameLen = 16
	maxSymbolNameLen  = 64
)

const (
	EM_SBPF    = 263
	EF_SBPF_V2 = 32
)

const (
	elfIdentABIVersion = 8
	elfIdentPadStart   = 9
)

const (
	progFlagX = 0x1
	progFlagR = 0x4
)

func (l *Loader) parseStrict() error {
	if err := l.validateStrictElfHeader(); err != nil {
		return err
	}

	programHeaders, err := l.readProgramHeaders()
	if err != nil {
		return err
	}

	expectedProgramHeaders := []struct {
		flags uint32
		vaddr uint64
	}{
		{progFlagR, 0},
		{progFlagX, sbpf.VaddrProgram},
	}

	skipRodataProgramHeader := programHeaders[0].Flags != expectedProgramHeaders[0].flags
	if skipRodataProgramHeader {
		expectedProgramHeaders = expectedProgramHeaders[1:]
	} else if l.eh.Phnum < 2 {
		return fmt.Errorf("invalid ELF file")
	}

	expectedOffset := uint64(ehLen) + uint64(l.eh.Phnum)*phEntLen
	for i, expected := range expectedProgramHeaders {
		ph := programHeaders[i]
		if err := l.validateStrictProgramHeader(ph, expected.flags, expected.vaddr, expectedOffset); err != nil {
			return err
		}
		expectedOffset = clampAddUint64(expectedOffset, ph.Filesz)
	}

	var bytecodeHeader elf.Prog64
	if skipRodataProgramHeader {
		l.rodataRange = addrRange{min: uint64(ehLen) + uint64(l.eh.Phnum)*phEntLen, max: uint64(ehLen) + uint64(l.eh.Phnum)*phEntLen}
		bytecodeHeader = programHeaders[0]
	} else {
		rodataHeader := programHeaders[0]
		l.rodataRange = addrRange{min: rodataHeader.Off, max: rodataHeader.Off + rodataHeader.Filesz}
		bytecodeHeader = programHeaders[1]
	}

	l.textAddr = bytecodeHeader.Vaddr
	l.textRange = addrRange{min: bytecodeHeader.Off, max: bytecodeHeader.Off + bytecodeHeader.Filesz}
	if !bytecodeHeaderContainsEntrypoint(bytecodeHeader, l.eh.Entry) || l.eh.Entry%sbpf.SlotSize != 0 {
		return fmt.Errorf("invalid ELF file")
	}

	return nil
}

func (l *Loader) newShTableIter() *shTableIter {
	eh := &l.eh
	return &shTableIter{
		l:        l,
		off:      eh.Shoff,
		count:    uint32(eh.Shnum),
		elemSize: shEntLen,
	}
}

func (l *Loader) newPhTableIter() *phTableIter {
	eh := &l.eh
	return &phTableIter{
		l:        l,
		off:      eh.Phoff,
		count:    uint32(eh.Phnum),
		elemSize: phEntLen,
	}
}

func (l *Loader) newSymTableIter(sh *elf.Section64) (*symTableIter, error) {
	return &symTableIter{
		l:        l,
		off:      sh.Off,
		count:    uint32(sh.Size / symLen),
		elemSize: symLen,
	}, nil
}

func (l *Loader) readHeader() error {
	if l.fileSize < ehLen {
		return ErrOutOfBounds
	}

	var hdrBuf [ehLen]byte
	if _, err := io.ReadFull(io.NewSectionReader(l.rd, 0, ehLen), hdrBuf[:]); err != nil {
		return err
	}

	copy(l.eh.Ident[:], hdrBuf[:16])
	l.eh.Type = binary.LittleEndian.Uint16(hdrBuf[16:18])
	l.eh.Machine = binary.LittleEndian.Uint16(hdrBuf[18:20])
	l.eh.Version = binary.LittleEndian.Uint32(hdrBuf[20:24])
	l.eh.Entry = binary.LittleEndian.Uint64(hdrBuf[24:32])
	l.eh.Phoff = binary.LittleEndian.Uint64(hdrBuf[32:40])
	l.eh.Shoff = binary.LittleEndian.Uint64(hdrBuf[40:48])
	l.eh.Flags = binary.LittleEndian.Uint32(hdrBuf[48:52])
	l.eh.Ehsize = binary.LittleEndian.Uint16(hdrBuf[52:54])
	l.eh.Phentsize = binary.LittleEndian.Uint16(hdrBuf[54:56])
	l.eh.Phnum = binary.LittleEndian.Uint16(hdrBuf[56:58])
	l.eh.Shentsize = binary.LittleEndian.Uint16(hdrBuf[58:60])
	l.eh.Shnum = binary.LittleEndian.Uint16(hdrBuf[60:62])
	l.eh.Shstrndx = binary.LittleEndian.Uint16(hdrBuf[62:64])

	return nil
}

func isAligned(val uint64, alignment uint64) bool {
	return (val % alignment) == 0
}

func (l *Loader) validateElfHeader() error {
	eh := &l.eh
	ident := &eh.Ident

	if string(ident[:elf.EI_CLASS]) != elf.ELFMAG {
		return fmt.Errorf("not an ELF file")
	}

	if elf.Class(ident[elf.EI_CLASS]) != elf.ELFCLASS64 ||
		elf.Data(ident[elf.EI_DATA]) != elf.ELFDATA2LSB ||
		elf.Version(ident[elf.EI_VERSION]) != elf.EV_CURRENT ||
		elf.OSABI(ident[elf.EI_OSABI]) != elf.ELFOSABI_NONE ||
		elf.Type(eh.Type) != elf.ET_DYN ||
		(elf.Machine(eh.Machine) != elf.EM_BPF && elf.Machine(eh.Machine) != EM_SBPF) ||
		eh.Version != 1 {
		return fmt.Errorf("incompatible binary")
	}

	if eh.Version != uint32(elf.EV_CURRENT) ||
		eh.Ehsize != ehLen ||
		eh.Phentsize != phEntLen ||
		eh.Shentsize != shEntLen ||
		eh.Shstrndx >= eh.Shnum {
		return fmt.Errorf("invalid ELF file")
	}

	if eh.Flags == EF_SBF_V2 {
		return fmt.Errorf("invalid sbpf version")
	}
	if eh.Flags < l.minSbpfVersion || eh.Flags > l.maxSbpfVersion {
		return fmt.Errorf("invalid sbpf version")
	}

	phTableSize := uint64(eh.Phnum) * phEntLen
	if phTableSize > 0 && eh.Phoff < ehLen {
		return fmt.Errorf("program header overlaps with file header")
	}
	if eh.Shoff < ehLen {
		return fmt.Errorf("section header overlaps with file header")
	}

	overlap, err := isOverlap(eh.Phoff, uint64(eh.Phnum)*phEntLen, eh.Shoff, uint64(eh.Shnum)*shEntLen)
	if err != nil {
		return err
	}
	if overlap {
		return fmt.Errorf("program and section header overlap")
	}

	return nil
}

func (l *Loader) validateSbpfVersion() error {
	eh := &l.eh
	if eh.Flags == EF_SBF_V2 {
		return fmt.Errorf("invalid sbpf version")
	}
	if eh.Flags < l.minSbpfVersion || eh.Flags > l.maxSbpfVersion {
		return fmt.Errorf("invalid sbpf version")
	}
	return nil
}

func (l *Loader) validateStrictElfHeader() error {
	eh := &l.eh
	ident := &eh.Ident

	if err := l.validateSbpfVersion(); err != nil {
		return err
	}

	if string(ident[:elf.EI_CLASS]) != elf.ELFMAG ||
		elf.Class(ident[elf.EI_CLASS]) != elf.ELFCLASS64 ||
		elf.Data(ident[elf.EI_DATA]) != elf.ELFDATA2LSB ||
		elf.Version(ident[elf.EI_VERSION]) != elf.EV_CURRENT ||
		elf.OSABI(ident[elf.EI_OSABI]) != elf.ELFOSABI_NONE ||
		ident[elfIdentABIVersion] != 0 ||
		!allZeroBytes(ident[elfIdentPadStart:]) ||
		elf.Machine(eh.Machine) != elf.EM_BPF ||
		eh.Version != uint32(elf.EV_CURRENT) ||
		eh.Phoff != ehLen ||
		eh.Ehsize != ehLen ||
		eh.Phentsize != phEntLen ||
		eh.Phnum == 0 {
		return fmt.Errorf("invalid ELF file")
	}

	phTableEnd := uint64(ehLen) + uint64(eh.Phnum)*phEntLen
	if phTableEnd > l.fileSize {
		return fmt.Errorf("invalid ELF file")
	}

	return nil
}

func allZeroBytes(bytes []byte) bool {
	for _, b := range bytes {
		if b != 0 {
			return false
		}
	}
	return true
}

func (l *Loader) readProgramHeaders() ([]elf.Prog64, error) {
	programHeaders := make([]elf.Prog64, 0, l.eh.Phnum)
	iter := l.newPhTableIter()
	for iter.Next() && iter.Err() == nil {
		programHeaders = append(programHeaders, iter.Item())
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	return programHeaders, nil
}

func (l *Loader) validateStrictProgramHeader(ph elf.Prog64, expectedFlags uint32, expectedVaddr uint64, expectedOffset uint64) error {
	if elf.ProgType(ph.Type) != elf.PT_LOAD ||
		ph.Flags != expectedFlags ||
		ph.Off != expectedOffset ||
		ph.Off >= l.fileSize ||
		ph.Off%sbpf.SlotSize != 0 ||
		ph.Vaddr != expectedVaddr ||
		ph.Paddr != expectedVaddr ||
		ph.Filesz != ph.Memsz ||
		ph.Filesz > l.fileSize-ph.Off ||
		ph.Filesz%sbpf.SlotSize != 0 ||
		ph.Memsz >= sbpf.VaddrProgram {
		return fmt.Errorf("invalid program header")
	}
	return nil
}

func bytecodeHeaderContainsEntrypoint(ph elf.Prog64, entrypoint uint64) bool {
	entrypointEnd := clampAddUint64(entrypoint, sbpf.SlotSize-1)
	bytecodeEnd := clampAddUint64(ph.Vaddr, ph.Memsz)
	return ph.Vaddr <= entrypointEnd && entrypointEnd < bytecodeEnd
}

// scan the program header table and remember the last PT_LOAD segment
func (l *Loader) loadProgramHeaderTable() error {
	iter := l.newPhTableIter()

	for iter.Next() && iter.Err() == nil {
		ph := iter.Item()

		switch elf.ProgType(ph.Type) {
		case elf.PT_DYNAMIC:
			{
				// remember first segment with PT_DYNAMIC in case we need it later
				if l.phDynamic == nil {
					l.phDynamic = new(elf.Prog64)
					*l.phDynamic = ph
				}
			}
		case elf.PT_LOAD:
			{
				// vaddr must be ascending
				if ph.Vaddr < l.phLoad.Vaddr {
					return fmt.Errorf("invalid program header")
				}

				segmentEnd, overflow := bits.Add64(ph.Off, ph.Filesz, 0)
				if segmentEnd > l.fileSize || overflow > 0 {
					return fmt.Errorf("segment out of bounds")
				}

				l.phLoad = ph
			}
		default:
			// ignoring other segment types
		}
	}
	return iter.Err()
}

// reads and validates the section header table.
// remembers the section header table.
func (l *Loader) readSectionHeaderTable() error {
	eh := &l.eh
	iter := l.newShTableIter()

	if !iter.Next() {
		return fmt.Errorf("missing section 0")
	}
	if elf.SectionType(iter.Item().Type) != elf.SHT_NULL {
		return fmt.Errorf("section 0 is not SHT_NULL")
	}

	for iter.Next() && iter.Err() == nil {
		i, sh := iter.Index(), iter.Item()
		switch elf.SectionType(sh.Type) {
		case elf.SHT_NOBITS:
			continue
		case elf.SHT_DYNAMIC:
			// remember first section with SHT_DYNAMIC in case we need it later
			if l.shDynamic == nil {
				l.shDynamic = new(elf.Section64)
				*l.shDynamic = sh
			}
		default:
			// ok
		}

		// Ensure section data is not overlapping with ELF headers
		shend, overflow := bits.Add64(sh.Off, sh.Size, 0)
		if overflow != 0 {
			return fmt.Errorf("integer overflow in section %d", i)
		}
		if sh.Off < ehLen {
			return fmt.Errorf("section %d overlaps with file header", i)
		}

		overlap, err := isOverlap(eh.Phoff, uint64(eh.Phnum)*phEntLen, sh.Off, sh.Size)
		if err != nil {
			return err
		}
		if overlap {
			return fmt.Errorf("section %d overlaps with program header", i)
		}

		overlap, err = isOverlap(eh.Shoff, uint64(eh.Shnum)*shEntLen, sh.Off, sh.Size)
		if err != nil {
			return err
		}
		if overlap {
			return fmt.Errorf("section %d overlaps with section header", i)
		}

		if shend > l.fileSize {
			return fmt.Errorf("section %d out of bounds", i)
		}

		// Remember section header string table.
		if eh.Shstrndx != uint16(elf.SHN_UNDEF) && uint32(eh.Shstrndx) == i {
			l.shShstrtab = sh
		}
	}
	// TODO validate offset and size (?)
	if elf.SectionType(l.shShstrtab.Type) != elf.SHT_STRTAB {
		return fmt.Errorf("invalid .shstrtab")
	}
	return iter.Err()
}

func (l *Loader) getString(strtab *elf.Section64, stroff uint32, maxLen uint16) (string, error) {
	if elf.SectionType(strtab.Type) != elf.SHT_STRTAB {
		return "", fmt.Errorf("invalid strtab")
	}
	if uint64(stroff) >= strtab.Size {
		return "", fmt.Errorf("string offset out of bounds")
	}
	offset := strtab.Off + uint64(stroff)
	remaining := strtab.Size - uint64(stroff)
	readLen := uint64(maxLen)
	if readLen > remaining {
		readLen = remaining
	}
	if offset > l.fileSize || offset+readLen > l.fileSize {
		return "", io.ErrUnexpectedEOF
	}
	if directELFStrings && l.input != nil {
		// Constructors retain the same immutable input already held by rd.
		// Check against the actual slice too, so a malformed loader cannot panic.
		if offset > uint64(len(l.input)) || readLen > uint64(len(l.input))-offset {
			return "", io.ErrUnexpectedEOF
		}
		window := l.input[offset : offset+readLen]
		if end := bytes.IndexByte(window, 0); end >= 0 {
			return string(window[:end]), nil
		}
		return "", io.EOF
	}

	// Retain ReaderAt behavior for loaders constructed around another source.
	rd := bufio.NewReader(io.NewSectionReader(l.rd, int64(offset), int64(readLen)))
	var builder strings.Builder
	for {
		b, err := rd.ReadByte()
		if err != nil {
			return "", err
		}
		if b == 0 {
			break
		}
		builder.WriteByte(b)
	}
	return builder.String(), nil
}

// Iterate sections, validate them, and remember special sections by name.
func (l *Loader) parseSections() error {
	shShstrtab := &l.shShstrtab
	iter := l.newShTableIter()
	for iter.Next() && iter.Err() == nil {
		sh := iter.Item()
		sectionName, err := l.getString(shShstrtab, sh.Name, maxSectionNameLen)
		if err != nil {
			return fmt.Errorf("getString: %w", err)
		}

		// Remember special section or error if it already exists.
		setSection := func(shPtr **elf.Section64) error {
			if *shPtr != nil {
				return fmt.Errorf("duplicate section: %s", sectionName)
			}
			*shPtr = new(elf.Section64)
			**shPtr = sh
			return nil
		}
		switch sectionName {
		case ".bss":
			return fmt.Errorf("unsupported section .bss")
		case ".text":
			if elf.SectionType(sh.Type) == elf.SHT_NOBITS {
				sh.Size = 0
			}
			err = setSection(&l.shText)
		case ".symtab":
			err = setSection(&l.shSymtab)
		case ".strtab":
			err = setSection(&l.shStrtab)
		case ".dynstr":
			err = setSection(&l.shDynstr)
		}
		if err != nil {
			return err
		}

		// ElfError::WritableSectionNotSupported
		if strings.HasPrefix(sectionName, ".bss") {
			return fmt.Errorf("unsupported bss-like section")
		}
		if (sh.Flags&uint64(elf.SHF_ALLOC|elf.SHF_WRITE)) == uint64(elf.SHF_ALLOC|elf.SHF_WRITE) &&
			strings.HasPrefix(sectionName, ".data") &&
			!strings.HasPrefix(sectionName, ".data.rel") {
			return fmt.Errorf("unsupported data-like section")
		}

		// bounds check
		// ElfError::ValueOutOfBounds
		if sh.Off+sh.Size < sh.Off || sh.Off+sh.Size > l.fileSize {
			return io.ErrUnexpectedEOF
		}
	}
	return iter.Err()
}

func (l *Loader) newDynIter() (*dynTableIter, error) {
	var off uint64
	var size uint64
	if ph := l.phDynamic; ph != nil && ph.Off%8 == 0 && (ph.Off+ph.Filesz) <= l.fileSize && (ph.Off+ph.Filesz) >= ph.Off {
		off, size = ph.Off, ph.Filesz
	} else if sh := l.shDynamic; sh != nil {
		off, size = sh.Off, sh.Size
	} else {
		return nil, nil
	}

	if (off+size) > l.fileSize || (off+size) < off {
		return nil, io.ErrUnexpectedEOF
	}

	iter := &dynTableIter{
		l:        l,
		off:      off,
		count:    uint32(size / dynLen),
		elemSize: dynLen,
	}

	return iter, nil
}

func (l *Loader) parseDynamicTable() error {
	iter, err := l.newDynIter()
	if err != nil {
		return err
	}
	if iter == nil {
		// static file, nothing to do
		return nil
	}

	for iter.Next() && iter.Err() == nil {
		dyn := iter.Item()
		if dyn.Tag == int64(elf.DT_NULL) {
			break
		}
		if uint64(dyn.Tag) >= 35 { /* DT_NUM in rBPF */
			continue
		}
		l.dynamic[dyn.Tag] = dyn.Val
	}
	return iter.Err()
}

// sectionAt finds the section that has a start address matching vaddr.
func (l *Loader) sectionAt(vaddr uint64) (*elf.Section64, error) {
	iter := l.newShTableIter()
	for iter.Next() && iter.Err() == nil {
		sh := iter.Item()
		if sh.Addr == vaddr {
			return &sh, nil
		}
	}
	return nil, iter.Err()
}

// segmentByVaddr finds the segment which vaddr lies within.
func (l *Loader) segmentByVaddr(vaddr uint64) (*elf.Prog64, error) {
	iter := l.newPhTableIter()
	for iter.Next() && iter.Err() == nil {
		ph := iter.Item()
		if ph.Vaddr+ph.Memsz < ph.Vaddr {
			return nil, fmt.Errorf("segment ends past math.MaxUint64")
		}
		if ph.Vaddr <= vaddr && vaddr < ph.Vaddr+ph.Memsz {
			return &ph, nil
		}
	}
	return nil, iter.Err()
}

func (l *Loader) parseRelocs() error {
	vaddr := l.dynamic[elf.DT_REL]
	if vaddr == 0 {
		return nil
	}
	if l.dynamic[elf.DT_RELENT] != relLen {
		return fmt.Errorf("invalid DT_RELENT")
	}
	size := l.dynamic[elf.DT_RELSZ]
	if size == 0 || size%relLen != 0 || size > math.MaxUint32 {
		return fmt.Errorf("invalid DT_RELSZ")
	}
	ph, err := l.segmentByVaddr(vaddr)
	if err != nil {
		return err
	}
	offset := vaddr
	if ph != nil {
		var overflow uint64
		offset, overflow = bits.Sub64(offset, ph.Vaddr, 0)
		if overflow != 0 {
			return fmt.Errorf("offset underflow")
		}
		offset, overflow = bits.Add64(offset, ph.Off, 0)
		if overflow != 0 {
			return fmt.Errorf("offset overflow")
		}
	} else {
		// Handle invalid dynamic sections where DT_REL is not in any program segment.
		sh, err := l.sectionAt(vaddr)
		if err != nil {
			return err
		}
		if sh == nil {
			return fmt.Errorf("cannot find physical address of relocation table")
		}
		offset = sh.Off
	}
	l.relocsIter, err = newRelTableIter(l, offset, offset+size, relLen)
	return err
}

// getSymtab returns an iterator over the symbols in a symtab-like section.
//
// Performs necessary bounds checking.
func (l *Loader) getSymtab(sh *elf.Section64) (*symTableIter, error) {
	switch elf.SectionType(sh.Type) {
	case elf.SHT_SYMTAB, elf.SHT_DYNSYM:
		break
	default:
		return nil, fmt.Errorf("not a symtab section")
	}

	return l.newSymTableIter(sh)
}

func (l *Loader) parseDynSymtab() (err error) {
	vaddr := l.dynamic[elf.DT_SYMTAB]
	if vaddr == 0 {
		return nil
	}

	l.shDynsym, err = l.sectionAt(vaddr)
	if err != nil {
		return err
	}
	if l.shDynsym == nil {
		return fmt.Errorf("cannot find DT_SYMTAB section")
	}

	l.dynSymIter, err = l.getSymtab(l.shDynsym)
	return err
}

func (l *Loader) parseDynamic() error {
	if err := l.parseDynamicTable(); err != nil {
		return err
	}
	if err := l.parseRelocs(); err != nil {
		return err
	}
	if err := l.parseDynSymtab(); err != nil {
		return err
	}
	return nil
}

// validate performs additional checks after parsing.
func (l *Loader) validate() error {
	if l.shText == nil {
		return fmt.Errorf("missing .text section")
	}
	if !l.checkEntrypoint() {
		return fmt.Errorf("invalid entrypoint")
	}
	return nil
}

func (l *Loader) checkEntrypoint() bool {
	start := l.shText.Addr
	entry := l.eh.Entry
	if (entry-start)%8 != 0 || entry < start {
		return false
	}
	if l.shText.Size == 0 {
		return true
	}
	end, overflow := bits.Add64(start, l.shText.Size, 0)
	if overflow != 0 {
		end = math.MaxUint64
	}
	return entry < end
}

type shTableIter struct {
	l        *Loader
	off      uint64
	i        uint32 // one ahead
	count    uint32
	elemSize uint16
	elem     elf.Section64
	err      error
	scratch  [shEntLen]byte
}

type phTableIter struct {
	l        *Loader
	off      uint64
	i        uint32 // one ahead
	count    uint32
	elemSize uint16
	elem     elf.Prog64
	err      error
	scratch  [phEntLen]byte
}

type dynTableIter struct {
	l        *Loader
	off      uint64
	i        uint32 // one ahead
	count    uint32
	elemSize uint16
	elem     elf.Dyn64
	err      error
	scratch  [dynLen]byte
}

type symTableIter struct {
	l        *Loader
	off      uint64
	i        uint32 // one ahead
	count    uint32
	elemSize uint16
	elem     elf.Sym64
	err      error
}

type relTableIter struct {
	l        *Loader
	off      uint64
	i        uint32 // one ahead
	count    uint32
	elemSize uint16
	elem     elf.Rel64
	err      error
	scratch  [relLen]byte
}

// newRelTableIter is like newTableIterator, but with all necessary bounds checks.
func newRelTableIter(l *Loader, start uint64, end uint64, elemSize uint16) (*relTableIter, error) {
	if end < start || end > l.fileSize {
		return nil, io.ErrUnexpectedEOF
	}
	size := end - start
	if size%uint64(elemSize) != 0 {
		return nil, fmt.Errorf("misaligned table")
	}
	if size > math.MaxInt32 {
		return nil, io.ErrUnexpectedEOF
	}

	iter := &relTableIter{
		l:        l,
		off:      start,
		count:    uint32(size / uint64(elemSize)),
		elemSize: elemSize,
	}
	return iter, nil
}

func (it *shTableIter) Next() (ok bool) {
	ok, it.err = it.getNext()
	if ok && it.err != nil {
		panic("unreachable")
	}
	return
}

func (it *shTableIter) Index() uint32 {
	return it.i - 1
}

func (it *shTableIter) Err() error {
	return it.err
}

func (it *shTableIter) Item() elf.Section64 {
	return it.elem
}

func (it *shTableIter) getNext() (bool, error) {
	if it.i >= it.count {
		return false, nil
	}
	if it.off >= math.MaxInt64 || it.off+uint64(it.elemSize) > math.MaxInt64 {
		return false, io.ErrUnexpectedEOF
	}

	rd := io.NewSectionReader(it.l.rd, int64(it.off), int64(it.elemSize))

	var readBytes []byte
	if iteratorScratch {
		clear(it.scratch[:])
		readBytes = it.scratch[:]
	} else {
		var entryBytes [shEntLen]byte
		readBytes = entryBytes[:]
	}
	_, err := rd.Read(readBytes)
	if err != nil {
		return false, err
	}

	it.elem.Name = binary.LittleEndian.Uint32(readBytes[:4])
	it.elem.Type = binary.LittleEndian.Uint32(readBytes[4:8])
	it.elem.Flags = binary.LittleEndian.Uint64(readBytes[8:16])
	it.elem.Addr = binary.LittleEndian.Uint64(readBytes[16:24])
	it.elem.Off = binary.LittleEndian.Uint64(readBytes[24:32])
	it.elem.Size = binary.LittleEndian.Uint64(readBytes[32:40])
	it.elem.Link = binary.LittleEndian.Uint32(readBytes[40:44])
	it.elem.Info = binary.LittleEndian.Uint32(readBytes[44:48])
	it.elem.Addralign = binary.LittleEndian.Uint64(readBytes[48:56])
	it.elem.Entsize = binary.LittleEndian.Uint64(readBytes[56:64])

	it.off += uint64(it.elemSize)
	it.i++

	return true, nil
}

func (it *phTableIter) Next() (ok bool) {
	ok, it.err = it.getNext()
	if ok && it.err != nil {
		panic("unreachable")
	}
	return
}

func (it *phTableIter) Index() uint32 {
	return it.i - 1
}

func (it *phTableIter) Err() error {
	return it.err
}

func (it *phTableIter) Item() elf.Prog64 {
	return it.elem
}

func (it *phTableIter) getNext() (bool, error) {
	if it.i >= it.count {
		return false, nil
	}
	if it.off >= math.MaxInt64 || it.off+uint64(it.elemSize) > math.MaxInt64 {
		return false, io.ErrUnexpectedEOF
	}

	rd := io.NewSectionReader(it.l.rd, int64(it.off), int64(it.elemSize))

	var readBytes []byte
	if iteratorScratch {
		clear(it.scratch[:])
		readBytes = it.scratch[:]
	} else {
		var entryBytes [phEntLen]byte
		readBytes = entryBytes[:]
	}
	_, err := rd.Read(readBytes)
	if err != nil {
		return false, err
	}

	it.elem.Type = binary.LittleEndian.Uint32(readBytes[:4])
	it.elem.Flags = binary.LittleEndian.Uint32(readBytes[4:8])
	it.elem.Off = binary.LittleEndian.Uint64(readBytes[8:16])
	it.elem.Vaddr = binary.LittleEndian.Uint64(readBytes[16:24])
	it.elem.Paddr = binary.LittleEndian.Uint64(readBytes[24:32])
	it.elem.Filesz = binary.LittleEndian.Uint64(readBytes[32:40])
	it.elem.Memsz = binary.LittleEndian.Uint64(readBytes[40:48])
	it.elem.Align = binary.LittleEndian.Uint64(readBytes[48:56])

	it.off += uint64(it.elemSize)
	it.i++

	return true, nil
}

func (it *dynTableIter) Next() (ok bool) {
	ok, it.err = it.getNext()
	if ok && it.err != nil {
		panic("unreachable")
	}
	return
}

func (it *dynTableIter) Index() uint32 {
	return it.i - 1
}

func (it *dynTableIter) Err() error {
	return it.err
}

func (it *dynTableIter) Item() elf.Dyn64 {
	return it.elem
}

func (it *dynTableIter) getNext() (bool, error) {
	if it.i >= it.count {
		return false, nil
	}
	if it.off >= math.MaxInt64 || it.off+uint64(it.elemSize) > math.MaxInt64 {
		return false, io.ErrUnexpectedEOF
	}

	rd := io.NewSectionReader(it.l.rd, int64(it.off), int64(it.elemSize))

	var readBytes []byte
	if iteratorScratch {
		clear(it.scratch[:])
		readBytes = it.scratch[:]
	} else {
		var entryBytes [dynLen]byte
		readBytes = entryBytes[:]
	}
	_, err := rd.Read(readBytes)
	if err != nil {
		return false, err
	}

	it.elem.Tag = int64(binary.LittleEndian.Uint64(readBytes[:8]))
	it.elem.Val = binary.LittleEndian.Uint64(readBytes[8:16])

	it.off += uint64(it.elemSize)
	it.i++

	return true, nil
}

func (it *relTableIter) Next() (ok bool) {
	ok, it.err = it.getNext()
	if ok && it.err != nil {
		panic("unreachable")
	}
	return
}

func (it *relTableIter) Index() uint32 {
	return it.i - 1
}

func (it *relTableIter) Err() error {
	return it.err
}

func (it *relTableIter) Item() elf.Rel64 {
	return it.elem
}

func (it *relTableIter) getNext() (bool, error) {
	if it.i >= it.count {
		return false, nil
	}
	if it.off >= math.MaxInt64 || it.off+uint64(it.elemSize) > math.MaxInt64 {
		return false, io.ErrUnexpectedEOF
	}

	rd := io.NewSectionReader(it.l.rd, int64(it.off), int64(it.elemSize))

	var readBytes []byte
	if iteratorScratch {
		clear(it.scratch[:])
		readBytes = it.scratch[:]
	} else {
		var entryBytes [relLen]byte
		readBytes = entryBytes[:]
	}
	_, err := rd.Read(readBytes)
	if err != nil {
		return false, err
	}

	it.elem.Off = binary.LittleEndian.Uint64(readBytes[:8])
	it.elem.Info = binary.LittleEndian.Uint64(readBytes[8:16])

	it.off += uint64(it.elemSize)
	it.i++

	return true, nil
}

// lookupFromTable does a point select in a densely packed table.
func lookupFromTable(l *Loader, section *elf.Section64, i uint32, elemSize uint16) (ret elf.Sym64, err error) {
	off := uint64(i) * uint64(elemSize)
	if off > section.Size {
		return ret, io.ErrUnexpectedEOF
	}

	rd := io.NewSectionReader(l.rd, int64(section.Off+off), int64(elemSize))
	var symBytes [24]byte
	_, err = rd.Read(symBytes[:])
	if err != nil {
		return
	}

	ret.Name = binary.LittleEndian.Uint32(symBytes[:4])
	ret.Info = symBytes[4]
	ret.Other = symBytes[5]
	ret.Shndx = binary.LittleEndian.Uint16(symBytes[6:8])
	ret.Value = binary.LittleEndian.Uint64(symBytes[8:16])
	ret.Size = binary.LittleEndian.Uint64(symBytes[16:24])

	return ret, nil
}

func (l *Loader) getDynsym(idx uint32) (elf.Sym64, error) {
	if l.shDynsym == nil {
		return elf.Sym64{}, fmt.Errorf("unknown symbol: no dynamic symbol table")
	}
	return lookupFromTable(l, l.shDynsym, idx, symLen)
}

func (l *Loader) getDynstr(name uint32) (string, error) {
	if l.shDynstr == nil {
		return "", fmt.Errorf("unknown symbol: no dynamic string table")
	}
	return l.getString(l.shDynstr, name, maxSymbolNameLen)
}

func isOverlap(startA uint64, sizeA uint64, startB uint64, sizeB uint64) (bool, error) {
	if startA > startB {
		startA, sizeA, startB, sizeB = startB, sizeB, startA, sizeA
	}
	endA, endB := startA+sizeA, startB+sizeB
	if endA < startA || endB < startB {
		return false, fmt.Errorf("isOverlap: integer overflow")
	}
	return sizeA != 0 && sizeB != 0 && (startA == startB || endA > startB), nil
}

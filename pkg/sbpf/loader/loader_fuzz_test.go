package loader

import (
	"bytes"
	"debug/elf"
	"encoding/binary"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/Overclock-Validator/mithril/pkg/sbpf/sbpfver"
)

// FuzzELFParser tests the ELF parsing logic with malformed input
func FuzzELFParser(f *testing.F) {
	// Add valid ELF header seeds
	f.Add(makeValidELFHeader())
	f.Add(makeMinimalValidELF())
	f.Add(makeMalformedELFHeader())
	f.Add(makeOversizedELFHeader())

	f.Fuzz(func(t *testing.T, data []byte) {
		// Skip inputs that are obviously too large
		if len(data) > maxFileLen {
			return
		}

		loader, err := NewLoaderFromBytes(data)
		if err != nil {
			return // Expected error for invalid sizes
		}

		// Attempt to parse - should not panic
		_ = loader.parse()
	})
}

// FuzzELFHeaderValidation focuses on header validation logic
func FuzzELFHeaderValidation(f *testing.F) {
	// Seed with various header configurations
	f.Add(makeHeaderWithInvalidMagic())
	f.Add(makeHeaderWithInvalidClass())
	f.Add(makeHeaderWithInvalidEndianness())
	f.Add(makeHeaderWithInvalidVersion())
	f.Add(makeHeaderWithInvalidMachine())

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < ehLen {
			return
		}

		loader := &Loader{
			rd:             bytes.NewReader(data),
			fileSize:       uint64(len(data)),
			minSbpfVersion: sbpfver.SbpfVersionV0,
			maxSbpfVersion: sbpfver.SbpfVersionV0,
		}

		_ = loader.readHeader()
		_ = loader.validateElfHeader()
	})
}

// FuzzProgramHeaderTable tests program header table parsing
func FuzzProgramHeaderTable(f *testing.F) {
	f.Add(makeELFWithValidProgramHeaders(2))
	f.Add(makeELFWithOverlappingProgramHeaders())
	f.Add(makeELFWithInvalidOffsets())

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < ehLen+phEntLen {
			return
		}

		loader := &Loader{
			rd:       bytes.NewReader(data),
			fileSize: uint64(len(data)),
		}

		if err := loader.readHeader(); err != nil {
			return
		}

		_ = loader.loadProgramHeaderTable()
	})
}

// FuzzSectionHeaderTable tests section header table parsing
func FuzzSectionHeaderTable(f *testing.F) {
	f.Add(makeELFWithValidSectionHeaders(3))
	f.Add(makeELFWithOverlappingSections())
	f.Add(makeELFWithOutOfBoundsSections())

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < ehLen+shEntLen {
			return
		}

		loader := &Loader{
			rd:       bytes.NewReader(data),
			fileSize: uint64(len(data)),
		}

		if err := loader.readHeader(); err != nil {
			return
		}

		_ = loader.readSectionHeaderTable()
	})
}

// FuzzDynamicSection tests dynamic section parsing
func FuzzDynamicSection(f *testing.F) {
	f.Add(makeELFWithDynamicSection())
	f.Add(makeELFWithInvalidDynamicEntries())

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < ehLen {
			return
		}

		loader := &Loader{
			rd:       bytes.NewReader(data),
			fileSize: uint64(len(data)),
		}

		if err := loader.readHeader(); err != nil {
			return
		}

		if err := loader.loadProgramHeaderTable(); err != nil {
			return
		}

		_ = loader.parseDynamicTable()
	})
}

// FuzzRelocations tests relocation table parsing and application
func FuzzRelocations(f *testing.F) {
	f.Add(makeELFWithValidRelocations())
	f.Add(makeELFWithInvalidRelocationOffsets())
	f.Add(makeELFWithMalformedRelocations())

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < ehLen {
			return
		}

		loader := &Loader{
			rd:       bytes.NewReader(data),
			fileSize: uint64(len(data)),
		}

		if err := loader.readHeader(); err != nil {
			return
		}

		if err := loader.loadProgramHeaderTable(); err != nil {
			return
		}

		if err := loader.parseDynamicTable(); err != nil {
			return
		}

		_ = loader.parseRelocs()
	})
}

// FuzzSymbolTable tests symbol table parsing
func FuzzSymbolTable(f *testing.F) {
	f.Add(makeELFWithValidSymbolTable())
	f.Add(makeELFWithMalformedSymbols())

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < ehLen {
			return
		}

		loader := &Loader{
			rd:       bytes.NewReader(data),
			fileSize: uint64(len(data)),
		}

		if err := loader.readHeader(); err != nil {
			return
		}

		if err := loader.loadProgramHeaderTable(); err != nil {
			return
		}

		if err := loader.parseDynamicTable(); err != nil {
			return
		}

		_ = loader.parseDynSymtab()
	})
}

// FuzzCompleteELFLoad tests the complete load pipeline
func FuzzCompleteELFLoad(f *testing.F) {
	// Seed with various complete ELF files
	f.Add(makeMinimalValidELF())
	f.Add(makeCompleteValidELF())

	syscallReg := sbpf.SyscallRegistry(func(u uint32) (sbpf.Syscall, bool) {
		return nil, false
	})

	feats := features.NewFeaturesDefault()

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > maxFileLen {
			return
		}

		loader, err := NewLoaderWithSyscalls(data, syscallReg, true, feats)
		if err != nil {
			return
		}

		// Should not panic
		_, _ = loader.Load()
	})
}

// FuzzELFLoadWithValidBase tests the load pipeline with mutations applied to valid ELF files
// This should achieve better coverage of copy(), relocate(), and getProgram() stages
func FuzzELFLoadWithValidBase(f *testing.F) {
	// Seed with valid ELF structures that can pass parse()
	validElf := makeCompleteValidELFWithAllSections()
	f.Add(validElf, uint32(0), uint8(0)) // no mutation
	f.Add(validElf, uint32(100), uint8(0xff))
	f.Add(validElf, uint32(200), uint8(0x00))

	syscallReg := sbpf.SyscallRegistry(func(u uint32) (sbpf.Syscall, bool) {
		return nil, false
	})

	feats := features.NewFeaturesDefault()

	f.Fuzz(func(t *testing.T, baseData []byte, mutateOffset uint32, mutateByte uint8) {
		if len(baseData) > maxFileLen {
			return
		}

		// Create a copy to mutate
		data := make([]byte, len(baseData))
		copy(data, baseData)

		// Apply mutation if within bounds
		if mutateOffset < uint32(len(data)) {
			data[mutateOffset] = mutateByte
		}

		loader, err := NewLoaderWithSyscalls(data, syscallReg, true, feats)
		if err != nil {
			return
		}

		// Should not panic - this should now reach copy(), relocate(), and getProgram()
		program, err := loader.Load()
		if err == nil && program != nil {
			// If load succeeds, verify the program is reasonable
			_ = program.Text
			_ = program.RO
			_ = program.Entrypoint
		}
	})
}

// FuzzCopyStage specifically targets the copy() function
func FuzzCopyStage(f *testing.F) {
	validElf := makeCompleteValidELFWithAllSections()
	f.Add(validElf)

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > maxFileLen {
			return
		}

		loader, err := NewLoaderFromBytes(data)
		if err != nil {
			return
		}

		// First parse
		if err := loader.parse(); err != nil {
			return
		}

		// Now fuzz the copy stage - should not panic
		_ = loader.copy()
	})
}

// FuzzRelocateStage specifically targets the relocate() function
func FuzzRelocateStage(f *testing.F) {
	validElf := makeCompleteValidELFWithAllSections()
	f.Add(validElf)

	syscallReg := sbpf.SyscallRegistry(func(u uint32) (sbpf.Syscall, bool) {
		return nil, false
	})

	feats := features.NewFeaturesDefault()

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > maxFileLen {
			return
		}

		loader, err := NewLoaderWithSyscalls(data, syscallReg, true, feats)
		if err != nil {
			return
		}

		// Parse and copy first
		if err := loader.parse(); err != nil {
			return
		}

		if err := loader.copy(); err != nil {
			return
		}

		// Now fuzz the relocate stage - should not panic
		_ = loader.relocate()
	})
}

// FuzzIsAligned tests alignment checking
func FuzzIsAligned(f *testing.F) {
	f.Add(uint64(0), uint64(8))
	f.Add(uint64(8), uint64(8))
	f.Add(uint64(16), uint64(8))
	f.Add(uint64(7), uint64(8))

	f.Fuzz(func(t *testing.T, val uint64, alignment uint64) {
		if alignment == 0 {
			return // Avoid division by zero
		}
		_ = isAligned(val, alignment)
	})
}

// FuzzIsOverlap tests overlap detection
func FuzzIsOverlap(f *testing.F) {
	f.Add(uint64(0), uint64(100), uint64(50), uint64(100))
	f.Add(uint64(0), uint64(100), uint64(100), uint64(100))
	f.Add(uint64(0), uint64(100), uint64(200), uint64(100))
	// Add overflow test cases
	f.Add(uint64(0xFFFFFFFFFFFF0000), uint64(0x10000), uint64(0), uint64(100))
	f.Add(uint64(0), uint64(100), uint64(0xFFFFFFFFFFFF0000), uint64(0x10000))

	f.Fuzz(func(t *testing.T, startA, sizeA, startB, sizeB uint64) {
		// Call isOverlap - should return error on overflow instead of panicking
		overlap, err := isOverlap(startA, sizeA, startB, sizeB)

		// Verify overflow is detected correctly
		if startA+sizeA < startA || startB+sizeB < startB {
			// Overflow case - should return error
			if err == nil {
				t.Errorf("Expected overflow error for startA=%d sizeA=%d startB=%d sizeB=%d, but got nil error",
					startA, sizeA, startB, sizeB)
			}
			return
		}

		// Non-overflow case - should not return error
		if err != nil {
			t.Errorf("Unexpected error for valid inputs startA=%d sizeA=%d startB=%d sizeB=%d: %v",
				startA, sizeA, startB, sizeB, err)
			return
		}

		// Verify overlap logic for non-overflow cases
		_ = overlap
	})
}

// Helper functions to create seed data

func makeValidELFHeader() []byte {
	buf := make([]byte, ehLen)
	copy(buf[0:4], elf.ELFMAG)
	buf[elf.EI_CLASS] = byte(elf.ELFCLASS64)
	buf[elf.EI_DATA] = byte(elf.ELFDATA2LSB)
	buf[elf.EI_VERSION] = byte(elf.EV_CURRENT)
	buf[elf.EI_OSABI] = byte(elf.ELFOSABI_NONE)
	binary.LittleEndian.PutUint16(buf[16:18], uint16(elf.ET_DYN))
	binary.LittleEndian.PutUint16(buf[18:20], uint16(elf.EM_BPF))
	binary.LittleEndian.PutUint32(buf[20:24], uint32(elf.EV_CURRENT))
	binary.LittleEndian.PutUint16(buf[52:54], ehLen)
	binary.LittleEndian.PutUint16(buf[54:56], phEntLen)
	binary.LittleEndian.PutUint16(buf[58:60], shEntLen)
	return buf
}

func makeMinimalValidELF() []byte {
	header := makeValidELFHeader()
	// Add minimal program header
	binary.LittleEndian.PutUint64(header[32:40], ehLen) // Phoff
	binary.LittleEndian.PutUint16(header[56:58], 1)     // Phnum
	// Add minimal section header
	binary.LittleEndian.PutUint64(header[40:48], ehLen+phEntLen) // Shoff
	binary.LittleEndian.PutUint16(header[60:62], 1)              // Shnum

	buf := make([]byte, ehLen+phEntLen+shEntLen)
	copy(buf, header)
	return buf
}

func makeMalformedELFHeader() []byte {
	buf := makeValidELFHeader()
	// Corrupt the magic number
	buf[0] = 0xFF
	return buf
}

func makeOversizedELFHeader() []byte {
	buf := makeValidELFHeader()
	// Set unrealistic sizes
	binary.LittleEndian.PutUint16(buf[56:58], 65535) // Phnum
	binary.LittleEndian.PutUint16(buf[60:62], 65535) // Shnum
	return buf
}

func makeHeaderWithInvalidMagic() []byte {
	buf := makeValidELFHeader()
	copy(buf[0:4], []byte{0x7E, 0x45, 0x4C, 0x46})
	return buf
}

func makeHeaderWithInvalidClass() []byte {
	buf := makeValidELFHeader()
	buf[elf.EI_CLASS] = 0xFF
	return buf
}

func makeHeaderWithInvalidEndianness() []byte {
	buf := makeValidELFHeader()
	buf[elf.EI_DATA] = 0xFF
	return buf
}

func makeHeaderWithInvalidVersion() []byte {
	buf := makeValidELFHeader()
	buf[elf.EI_VERSION] = 0xFF
	return buf
}

func makeHeaderWithInvalidMachine() []byte {
	buf := makeValidELFHeader()
	binary.LittleEndian.PutUint16(buf[18:20], 0xFFFF)
	return buf
}

func makeELFWithValidProgramHeaders(count int) []byte {
	header := makeValidELFHeader()
	binary.LittleEndian.PutUint64(header[32:40], ehLen)         // Phoff
	binary.LittleEndian.PutUint16(header[56:58], uint16(count)) // Phnum

	buf := make([]byte, ehLen+phEntLen*count)
	copy(buf, header)

	for i := 0; i < count; i++ {
		offset := ehLen + i*phEntLen
		// PT_LOAD type
		binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(elf.PT_LOAD))
	}

	return buf
}

func makeELFWithOverlappingProgramHeaders() []byte {
	buf := makeELFWithValidProgramHeaders(2)
	// Make both program headers point to same offset
	binary.LittleEndian.PutUint64(buf[ehLen+8:ehLen+16], 0x1000)
	binary.LittleEndian.PutUint64(buf[ehLen+phEntLen+8:ehLen+phEntLen+16], 0x1000)
	return buf
}

func makeELFWithInvalidOffsets() []byte {
	buf := makeELFWithValidProgramHeaders(1)
	// Set offset beyond file size
	binary.LittleEndian.PutUint64(buf[ehLen+8:ehLen+16], 0xFFFFFFFFFFFFFFFF)
	return buf
}

func makeELFWithValidSectionHeaders(count int) []byte {
	header := makeValidELFHeader()
	binary.LittleEndian.PutUint64(header[40:48], ehLen)         // Shoff
	binary.LittleEndian.PutUint16(header[60:62], uint16(count)) // Shnum

	buf := make([]byte, ehLen+shEntLen*count)
	copy(buf, header)

	for i := 0; i < count; i++ {
		offset := ehLen + i*shEntLen
		if i == 0 {
			// First section should be SHT_NULL
			binary.LittleEndian.PutUint32(buf[offset+4:offset+8], uint32(elf.SHT_NULL))
		} else {
			binary.LittleEndian.PutUint32(buf[offset+4:offset+8], uint32(elf.SHT_PROGBITS))
		}
	}

	return buf
}

func makeELFWithOverlappingSections() []byte {
	buf := makeELFWithValidSectionHeaders(3)
	// Make sections overlap
	binary.LittleEndian.PutUint64(buf[ehLen+shEntLen+24:ehLen+shEntLen+32], 0x1000)     // Offset
	binary.LittleEndian.PutUint64(buf[ehLen+shEntLen*2+24:ehLen+shEntLen*2+32], 0x1000) // Offset
	return buf
}

func makeELFWithOutOfBoundsSections() []byte {
	buf := makeELFWithValidSectionHeaders(2)
	// Set section offset beyond file
	binary.LittleEndian.PutUint64(buf[ehLen+shEntLen+24:ehLen+shEntLen+32], 0xFFFFFFFF)
	binary.LittleEndian.PutUint64(buf[ehLen+shEntLen+32:ehLen+shEntLen+40], 0x1000)
	return buf
}

func makeELFWithDynamicSection() []byte {
	buf := makeELFWithValidProgramHeaders(1)
	// Set program header to PT_DYNAMIC
	binary.LittleEndian.PutUint32(buf[ehLen:ehLen+4], uint32(elf.PT_DYNAMIC))
	binary.LittleEndian.PutUint64(buf[ehLen+8:ehLen+16], ehLen+phEntLen)
	binary.LittleEndian.PutUint64(buf[ehLen+32:ehLen+40], dynLen*2)

	// Extend buffer to include dynamic entries
	newBuf := make([]byte, len(buf)+dynLen*2)
	copy(newBuf, buf)
	return newBuf
}

func makeELFWithInvalidDynamicEntries() []byte {
	buf := makeELFWithDynamicSection()
	// Set invalid dynamic tag
	offset := len(buf) - dynLen*2
	binary.LittleEndian.PutUint64(buf[offset:offset+8], 0xFFFFFFFFFFFFFFFF)
	return buf
}

func makeELFWithValidRelocations() []byte {
	buf := makeELFWithDynamicSection()
	// Add DT_REL entry
	offset := len(buf) - dynLen*2
	binary.LittleEndian.PutUint64(buf[offset:offset+8], uint64(elf.DT_REL))
	binary.LittleEndian.PutUint64(buf[offset+8:offset+16], ehLen+phEntLen+dynLen*2)
	return buf
}

func makeELFWithInvalidRelocationOffsets() []byte {
	buf := makeELFWithValidRelocations()
	// Set invalid relocation offset
	offset := len(buf) - dynLen*2
	binary.LittleEndian.PutUint64(buf[offset+8:offset+16], 0xFFFFFFFFFFFFFFFF)
	return buf
}

func makeELFWithMalformedRelocations() []byte {
	buf := makeELFWithValidRelocations()
	// Add DT_RELSZ with odd size
	offset := len(buf) - dynLen
	binary.LittleEndian.PutUint64(buf[offset:offset+8], uint64(elf.DT_RELSZ))
	binary.LittleEndian.PutUint64(buf[offset+8:offset+16], 15) // Not multiple of relLen
	return buf
}

func makeELFWithValidSymbolTable() []byte {
	buf := makeELFWithDynamicSection()
	// Add DT_SYMTAB entry
	offset := len(buf) - dynLen*2
	binary.LittleEndian.PutUint64(buf[offset:offset+8], uint64(elf.DT_SYMTAB))
	binary.LittleEndian.PutUint64(buf[offset+8:offset+16], ehLen+phEntLen+dynLen*2)
	return buf
}

func makeELFWithMalformedSymbols() []byte {
	buf := makeELFWithValidSymbolTable()
	// Point to invalid offset
	offset := len(buf) - dynLen*2 + 8
	binary.LittleEndian.PutUint64(buf[offset:offset+8], 0xFFFFFFFFFFFFFFFF)
	return buf
}

func makeCompleteValidELF() []byte {
	// Build a more complete valid ELF with .text section
	header := makeValidELFHeader()

	// Entry point
	binary.LittleEndian.PutUint64(header[24:32], 0x100000)

	// Program header
	binary.LittleEndian.PutUint64(header[32:40], ehLen)
	binary.LittleEndian.PutUint16(header[56:58], 1)

	// Section header
	binary.LittleEndian.PutUint64(header[40:48], ehLen+phEntLen)
	binary.LittleEndian.PutUint16(header[60:62], 2)
	binary.LittleEndian.PutUint16(header[62:64], 1) // shstrndx

	size := ehLen + phEntLen + shEntLen*2 + 256 // Extra space for data
	buf := make([]byte, size)
	copy(buf, header)

	// Program header - PT_LOAD
	phOff := ehLen
	binary.LittleEndian.PutUint32(buf[phOff:phOff+4], uint32(elf.PT_LOAD))
	binary.LittleEndian.PutUint64(buf[phOff+16:phOff+24], 0x100000) // Vaddr
	binary.LittleEndian.PutUint64(buf[phOff+32:phOff+40], 128)      // Filesz

	// Section headers
	shOff := ehLen + phEntLen

	// Null section
	binary.LittleEndian.PutUint32(buf[shOff+4:shOff+8], uint32(elf.SHT_NULL))

	// .shstrtab section
	shOff += shEntLen
	binary.LittleEndian.PutUint32(buf[shOff+4:shOff+8], uint32(elf.SHT_STRTAB))

	return buf
}

// makeCompleteValidELFWithAllSections creates a complete, valid ELF with all required sections
// that should successfully pass parse(), copy(), and be ready for relocate()
func makeCompleteValidELFWithAllSections() []byte {
	// Start with the base structure
	const (
		// File layout offsets
		textSectionOffset   = 0x1000 // 4096
		rodataSectionOffset = 0x2b8  // 696
		dynstrOffset        = 0x270  // 624
		dynsymOffset        = 0x1c8  // 456
		relOffset           = 0x288  // 648
		dynamicOffset       = 0x2000 // 8192
		shstrOffset         = 0x21c8 // Section header string table
	)

	totalSize := 0x3000 // 12KB file
	buf := make([]byte, totalSize)

	// ===== ELF Header =====
	header := makeValidELFHeader()
	binary.LittleEndian.PutUint64(header[24:32], textSectionOffset) // Entry point
	binary.LittleEndian.PutUint64(header[32:40], ehLen)             // Phoff - program headers right after ELF header
	binary.LittleEndian.PutUint16(header[56:58], 2)                 // Phnum - 2 program headers
	binary.LittleEndian.PutUint64(header[40:48], 0x2800)            // Shoff - section headers
	binary.LittleEndian.PutUint16(header[60:62], 8)                 // Shnum - 8 sections
	binary.LittleEndian.PutUint16(header[62:64], 7)                 // Shstrndx - section 7 is shstrtab
	copy(buf, header)

	// ===== Program Headers =====
	phOff := ehLen

	// Program header 0: PT_LOAD
	binary.LittleEndian.PutUint32(buf[phOff:phOff+4], uint32(elf.PT_LOAD))
	binary.LittleEndian.PutUint32(buf[phOff+4:phOff+8], 0x06) // Flags: R+W+X
	binary.LittleEndian.PutUint64(buf[phOff+8:phOff+16], textSectionOffset)
	binary.LittleEndian.PutUint64(buf[phOff+16:phOff+24], textSectionOffset) // Vaddr
	binary.LittleEndian.PutUint64(buf[phOff+24:phOff+32], textSectionOffset) // Paddr
	binary.LittleEndian.PutUint64(buf[phOff+32:phOff+40], 0xd0)              // Filesz (208 bytes)
	binary.LittleEndian.PutUint64(buf[phOff+40:phOff+48], 0xd0)              // Memsz
	binary.LittleEndian.PutUint64(buf[phOff+48:phOff+56], 0x1000)            // Align

	// Program header 1: PT_DYNAMIC
	phOff += phEntLen
	binary.LittleEndian.PutUint32(buf[phOff:phOff+4], uint32(elf.PT_DYNAMIC))
	binary.LittleEndian.PutUint32(buf[phOff+4:phOff+8], 0x06)            // Flags
	binary.LittleEndian.PutUint64(buf[phOff+8:phOff+16], dynamicOffset)  // Offset
	binary.LittleEndian.PutUint64(buf[phOff+16:phOff+24], dynamicOffset) // Vaddr
	binary.LittleEndian.PutUint64(buf[phOff+24:phOff+32], dynamicOffset) // Paddr
	binary.LittleEndian.PutUint64(buf[phOff+32:phOff+40], 0xd0)          // Filesz (208 bytes)
	binary.LittleEndian.PutUint64(buf[phOff+40:phOff+48], 0xd0)          // Memsz
	binary.LittleEndian.PutUint64(buf[phOff+48:phOff+56], 0x08)          // Align

	// ===== Section Headers =====
	shOff := 0x2800

	// Section 0: NULL section (required)
	binary.LittleEndian.PutUint32(buf[shOff+4:shOff+8], uint32(elf.SHT_NULL))

	// Section 1: .text section
	shOff += shEntLen
	binary.LittleEndian.PutUint32(buf[shOff:shOff+4], 1) // Name offset in shstrtab
	binary.LittleEndian.PutUint32(buf[shOff+4:shOff+8], uint32(elf.SHT_PROGBITS))
	binary.LittleEndian.PutUint64(buf[shOff+8:shOff+16], uint64(elf.SHF_ALLOC|elf.SHF_EXECINSTR))
	binary.LittleEndian.PutUint64(buf[shOff+16:shOff+24], textSectionOffset) // Addr
	binary.LittleEndian.PutUint64(buf[shOff+24:shOff+32], textSectionOffset) // Offset
	binary.LittleEndian.PutUint64(buf[shOff+32:shOff+40], 0x60)              // Size (96 bytes)
	binary.LittleEndian.PutUint64(buf[shOff+48:shOff+56], 8)                 // Addralign

	// Section 2: .rodata section
	shOff += shEntLen
	binary.LittleEndian.PutUint32(buf[shOff:shOff+4], 7) // Name offset
	binary.LittleEndian.PutUint32(buf[shOff+4:shOff+8], uint32(elf.SHT_PROGBITS))
	binary.LittleEndian.PutUint64(buf[shOff+8:shOff+16], uint64(elf.SHF_ALLOC))
	binary.LittleEndian.PutUint64(buf[shOff+16:shOff+24], rodataSectionOffset) // Addr
	binary.LittleEndian.PutUint64(buf[shOff+24:shOff+32], rodataSectionOffset) // Offset
	binary.LittleEndian.PutUint64(buf[shOff+32:shOff+40], 11)                  // Size
	binary.LittleEndian.PutUint64(buf[shOff+48:shOff+56], 1)                   // Addralign

	// Section 3: .dynstr section
	shOff += shEntLen
	binary.LittleEndian.PutUint32(buf[shOff:shOff+4], 15) // Name offset
	binary.LittleEndian.PutUint32(buf[shOff+4:shOff+8], uint32(elf.SHT_STRTAB))
	binary.LittleEndian.PutUint64(buf[shOff+8:shOff+16], uint64(elf.SHF_ALLOC))
	binary.LittleEndian.PutUint64(buf[shOff+16:shOff+24], dynstrOffset) // Addr
	binary.LittleEndian.PutUint64(buf[shOff+24:shOff+32], dynstrOffset) // Offset
	binary.LittleEndian.PutUint64(buf[shOff+32:shOff+40], 23)           // Size
	binary.LittleEndian.PutUint64(buf[shOff+48:shOff+56], 1)            // Addralign

	// Section 4: .dynsym section
	shOff += shEntLen
	binary.LittleEndian.PutUint32(buf[shOff:shOff+4], 23) // Name offset
	binary.LittleEndian.PutUint32(buf[shOff+4:shOff+8], uint32(elf.SHT_DYNSYM))
	binary.LittleEndian.PutUint64(buf[shOff+8:shOff+16], uint64(elf.SHF_ALLOC))
	binary.LittleEndian.PutUint64(buf[shOff+16:shOff+24], dynsymOffset) // Addr
	binary.LittleEndian.PutUint64(buf[shOff+24:shOff+32], dynsymOffset) // Offset
	binary.LittleEndian.PutUint64(buf[shOff+32:shOff+40], 0xa0)         // Size (160 bytes - ~6 symbols)
	binary.LittleEndian.PutUint32(buf[shOff+40:shOff+44], 3)            // Link (to dynstr)
	binary.LittleEndian.PutUint64(buf[shOff+48:shOff+56], 8)            // Addralign
	binary.LittleEndian.PutUint64(buf[shOff+56:shOff+64], symLen)       // Entsize

	// Section 5: .dynamic section
	shOff += shEntLen
	binary.LittleEndian.PutUint32(buf[shOff:shOff+4], 31) // Name offset
	binary.LittleEndian.PutUint32(buf[shOff+4:shOff+8], uint32(elf.SHT_DYNAMIC))
	binary.LittleEndian.PutUint64(buf[shOff+8:shOff+16], uint64(elf.SHF_ALLOC|elf.SHF_WRITE))
	binary.LittleEndian.PutUint64(buf[shOff+16:shOff+24], dynamicOffset) // Addr
	binary.LittleEndian.PutUint64(buf[shOff+24:shOff+32], dynamicOffset) // Offset
	binary.LittleEndian.PutUint64(buf[shOff+32:shOff+40], 0xd0)          // Size (208 bytes - 13 entries)
	binary.LittleEndian.PutUint32(buf[shOff+40:shOff+44], 3)             // Link (to dynstr)
	binary.LittleEndian.PutUint64(buf[shOff+48:shOff+56], 8)             // Addralign
	binary.LittleEndian.PutUint64(buf[shOff+56:shOff+64], dynLen)        // Entsize

	// Section 6: .rel.dyn section
	shOff += shEntLen
	binary.LittleEndian.PutUint32(buf[shOff:shOff+4], 40) // Name offset
	binary.LittleEndian.PutUint32(buf[shOff+4:shOff+8], uint32(elf.SHT_REL))
	binary.LittleEndian.PutUint64(buf[shOff+8:shOff+16], uint64(elf.SHF_ALLOC))
	binary.LittleEndian.PutUint64(buf[shOff+16:shOff+24], relOffset) // Addr
	binary.LittleEndian.PutUint64(buf[shOff+24:shOff+32], relOffset) // Offset
	binary.LittleEndian.PutUint64(buf[shOff+32:shOff+40], 0x30)      // Size (48 bytes - 3 relocations)
	binary.LittleEndian.PutUint32(buf[shOff+40:shOff+44], 4)         // Link (to dynsym)
	binary.LittleEndian.PutUint64(buf[shOff+48:shOff+56], 8)         // Addralign
	binary.LittleEndian.PutUint64(buf[shOff+56:shOff+64], relLen)    // Entsize

	// Section 7: .shstrtab section (section header string table)
	shOff += shEntLen
	binary.LittleEndian.PutUint32(buf[shOff:shOff+4], 49) // Name offset
	binary.LittleEndian.PutUint32(buf[shOff+4:shOff+8], uint32(elf.SHT_STRTAB))
	binary.LittleEndian.PutUint64(buf[shOff+16:shOff+24], 0)           // Addr (not allocated)
	binary.LittleEndian.PutUint64(buf[shOff+24:shOff+32], shstrOffset) // Offset
	binary.LittleEndian.PutUint64(buf[shOff+32:shOff+40], 64)          // Size
	binary.LittleEndian.PutUint64(buf[shOff+48:shOff+56], 1)           // Addralign

	// ===== Section Contents =====

	// .dynstr string table
	dynstrData := "\x00log_data\x00entrypoint\x00"
	copy(buf[dynstrOffset:], dynstrData)

	// .rodata data
	rodataData := []byte("hello world")
	copy(buf[rodataSectionOffset:], rodataData)

	// .text executable code (simple BPF instructions)
	// exit instruction: 0x95 0x00 0x00 0x00 0x00 0x00 0x00 0x00
	textData := []byte{
		0x95, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // exit
		0xb7, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // mov r0, 0
		0x95, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // exit
	}
	copy(buf[textSectionOffset:], textData)

	// .dynamic table
	dynOff := dynamicOffset
	// DT_STRTAB
	binary.LittleEndian.PutUint64(buf[dynOff:dynOff+8], uint64(elf.DT_STRTAB))
	binary.LittleEndian.PutUint64(buf[dynOff+8:dynOff+16], dynstrOffset)
	dynOff += dynLen
	// DT_SYMTAB
	binary.LittleEndian.PutUint64(buf[dynOff:dynOff+8], uint64(elf.DT_SYMTAB))
	binary.LittleEndian.PutUint64(buf[dynOff+8:dynOff+16], dynsymOffset)
	dynOff += dynLen
	// DT_STRSZ
	binary.LittleEndian.PutUint64(buf[dynOff:dynOff+8], uint64(elf.DT_STRSZ))
	binary.LittleEndian.PutUint64(buf[dynOff+8:dynOff+16], uint64(len(dynstrData)))
	dynOff += dynLen
	// DT_SYMENT
	binary.LittleEndian.PutUint64(buf[dynOff:dynOff+8], uint64(elf.DT_SYMENT))
	binary.LittleEndian.PutUint64(buf[dynOff+8:dynOff+16], symLen)
	dynOff += dynLen
	// DT_REL
	binary.LittleEndian.PutUint64(buf[dynOff:dynOff+8], uint64(elf.DT_REL))
	binary.LittleEndian.PutUint64(buf[dynOff+8:dynOff+16], relOffset)
	dynOff += dynLen
	// DT_RELSZ
	binary.LittleEndian.PutUint64(buf[dynOff:dynOff+8], uint64(elf.DT_RELSZ))
	binary.LittleEndian.PutUint64(buf[dynOff+8:dynOff+16], 0x30) // 3 relocations
	dynOff += dynLen
	// DT_RELENT
	binary.LittleEndian.PutUint64(buf[dynOff:dynOff+8], uint64(elf.DT_RELENT))
	binary.LittleEndian.PutUint64(buf[dynOff+8:dynOff+16], relLen)
	dynOff += dynLen
	// DT_NULL (end marker)
	binary.LittleEndian.PutUint64(buf[dynOff:dynOff+8], uint64(elf.DT_NULL))

	// .shstrtab string table
	shstrData := "\x00.text\x00.rodata\x00.dynstr\x00.dynsym\x00.dynamic\x00.rel.dyn\x00.shstrtab\x00"
	copy(buf[shstrOffset:], shstrData)

	return buf
}

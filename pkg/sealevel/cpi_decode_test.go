package sealevel

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// putFields writes distinct little-endian values at the given offsets: an
// 8-byte value at each u64 offset and a byte at each flag offset.
func putFields(size int, u64Offsets, flagOffsets []int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = 0xee // padding the decoders must ignore
	}
	for i, off := range u64Offsets {
		binary.LittleEndian.PutUint64(b[off:], 0x0102030405060700+uint64(i))
	}
	for i, off := range flagOffsets {
		b[off] = byte(i + 1)
	}
	return b
}

// Each record decodes every field from its ABI offset (the Solana C and Rust
// AccountInfo layouts and the Rust Rc<RefCell<..>> layouts), and Unmarshal
// reads the same record.
func TestCPIRecordLayouts(t *testing.T) {
	c := putFields(SolAccountInfoCSize, []int{0, 8, 16, 24, 32, 40}, []int{48, 49, 50})
	c[49] = 0 // a zero flag decodes as false
	wantC := SolAccountInfoC{KeyAddr: 0x0102030405060700, LamportsAddr: 0x0102030405060701, DataLen: 0x0102030405060702,
		DataAddr: 0x0102030405060703, OwnerAddr: 0x0102030405060704, RentEpoch: 0x0102030405060705,
		IsSigner: true, IsWritable: false, Executable: true}
	var gotC, readC SolAccountInfoC
	gotC.decode(c)
	if err := readC.Unmarshal(bytes.NewReader(c)); err != nil || gotC != wantC || readC != wantC {
		t.Fatalf("C AccountInfo: decode %+v, Unmarshal %+v (%v), want %+v", gotC, readC, err, wantC)
	}

	r := putFields(SolAccountInfoRustSize, []int{0, 8, 16, 24, 32}, []int{40, 41, 42})
	wantRust := SolAccountInfoRust{PubkeyAddr: 0x0102030405060700, LamportsBoxAddr: 0x0102030405060701, DataBoxAddr: 0x0102030405060702,
		OwnerAddr: 0x0102030405060703, RentEpoch: 0x0102030405060704, IsSigner: 1, IsWritable: 2, Executable: 3}
	var gotRust, readRust SolAccountInfoRust
	gotRust.decode(r)
	if err := readRust.Unmarshal(bytes.NewReader(r)); err != nil || gotRust != wantRust || readRust != wantRust {
		t.Fatalf("Rust AccountInfo: decode %+v, Unmarshal %+v (%v), want %+v", gotRust, readRust, err, wantRust)
	}

	cell := putFields(RefCellRustSize, []int{0, 8, 16, 24}, nil)
	wantCell := RefCellRust{Strong: 0x0102030405060700, Weak: 0x0102030405060701, Borrow: 0x0102030405060702, Addr: 0x0102030405060703}
	var gotCell, readCell RefCellRust
	gotCell.decode(cell)
	if err := readCell.Unmarshal(bytes.NewReader(cell)); err != nil || gotCell != wantCell || readCell != wantCell {
		t.Fatalf("RefCell: decode %+v, Unmarshal %+v (%v), want %+v", gotCell, readCell, err, wantCell)
	}

	vec := putFields(RefCellVecRustSize, []int{0, 8, 16, 24, 32}, nil)
	wantVec := RefCellVecRust{Strong: 0x0102030405060700, Weak: 0x0102030405060701, Borrow: 0x0102030405060702,
		Addr: 0x0102030405060703, Len: 0x0102030405060704}
	var gotVec, readVec RefCellVecRust
	gotVec.decode(vec)
	if err := readVec.Unmarshal(bytes.NewReader(vec)); err != nil || gotVec != wantVec || readVec != wantVec {
		t.Fatalf("RefCell<Vec>: decode %+v, Unmarshal %+v (%v), want %+v", gotVec, readVec, err, wantVec)
	}
}

// decode needs a complete record; a short slice is a caller bug and panics
// rather than decoding zeros.
func TestCPIRecordDecodeRejectsShortInput(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("decoding a short record did not panic")
		}
	}()
	var info SolAccountInfoC
	info.decode(make([]byte, SolAccountInfoCSize-1))
}

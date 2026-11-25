package sbpf

import (
	"encoding/binary"
	"testing"
	"unsafe"
)

func TestLoadedTextViews(t *testing.T) {
	var aligned [4]uint64
	buf := unsafe.Slice((*byte)(unsafe.Pointer(&aligned[0])), 32)
	for _, tt := range []struct {
		name       string
		ro, text   []byte
		wantShared bool
	}{
		{"legacy", buf, buf[8:24], true},
		{"unaligned legacy", buf, buf[1:17], false},
		{"strict", make([]byte, 32), buf[8:24], true},
		{"strict no rodata", nil, buf[:16], true},
		{"empty", buf, nil, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := &Program{RO: tt.ro}
			for i := range tt.text {
				tt.text[i] = byte(31*i + 7)
			}
			p.SetLoadedText(tt.text)
			if len(p.Text) != len(tt.text)/8 {
				t.Fatal("slot count")
			}
			for i, slot := range p.Text {
				if uint64(slot) != binary.LittleEndian.Uint64(tt.text[i*8:]) {
					t.Fatal("instruction changed")
				}
			}
			shared := len(p.Text) > 0 && unsafe.Pointer(&p.Text[0]) == unsafe.Pointer(&tt.text[0])
			wantShared := tt.wantShared && binary.NativeEndian.Uint16([]byte{1, 0}) == 1
			if shared != wantShared {
				t.Fatalf("shared=%t want %t", shared, wantShared)
			}
			want := len(tt.ro)
			if tt.name == "strict" || tt.name == "strict no rodata" {
				want += len(tt.text)
			}
			if !shared {
				want += len(tt.text)
			}
			if p.MemoryBytes() != uint64(want) {
				t.Fatalf("bytes=%d want %d", p.MemoryBytes(), want)
			}
		})
	}
}

func TestSharedProgramTextRemainsReadOnly(t *testing.T) {
	const instruction = Slot(OpExit)
	words := [2]uint64{0, uint64(instruction)}
	ro := unsafe.Slice((*byte)(unsafe.Pointer(&words[0])), 16)
	binary.LittleEndian.PutUint64(ro[8:], uint64(instruction))
	p := &Program{RO: ro, TextVA: VaddrProgram + 8}
	p.SetLoadedText(ro[8:])
	ip := Interpreter{ro: p.RO, text: p.Text}
	ip.initRegions()
	if _, err := ip.Translate(p.TextVA, 8, true); err == nil || ip.fastWrite(p.TextVA, 8) != nil {
		t.Fatal("instruction bytes must remain read-only in both memory paths")
	}
	b, err := ip.Translate(p.TextVA, 8, false)
	if err != nil || Slot(binary.LittleEndian.Uint64(b)) != instruction || p.Text[0] != instruction {
		t.Fatal("guest and interpreter must see the relocated instruction")
	}
}

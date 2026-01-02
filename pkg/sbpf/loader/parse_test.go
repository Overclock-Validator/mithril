package loader

import (
	"debug/elf"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/sbpf"
)

func TestLoader_getString(t *testing.T) {
	tests := map[string]struct {
		buf     []byte
		strtab  *elf.Section64
		stroff  uint32
		maxLen  uint16
		want    string
		wantErr error
	}{
		"valid string in section": {
			buf:     []byte(".text\x00"),
			strtab:  &elf.Section64{Off: 0, Size: 6, Type: uint32(elf.SHT_STRTAB)},
			stroff:  0,
			maxLen:  16,
			want:    ".text",
			wantErr: nil,
		},
		"invalid section header": {
			buf:     []byte(".text\x00"),
			strtab:  &elf.Section64{Off: 0, Size: 6},
			stroff:  0,
			maxLen:  16,
			want:    "",
			wantErr: sbpf.ErrInvalidSectionHeader,
		},
		"out of bounds": {
			buf:     []byte(".text\x00"),
			strtab:  &elf.Section64{Off: 6, Size: 6, Type: uint32(elf.SHT_STRTAB)},
			stroff:  0,
			maxLen:  16,
			want:    "",
			wantErr: sbpf.ErrOutOfBounds,
		},
		"section header too long": {
			buf:     []byte(".data.rel.ro\x00"),
			strtab:  &elf.Section64{Off: 0, Size: 6, Type: uint32(elf.SHT_STRTAB)},
			stroff:  0,
			maxLen:  16,
			want:    "",
			wantErr: &sbpf.ErrStringTooLong{Name: ".data.", Len: 6},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			l, err := NewLoaderFromBytes(tt.buf)
			if err != nil {
				t.Fatalf("could not construct receiver type: %v", err)
			}
			got, gotErr := l.getString(tt.strtab, tt.stroff, tt.maxLen)

			if gotErr != nil && gotErr.Error() != tt.wantErr.Error() {
				t.Errorf("getString() failed: %+v", gotErr)
				return
			}

			if got != tt.want {
				t.Errorf("getString() = %v, want %v", got, tt.want)
			}
		})
	}
}

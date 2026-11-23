package snapshot

import (
	"testing"

	bin "github.com/gagliardetto/binary"
)

func TestManifestOptionalPersistenceRejectsTruncation(t *testing.T) {
	// A zero-valued bank, accounts DB, and lamports-per-signature prefix.
	prefix := make([]byte, 687)
	decode := func(suffix []byte) error {
		data := append(append([]byte(nil), prefix...), suffix...)
		var manifest SnapshotManifest
		return manifest.UnmarshalWithDecoder(bin.NewBinDecoder(data))
	}
	for _, suffix := range [][]byte{nil, {0}, append([]byte{1}, make([]byte, 88)...)} {
		if err := decode(suffix); err != nil {
			t.Fatalf("valid optional field rejected: %v", err)
		}
	}
	for length := 0; length < 88; length++ {
		if err := decode(append([]byte{1}, make([]byte, length)...)); err == nil {
			t.Fatalf("optional persistence accepted %d of 88 payload bytes", length)
		}
	}
}

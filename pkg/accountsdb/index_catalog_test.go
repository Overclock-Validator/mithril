package accountsdb

import (
	"crypto/sha256"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func catalogTestArtifact(name string, seed byte) IndexCatalogArtifact {
	artifact := IndexCatalogArtifact{RelativePath: name, Size: uint64(seed) + 1}
	artifact.SHA256[0] = seed + 1
	artifact.SHA256[sha256.Size-1] = seed ^ 0xa5
	return artifact
}

func validRootIndexCatalogForTest(t *testing.T, shardCount int, generation uint64) *RootIndexCatalog {
	t.Helper()
	catalog, err := NewRootIndexCatalog(shardCount)
	require.NoError(t, err)
	catalog.Generation = generation
	catalog.RootedBatchSequence = 100
	catalog.RootedSlot = 90
	catalog.Lineage[0] = 0x45
	catalog.Lineage[len(catalog.Lineage)-1] = 0x91
	catalog.SharedExtentCatalog = catalogTestArtifact("extents/catalog-000001.bin", 1)
	for i := range catalog.Shards {
		shard := &catalog.Shards[i]
		shard.BaseGeneration = 7
		shard.BaseCoveredSequence = 40 + uint64(i)
		shard.BaseIndex = catalogTestArtifact("base/shard-"+fourDigits(i)+".stream", byte(2+i))
		shard.BaseRecords = catalogTestArtifact("base/shard-"+fourDigits(i)+".records", byte(20+i))
		if i%2 == 1 {
			shard.DeltaGeneration = 8
			shard.DeltaCoveredSequence = 70 + uint64(i)
			shard.DeltaIndex = catalogTestArtifact("delta/shard-"+fourDigits(i)+".stream", byte(40+i))
			shard.DeltaRecords = catalogTestArtifact("delta/shard-"+fourDigits(i)+".records", byte(60+i))
		}
	}
	catalog.CoveredSequence = catalog.Shards[0].effectiveCoveredSequence()
	for i := 1; i < len(catalog.Shards); i++ {
		catalog.CoveredSequence = min(catalog.CoveredSequence, catalog.Shards[i].effectiveCoveredSequence())
	}
	require.NoError(t, catalog.Validate())
	return catalog
}

func fourDigits(value int) string {
	const digits = "0123456789abcdef"
	return string([]byte{
		digits[(value>>12)&0xf],
		digits[(value>>8)&0xf],
		digits[(value>>4)&0xf],
		digits[value&0xf],
	})
}

func repairCatalogTestCRC(encoded []byte) {
	binary.LittleEndian.PutUint32(
		encoded[len(encoded)-4:],
		crc32.Checksum(encoded[:len(encoded)-4], rootIndexCatalogCRC),
	)
}

func TestRootIndexCatalogBinaryRoundTrip(t *testing.T) {
	t.Parallel()

	want := validRootIndexCatalogForTest(t, 4, 11)
	encoded, err := want.MarshalBinary()
	require.NoError(t, err)
	require.LessOrEqual(t, uint64(len(encoded)), rootIndexCatalogMaxSize)

	got, err := UnmarshalRootIndexCatalog(encoded)
	require.NoError(t, err)
	require.Equal(t, want, got)

	// Decoding owns its storage; later changes to either value do not alias.
	want.Shards[0].BaseGeneration++
	require.NotEqual(t, want.Shards[0].BaseGeneration, got.Shards[0].BaseGeneration)
}

func TestRootIndexCatalogRejectsCorruptOrNonCanonicalEncoding(t *testing.T) {
	t.Parallel()

	catalog := validRootIndexCatalogForTest(t, 2, 1)
	encoded, err := catalog.MarshalBinary()
	require.NoError(t, err)

	t.Run("crc", func(t *testing.T) {
		corrupt := append([]byte(nil), encoded...)
		corrupt[64] ^= 0xff
		_, err := UnmarshalRootIndexCatalog(corrupt)
		require.ErrorIs(t, err, ErrInvalidRootIndexCatalog)
	})
	t.Run("magic", func(t *testing.T) {
		corrupt := append([]byte(nil), encoded...)
		corrupt[0] ^= 1
		repairCatalogTestCRC(corrupt)
		_, err := UnmarshalRootIndexCatalog(corrupt)
		require.ErrorIs(t, err, ErrInvalidRootIndexCatalog)
	})
	t.Run("version", func(t *testing.T) {
		corrupt := append([]byte(nil), encoded...)
		binary.LittleEndian.PutUint32(corrupt[8:12], rootIndexCatalogVersion+1)
		repairCatalogTestCRC(corrupt)
		_, err := UnmarshalRootIndexCatalog(corrupt)
		require.ErrorIs(t, err, ErrInvalidRootIndexCatalog)
	})
	t.Run("declared size", func(t *testing.T) {
		corrupt := append([]byte(nil), encoded...)
		binary.LittleEndian.PutUint64(corrupt[16:24], uint64(len(corrupt)+1))
		repairCatalogTestCRC(corrupt)
		_, err := UnmarshalRootIndexCatalog(corrupt)
		require.ErrorIs(t, err, ErrInvalidRootIndexCatalog)
	})
	t.Run("reserved header", func(t *testing.T) {
		corrupt := append([]byte(nil), encoded...)
		corrupt[36] = 1
		repairCatalogTestCRC(corrupt)
		_, err := UnmarshalRootIndexCatalog(corrupt)
		require.ErrorIs(t, err, ErrInvalidRootIndexCatalog)
	})
	t.Run("zero routing key", func(t *testing.T) {
		corrupt := append([]byte(nil), encoded...)
		clear(corrupt[96:112])
		repairCatalogTestCRC(corrupt)
		_, err := UnmarshalRootIndexCatalog(corrupt)
		require.ErrorIs(t, err, ErrInvalidRootIndexCatalog)
		require.ErrorContains(t, err, "zero routing key")
	})
	t.Run("impossible shard body bound", func(t *testing.T) {
		corrupt := append([]byte(nil), encoded...)
		binary.LittleEndian.PutUint32(corrupt[32:36], MaxPersistentIndexShards)
		repairCatalogTestCRC(corrupt)
		_, err := UnmarshalRootIndexCatalog(corrupt)
		require.ErrorIs(t, err, ErrInvalidRootIndexCatalog)
	})
	t.Run("trailing bytes", func(t *testing.T) {
		corrupt := append([]byte(nil), encoded[:len(encoded)-4]...)
		corrupt = append(corrupt, 0)
		corrupt = append(corrupt, make([]byte, 4)...)
		binary.LittleEndian.PutUint64(corrupt[16:24], uint64(len(corrupt)))
		repairCatalogTestCRC(corrupt)
		_, err := UnmarshalRootIndexCatalog(corrupt)
		require.ErrorIs(t, err, ErrInvalidRootIndexCatalog)
	})
	t.Run("truncated", func(t *testing.T) {
		_, err := UnmarshalRootIndexCatalog(encoded[:len(encoded)-1])
		require.ErrorIs(t, err, ErrInvalidRootIndexCatalog)
	})
}

func TestRootIndexCatalogValidationFailsClosed(t *testing.T) {
	t.Parallel()

	tests := map[string]func(*RootIndexCatalog){
		"zero generation": func(c *RootIndexCatalog) { c.Generation = 0 },
		"zero lineage":    func(c *RootIndexCatalog) { c.Lineage = [32]byte{} },
		"zero routing key": func(c *RootIndexCatalog) {
			c.RoutingKey = PersistentIndexRoutingKey{}
		},
		"missing shard":  func(c *RootIndexCatalog) { c.Shards = c.Shards[:1] },
		"misnamed shard": func(c *RootIndexCatalog) { c.Shards[1].ShardID = 0 },
		"missing base":   func(c *RootIndexCatalog) { c.Shards[0].BaseIndex = IndexCatalogArtifact{} },
		"partial delta": func(c *RootIndexCatalog) {
			c.Shards[0].DeltaCoveredSequence = 3
		},
		"delta predates base": func(c *RootIndexCatalog) {
			c.Shards[1].DeltaCoveredSequence = c.Shards[1].BaseCoveredSequence - 1
		},
		"wrong coverage watermark": func(c *RootIndexCatalog) { c.CoveredSequence++ },
		"duplicate artifact": func(c *RootIndexCatalog) {
			c.Shards[0].BaseIndex = c.SharedExtentCatalog
		},
		"absolute path": func(c *RootIndexCatalog) { c.Shards[0].BaseIndex.RelativePath = "/tmp/base" },
		"traversal path": func(c *RootIndexCatalog) {
			c.Shards[0].BaseIndex.RelativePath = "base/../outside"
		},
		"backslash path": func(c *RootIndexCatalog) { c.Shards[0].BaseIndex.RelativePath = `base\outside` },
		"unsupported path byte": func(c *RootIndexCatalog) {
			c.Shards[0].BaseIndex.RelativePath = "base/a b"
		},
		"oversized component": func(c *RootIndexCatalog) {
			c.Shards[0].BaseIndex.RelativePath = "base/" + strings.Repeat("a", 256)
		},
		"zero artifact size": func(c *RootIndexCatalog) { c.Shards[0].BaseIndex.Size = 0 },
		"zero artifact hash": func(c *RootIndexCatalog) { c.Shards[0].BaseIndex.SHA256 = [32]byte{} },
	}

	for name, mutate := range tests {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			catalog := validRootIndexCatalogForTest(t, 2, 1)
			mutate(catalog)
			require.ErrorIs(t, catalog.Validate(), ErrInvalidRootIndexCatalog)
			_, err := catalog.MarshalBinary()
			require.Error(t, err)
		})
	}
}

func TestRootIndexCatalogAtomicPublication(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	want := validRootIndexCatalogForTest(t, 2, 1)
	require.NoError(t, WriteRootIndexCatalogAtomic(root, want))
	got, err := ReadRootIndexCatalog(root)
	require.NoError(t, err)
	require.Equal(t, want, got)

	next := want.Clone()
	next.Generation++
	next.RootedSlot++
	require.NoError(t, WriteRootIndexCatalogAtomic(root, next))
	got, err = ReadRootIndexCatalog(root)
	require.NoError(t, err)
	require.Equal(t, next, got)

	entries, err := os.ReadDir(root)
	require.NoError(t, err)
	for _, entry := range entries {
		require.False(t, strings.HasPrefix(entry.Name(), RootIndexCatalogFileName+".tmp"))
	}
}

func TestRootIndexCatalogWALCoverageIsIndependentOfFoldBatchSequence(t *testing.T) {
	t.Parallel()

	// Index-only operations such as compaction append WAL frames without
	// advancing the rooted fold batch. These fields intentionally occupy
	// different sequence spaces.
	catalog := validRootIndexCatalogForTest(t, 2, 1)
	catalog.RootedBatchSequence = 3
	catalog.Shards[0].BaseCoveredSequence = 10_000
	catalog.Shards[1].BaseCoveredSequence = 20_000
	catalog.Shards[1].DeltaCoveredSequence = 30_000
	catalog.CoveredSequence = 10_000
	require.NoError(t, catalog.Validate())

	encoded, err := catalog.MarshalBinary()
	require.NoError(t, err)
	decoded, err := UnmarshalRootIndexCatalog(encoded)
	require.NoError(t, err)
	require.Equal(t, catalog, decoded)
}

func TestRootIndexCatalogRejectsSymlinkSelector(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	external := filepath.Join(t.TempDir(), "external")
	require.NoError(t, os.WriteFile(external, []byte("do not replace"), 0o600))
	require.NoError(t, os.Symlink(external, filepath.Join(root, RootIndexCatalogFileName)))

	catalog := validRootIndexCatalogForTest(t, 2, 1)
	require.Error(t, WriteRootIndexCatalogAtomic(root, catalog))
	_, err := ReadRootIndexCatalog(root)
	require.ErrorIs(t, err, ErrInvalidRootIndexCatalog)
	content, err := os.ReadFile(external)
	require.NoError(t, err)
	require.Equal(t, []byte("do not replace"), content)
}

func TestRootIndexCatalogReadRejectsOversizedSparseSelector(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	file, err := os.OpenFile(filepath.Join(root, RootIndexCatalogFileName), os.O_CREATE|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	require.NoError(t, file.Truncate(int64(rootIndexCatalogMaxSize)+1))
	require.NoError(t, file.Close())

	_, err = ReadRootIndexCatalog(root)
	require.ErrorIs(t, err, ErrInvalidRootIndexCatalog)
	require.ErrorContains(t, err, "bounded")
}

func TestIndexCatalogArtifactIdentityAndPathContainment(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "base"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "base", "index.bin"), []byte("immutable index"), 0o600))

	artifact, err := ComputeIndexCatalogArtifact(root, "base/index.bin")
	require.NoError(t, err)
	require.Equal(t, uint64(len("immutable index")), artifact.Size)
	require.NoError(t, VerifyIndexCatalogArtifact(root, artifact))

	require.NoError(t, os.WriteFile(filepath.Join(root, "base", "index.bin"), []byte("changed"), 0o600))
	require.ErrorIs(t, VerifyIndexCatalogArtifact(root, artifact), ErrInvalidCatalogArtifact)

	for _, unsafePath := range []string{"../outside", "/tmp/outside", `base\index.bin`, "base//index.bin"} {
		_, err := ComputeIndexCatalogArtifact(root, unsafePath)
		require.ErrorIs(t, err, ErrInvalidCatalogArtifact, unsafePath)
	}

	external := filepath.Join(t.TempDir(), "external")
	require.NoError(t, os.WriteFile(external, []byte("outside"), 0o600))
	require.NoError(t, os.Symlink(external, filepath.Join(root, "base", "link.bin")))
	_, err = ComputeIndexCatalogArtifact(root, "base/link.bin")
	require.ErrorIs(t, err, ErrInvalidCatalogArtifact)

	externalDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(externalDir, "index.bin"), []byte("outside directory"), 0o600))
	require.NoError(t, os.Symlink(externalDir, filepath.Join(root, "linked-directory")))
	_, err = ComputeIndexCatalogArtifact(root, "linked-directory/index.bin")
	require.ErrorIs(t, err, ErrInvalidCatalogArtifact)
}

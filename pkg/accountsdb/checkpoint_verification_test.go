package accountsdb

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type checkpointVerificationFixture struct {
	root        string
	initial     *ShardedImmutableIndex
	manager     *IndexViewManager
	nextRoot    *RootIndexCatalog
	publication ShardedMutableCheckpointPublication
	verified    verifiedShardedDeltaCheckpointArtifacts
}

func newCheckpointVerificationFixture(t *testing.T) checkpointVerificationFixture {
	t.Helper()
	root := t.TempDir()
	build, err := BuildShardedStreamBaseWithShardCount(t.Context(), &streamIndexTestSource{}, root, 1, 1, testPersistentIndexRoutingKey(), nil, 1)
	require.NoError(t, err)
	catalog := rootCatalogForImmutableBuildTest(t, build, 1)
	initial, err := OpenShardedImmutableIndex(root, catalog)
	require.NoError(t, err)
	manager, err := NewIndexViewManager(catalog, initial, initial.Resources())
	require.NoError(t, err)
	var handle *ShardedDeltaCheckpointHandle
	t.Cleanup(func() {
		// Release the builder before shutdown drains generation resources.
		require.NoError(t, handle.Release())
		require.NoError(t, manager.Shutdown(context.Background()))
		if handle != nil {
			waitImmutableHandle(t, handle)
		}
	})
	directory := ShardedMutableCheckpointDirectory(root, 0)
	require.NoError(t, os.MkdirAll(directory, 0o755))
	checkpoint, err := BuildDeltaCheckpoint(t.Context(), directory, nil,
		map[solana.PublicKey]deltaIndexValue{streamIndexTestKey(0, 1): {Entry: AccountIndexEntry{Slot: 2, FileId: 3, Offset: 8}}}, 5, 1)
	require.NoError(t, err)
	handle, err = NewShardedDeltaCheckpointHandle(checkpoint)
	require.NoError(t, err)
	verified, err := identifyShardedDeltaCheckpointArtifacts(root, directory, handle)
	require.NoError(t, err)
	nextRoot := catalog.Clone()
	nextRoot.Generation++
	nextRoot.CoveredSequence = 5
	nextRoot.Shards[0].DeltaGeneration = verified.artifacts.Generation
	nextRoot.Shards[0].DeltaCoveredSequence = 5
	nextRoot.Shards[0].DeltaIndex = verified.artifacts.Index
	nextRoot.Shards[0].DeltaRecords = verified.artifacts.Records
	return checkpointVerificationFixture{root, initial, manager, nextRoot, ShardedMutableCheckpointPublication{
		ShardID: 0, Directory: directory, Next: handle, CoveredSequence: 5, ProposedCoveredSequences: []uint64{5},
	}, verified}
}

func TestCheckpointVerifiedHandoffPublishesReadableGeneration(t *testing.T) {
	f := newCheckpointVerificationFixture(t)
	next, resources, obsolete, err := f.initial.deriveWithCheckpoint(f.nextRoot, f.publication, &f.verified)
	require.NoError(t, err)
	require.Empty(t, obsolete)
	require.Equal(t, int64(2), f.publication.Next.refs.Load())
	require.NoError(t, f.manager.Publish(f.nextRoot, next, resources, obsolete))
	got, source, found, err := next.LookupCandidate(streamIndexTestKey(0, 1))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, accountIndexSourceDelta, source)
	require.Equal(t, AccountIndexEntry{Slot: 2, FileId: 3, Offset: 8}, got.Entry)
	// Reopen still performs full artifact validation independently of the receipt.
	reopened, err := OpenShardedImmutableIndex(f.root, f.nextRoot)
	require.NoError(t, err)
	require.NoError(t, reopened.closeUnmanaged())
}

func TestCheckpointVerifiedHandoffRejectsChangedFiles(t *testing.T) {
	for _, artifact := range []string{"index", "records", "descriptor"} {
		for _, change := range []string{"replace", "modify", "extend", "symlink"} {
			t.Run(artifact+"/"+change, func(t *testing.T) {
				f := newCheckpointVerificationFixture(t)
				paths := makeDeltaCheckpointPaths(f.publication.Directory, f.verified.artifacts.Generation)
				path := map[string]string{"index": paths.index, "records": paths.records, "descriptor": paths.descriptor}[artifact]
				info, err := os.Lstat(path)
				require.NoError(t, err)
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				switch change {
				case "replace":
					// Identical contents, length and timestamp still have a new inode.
					require.NoError(t, os.WriteFile(path+".replacement", data, 0o600))
					require.NoError(t, os.Chtimes(path+".replacement", info.ModTime(), info.ModTime()))
					require.NoError(t, os.Rename(path+".replacement", path))
				case "modify":
					file, err := os.OpenFile(path, os.O_WRONLY, 0)
					require.NoError(t, err)
					_, err = file.WriteAt([]byte{data[len(data)-1] ^ 0xff}, int64(len(data)-1))
					require.NoError(t, err)
					require.NoError(t, file.Close())
					require.NoError(t, os.Chtimes(path, info.ModTime(), info.ModTime().Add(time.Second)))
				case "extend":
					require.NoError(t, os.Truncate(path, info.Size()+1))
				case "symlink":
					require.NoError(t, os.Rename(path, path+".original"))
					require.NoError(t, os.Symlink(path+".original", path))
				}
				next, resources, obsolete, err := f.initial.deriveWithCheckpoint(f.nextRoot, f.publication, &f.verified)
				require.Error(t, err)
				require.Nil(t, next)
				require.Empty(t, resources)
				require.Empty(t, obsolete)
				require.Equal(t, int64(1), f.publication.Next.refs.Load(), "rejected handoff leaked a handle")
			})
		}
	}
}

func TestCheckpointVerifiedHandoffRejectsMismatchedPublication(t *testing.T) {
	for _, mismatch := range []string{"root", "directory", "handle", "empty receipt", "root hash", "coverage"} {
		t.Run(mismatch, func(t *testing.T) {
			f := newCheckpointVerificationFixture(t)
			switch mismatch {
			case "root":
				f.verified.root = t.TempDir()
			case "directory":
				f.verified.directory = t.TempDir()
			case "handle":
				f.verified.handle = &ShardedDeltaCheckpointHandle{}
			case "empty receipt":
				f.verified = verifiedShardedDeltaCheckpointArtifacts{}
			case "root hash":
				f.nextRoot.Shards[0].DeltaIndex.SHA256[0] ^= 0xff
			case "coverage":
				f.publication.CoveredSequence++
			}
			next, resources, obsolete, err := f.initial.deriveWithCheckpoint(f.nextRoot, f.publication, &f.verified)
			require.ErrorIs(t, err, ErrInvalidRootIndexCatalog)
			require.Nil(t, next)
			require.Empty(t, resources)
			require.Empty(t, obsolete)
			require.Equal(t, int64(1), f.publication.Next.refs.Load())
		})
	}
}

func TestIndependentCheckpointDerivationStillHashesArtifacts(t *testing.T) {
	for _, artifact := range []string{"index", "records", "descriptor"} {
		t.Run(artifact, func(t *testing.T) {
			f := newCheckpointVerificationFixture(t)
			paths := makeDeltaCheckpointPaths(f.publication.Directory, f.verified.artifacts.Generation)
			path := map[string]string{"index": paths.index, "records": paths.records, "descriptor": paths.descriptor}[artifact]
			info, err := os.Lstat(path)
			require.NoError(t, err)
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			file, err := os.OpenFile(path, os.O_WRONLY, 0)
			require.NoError(t, err)
			_, err = file.WriteAt([]byte{data[len(data)-1] ^ 0xff}, int64(len(data)-1))
			require.NoError(t, err)
			require.NoError(t, file.Close())
			// Across independent calls, metadata alone is not a verification.
			require.NoError(t, os.Chtimes(path, info.ModTime(), info.ModTime()))
			_, _, _, err = f.initial.DeriveWithCheckpoint(f.nextRoot, f.publication)
			require.Error(t, err)
			_, err = IdentifyShardedDeltaCheckpointArtifacts(f.root, f.publication.Directory, f.publication.Next)
			require.Error(t, err)
			require.Equal(t, int64(1), f.publication.Next.refs.Load())
		})
	}
}

// Compare just the artifact verification work in a publication: previously
// both the publisher and derivation hashed the files; now derivation checks
// the receipt's file identities. Checkpoint building and fsync are excluded.
func BenchmarkCheckpointPublicationArtifactVerification(b *testing.B) {
	root := b.TempDir()
	directory := filepath.Join(root, "checkpoint")
	require.NoError(b, os.Mkdir(directory, 0o755))
	values := make(map[solana.PublicKey]deltaIndexValue, 262144)
	for i := uint64(0); i < 262144; i++ {
		var key solana.PublicKey
		binary.BigEndian.PutUint64(key[:8], i)
		values[key] = deltaIndexValue{Entry: AccountIndexEntry{Slot: 2, FileId: 3, Offset: i * 8}}
	}
	checkpoint, err := BuildDeltaCheckpoint(b.Context(), directory, nil, values, 5, 1)
	require.NoError(b, err)
	handle, err := NewShardedDeltaCheckpointHandle(checkpoint)
	require.NoError(b, err)
	b.Cleanup(func() {
		require.NoError(b, handle.Release())
		<-handle.Done()
		require.NoError(b, handle.Err())
	})
	for _, reuse := range []bool{false, true} {
		name := "two-hash-passes"
		if reuse {
			name = "one-hash-pass-with-identity-handoff"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				verified, err := identifyShardedDeltaCheckpointArtifacts(root, directory, handle)
				require.NoError(b, err)
				if reuse {
					err = verified.validate(root, directory, handle)
				} else {
					_, err = IdentifyShardedDeltaCheckpointArtifacts(root, directory, handle)
				}
				require.NoError(b, err)
			}
		})
	}
}

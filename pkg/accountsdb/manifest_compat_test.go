package accountsdb

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/cockroachdb/pebble"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// Pin the original v1 wire layout independently of the current decoder. In
// particular the old reader used equality with 1, not a bit mask, for undo.
func TestManifestV1BooleanCompatibility(t *testing.T) {
	for _, prevValid := range []bool{false, true} {
		for _, vote := range []bool{false, true} {
			m := &SegmentManifest{Kind: ManifestKindFold, ThroughSlot: 20, FileId: 2,
				Records: []ManifestRecord{{Pubkey: [32]byte{1}, PrevValid: prevValid, Vote: vote,
					Prev: AccountIndexEntry{Slot: 10, FileId: 1, Offset: 136}}}}
			raw := m.encode()
			require.Equal(t, uint32(1), binary.LittleEndian.Uint32(raw[4:8]))
			// Fixed prefix + empty bankhash count + empty context length + record count.
			const recordStart = 4 + 4 + 1 + 5*8 + 4 + 4 + 4 + 8
			const booleanOffset = recordStart + 32 + 8 + 8
			require.LessOrEqual(t, raw[booleanOffset], byte(1))
			require.Equal(t, prevValid, raw[booleanOffset] == 1, "legacy undo decision")
			var previous AccountIndexEntry
			previous.Unmarshal((*[24]byte)(raw[booleanOffset+1 : booleanOffset+25]))
			require.Equal(t, m.Records[0].Prev, previous)
			require.Equal(t, crc32.ChecksumIEEE(raw[:len(raw)-4]), binary.LittleEndian.Uint32(raw[len(raw)-4:]))

			// Earlier versions of this RPC branch emitted bit 1 as a vote hint. The
			// fixed reader accepts those files; an explicit rewrite emits a boolean.
			raw[booleanOffset] |= 2
			binary.LittleEndian.PutUint32(raw[len(raw)-4:], crc32.ChecksumIEEE(raw[:len(raw)-4]))
			dir := t.TempDir()
			path := segmentManifestPath(dir, 20, 2)
			require.NoError(t, os.WriteFile(path, raw, 0600))
			got, err := ReadSegmentManifest(path)
			require.NoError(t, err)
			require.Equal(t, prevValid, got.Records[0].PrevValid)
			require.True(t, got.Records[0].Vote)
			require.NoError(t, WriteSegmentManifest(dir, got))
			canonical, err := os.ReadFile(path)
			require.NoError(t, err)
			require.LessOrEqual(t, canonical[booleanOffset], byte(1))
		}
	}
}

func TestManifestRecoveryDerivesVoteCandidates(t *testing.T) {
	for name, legacyFlags := range map[string]bool{"original-v1": false, "legacy-rpc-flags": true} {
		t.Run(name, func(t *testing.T) {
			db, dir := newFoldTestDb(t)
			require.NoError(t, db.SeedVoteAccountPubkeys(nil)) // No migration scan may mask recovery bugs.
			vote := foldAcct(1, 10, []byte("vote"))
			vote.Owner = addresses.VoteProgramAddr
			closed := foldAcct(2, 0, nil)
			closed.Owner = addresses.VoteProgramAddr
			ordinary := foldAcct(3, 10, []byte("ordinary"))
			res := commitTestBatch(t, db, 10, vote, closed, ordinary)
			manifest, err := ReadSegmentManifest(segmentManifestPath(db.AcctsDir, 10, res.FileId))
			require.NoError(t, err)
			for _, r := range manifest.Records {
				require.False(t, r.Vote, "vote hints must not be serialized")
			}
			if legacyFlags {
				// Even a stale legacy vote hint must not override the segment's owner
				// or zero balance. Mark all three records as votes in this fixture.
				path := segmentManifestPath(db.AcctsDir, 10, res.FileId)
				raw, err := os.ReadFile(path)
				require.NoError(t, err)
				recordStart := len(raw) - 4 - len(manifest.Records)*manifestRecordSize
				for i := range manifest.Records {
					raw[recordStart+i*manifestRecordSize+32+8+8] |= 2
				}
				binary.LittleEndian.PutUint32(raw[len(raw)-4:], crc32.ChecksumIEEE(raw[:len(raw)-4]))
				require.NoError(t, os.WriteFile(path, raw, 0600))
			}
			require.NoError(t, db.Index.Delete(voteIndexKey(vote.Key), pebble.Sync))
			require.NoError(t, db.Index.Delete(metaKeyLastBatch, pebble.Sync))
			for _, a := range []*accounts.Account{vote, closed, ordinary} {
				require.NoError(t, db.Index.Delete(a.Key[:], pebble.Sync))
			}
			db = reopenFoldTestDb(t, db, dir)
			defer db.CloseDb()
			recovered, err := db.RecoverFoldState()
			require.NoError(t, err)
			require.Equal(t, []uint64{1}, recovered.ReplayedBatches)
			got, err := db.VoteAccountPubkeys(context.Background())
			require.NoError(t, err)
			require.Equal(t, []solana.PublicKey{vote.Key}, got)
			require.Equal(t, uint64(10), mustColdRead(t, db, 10, vote.Key).Lamports)
			again, err := db.RecoverFoldState()
			require.NoError(t, err)
			require.Empty(t, again.ReplayedBatches)
		})
	}
}

func TestVoteManifestRewindRestoresExistingAccount(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	old := foldAcct(1, 10, []byte("old"))
	old.Owner = addresses.VoteProgramAddr
	commitTestBatch(t, db, 10, old)
	newer := foldAcct(1, 20, []byte("new"))
	newer.Owner = addresses.VoteProgramAddr
	created := foldAcct(2, 30, nil)
	created.Owner = addresses.VoteProgramAddr
	commitTestBatch(t, db, 20, newer, created)
	_, err := db.RewindToBatchBoundary(10)
	require.NoError(t, err)
	restored := mustColdRead(t, db, 10, old.Key)
	require.Equal(t, old.Lamports, restored.Lamports)
	require.Equal(t, old.Data, restored.Data)
	_, closer, err := db.Index.Get(created.Key[:])
	if closer != nil {
		closer.Close()
	}
	require.ErrorIs(t, err, pebble.ErrNotFound)
}

// A structurally valid, CRC-valid manifest with invalid record metadata must
// fail before changing the index, and must not be deleted as an orphan.
func TestManifestVoteRecoveryMetadataErrorPreservesFiles(t *testing.T) {
	for _, kind := range []string{"offset", "pubkey", "payload"} {
		t.Run(kind, func(t *testing.T) {
			db, _ := newFoldTestDb(t)
			defer db.CloseDb()
			vote := foldAcct(1, 10, nil)
			vote.Owner = addresses.VoteProgramAddr
			res := commitTestBatch(t, db, 10, vote)
			path := segmentManifestPath(db.AcctsDir, 10, res.FileId)
			m, err := ReadSegmentManifest(path)
			require.NoError(t, err)
			dataPath := filepath.Join(db.AcctsDir, SegmentDataName(10, res.FileId))
			switch kind {
			case "offset":
				m.Records[0].Offset = ^uint64(0)
			case "pubkey":
				m.Records[0].Pubkey[0]++
			case "payload":
				data, err := os.ReadFile(dataPath)
				require.NoError(t, err)
				binary.LittleEndian.PutUint64(data[dataLenOffset:], ^uint64(0))
				require.NoError(t, os.WriteFile(dataPath, data, 0600))
				m.DataCRC = crc32.ChecksumIEEE(data)
			}
			require.NoError(t, WriteSegmentManifest(db.AcctsDir, m))
			require.NoError(t, db.Index.Delete(metaKeyLastBatch, pebble.Sync))
			require.NoError(t, db.Index.Delete(voteIndexKey(vote.Key), pebble.Sync))
			before := snapshotIndex(t, db)
			_, err = db.RecoverFoldState()
			require.ErrorContains(t, err, "restore vote candidates")
			require.Equal(t, before, snapshotIndex(t, db))
			require.FileExists(t, path)
			require.FileExists(t, dataPath)
			_, haveMeta, err := db.readFoldMeta()
			require.NoError(t, err)
			require.False(t, haveMeta)
		})
	}
}

package node

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/state"
	solana "github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testDurableStatusContext(t *testing.T, accountsDbPath string, root uint64) *state.ResumeContext {
	t.Helper()
	cache := replay.NewTransactionStatusCache()
	var tipID solana.Hash
	var parentID solana.Hash
	for slot := uint64(1); slot <= root; slot++ {
		var blockID solana.Hash
		blockID[0] = byte(slot)
		blockID[31] = byte(slot >> 8)
		require.NoError(t, cache.CommitBlock(&block.Block{
			Slot:                      slot,
			ParentSlot:                slot - 1,
			AlpenglowBlockID:          [32]byte(blockID),
			HasAlpenglowBlockID:       true,
			AlpenglowParentBlockID:    [32]byte(parentID),
			HasAlpenglowParentBlockID: slot > 1,
		}))
		tipID = blockID
		parentID = blockID
	}
	// Fold construction snapshots the prospective root before the completion is
	// applied to the live cache; mirror that ordering here.
	payload, err := cache.SnapshotThrough(root)
	require.NoError(t, err)
	ref, err := replay.PrepareTransactionStatusCheckpoint(accountsDbPath, root, payload)
	require.NoError(t, err)
	return &state.ResumeContext{
		Slot:                        root,
		Bankhash:                    fmt.Sprintf("bank-%d", root),
		AlpenglowBlockID:            tipID.String(),
		AlpenglowChainedMerkleRoot:  solana.Hash{0xcc}.String(),
		TransactionStatusCheckpoint: ref,
	}
}

func TestValidateDurableResumeContextChecksSidecarCoverageRootTipAndIdentity(t *testing.T) {
	accountsDbPath := t.TempDir()
	ctx := testDurableStatusContext(t, accountsDbPath, 7)
	require.NoError(t, validateDurableResumeContext(accountsDbPath, 7, ctx))

	withoutID := *ctx
	withoutID.AlpenglowBlockID = ""
	assert.ErrorContains(t, validateDurableResumeContext(accountsDbPath, 7, &withoutID), "has no Alpenglow block id")

	withoutRoot := *ctx
	withoutRoot.AlpenglowChainedMerkleRoot = ""
	assert.ErrorContains(t, validateDurableResumeContext(accountsDbPath, 7, &withoutRoot), "has no Alpenglow chained merkle root")

	malformedRoot := *ctx
	malformedRoot.AlpenglowChainedMerkleRoot = "not-base58-0"
	assert.ErrorContains(t, validateDurableResumeContext(accountsDbPath, 7, &malformedRoot), "decode resume context Alpenglow chained merkle root")

	wrongID := *ctx
	var other solana.Hash
	other[0] = 99
	wrongID.AlpenglowBlockID = other.String()
	assert.ErrorContains(t, validateDurableResumeContext(accountsDbPath, 7, &wrongID), "lineage")

	checkpointPath := filepath.Join(accountsDbPath, replay.TransactionStatusCheckpointDirectory, ctx.TransactionStatusCheckpoint.File)
	payload, err := os.ReadFile(checkpointPath)
	require.NoError(t, err)

	wrongRootRef, err := replay.PrepareTransactionStatusCheckpoint(accountsDbPath, 8, payload)
	require.NoError(t, err)
	wrongRoot := *ctx
	wrongRoot.Slot = 8
	wrongRoot.TransactionStatusCheckpoint = wrongRootRef
	assert.ErrorContains(t, validateDurableResumeContext(accountsDbPath, 8, &wrongRoot), "rooted through slot 7, want 8")

	incompletePayload := append([]byte(nil), payload...)
	incompletePayload[4] = 0 // clear complete + genesis-origin proof flags
	incompleteRef, err := replay.PrepareTransactionStatusCheckpoint(accountsDbPath, 7, incompletePayload)
	require.NoError(t, err)
	incomplete := *ctx
	incomplete.TransactionStatusCheckpoint = incompleteRef
	assert.ErrorContains(t, validateDurableResumeContext(accountsDbPath, 7, &incomplete), "incomplete retained-root coverage")

	payload[len(payload)-1] ^= 0xff
	require.NoError(t, os.WriteFile(checkpointPath, payload, 0o644))
	assert.ErrorContains(t, validateDurableResumeContext(accountsDbPath, 7, ctx), "SHA-256 mismatch")
}

func TestRecoveredManifestContextIsAuthoritativeAtEqualRoot(t *testing.T) {
	accountsDbPath := t.TempDir()
	manifestCtx := testDurableStatusContext(t, accountsDbPath, 5)
	manifestCtx.Bankhash = "manifest-bankhash"
	encoded, err := json.Marshal(manifestCtx)
	require.NoError(t, err)

	s := &state.MithrilState{
		LastRootedSlot:     5,
		LastRootedBankhash: "stale-bankhash",
		LastRootedContext:  &state.ResumeContext{Slot: 5, Bankhash: "stale-bankhash"},
	}
	require.NoError(t, adoptRecoveredManifestContextBeforeIntegrity(accountsDbPath, s, accountsdb.RecoveryResult{
		DurableThrough: 5,
		ResumeCtx:      encoded,
	}))
	assert.Equal(t, "manifest-bankhash", s.LastRootedBankhash)
	assert.Equal(t, manifestCtx.TransactionStatusCheckpoint, s.LastRootedContext.TransactionStatusCheckpoint)
}

func TestRecoveredManifestContextAdvancesStaleStateBeforeIntegrity(t *testing.T) {
	accountsDbPath := t.TempDir()
	manifestCtx := testDurableStatusContext(t, accountsDbPath, 5)
	encoded, err := json.Marshal(manifestCtx)
	require.NoError(t, err)

	s := &state.MithrilState{LastRootedSlot: 0, LastSlot: 0}
	require.NoError(t, adoptRecoveredManifestContextBeforeIntegrity(accountsDbPath, s, accountsdb.RecoveryResult{
		DurableThrough: 5,
		ResumeCtx:      encoded,
	}))
	assert.Equal(t, uint64(5), s.LastRootedSlot)
	assert.Equal(t, uint64(5), s.LastSlot)
	assert.Equal(t, manifestCtx, s.LastRootedContext)
}

type recordingRPCBankState struct {
	slot             uint64
	blockHeight      uint64
	transactionCount uint64
}

func (state *recordingRPCBankState) SetRootedBankState(slot, blockHeight, transactionCount uint64) {
	state.slot = slot
	state.blockHeight = blockHeight
	state.transactionCount = transactionCount
}

func TestPublishRPCBankStateUsesRecoveredRoot(t *testing.T) {
	transactionCount := uint64(789)
	server := new(recordingRPCBankState)
	publishRPCBankState(server, &state.MithrilState{
		ManifestParentSlot:       10,
		ManifestBlockHeight:      20,
		ManifestTransactionCount: 30,
		LastRootedContext: &state.ResumeContext{
			Slot:             123,
			BlockHeight:      456,
			TransactionCount: &transactionCount,
		},
	})
	assert.Equal(t, recordingRPCBankState{slot: 123, blockHeight: 456, transactionCount: 789}, *server)
}

func writeCheckpointManifest(t *testing.T, accountsDbPath, manifestsDir string, seq, root, fileID uint64, suffix string) *state.TransactionStatusCheckpointRef {
	t.Helper()
	payload := []byte(fmt.Sprintf("checkpoint-payload-%d", root))
	ref, err := replay.PrepareTransactionStatusCheckpoint(accountsDbPath, root, payload)
	require.NoError(t, err)
	ctxJSON, err := json.Marshal(&state.ResumeContext{Slot: root, TransactionStatusCheckpoint: ref})
	require.NoError(t, err)
	require.NoError(t, accountsdb.WriteSegmentManifest(manifestsDir, &accountsdb.SegmentManifest{
		Kind:        accountsdb.ManifestKindFold,
		BatchSeq:    seq,
		FromSlot:    root - 1,
		ThroughSlot: root,
		FileId:      fileID,
		ResumeCtx:   ctxJSON,
	}))
	if suffix != "" {
		manifestPath := filepath.Join(manifestsDir, fmt.Sprintf("%d.%d.manifest", root, fileID))
		require.NoError(t, os.Rename(manifestPath, manifestPath+suffix))
	}
	return ref
}

func checkpointFilePath(accountsDbPath string, ref *state.TransactionStatusCheckpointRef) string {
	return filepath.Join(accountsDbPath, replay.TransactionStatusCheckpointDirectory, ref.File)
}

func TestCheckpointRetentionKeepsHPlusOneCurrentAndParked(t *testing.T) {
	accountsDbPath := t.TempDir()
	manifestsDir := filepath.Join(accountsDbPath, "accounts")
	require.NoError(t, os.MkdirAll(manifestsDir, 0o755))
	db := &accountsdb.AccountsDb{AcctsDir: manifestsDir}

	active := make([]*state.TransactionStatusCheckpointRef, 0, 5)
	for seq := uint64(1); seq <= 5; seq++ {
		active = append(active, writeCheckpointManifest(t, accountsDbPath, manifestsDir, seq, seq*10, seq, ""))
	}
	current, err := replay.PrepareTransactionStatusCheckpoint(accountsDbPath, 999, []byte("current-selected"))
	require.NoError(t, err)
	parked := writeCheckpointManifest(t, accountsDbPath, manifestsDir, 6, 60, 6, ".rewound")

	completedDir := filepath.Join(manifestsDir, "rewound")
	require.NoError(t, os.MkdirAll(completedDir, 0o755))
	completed := writeCheckpointManifest(t, accountsDbPath, completedDir, 7, 70, 7, ".rewound")

	removed, err := cleanupRetainedTransactionStatusCheckpoints(accountsDbPath, db, 2, current)
	require.NoError(t, err)
	assert.NotEmpty(t, removed)

	// H=2 pins the newest three active boundaries: the two suffix batches and
	// their rewind target. The selected and in-progress parked refs are extra.
	for index, ref := range active {
		_, statErr := os.Stat(checkpointFilePath(accountsDbPath, ref))
		if index < 2 {
			assert.ErrorIs(t, statErr, os.ErrNotExist)
		} else {
			assert.NoError(t, statErr)
		}
	}
	assert.FileExists(t, checkpointFilePath(accountsDbPath, current))
	assert.FileExists(t, checkpointFilePath(accountsDbPath, parked))
	assert.NoFileExists(t, checkpointFilePath(accountsDbPath, completed))
}

func TestCheckpointRetentionInvalidCurrentFailsBeforeDeletion(t *testing.T) {
	accountsDbPath := t.TempDir()
	manifestsDir := filepath.Join(accountsDbPath, "accounts")
	require.NoError(t, os.MkdirAll(manifestsDir, 0o755))
	db := &accountsdb.AccountsDb{AcctsDir: manifestsDir}
	orphan, err := replay.PrepareTransactionStatusCheckpoint(accountsDbPath, 1, []byte("orphan"))
	require.NoError(t, err)

	bad := *orphan
	bad.File = "../escape"
	_, err = cleanupRetainedTransactionStatusCheckpoints(accountsDbPath, db, 2, &bad)
	assert.Error(t, err)
	assert.FileExists(t, checkpointFilePath(accountsDbPath, orphan), "collector failure must occur before cleanup deletes anything")
}

func TestCheckpointRetentionInvalidActionableManifestFailsBeforeDeletion(t *testing.T) {
	for _, parked := range []bool{false, true} {
		name := "active"
		if parked {
			name = "parked"
		}
		t.Run(name, func(t *testing.T) {
			accountsDbPath := t.TempDir()
			manifestsDir := filepath.Join(accountsDbPath, "accounts")
			require.NoError(t, os.MkdirAll(manifestsDir, 0o755))
			db := &accountsdb.AccountsDb{AcctsDir: manifestsDir}
			orphan, err := replay.PrepareTransactionStatusCheckpoint(accountsDbPath, 1, []byte("must-survive-aborted-cleanup"))
			require.NoError(t, err)

			ctxJSON, err := json.Marshal(&state.ResumeContext{Slot: 10}) // missing required ref
			require.NoError(t, err)
			require.NoError(t, accountsdb.WriteSegmentManifest(manifestsDir, &accountsdb.SegmentManifest{
				Kind: accountsdb.ManifestKindFold, BatchSeq: 1, FromSlot: 9,
				ThroughSlot: 10, FileId: 10, ResumeCtx: ctxJSON,
			}))
			if parked {
				path := filepath.Join(manifestsDir, "10.10.manifest")
				require.NoError(t, os.Rename(path, path+".rewound"))
			}

			_, err = cleanupRetainedTransactionStatusCheckpoints(accountsDbPath, db, 2, nil)
			assert.Error(t, err)
			assert.FileExists(t, checkpointFilePath(accountsDbPath, orphan), "actionable-manifest failure must abort before deletion")
		})
	}
}

func TestRewindHorizonIncludesTargetBoundary(t *testing.T) {
	points := []accountsdb.RewindPoint{
		{BatchSeq: 1, ThroughSlot: 10},
		{BatchSeq: 2, ThroughSlot: 20},
		{BatchSeq: 3, ThroughSlot: 30},
		{BatchSeq: 4, ThroughSlot: 40},
	}
	assert.Equal(t, []accountsdb.RewindPoint{
		{BatchSeq: 2, ThroughSlot: 20},
		{BatchSeq: 3, ThroughSlot: 30},
		{BatchSeq: 4, ThroughSlot: 40},
	}, rewindPointsWithinHorizon(points, 2))
}

package replay

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/arena"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/genesis"
	"github.com/Overclock-Validator/mithril/pkg/genesisinit"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func checkpointTestRuntime(t *testing.T) {
	preserveGenesisEpochGlobals(t)
	oldCache, oldCount, oldArenas := sealevel.SysvarCache, global.TransactionCount(), sealevel.BorrowedAccountArenas
	sealevel.BorrowedAccountArenas = []*arena.Arena[sealevel.BorrowedAccount]{arena.New[sealevel.BorrowedAccount](512), arena.New[sealevel.BorrowedAccount](512)}
	t.Cleanup(func() {
		sealevel.SysvarCache, sealevel.BorrowedAccountArenas = oldCache, oldArenas
		global.SetTransactionCount(oldCount)
		ResetLocalLeaderCommits()
	})
}

func checkpointTestBlocks(t *testing.T, s *GenesisReplay) []*b.Block {
	g, _, err := genesis.ReadGenesisFromFile(filepath.Join(s.root, "genesis.bin"))
	require.NoError(t, err)
	f, err := os.Open(filepath.Join("testdata", "genesis-native-producer.json.gz"))
	require.NoError(t, err)
	defer f.Close()
	z, err := gzip.NewReader(f)
	require.NoError(t, err)
	defer z.Close()
	var oracle replayOracle
	require.NoError(t, json.NewDecoder(z).Decode(&oracle))
	require.Equal(t, s.seed.metadata.GenesisHash, oracle.GenesisHash)
	ingress := newGenesisIngressFixture(t, g, s.seed)
	var blocks []*b.Block
	for _, expected := range oracle.Blocks[:2] {
		component, err := turbine.UnmarshalBlockComponent(expected.Entries)
		require.NoError(t, err)
		entries := component.EntryBatch
		sender := turbine.NewBroadcastSession(turbine.BroadcastSessionConfig{
			Leader: ingress.leader, Slot: expected.Slot, ParentSlot: expected.ParentSlot,
			ParentChainedMerkleRoot: ingress.parentRoot, Version: ingress.version, Broadcaster: ingress.sender,
		})
		require.NoError(t, sender.BroadcastHeader(ingress.parentID))
		if len(entries) > 1 {
			require.NoError(t, sender.BroadcastEntryBatch(entries[:len(entries)-1]))
		}
		require.NoError(t, sender.BroadcastFooter(solana.MustHashFromBase58(expected.Frozen.BankHash), expected.ProducerTimeNanos, nil, nil))
		ending, err := turbine.NewEntryBatch(entries[len(entries)-1:])
		require.NoError(t, err)
		require.NoError(t, sender.BroadcastComponent(ending, true))
		select {
		case block := <-ingress.receiver.Blocks():
			require.NotNil(t, block)
			require.True(t, block.TransactionSignaturesVerified())
			ingress.receiver.AcknowledgeBlockDelivery(block.Slot)
			ingress.parentID, ingress.parentRoot = solana.Hash(block.AlpenglowBlockID), solana.Hash(block.AlpenglowLastChainedRoot)
			blocks = append(blocks, block)
		case err := <-ingress.receiver.Errors():
			t.Fatalf("checkpoint ingress: %v", err)
		case <-time.After(10 * time.Second):
			t.Fatal("checkpoint ingress did not emit a block")
		}
	}
	return blocks
}

func TestGenesisCheckpointValidationAndCancellation(t *testing.T) {
	checkpointTestRuntime(t)
	g, _, err := genesis.Build(t.Context(), replayGenesisConfig())
	require.NoError(t, err)
	root := filepath.Join(t.TempDir(), "db")
	_, err = genesisinit.Initialize(t.Context(), g, root)
	require.NoError(t, err)
	s, err := OpenGenesisReplay(t.Context(), root)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	_, err = OpenGenesisReplay(t.Context(), root)
	require.ErrorIs(t, err, accountsdb.ErrAccountsDbInUse)
	require.ErrorContains(t, s.CheckpointTrusted(t.Context()), "no new")
	blocks := checkpointTestBlocks(t, s)
	bad := *blocks[0]
	bad.AlpenglowParentBlockID[0] = 1
	_, err = s.ReplayBlock(t.Context(), &bad, 2)
	require.ErrorContains(t, err, "parent identity")
	bad = *blocks[0]
	bad.Entries = nil
	_, err = s.ReplayBlock(t.Context(), &bad, 2)
	require.ErrorContains(t, err, "ending tick")
	bad = *blocks[0]
	entryCopy := *bad.Entries[0]
	entryCopy.Hash = bytes.Clone(entryCopy.Hash)
	entryCopy.Hash[0] ^= 1
	bad.Entries = []*b.TxEntry{&entryCopy}
	_, err = s.ReplayBlock(t.Context(), &bad, 2)
	require.ErrorContains(t, err, "entry hash")
	for _, block := range blocks {
		_, err = s.ReplayBlock(t.Context(), block, 2)
		require.NoError(t, err)
	}
	for _, stage := range []string{"before-capture", "sidecar", "before-commit"} {
		t.Run(stage, func(t *testing.T) {
			cancelled := errors.New("checkpoint interrupted")
			err := s.checkpoint(t.Context(), func(at string) error {
				if at == stage {
					return cancelled
				}
				return nil
			})
			require.ErrorIs(t, err, cancelled)
			recovery, err := s.db.RecoverFoldState()
			require.NoError(t, err)
			require.Zero(t, recovery.BatchSeq)
			require.Equal(t, uint64(3), s.NextReplaySlot(), "cancellation keeps the speculative bank available")
		})
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, s.CheckpointTrusted(cancelled), context.Canceled)
	require.NoError(t, s.CheckpointTrusted(t.Context()))
	require.ErrorContains(t, s.CheckpointTrusted(t.Context()), "no new")
	recovery, err := s.db.RecoverFoldState()
	require.NoError(t, err)
	var cp GenesisCheckpoint
	require.NoError(t, json.Unmarshal(recovery.ResumeCtx, &cp))
	for _, name := range []string{"version", "profile", "hash", "parent", "fees", "identity", "status", "count", "tick", "position", "digest", "epoch_stakes", "vote_history"} {
		t.Run(name, func(t *testing.T) {
			var bad GenesisCheckpoint
			require.NoError(t, json.Unmarshal(recovery.ResumeCtx, &bad))
			switch name {
			case "version":
				bad.Version++
			case "profile":
				bad.GenesisBank.Features = nil
			case "hash":
				bad.Context.Bankhash = solana.Hash{5}.String()
			case "parent":
				bad.ParentBankhash[0] ^= 1
			case "fees":
				bad.Fees.LamportsPerSignature++
			case "identity":
				bad.Context.AlpenglowBlockID = solana.Hash{5}.String()
			case "status":
				bad.Context.TransactionStatusCheckpoint = nil
			case "count":
				bad.Context.TransactionCount = nil
			case "tick":
				bad.TickHeight--
			case "position":
				bad.ParentSlot++
			case "digest":
				bad.AccountsSHA256 = "bad"
			case "epoch_stakes":
				delete(bad.EpochState.Stakes, 1)
			case "vote_history":
				delete(bad.EpochState.VoteStates, 0)
			}
			raw, err := json.Marshal(bad)
			require.NoError(t, err)
			_, _, err = decodeGenesisCheckpoint(root, raw, cp.Context.Slot, s.seed)
			require.Error(t, err)
		})
	}
	// Version 1 epoch-zero checkpoints remain readable; they reconstruct the
	// historical seeds from the validated genesis. New writers always emit v2.
	legacy := cp
	legacy.Version = 1
	legacy.EpochState = GenesisEpochState{}
	legacyRaw, err := json.Marshal(legacy)
	require.NoError(t, err)
	_, _, err = decodeGenesisCheckpoint(root, legacyRaw, cp.Context.Slot, s.seed)
	require.NoError(t, err)
	// A restored cache rejects an old transfer in a new child, including when
	// the block signature/identity differs. This is execution history, not RPC.
	require.NoError(t, s.Close())
	s, err = OpenGenesisReplay(t.Context(), root)
	require.NoError(t, err)
	duplicate := &b.Block{Slot: 3, ParentSlot: 2, Transactions: blocks[1].Transactions,
		HasAlpenglowParentBlockID: true, AlpenglowParentBlockID: blocks[1].AlpenglowBlockID}
	require.Error(t, s.statuses.ValidateBlock(duplicate))
	feature := solana.MustPublicKeyFromBase58(cp.GenesisBank.Features[0].Address)
	entryBytes, closer, err := s.db.Index.Get(feature[:])
	require.NoError(t, err)
	entry, err := accountsdb.UnmarshalAcctIdxEntryValue(entryBytes)
	require.NoError(t, err)
	require.NoError(t, closer.Close())
	require.Zero(t, entry.Slot)
	require.NoError(t, s.Close())
	// Rent epoch is deliberately outside AccountsLtHash. The full account
	// digest must still detect corruption in an untouched bootstrap account.
	accountPath := filepath.Join(root, "accounts", fmt.Sprintf("%d.%d", entry.Slot, entry.FileId))
	accountBytes, err := os.ReadFile(accountPath)
	require.NoError(t, err)
	tampered := bytes.Clone(accountBytes)
	tampered[int(entry.Offset)+56] ^= 1
	require.NoError(t, os.WriteFile(accountPath, tampered, 0644))
	_, err = OpenGenesisReplay(t.Context(), root)
	require.ErrorContains(t, err, "account state mismatch")
	require.NoError(t, os.WriteFile(accountPath, accountBytes, 0644))
	_, _, err = genesisinit.Open(t.Context(), root)
	require.ErrorContains(t, err, "OpenGenesisReplay")
	_, err = state.CheckAndLoadValidState(root)
	require.ErrorContains(t, err, "manifest-aware")
	require.ErrorContains(t, state.RejectGenesisLaunch(root), "preserved")
	// Missing/corrupt selected status sidecars fail before recovery cleanup.
	statusPath := filepath.Join(root, TransactionStatusCheckpointDirectory, cp.Context.TransactionStatusCheckpoint.File)
	statusBytes, err := os.ReadFile(statusPath)
	require.NoError(t, err)
	orphan := filepath.Join(root, "accounts", "99.999")
	require.NoError(t, accountsdb.WriteLargestFileID(root, 999)) // allocation precedes segment creation
	require.NoError(t, os.WriteFile(orphan, []byte("preserve on invalid checkpoint"), 0644))
	require.NoError(t, os.Remove(statusPath))
	_, err = OpenGenesisReplay(t.Context(), root)
	require.Error(t, err)
	got, err := os.ReadFile(orphan)
	require.NoError(t, err)
	require.Equal(t, "preserve on invalid checkpoint", string(got))
	corrupt := bytes.Clone(statusBytes)
	corrupt[len(corrupt)-1] ^= 1
	require.NoError(t, os.WriteFile(statusPath, corrupt, 0644))
	_, err = OpenGenesisReplay(t.Context(), root)
	require.ErrorContains(t, err, "SHA-256")
	require.NoError(t, os.WriteFile(statusPath, statusBytes, 0644))
	require.NoError(t, os.Remove(orphan))
	// A corrupt decided manifest must never fall back to the valid slot-0 bank.
	headers, err := accountsdb.ListFoldManifests(filepath.Join(root, "accounts"))
	require.NoError(t, err)
	require.Len(t, headers, 1)
	raw, err := os.ReadFile(headers[0].Path)
	require.NoError(t, err)
	raw[len(raw)-1] ^= 1
	require.NoError(t, os.WriteFile(headers[0].Path, raw, 0644))
	_, err = OpenGenesisReplay(t.Context(), root)
	require.Error(t, err)
	still, err := os.ReadFile(headers[0].Path)
	require.NoError(t, err)
	require.Equal(t, raw, still)
}

// The subprocess exits without Shutdown. OS lock release and fsynced artifacts,
// not Go defers or retained in-memory state, determine the recovered bank.
func TestGenesisCheckpointProcessExit(t *testing.T) {
	if root := os.Getenv("MITHRIL_CHECKPOINT_CRASH_ROOT"); root != "" {
		checkpointTestRuntime(t)
		s, err := OpenGenesisReplay(t.Context(), root)
		require.NoError(t, err)
		for _, block := range checkpointTestBlocks(t, s) {
			_, err = s.ReplayBlock(t.Context(), block, 2)
			require.NoError(t, err)
		}
		require.NoError(t, s.checkpoint(t.Context(), func(stage string) error {
			if stage == os.Getenv("MITHRIL_CHECKPOINT_CRASH_STAGE") {
				os.Exit(77)
			}
			return nil
		}))
		t.Fatal("crash hook did not run")
	}
	for _, phase := range []string{"sidecar", "before-commit", "committed"} {
		t.Run(phase, func(t *testing.T) {
			g, _, err := genesis.Build(t.Context(), replayGenesisConfig())
			require.NoError(t, err)
			root := filepath.Join(t.TempDir(), "db")
			_, err = genesisinit.Initialize(t.Context(), g, root)
			require.NoError(t, err)
			cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestGenesisCheckpointProcessExit$", "-test.timeout=2m")
			cmd.Env = append(os.Environ(), "MITHRIL_CHECKPOINT_CRASH_ROOT="+root, "MITHRIL_CHECKPOINT_CRASH_STAGE="+phase)
			log, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			require.ErrorAs(t, err, &exit, string(log))
			require.Equal(t, 77, exit.ExitCode(), string(log))
			s, err := OpenGenesisReplay(t.Context(), root)
			require.NoError(t, err)
			if phase == "committed" {
				require.Equal(t, uint64(3), s.NextReplaySlot())
				require.Equal(t, uint64(1), s.transactionCount)
				require.True(t, s.statuses.CoverageComplete())
			} else {
				require.Equal(t, uint64(1), s.NextReplaySlot())
			}
			require.NoError(t, s.Close())
		})
	}
}

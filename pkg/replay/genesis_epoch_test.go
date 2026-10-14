package replay

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/genesis"
	"github.com/Overclock-Validator/mithril/pkg/genesisinit"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/programs/system"
	"github.com/stretchr/testify/require"
)

// Entries are generated without consulting oracle bank state. Agave only
// executes these bytes; its hashes then form independently signed test footers.
func fastEpochRequests(t *testing.T, g *genesis.Genesis, seed *GenesisReplayBootstrap) []replayOracleBlock {
	last := solana.MustHashFromBase58(seed.metadata.LastBlockhash)
	payer := solana.PrivateKey(ed25519.NewKeyFromSeed(bytes.Repeat([]byte{7}, 32)))
	recipient := solana.PrivateKey(ed25519.NewKeyFromSeed(bytes.Repeat([]byte{8}, 32))).PublicKey()
	var requests []replayOracleBlock
	for slot := uint64(1); slot <= 66; slot++ {
		var entries []turbine.Entry
		if slot%2 == 0 || slot == 31 || slot == 33 || slot == 63 || slot == 65 {
			tx, err := solana.NewTransaction([]solana.Instruction{system.NewTransferInstruction(slot*1000, payer.PublicKey(), recipient).Build()}, last)
			require.NoError(t, err)
			_, err = tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
				if key == payer.PublicKey() {
					return &payer
				}
				return nil
			})
			require.NoError(t, err)
			txs := []solana.Transaction{*tx}
			last = turbine.NextAlpenglowEntryHash(last, 1, txs)
			entries = append(entries, turbine.Entry{NumHashes: 1, Hash: last, Txns: txs})
		}
		last = turbine.NextAlpenglowEntryHash(last, 1, nil)
		entries = append(entries, turbine.Entry{NumHashes: 1, Hash: last})
		component, err := turbine.NewEntryBatch(entries)
		require.NoError(t, err)
		raw, err := turbine.MarshalBlockComponent(component)
		require.NoError(t, err)
		requests = append(requests, replayOracleBlock{Slot: slot, ParentSlot: slot - 1, ProducerTimeNanos: uint64(g.CreationTime.UnixNano()) + slot*400_000_000, Entries: raw})
	}
	return requests
}

func receiveFastEpochBlock(t *testing.T, ingress *genesisIngressFixture, expected replayOracleBlock) *b.Block {
	t.Helper()
	component, err := turbine.UnmarshalBlockComponent(expected.Entries)
	require.NoError(t, err)
	entries := component.EntryBatch
	capture := new(recordedShredBatches)
	sender := turbine.NewBroadcastSession(turbine.BroadcastSessionConfig{Leader: ingress.leader, Slot: expected.Slot, ParentSlot: expected.ParentSlot, ParentChainedMerkleRoot: ingress.parentRoot, Version: ingress.version, Broadcaster: capture})
	require.NoError(t, sender.BroadcastHeader(ingress.parentID))
	if len(entries) > 1 {
		require.NoError(t, sender.BroadcastEntryBatch(entries[:len(entries)-1]))
	}
	require.NoError(t, sender.BroadcastFooter(solana.MustHashFromBase58(expected.Frozen.BankHash), expected.ProducerTimeNanos, nil, nil))
	ending, err := turbine.NewEntryBatch(entries[len(entries)-1:])
	require.NoError(t, err)
	require.NoError(t, sender.BroadcastComponent(ending, true))
	for _, batch := range capture.batches {
		ingress.send(t, batch)
	}
	select {
	case block := <-ingress.receiver.Blocks():
		require.NotNil(t, block)
		require.True(t, block.TransactionSignaturesVerified())
		ingress.receiver.AcknowledgeBlockDelivery(block.Slot)
		ingress.parentID, ingress.parentRoot = solana.Hash(block.AlpenglowBlockID), solana.Hash(block.AlpenglowLastChainedRoot)
		return block
	case err := <-ingress.receiver.Errors():
		t.Fatalf("epoch ingress: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatalf("no slot %d from ingress: %+v", expected.Slot, ingress.receiver.Stats())
	}
	return nil
}

func TestGenesisFastEpochCheckpointAgave(t *testing.T) {
	checkpointTestRuntime(t)
	cfg := replayGenesisConfig()
	cfg.TestSlotsPerEpoch = 32
	g, _, err := genesis.Build(t.Context(), cfg)
	require.NoError(t, err)
	seed, err := NewGenesisReplayBootstrap(t.Context(), g)
	require.NoError(t, err)
	requests := fastEpochRequests(t, g, seed)
	oracle := nativeProducerOracleFixture(t, g, requests, "genesis-fast-epochs.json.gz")
	require.Len(t, oracle.Blocks, len(requests))
	root := filepath.Join(t.TempDir(), "db")
	_, err = genesisinit.Initialize(t.Context(), g, root)
	require.NoError(t, err)
	s, err := OpenGenesisReplay(t.Context(), root)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	// A different offline database may have populated the legacy process cache.
	// The boundary must reload this session's own stake index before scanning.
	global.InvalidateStakePubkeyIndexCache()
	foreign := t.TempDir()
	require.NoError(t, accountsdb.WriteStakePubkeyIndex(filepath.Join(foreign, global.StakePubkeyIndexFileName), []accountsdb.StakeIndexEntry{{Pubkey: solana.PublicKey{99}}}))
	_, err = global.LoadStakePubkeyIndex(foreign)
	require.NoError(t, err)
	t.Cleanup(global.InvalidateStakePubkeyIndexCache)
	ingress := newGenesisIngressFixture(t, g, seed)
	// The callback follows the current session after each reopen.
	var active atomic.Pointer[GenesisReplay]
	active.Store(s)
	ingress.receiver.SetLeaderForSlot(func(slot uint64) (solana.PublicKey, bool) { return active.Load().LeaderForSlot(slot) })
	current := make(map[solana.PublicKey]*accounts.Account)
	for _, a := range oracle.Genesis.Accounts {
		key := solana.MustPublicKeyFromBase58(a.Pubkey)
		acct, err := s.db.GetAccount(0, key)
		require.NoError(t, err)
		current[key] = acct.Clone()
	}
	reopen := func(next uint64) {
		require.NoError(t, s.Close())
		// Prove that restart reconstructs state rather than inheriting process caches.
		for _, e := range global.GetAllCachedEpochs() {
			global.ClearEpochStakes(e)
		}
		global.ClearEpochVoteStateSnapshots()
		reflect.ValueOf(&sealevel.SysvarCache).Elem().SetZero()
		s, err = OpenGenesisReplay(t.Context(), root)
		require.NoError(t, err)
		active.Store(s)
		require.Equal(t, next, s.NextReplaySlot())
	}
	for i, expected := range oracle.Blocks {
		require.Equal(t, requests[i].Entries, expected.Entries)
		block := receiveFastEpochBlock(t, ingress, expected)
		if expected.Slot%32 == 0 {
			// An unfolded parent must be explicitly selected for this offline branch.
			_, err = s.ReplayBlock(t.Context(), block, 2)
			require.ErrorContains(t, err, "requires durable parent")
			require.NoError(t, s.CheckpointTrusted(t.Context()))
			reopen(expected.Slot)
		}
		tip, err := s.ReplayBlock(t.Context(), block, 2)
		require.NoError(t, err, "slot %d", expected.Slot)
		if expected.Slot%32 == 0 {
			require.ErrorContains(t, s.CheckpointTrusted(t.Context()), "reward distribution")
			hash := append([]byte(nil), tip.FinalBankhash...)
			reopen(expected.Slot)
			tip, err = s.ReplayBlock(t.Context(), block, 2)
			require.NoError(t, err)
			require.Equal(t, hash, tip.FinalBankhash, "re-executed boundary")
		}
		for _, a := range tip.Accounts.AllAccounts() {
			current[a.Key] = a.Clone()
		}
		compareReplayAccounts(t, expected.Frozen, current)
		require.Equal(t, expected.Frozen.BankHash, solana.HashFromBytes(tip.FinalBankhash).String(), "slot %d", expected.Slot)
		require.Equal(t, expected.Frozen.TransactionCount, s.transactionCount)
		require.Equal(t, expected.Frozen.Capitalization, s.runtime.Capitalization)
		clock, ok := tip.BankSysvars().Clock()
		require.True(t, ok)
		require.Equal(t, expected.Slot/32, clock.Epoch)
		recent, ok := tip.BankSysvars().RecentBlockhashes()
		require.True(t, ok)
		require.Len(t, recent, len(expected.Frozen.RecentBlockhashes))
		for j, h := range expected.Frozen.RecentBlockhashes {
			require.Equal(t, h.Hash, solana.Hash(recent[j].Blockhash).String())
			require.Equal(t, h.LamportsPerSignature, recent[j].FeeCalculator.LamportsPerSignature)
		}
		for _, epoch := range expected.Frozen.EpochStakes {
			actual, err := s.epochs.stakes(epoch.Epoch)
			require.NoError(t, err)
			require.Equal(t, epoch.TotalStake, actual.TotalStake)
			if epoch.Epoch == expected.Slot/32 {
				require.Equal(t, actual.TotalStake, tip.TotalEpochStake)
				require.Equal(t, actual.Stakes, tip.VoteAccts)
			}
			for _, vote := range epoch.Votes {
				key := solana.MustPublicKeyFromBase58(vote.VoteAccount)
				require.Equal(t, vote.Stake, actual.Stakes[key])
				require.Equal(t, vote.Node, actual.VoteAccounts[key].NodePubkey.String())
				require.Equal(t, vote.Lamports, actual.VoteAccounts[key].Lamports)
				require.Equal(t, vote.BLSPublicKey, fmt.Sprintf("%x", actual.VoteAccounts[key].BlsPubkeyCompressed[:]))
			}
		}
		if expected.Slot == 33 || expected.Slot == 65 || expected.Slot == 66 {
			reward, ok := tip.BankSysvars().EpochRewards()
			require.True(t, ok)
			require.False(t, reward.Active)
			require.NoError(t, s.CheckpointTrusted(t.Context()))
			cp, err := s.capture(t.Context())
			require.NoError(t, err)
			raw, err := json.Marshal(cp.EpochState)
			require.NoError(t, err)
			reopen(expected.Slot + 1)
			restored, err := json.Marshal(s.epochs)
			require.NoError(t, err)
			require.JSONEq(t, string(raw), string(restored))
			require.Equal(t, expected.Frozen.BankHash, solana.HashFromBytes(s.tip.FinalBankhash).String())
		}
	}
	require.Equal(t, uint64(67), s.NextReplaySlot())
	// This fixture deliberately funds only one admission ticket. The computed
	// future epoch is empty after the vote balance falls below admission rent;
	// a restart must never replace it with the funded genesis seed.
	future, err := s.epochs.stakes(3)
	require.NoError(t, err)
	require.Zero(t, future.TotalStake)
	_, hasLeader := s.LeaderForSlot(96)
	require.False(t, hasLeader)
	require.NoError(t, s.Close())

}

func preserveGenesisEpochGlobals(t *testing.T) {
	stakes := serializeAllEpochStakes()
	votes := global.VoteCacheSnapshot()
	snapshots := make(map[uint64]map[solana.PublicKey]*sealevel.VoteStateVersions)
	for e := range stakes {
		if snapshot := global.EpochVoteStateSnapshot(e); snapshot != nil {
			snapshots[e] = snapshot
		}
	}
	t.Cleanup(func() {
		for _, e := range global.GetAllCachedEpochs() {
			global.ClearEpochStakes(e)
		}
		for _, raw := range stakes {
			_, err := global.DeserializeAndLoadEpochStakes(raw)
			require.NoError(t, err)
		}
		for key := range global.VoteCacheSnapshot() {
			global.DeleteVoteCacheItem(key)
		}
		for key, vote := range votes {
			global.PutVoteCacheItem(key, vote)
		}
		global.ClearEpochVoteStateSnapshots()
		epochs := make([]uint64, 0, len(snapshots))
		for e := range snapshots {
			epochs = append(epochs, e)
		}
		sort.Slice(epochs, func(i, j int) bool { return epochs[i] < epochs[j] })
		for _, e := range epochs {
			global.PutEpochVoteStateSnapshot(e, snapshots[e])
		}
	})
}

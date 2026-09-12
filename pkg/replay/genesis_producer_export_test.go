package replay

// Test-only bridge for the external replay_test package. It allows the real
// blockprod package to participate without introducing a replay -> blockprod
// production import cycle or exporting the unrooted-tail implementation.
import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/arena"
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

type GenesisProducerInput struct {
	DB                   *accountsdb.AccountsDb
	Seed                 *GenesisReplayBootstrap
	Next                 *b.Block
	ParentSysvars        *sealevel.BankSysvars
	ParentReader         sealevel.AccountReader
	Statuses             *TransactionStatusView
	ParentID, ParentRoot solana.Hash
	ParentNano           *accounts.Account
	Broadcaster          turbine.PacketBroadcaster
	Version              uint16
	ProducerTime         uint64
	Transactions         []*solana.Transaction
	Prepared             func(*sealevel.SlotCtx)
}

func RunGenesisProducerFixture(t *testing.T, produce func(*testing.T, GenesisProducerInput) (*b.Block, *sealevel.SlotCtx)) {
	runGenesisProducerFixture(t, produce, false)
}

func RunGenesisProducerCheckpointFixture(t *testing.T, produce func(*testing.T, GenesisProducerInput) (*b.Block, *sealevel.SlotCtx)) {
	runGenesisProducerFixture(t, produce, true)
}

func runGenesisProducerFixture(t *testing.T, produce func(*testing.T, GenesisProducerInput) (*b.Block, *sealevel.SlotCtx), checkpoints bool) {
	t.Helper()
	preserveGenesisEpochGlobals(t)
	g, _, err := genesis.Build(t.Context(), replayGenesisConfig())
	require.NoError(t, err)
	seed, err := NewGenesisReplayBootstrap(t.Context(), g)
	require.NoError(t, err)
	initial, err := genesis.ConstructInitialBank(t.Context(), g)
	require.NoError(t, err)
	open := func(name string) (*accountsdb.AccountsDb, *unrootedTail) {
		root := filepath.Join(t.TempDir(), name)
		_, err := genesisinit.Initialize(t.Context(), g, root)
		require.NoError(t, err)
		db, _, err := genesisinit.Open(t.Context(), root)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Shutdown(context.Background())) })
		return db, newUnrootedTail(db, nil, 16, 16, root)
	}
	producerDB, producerTail := open("producer")
	receiverDB, receiverTail := open("receiver")
	var resumed *GenesisReplay
	if checkpoints {
		root := filepath.Dir(receiverDB.AcctsDir)
		require.NoError(t, receiverDB.Shutdown(context.Background()))
		resumed, err = OpenGenesisReplay(t.Context(), root)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, resumed.Close()) })
		receiverDB, receiverTail = resumed.db, resumed.tail
	}
	require.NotEqual(t, producerDB.AcctsDir, receiverDB.AcctsDir)
	producerStatuses, receiverStatuses := NewTransactionStatusCache(), NewTransactionStatusCache()
	oldCache, oldCount, oldArenas := sealevel.SysvarCache, global.TransactionCount(), sealevel.BorrowedAccountArenas
	oldFrontier := global.ReplayFrontier()
	sealevel.BorrowedAccountArenas = []*arena.Arena[sealevel.BorrowedAccount]{arena.New[sealevel.BorrowedAccount](512), arena.New[sealevel.BorrowedAccount](512)}
	global.SetTransactionCount(0)
	global.ResetAlpenglowChainMetadata()
	t.Cleanup(func() {
		ResetLocalLeaderCommits()
		sealevel.SysvarCache, sealevel.BorrowedAccountArenas = oldCache, oldArenas
		global.SetTransactionCount(oldCount)
		global.SetReplayFrontier(oldFrontier)
		global.ResetAlpenglowChainMetadata()
	})
	ingress := newGenesisIngressFixture(t, g, seed)
	parentRoot := ingress.genesisRoot
	var parentID solana.Hash
	var producerParent, receiverParent *sealevel.SlotCtx
	var requests []replayOracleBlock
	var bankHashes []string
	var producedBanks []*sealevel.SlotCtx
	var producedStates, receivedStates, initializedStates []map[solana.PublicKey]*accounts.Account
	producerCurrent, receiverCurrent := make(map[solana.PublicKey]*accounts.Account), make(map[solana.PublicKey]*accounts.Account)
	for _, entry := range initial.Frozen.Accounts {
		for _, pair := range []struct {
			db      *accountsdb.AccountsDb
			current map[solana.PublicKey]*accounts.Account
		}{{producerDB, producerCurrent}, {receiverDB, receiverCurrent}} {
			a, err := pair.db.GetAccount(0, entry.Key)
			require.NoError(t, err)
			pair.current[entry.Key] = a.Clone()
		}
	}
	cloneMap := func(current map[solana.PublicKey]*accounts.Account) map[solana.PublicKey]*accounts.Account {
		out := make(map[solana.PublicKey]*accounts.Account, len(current))
		for key, a := range current {
			out[key] = a.Clone()
		}
		return out
	}
	configure := func(slot uint64, parent *sealevel.SlotCtx) (*b.Block, *sealevel.BankSysvars) {
		next := &b.Block{Slot: slot, SourceParentSlot: slot - 1}
		if parent == nil {
			view, err := seed.ConfigureFirstBlock(next)
			require.NoError(t, err)
			return next, view
		}
		next.ParentSlot, next.BlockHeight = parent.Slot, slot
		next.ParentBankhash = solana.HashFromBytes(parent.FinalBankhash)
		next.Features, next.AcctsLtHash = parent.Features.Clone(), parent.AcctsLtHash.Clone()
		next.LastBlockhash, next.PrevFeeRateGovernor, next.PrevNumSignatures = parent.Blockhash, parent.FeeRateGovernor, parent.NumSignatures
		next.EpochStakesPerVoteAcct, next.TotalEpochStake, next.VoteTimestamps = parent.VoteAccts, parent.TotalEpochStake, parent.VoteTimestamps
		next.Leader, _ = seed.LeaderForSlot(slot)
		return next, parent.BankSysvars()
	}
	for slot := uint64(1); slot <= 4; slot++ {
		global.SetReplayFrontier(slot - 1)
		next, producerSysvars := configure(slot, producerParent)
		var txs []*solana.Transaction
		if slot%2 == 0 {
			payer := solana.PrivateKey(ed25519.NewKeyFromSeed(bytes.Repeat([]byte{7}, 32)))
			recipient := solana.PrivateKey(ed25519.NewKeyFromSeed(bytes.Repeat([]byte{8}, 32))).PublicKey()
			tx, err := solana.NewTransaction([]solana.Instruction{system.NewTransferInstruction(slot*1_000_000, payer.PublicKey(), recipient).Build()}, solana.Hash(next.LastBlockhash))
			require.NoError(t, err)
			_, err = tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
				if key == payer.PublicKey() {
					return &payer
				}
				return nil
			})
			require.NoError(t, err)
			txs = append(txs, tx)
		}
		var nano *accounts.Account
		if producerParent != nil {
			nano, _ = producerParent.GetAccount(NanosecondClockAccountAddr(next.Features))
		}
		input := GenesisProducerInput{DB: producerDB, Seed: seed, Next: next, ParentSysvars: producerSysvars, ParentReader: producerTail,
			Statuses: producerStatuses.View(), ParentID: parentID, ParentRoot: parentRoot, ParentNano: nano,
			Broadcaster: ingress.sender, Version: ingress.version, ProducerTime: uint64(g.CreationTime.Unix())*1_000_000_000 + slot*400_000_000,
			Transactions: txs, Prepared: func(ctx *sealevel.SlotCtx) {
				state := cloneMap(producerCurrent)
				for _, a := range ctx.Accounts.AllAccounts() {
					state[a.Key] = a.Clone()
				}
				initializedStates = append(initializedStates, state)
			}}
		produced, producerCtx := produce(t, input)
		require.NotNil(t, produced)
		require.NotNil(t, producerCtx)
		// Remove the process-local adoption optimization. This receiver must
		// independently execute bytes received over UDP into its own Pebble-backed tail.
		_, present := TakeLocalLeaderCommit(slot)
		require.False(t, present, "producer callback must consume its local commit")
		var received *b.Block
		select {
		case received = <-ingress.receiver.Blocks():
			require.NotNil(t, received)
		case err := <-ingress.receiver.Errors():
			t.Fatalf("native producer ingress: %v", err)
		case <-time.After(10 * time.Second):
			t.Fatalf("no native slot %d: %+v", slot, ingress.receiver.Stats())
		}
		ingress.receiver.AcknowledgeBlockDelivery(slot)
		require.True(t, received.TransactionSignaturesVerified())
		require.Equal(t, produced.ExpectedBankhash, received.ExpectedBankhash)
		require.Equal(t, produced.AlpenglowBlockID, received.AlpenglowBlockID)
		require.Equal(t, parentID, solana.Hash(received.AlpenglowParentBlockID))
		require.True(t, received.HasAlpenglowParentBlockID)
		require.Equal(t, produced.FooterProducerTimeNanos, received.FooterProducerTimeNanos)
		entries := make([]turbine.Entry, len(received.Entries))
		last := solana.Hash(next.LastBlockhash)
		for i, e := range received.Entries {
			entries[i] = turbine.Entry{NumHashes: e.NumHashes, Hash: solana.HashFromBytes(e.Hash)}
			for _, index := range e.Indices {
				entries[i].Txns = append(entries[i].Txns, *received.Transactions[index])
			}
			require.Equal(t, uint64(1), e.NumHashes)
			require.Equal(t, entries[i].Hash, turbine.NextAlpenglowEntryHash(last, 1, entries[i].Txns))
			last = entries[i].Hash
		}
		require.Empty(t, entries[len(entries)-1].Txns)
		require.Len(t, entries, 1+len(txs))
		component, err := turbine.NewEntryBatch(entries)
		require.NoError(t, err)
		raw, err := turbine.MarshalBlockComponent(component)
		require.NoError(t, err)
		requests = append(requests, replayOracleBlock{Slot: slot, ParentSlot: slot - 1, ProducerTimeNanos: received.FooterProducerTimeNanos, Entries: raw})
		replayBlock, receiverSysvars := configure(slot, receiverParent)
		// Keep ingress entries, footer and signed identities; install only the
		// independently selected receiver parent's execution metadata.
		replayBlock.Transactions, replayBlock.Entries, replayBlock.Versions = received.Transactions, received.Entries, received.Versions
		replayBlock.Blockhash, replayBlock.NumSignatures = received.Blockhash, received.NumSignatures
		replayBlock.FromLiveStream = true
		replayBlock.HasAlpenglowFooter, replayBlock.FooterProducerTimeNanos = true, received.FooterProducerTimeNanos
		replayBlock.HasExpectedBankhash, replayBlock.ExpectedBankhash = true, received.ExpectedBankhash
		replayBlock.HasAlpenglowBlockID, replayBlock.AlpenglowBlockID = true, received.AlpenglowBlockID
		replayBlock.HasAlpenglowParentBlockID, replayBlock.AlpenglowParentBlockID = true, received.AlpenglowParentBlockID
		replayBlock.HasAlpenglowLastChainedRoot, replayBlock.AlpenglowLastChainedRoot = true, received.AlpenglowLastChainedRoot
		replayBlock.AlpenglowShredVersion = received.AlpenglowShredVersion
		replayBlock.BlockReward = &b.BlockRewardsInfo{Leader: next.Leader}
		var genesisParent []*GenesisReplayBootstrap
		if slot == 1 {
			_, err = seed.ConfigureFirstBlock(replayBlock)
			require.NoError(t, err)
			genesisParent = []*GenesisReplayBootstrap{seed}
		}
		schedule, _ := receiverSysvars.EpochSchedule()
		var replayed *sealevel.SlotCtx
		if checkpoints {
			// The reopened session supplies every parent field. No previous
			// receiver SlotCtx or fixture-derived parent enters this call.
			replayed, err = resumed.ReplayBlock(t.Context(), received, 2)
		} else {
			replayed, err = ProcessBlock(receiverDB, replayBlock, &schedule, 2, nil, new(persistedTracker), receiverTail, receiverStatuses, true, receiverSysvars, genesisParent...)
		}
		require.NoError(t, err)
		if checkpoints && slot == 3 {
			uncheckpointedHash := bytes.Clone(replayed.FinalBankhash)
			root := resumed.root
			require.NoError(t, resumed.Close())
			resumed, err = OpenGenesisReplay(t.Context(), root)
			require.NoError(t, err)
			require.Equal(t, uint64(3), resumed.NextReplaySlot(), "discard the speculative slot-3 tail")
			global.SetTransactionCount(999)
			replayed, err = resumed.ReplayBlock(t.Context(), received, 2)
			require.NoError(t, err)
			require.Equal(t, uncheckpointedHash, replayed.FinalBankhash)
			receiverDB, receiverTail, receiverStatuses = resumed.db, resumed.tail, resumed.statuses
		}
		require.NotSame(t, producerCtx, replayed)
		require.Equal(t, producerCtx.FinalBankhash, replayed.FinalBankhash)
		require.Equal(t, producerCtx.AcctsLtHash.Hash(), replayed.AcctsLtHash.Hash())
		producerTail.Add(slot, collectAdoptAccounts(producerCtx, produced), bytes.Clone(producerCtx.FinalBankhash))
		require.NoError(t, producerStatuses.CommitBlock(produced))
		for _, pair := range []struct {
			ctx     *sealevel.SlotCtx
			tail    *unrootedTail
			current map[solana.PublicKey]*accounts.Account
		}{
			{producerCtx, producerTail, producerCurrent}, {replayed, receiverTail, receiverCurrent},
		} {
			require.NoError(t, pair.ctx.BankSysvars().RangeAccountViews(func(key solana.PublicKey, _ *accounts.Account) error { pair.current[key] = nil; return nil }))
			for key := range pair.ctx.ModifiedAccts {
				pair.current[key] = nil
			}
			for key := range pair.current {
				a, err := pair.tail.GetAccount(slot, key)
				require.NoError(t, err)
				pair.current[key] = a.Clone()
			}
		}
		producedStates = append(producedStates, cloneMap(producerCurrent))
		receivedStates = append(receivedStates, cloneMap(receiverCurrent))
		producerParent, receiverParent = producerCtx, replayed
		bankHashes = append(bankHashes, solana.HashFromBytes(replayed.FinalBankhash).String())
		producedBanks = append(producedBanks, producerCtx)
		parentID, parentRoot = solana.Hash(produced.AlpenglowBlockID), solana.Hash(produced.AlpenglowLastChainedRoot)
		t.Logf("native slot %d: %d transactions; bank %s", slot, len(txs), solana.HashFromBytes(replayed.FinalBankhash))
		if checkpoints && slot%2 == 0 {
			require.NoError(t, resumed.CheckpointTrusted(t.Context()))
			root := resumed.root
			require.NoError(t, resumed.Close())
			resumed, err = OpenGenesisReplay(t.Context(), root)
			require.NoError(t, err)
			require.Equal(t, slot+1, resumed.NextReplaySlot())
			require.Equal(t, replayed.FinalBankhash, resumed.tip.FinalBankhash)
			require.Equal(t, replayed.FeeRateGovernor, resumed.tip.FeeRateGovernor)
			require.Equal(t, replayed.VoteTimestamps, resumed.tip.VoteTimestamps)
			require.Equal(t, replayed.VoteAccts, resumed.tip.VoteAccts)
			for _, tx := range txs {
				found, err := resumed.statuses.View().ContainsTransaction(tx)
				require.NoError(t, err)
				require.True(t, found, "processed transfer must survive restart")
			}
			for key, expected := range receiverCurrent {
				actual, err := resumed.db.GetAccount(slot, key)
				require.NoError(t, err)
				actual.Slot, expected.Slot = 0, 0
				require.Equal(t, expected, actual)
			}
			receiverDB, receiverTail, receiverStatuses = resumed.db, resumed.tail, resumed.statuses
			global.SetTransactionCount(999) // next replay must restore the exact count
			if slot == 4 {
				global.SetTransactionCount(resumed.transactionCount)
			}
			t.Logf("reopened durable child slot %d; next slot %d", slot, resumed.NextReplaySlot())
		}
	}
	oracle := nativeProducerOracle(t, g, requests)
	require.Len(t, oracle.Blocks, 4)
	for i, expected := range oracle.Blocks {
		require.Equal(t, requests[i].Entries, expected.Entries)
		require.Equal(t, requests[i].ProducerTimeNanos, expected.ProducerTimeNanos)
		compareReplayAccounts(t, expected.Initialized, initializedStates[i])
		compareReplayAccounts(t, expected.Frozen, producedStates[i])
		compareReplayAccounts(t, expected.Frozen, receivedStates[i])
		// Footer hashes were computed by Mithril before invoking this oracle.
		require.Equal(t, expected.Frozen.BankHash, bankHashes[i])
		ctx := producedBanks[i]
		require.Equal(t, expected.Frozen.SignatureCount, ctx.NumSignatures)
		require.Equal(t, expected.Frozen.LastBlockhash, solana.Hash(ctx.Blockhash).String())
		require.Equal(t, expected.Frozen.LamportsPerSignature, ctx.FeeRateGovernor.LamportsPerSignature)
		for _, epoch := range expected.Frozen.EpochStakes {
			if epoch.Epoch == 0 {
				require.Equal(t, epoch.TotalStake, ctx.TotalEpochStake)
				for _, vote := range epoch.Votes {
					require.Equal(t, vote.Stake, ctx.VoteAccts[solana.MustPublicKeyFromBase58(vote.VoteAccount)])
				}
			}
		}
	}
	// Both durable databases are still verified slot-0 stores. Child banks are
	// speculative; this test neither invents finality nor publishes a checkpoint.
	require.Equal(t, oracle.Blocks[3].Frozen.TransactionCount, global.TransactionCount())
	genesisDBs := []*accountsdb.AccountsDb{producerDB}
	if !checkpoints {
		genesisDBs = append(genesisDBs, receiverDB)
	}
	for _, db := range genesisDBs {
		root := filepath.Dir(db.AcctsDir)
		require.NoError(t, db.Shutdown(context.Background()))
		reopened, metadata, err := genesisinit.Open(t.Context(), root)
		require.NoError(t, err)
		require.Equal(t, initial.Frozen.Metadata, *metadata)
		require.NoError(t, reopened.Shutdown(context.Background()))
	}
}

func nativeProducerOracle(t *testing.T, g *genesis.Genesis, requests []replayOracleBlock) replayOracle {
	return nativeProducerOracleFixture(t, g, requests, "genesis-native-producer.json.gz")
}

func nativeProducerOracleFixture(t *testing.T, g *genesis.Genesis, requests []replayOracleBlock, name string) replayOracle {
	t.Helper()
	fixture := filepath.Join("testdata", name)
	var data []byte
	if helper := os.Getenv("MITHRIL_AGAVE_ORACLE"); helper != "" {
		dir := t.TempDir()
		genPath, input, output := filepath.Join(dir, "genesis.bin"), filepath.Join(dir, "entries.json"), filepath.Join(dir, "banks.json")
		raw, err := genesis.Encode(g)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(genPath, raw, 0644))
		inputs := make([]map[string]any, len(requests))
		for i, r := range requests {
			inputs[i] = map[string]any{"slot": r.Slot, "parent_slot": r.ParentSlot, "producer_time_nanos": r.ProducerTimeNanos, "entries": r.Entries}
		}
		raw, err = json.Marshal(inputs)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(input, raw, 0644))
		log, err := exec.CommandContext(t.Context(), helper, "replay-native", genPath, input, output).CombinedOutput()
		require.NoError(t, err, string(log))
		data, err = os.ReadFile(output)
		require.NoError(t, err)
		if os.Getenv("MITHRIL_UPDATE_ORACLE_FIXTURES") == "1" {
			f, err := os.Create(fixture)
			require.NoError(t, err)
			z := gzip.NewWriter(f)
			_, err = z.Write(data)
			require.NoError(t, err)
			require.NoError(t, z.Close())
			require.NoError(t, f.Close())
		}
	} else {
		f, err := os.Open(fixture)
		require.NoError(t, err)
		defer f.Close()
		z, err := gzip.NewReader(f)
		require.NoError(t, err)
		defer z.Close()
		var buf bytes.Buffer
		_, err = buf.ReadFrom(z)
		require.NoError(t, err)
		data = buf.Bytes()
	}
	var result replayOracle
	require.NoError(t, json.Unmarshal(data, &result))
	require.Equal(t, genesis.AgaveRevision, result.Revision)
	raw, err := genesis.Encode(g)
	require.NoError(t, err)
	require.Equal(t, solana.Hash(sha256.Sum256(raw)).String(), result.GenesisHash)
	return result
}

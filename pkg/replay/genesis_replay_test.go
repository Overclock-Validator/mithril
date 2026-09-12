package replay

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/arena"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/genesis"
	"github.com/Overclock-Validator/mithril/pkg/genesisinit"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type replayOracleAccount struct {
	Pubkey     string `json:"pubkey"`
	Owner      string `json:"owner"`
	Lamports   uint64 `json:"lamports"`
	RentEpoch  uint64 `json:"rent_epoch"`
	Executable bool   `json:"executable"`
	Data       []byte `json:"data"`
}
type replayOracleBank struct {
	genesis.BankMetadata
	Accounts []replayOracleAccount `json:"accounts"`
}
type replayOracleBlock struct {
	Slot              uint64           `json:"slot"`
	ParentSlot        uint64           `json:"parent_slot"`
	ProducerTimeNanos uint64           `json:"producer_time_nanos"`
	Entries           []byte           `json:"entries"`
	Transactions      [][]byte         `json:"transactions"`
	Initialized       replayOracleBank `json:"initialized"`
	Frozen            replayOracleBank `json:"frozen"`
}
type replayOracle struct {
	Revision    string              `json:"agave_revision"`
	GenesisHash string              `json:"genesis_hash"`
	Genesis     replayOracleBank    `json:"genesis"`
	Blocks      []replayOracleBlock `json:"blocks"`
}

func replayGenesisConfig() genesis.Config {
	key := func(seed byte) string {
		return solana.PrivateKey(ed25519.NewKeyFromSeed(bytes.Repeat([]byte{seed}, 32))).PublicKey().String()
	}
	return genesis.Config{CreationTime: "2026-01-01T00:00:00Z",
		Accounts: []genesis.FundedAccount{{Address: key(7), Lamports: 1_000_000_000}, {Address: key(8), Lamports: 1_000_000}},
		Validators: []genesis.Validator{{Identity: key(1), VoteAccount: key(2), StakeAccount: key(3),
			BLSPublicKey:     "a55384b86c60c0e2a13ffdf9087c94495455083acf06ff23d8e7416ee1df1cafccfaad802b527ade7dfe1b02a6bfd79a",
			IdentityLamports: 500_000_000_000, VoteLamports: 2_000_000_000, StakeLamports: 10_000_000_000}},
	}
}

func loadReplayOracle(t *testing.T, g *genesis.Genesis) replayOracle {
	t.Helper()
	fixture := filepath.Join("testdata", "genesis-replay.json.gz")
	var data []byte
	if helper := os.Getenv("MITHRIL_AGAVE_ORACLE"); helper != "" {
		dir := t.TempDir()
		raw, err := genesis.Encode(g)
		require.NoError(t, err)
		input, output := filepath.Join(dir, "genesis.bin"), filepath.Join(dir, "oracle.json")
		require.NoError(t, os.WriteFile(input, raw, 0644))
		log, err := exec.CommandContext(t.Context(), helper, "replay", input, output).CombinedOutput()
		require.NoError(t, err, string(log))
		data, err = os.ReadFile(output)
		require.NoError(t, err)
		if os.Getenv("MITHRIL_UPDATE_ORACLE_FIXTURES") == "1" {
			require.NoError(t, os.MkdirAll(filepath.Dir(fixture), 0755))
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
	return result
}

func compareReplayAccounts(t *testing.T, expected replayOracleBank, current map[solana.PublicKey]*accounts.Account) {
	t.Helper()
	var count int
	var capitalization, dataLen uint64
	lt := new(lthash.LtHash)
	for _, a := range current {
		if a.Lamports == 0 {
			continue
		}
		count++
		capitalization += a.Lamports
		dataLen += uint64(len(a.Data))
		lt.MixIn(new(lthash.LtHash).InitWithAcct(a))
	}
	require.Equal(t, len(expected.Accounts), count, "nonzero account count")
	for _, a := range expected.Accounts {
		got := current[solana.MustPublicKeyFromBase58(a.Pubkey)]
		require.NotNil(t, got, a.Pubkey)
		require.Equal(t, a.Lamports, got.Lamports, a.Pubkey)
		require.Equal(t, a.Owner, solana.PublicKey(got.Owner).String(), a.Pubkey)
		require.Equal(t, a.Executable, got.Executable, a.Pubkey)
		require.Equal(t, a.RentEpoch, got.RentEpoch, a.Pubkey)
		require.Equal(t, len(a.Data), len(got.Data), a.Pubkey)
		require.True(t, bytes.Equal(a.Data, got.Data), "account data differs: %s", a.Pubkey)
	}
	require.Equal(t, expected.Capitalization, capitalization)
	require.Equal(t, expected.AccountsDataLen, dataLen)
	require.Equal(t, expected.AccountsLtHash, lt.Hash())
}

func TestGenesisFirstBlocksAgave(t *testing.T) {
	for _, workers := range []int{0, 2} {
		t.Run(fmt.Sprintf("workers-%d", workers), func(t *testing.T) { testGenesisFirstBlocksAgave(t, workers, false) })
	}
}

func testGenesisFirstBlocksAgave(t *testing.T, workers int, signedIngress bool) {
	g, _, err := genesis.Build(t.Context(), replayGenesisConfig())
	require.NoError(t, err)
	oracle := loadReplayOracle(t, g)
	seed, err := NewGenesisReplayBootstrap(t.Context(), g)
	require.NoError(t, err)
	require.Equal(t, seed.metadata.GenesisHash, oracle.GenesisHash)
	initial, err := genesis.ConstructInitialBank(t.Context(), g)
	require.NoError(t, err)
	current := make(map[solana.PublicKey]*accounts.Account)
	for _, a := range initial.Frozen.Accounts {
		current[a.Key] = a.Account.Clone()
	}
	compareReplayAccounts(t, oracle.Genesis, current)
	root := filepath.Join(t.TempDir(), "accountsdb")
	_, err = genesisinit.Initialize(t.Context(), g, root)
	require.NoError(t, err)
	db, metadata, err := genesisinit.Open(t.Context(), root)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Shutdown(context.Background())) }()
	require.Equal(t, seed.metadata, *metadata)
	tail := newUnrootedTail(db, nil, 16, 16, root)
	statuses := NewTransactionStatusCache()
	previousCache := sealevel.SysvarCache
	previousTxCount := global.TransactionCount()
	previousArenas := sealevel.BorrowedAccountArenas
	sealevel.BorrowedAccountArenas = make([]*arena.Arena[sealevel.BorrowedAccount], workers)
	for i := range sealevel.BorrowedAccountArenas {
		sealevel.BorrowedAccountArenas[i] = arena.New[sealevel.BorrowedAccount](512)
	}
	t.Cleanup(func() {
		sealevel.SysvarCache = previousCache
		global.SetTransactionCount(previousTxCount)
		sealevel.BorrowedAccountArenas = previousArenas
	})
	global.SetTransactionCount(0)
	var ingress *genesisIngressFixture
	if signedIngress {
		ingress = newGenesisIngressFixture(t, g, seed)
	}
	var replayedTransactions []*solana.Transaction
	var parent *sealevel.SlotCtx
	var parentSysvars *sealevel.BankSysvars
	for _, fixture := range oracle.Blocks {
		t.Run(fmt.Sprintf("slot-%d", fixture.Slot), func(t *testing.T) {
			decoder := bin.NewBinDecoder(fixture.Entries)
			count, err := decoder.ReadUint64(bin.LE)
			require.NoError(t, err)
			entries := make([]turbine.Entry, count)
			for i := range entries {
				require.NoError(t, entries[i].UnmarshalWithDecoder(decoder))
			}
			require.Equal(t, 0, decoder.Remaining())
			block := turbine.BlockFromEntries(fixture.Slot, fixture.ParentSlot, entries)
			block.HasAlpenglowFooter = true
			block.FooterProducerTimeNanos = fixture.ProducerTimeNanos
			block.ExpectedBankhash = solana.MustHashFromBase58(fixture.Frozen.BankHash)
			block.HasExpectedBankhash = true
			if ingress != nil {
				block = ingress.receive(t, fixture, entries)
			}
			var genesisParent []*GenesisReplayBootstrap
			if parent == nil {
				parentSysvars, err = seed.ConfigureFirstBlock(block)
				require.NoError(t, err)
				genesisParent = []*GenesisReplayBootstrap{seed}
			} else {
				block.ParentSlot = parent.Slot
				block.ParentBankhash = [32]byte(parent.FinalBankhash)
				block.BlockHeight = fixture.Slot
				block.AcctsLtHash = parent.AcctsLtHash.Clone()
				block.Features = parent.Features.Clone()
				block.LastBlockhash = parent.Blockhash
				block.PrevFeeRateGovernor = parent.FeeRateGovernor
				block.PrevNumSignatures = parent.NumSignatures
				block.EpochStakesPerVoteAcct = parent.VoteAccts
				block.VoteTimestamps = parent.VoteTimestamps
				block.TotalEpochStake = parent.TotalEpochStake
				block.Leader = solana.MustPublicKeyFromBase58(seed.metadata.Leader)
				parentSysvars = parent.BankSysvars()
			}
			block.BlockReward = &b.BlockRewardsInfo{Leader: block.Leader}
			lastEntry := solana.Hash(block.LastBlockhash)
			var ticks uint64
			for _, entry := range entries {
				require.Equal(t, entry.Hash, turbine.NextAlpenglowEntryHash(lastEntry, entry.NumHashes, entry.Txns), "entry hash")
				lastEntry = solana.Hash(entry.Hash)
				if len(entry.Txns) == 0 {
					ticks++
				}
			}
			require.Equal(t, seed.metadata.TicksPerSlot, ticks)
			require.Equal(t, fixture.Frozen.LastBlockhash, lastEntry.String())
			require.Equal(t, len(fixture.Transactions), len(block.Transactions))
			for i, tx := range block.Transactions {
				wire, err := tx.MarshalBinary()
				require.NoError(t, err)
				require.Equal(t, fixture.Transactions[i], wire)
				require.NoError(t, tx.VerifySignatures())
			}
			schedule, ok := parentSysvars.EpochSchedule()
			require.True(t, ok)
			loaded, _, _, _, err := loadBlockAccountsAndUpdateSysvars(tail, block, &schedule, true, parentSysvars, genesisParent...)
			require.NoError(t, err)
			before := make(map[solana.PublicKey]*accounts.Account)
			for key, a := range current {
				before[key] = a.Clone()
			}
			for _, key := range []solana.PublicKey{sealevel.SysvarClockAddr, sealevel.SysvarSlotHashesAddr} {
				a, err := loaded.GetAccount((*[32]byte)(&key))
				require.NoError(t, err)
				before[key] = a
			}
			compareReplayAccounts(t, fixture.Initialized, before)
			if fixture.Slot == 1 {
				// Ordinary snapshot/resume callers cannot claim genesis's absent
				// SlotHashes account merely by supplying parent slot zero.
				_, _, _, _, err := loadBlockAccountsAndUpdateSysvars(tail, block, &schedule, true, parentSysvars)
				require.ErrorContains(t, err, "required SlotHashes")
				bad := *block
				bad.AcctsLtHash = block.AcctsLtHash.Clone()
				bad.ExpectedBankhash[0] ^= 1
				_, err = ProcessBlock(db, &bad, &schedule, 0, nil, new(persistedTracker), tail, statuses, true, parentSysvars, seed)
				require.ErrorContains(t, err, "footer bank hash mismatch")
				require.Zero(t, tail.overlay.HeldSlots(), "rejected bank must not publish accounts")
				require.Nil(t, statuses.tip, "rejected bank must not publish transaction statuses")
			}
			// This invokes the production transaction execution, fees, sysvars,
			// footer validation and AccountsLtHash/bank-hash path.
			result, err := ProcessBlock(db, block, &schedule, workers, nil, new(persistedTracker), tail, statuses, true, parentSysvars, genesisParent...)
			require.NoError(t, err)
			for _, key := range []solana.PublicKey{sealevel.SysvarClockAddr, sealevel.SysvarSlotHashesAddr, sealevel.SysvarRecentBlockHashesAddr, sealevel.SysvarSlotHistoryAddr} {
				current[key] = nil
			}
			for key := range result.ModifiedAccts {
				current[key] = nil
			}
			for key := range current {
				a, err := tail.GetAccount(block.Slot, key)
				require.NoError(t, err)
				current[key] = a.Clone()
			}
			compareReplayAccounts(t, fixture.Frozen, current)
			require.Equal(t, fixture.Frozen.AccountsLtHash, result.AcctsLtHash.Hash())
			require.Equal(t, fixture.Frozen.BankHash, solana.Hash(result.FinalBankhash).String())
			require.Equal(t, fixture.Frozen.SignatureCount, result.NumSignatures)
			require.Equal(t, fixture.Frozen.TransactionCount, global.TransactionCount())
			require.Equal(t, fixture.Frozen.Leader, block.Leader.String())
			require.Equal(t, fixture.Frozen.BlockHeight, block.BlockHeight)
			require.Equal(t, fixture.Frozen.LamportsPerSignature, result.FeeRateGovernor.LamportsPerSignature)
			recent, ok := result.BankSysvars().RecentBlockhashes()
			require.True(t, ok)
			require.Equal(t, len(fixture.Frozen.RecentBlockhashes), len(recent))
			for i, entry := range fixture.Frozen.RecentBlockhashes {
				require.Equal(t, entry.Hash, solana.Hash(recent[i].Blockhash).String())
				require.Equal(t, entry.LamportsPerSignature, recent[i].FeeCalculator.LamportsPerSignature)
			}
			for _, epoch := range fixture.Frozen.EpochStakes {
				if epoch.Epoch != block.Epoch {
					continue
				}
				require.Equal(t, epoch.TotalStake, result.TotalEpochStake)
				for _, vote := range epoch.Votes {
					require.Equal(t, vote.Stake, result.VoteAccts[solana.MustPublicKeyFromBase58(vote.VoteAccount)])
				}
			}
			replayedTransactions = append(replayedTransactions, block.Transactions...)
			t.Logf("slot %d: %d transactions, bank %s", block.Slot, len(block.Transactions), fixture.Frozen.BankHash)
			parent = result
		})
		if t.Failed() {
			break
		}
	}
	if t.Failed() {
		return
	}
	if ingress != nil {
		ingress.verifyOracle(t, g)
	}
	require.Equal(t, uint64(4), parent.Slot)
	// Preserve known-empty genesis coverage when encoding transaction status
	// metadata for a future replay checkpoint or generated snapshot.
	statuses.Root(parent.Slot)
	statusData, err := statuses.SnapshotThrough(parent.Slot)
	require.NoError(t, err)
	restored, err := NewTransactionStatusCacheFromSnapshot(statusData)
	require.NoError(t, err)
	require.True(t, restored.View().CoverageComplete())
	for _, tx := range replayedTransactions {
		found, err := restored.View().ContainsTransaction(tx)
		require.NoError(t, err)
		require.True(t, found)
	}
	// These unrooted fixtures do not manufacture a durable consensus root.
	// Reopening must still find the independently verified initial bank.
	require.NoError(t, db.Shutdown(context.Background()))
	db, metadata, err = genesisinit.Open(t.Context(), root)
	require.NoError(t, err)
	require.Equal(t, initial.Frozen.Metadata, *metadata)
}

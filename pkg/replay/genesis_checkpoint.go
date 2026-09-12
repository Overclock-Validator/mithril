package replay

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/genesis"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/leaderschedule"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
)

const genesisCheckpointPolicy = "trusted-offline-replay"

// GenesisCheckpoint is a versioned, manifest-selected bank boundary. GenesisBank
// retains the immutable runtime profile and epoch stake seeds. Context describes
// the child, never the slot-0 certificate. This is not a finality certificate.
type GenesisCheckpoint struct {
	EpochState      GenesisEpochState                            `json:"epoch_state"`
	Version         uint32                                       `json:"version"`
	Policy          string                                       `json:"policy"`
	GenesisBank     genesis.BankMetadata                         `json:"genesis_bank"`
	Context         state.ResumeContext                          `json:"context"`
	ParentSlot      uint64                                       `json:"parent_slot"`
	ParentBankhash  solana.Hash                                  `json:"parent_bankhash"`
	Fees            sealevel.FeeRateGovernor                     `json:"fees"`
	VoteTimestamps  map[solana.PublicKey]sealevel.BlockTimestamp `json:"vote_timestamps"`
	TickHeight      uint64                                       `json:"tick_height"`
	AccountsCount   uint64                                       `json:"accounts_count"`
	AccountsDataLen uint64                                       `json:"accounts_data_len"`
	AccountsSHA256  string                                       `json:"accounts_sha256"`
}

// GenesisReplay owns the AccountsDB store lock and a speculative account tail. Calls are
// serialized, but execution still uses replay's process-wide runtime: only one
// replay executor may run at a time. Close discards uncheckpointed child banks.
// Epoch transitions reuse the normal replay machinery. The parent must be
// explicitly checkpointed before its account-wide scans, and active reward
// distribution stays speculative, as in ReplayBlocks. Live startup is separate.
type GenesisReplay struct {
	mu                                            sync.Mutex
	db                                            *accountsdb.AccountsDb
	root                                          string
	marker                                        *state.MithrilState
	seed                                          *GenesisReplayBootstrap
	tail                                          *unrootedTail
	statuses                                      *TransactionStatusCache
	tip                                           *sealevel.SlotCtx
	identity                                      ChainTipIdentity
	parentHash                                    solana.Hash
	parentSlot, height, transactionCount, durable uint64
	failed                                        error
	runtime                                       *ReplayCtx
	leaders                                       map[uint64]*leaderschedule.LeaderSchedule
	epochs                                        GenesisEpochState
	partitionedRewards                            *rewards.PartitionedRewardDistributionInfo
}

// OpenGenesisReplay validates the immutable seed, recovers the AccountsDB manifest
// decision, verifies the complete durable account state and restores its bank
// context. It does not contact RPC, start consensus or change process globals.
func OpenGenesisReplay(ctx context.Context, root string) (_ *GenesisReplay, retErr error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	guard, err := accountsdb.AcquireExclusiveAccountsDbStore(root)
	if err != nil {
		return nil, err
	}
	defer func() { retErr = errors.Join(retErr, guard.Close()) }()
	marker, err := state.LoadState(root)
	if err != nil {
		return nil, err
	}
	if !marker.HasGenesisRoot() || !marker.IsReady() {
		return nil, fmt.Errorf("no ready genesis-origin store")
	}
	if err := marker.ValidateGenesisSidecars(root); err != nil {
		return nil, err
	}
	g, _, err := genesis.ReadGenesisFromFile(filepath.Join(root, "genesis.bin"))
	if err != nil {
		return nil, err
	}
	seed, err := NewGenesisReplayBootstrap(ctx, g)
	if err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(filepath.Join(root, state.GenesisBankFileName))
	if err != nil {
		return nil, err
	}
	var initial genesis.BankMetadata
	if err = json.Unmarshal(raw, &initial); err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(initial, seed.metadata) {
		return nil, fmt.Errorf("genesis metadata differs from reconstructed bank")
	}
	// Validate selected metadata/sidecars before recovery can perform orphan GC.
	headers, err := accountsdb.ListFoldManifests(filepath.Join(root, "accounts"))
	if err != nil {
		return nil, err
	}
	if len(headers) > 0 && marker.StateSchemaVersion != state.GenesisReplayStateSchemaVersion {
		return nil, fmt.Errorf("genesis child manifests without replay schema fence")
	}
	for _, h := range headers {
		manifest, err := accountsdb.ReadSegmentManifest(h.Path)
		if err != nil {
			return nil, err
		}
		if _, _, err := decodeGenesisCheckpoint(root, manifest.ResumeCtx, manifest.ThroughSlot, seed); err != nil {
			return nil, err
		}
	}
	db, err := accountsdb.OpenDbWithStoreGuard(root, guard)
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, db.Shutdown(context.Background()))
		}
	}()
	recovery, err := db.RecoverGenesisFoldState()
	if err != nil {
		return nil, err
	}
	db.InitCaches()
	// Epoch helpers stage their writes in the block delta. As in ReplayBlocks,
	// direct StoreAccounts calls must stay non-durable until CommitBatch.
	db.RootedDurable = true
	if err := marker.ValidateAgainstBankhashDB(db); err != nil {
		return nil, err
	}
	s := &GenesisReplay{db: db, root: root, marker: marker, seed: seed, statuses: NewTransactionStatusCache(), durable: recovery.DurableThrough}
	s.tail = newUnrootedTail(db, db, unrootedTailHaltCap, defaultFoldBatchSlots, root)
	s.runtime = genesisReplayContext(seed.metadata)
	s.epochs, err = initialGenesisEpochState(g, seed)
	if err != nil {
		return nil, err
	}
	if recovery.BatchSeq == 0 {
		stats, _, err := scanGenesisBank(ctx, db, s.tail, 0)
		if err != nil {
			return nil, err
		}
		if stats.cap != initial.Capitalization || stats.dataLen != initial.AccountsDataLen || !bytes.Equal(stats.lt.Hash(), initial.AccountsLtHash) {
			return nil, fmt.Errorf("durable genesis account state mismatch")
		}
		bank, err := genesis.ConstructInitialBank(ctx, g)
		if err != nil {
			return nil, err
		}
		if stats.count != uint64(len(bank.Frozen.Accounts)) {
			return nil, fmt.Errorf("genesis account count mismatch")
		}
		for _, entry := range bank.Frozen.Accounts {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			a, err := db.GetAccount(0, entry.Key)
			if err != nil {
				return nil, err
			}
			if a == nil || a.Key != entry.Key || a.Owner != entry.Owner || a.Lamports != entry.Lamports || a.Executable != entry.Executable || a.RentEpoch != entry.RentEpoch || !bytes.Equal(a.Data, entry.Data) {
				return nil, fmt.Errorf("persisted genesis account mismatch: %s", entry.Key)
			}
		}
		return s, nil
	}
	cp, statuses, err := decodeGenesisCheckpoint(root, recovery.ResumeCtx, recovery.DurableThrough, seed)
	if err != nil {
		return nil, err
	}
	if solana.Hash(recovery.RootedBankhash).String() != cp.Context.Bankhash {
		return nil, fmt.Errorf("checkpoint bank hash differs from the durable commit decision")
	}
	stats, sysvars, err := scanGenesisBank(ctx, db, s.tail, cp.Context.Slot)
	if err != nil {
		return nil, err
	}
	if stats.count != cp.AccountsCount || stats.cap != cp.Context.Capitalization || stats.dataLen != cp.AccountsDataLen || stats.digest != cp.AccountsSHA256 || base64.StdEncoding.EncodeToString(stats.lt.Hash()) != cp.Context.AcctsLtHash {
		return nil, fmt.Errorf("checkpoint durable account state mismatch")
	}
	view, err := sealevel.NewBankSysvars(cp.Context.Slot, sysvars...)
	if err != nil {
		return nil, err
	}
	if err := view.ValidateForExecution(); err != nil {
		return nil, err
	}
	recent, _ := view.RecentBlockhashes()
	hashes, _ := view.SlotHashes()
	clock, _ := view.RawView(sealevel.SysvarClockAddr)
	if !reflect.DeepEqual(EncodeRecentBlockhashes(&recent), cp.Context.RecentBlockhashes) || !reflect.DeepEqual(EncodeSlotHashes(&hashes), cp.Context.SlotHashes) || base64.StdEncoding.EncodeToString(clock) != cp.Context.Clock {
		return nil, fmt.Errorf("checkpoint sysvars differ from bank metadata")
	}
	clockValue, _ := view.Clock()
	if clockValue.Slot != cp.Context.Slot || clockValue.Epoch != cp.Context.Epoch {
		return nil, fmt.Errorf("checkpoint clock position mismatch")
	}
	bankhash, err := db.GetBankHashForSlot(cp.Context.Slot)
	if err != nil || !bytes.Equal(bankhash, recovery.RootedBankhash[:]) {
		return nil, fmt.Errorf("checkpoint bankhash database mismatch: %v", err)
	}
	parentHash, err := db.GetBankHashForSlot(cp.ParentSlot)
	if err != nil || !bytes.Equal(parentHash, cp.ParentBankhash[:]) {
		return nil, fmt.Errorf("checkpoint parent bank hash mismatch: %v", err)
	}
	if cp.Version >= 2 {
		s.epochs = cp.EpochState
	}
	stakes, err := s.epochs.stakes(cp.Context.Epoch)
	if err != nil {
		return nil, err
	}
	epochRewards, ok := view.EpochRewards()
	if !ok {
		return nil, fmt.Errorf("checkpoint missing EpochRewards")
	}
	if err := validatePartitionedRewardsResume(cp.Context.Slot+1, &epochRewards); err != nil {
		return nil, err
	}
	s.runtime.Capitalization = cp.Context.Capitalization
	s.tip = &sealevel.SlotCtx{Accounts: accounts.NewMemAccounts(), AccountsDb: db, UnrootedRead: s.tail, Slot: cp.Context.Slot, ParentSlot: cp.ParentSlot, FinalBankhash: bankhash, Blockhash: solana.MustHashFromBase58(cp.Context.Blockhash),
		AcctsLtHash: stats.lt, Features: seed.features.Clone(), NumSignatures: cp.Context.NumSignatures, FeeRateGovernor: &cp.Fees,
		VoteTimestamps: cp.VoteTimestamps, VoteAccts: stakes.Stakes, TotalEpochStake: stakes.TotalStake,
		LatestEvictedBlockhash: solana.MustHashFromBase58(cp.Context.EvictedBlockhash)}
	if err := s.tip.PublishBankSysvars(view); err != nil {
		return nil, err
	}
	s.statuses, s.parentHash, s.parentSlot, s.height, s.transactionCount = statuses, cp.ParentBankhash, cp.ParentSlot, cp.Context.BlockHeight, *cp.Context.TransactionCount
	s.identity = ChainTipIdentity{AlpenglowBlockID: solana.MustHashFromBase58(cp.Context.AlpenglowBlockID), HasAlpenglowBlockID: true,
		AlpenglowChainedMerkleRoot: solana.MustHashFromBase58(cp.Context.AlpenglowChainedMerkleRoot), HasAlpenglowChainedMerkleRoot: true}
	return s, nil
}

func (s *GenesisReplay) NextReplaySlot() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.height + 1
}

// ReplayBlock consumes the independently received block and replaces only its
// parent execution metadata. The signed parent identity and footer must match.
// Returned SlotCtx is for inspection only; callers must not mutate it.
func (s *GenesisReplay) ReplayBlock(ctx context.Context, block *b.Block, workers int) (_ *sealevel.SlotCtx, retErr error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil || s.failed != nil {
		return nil, fmt.Errorf("genesis replay is closed or failed: %v", s.failed)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if block == nil || block.Slot != s.height+1 || block.SourceParentSlot != s.height || !block.TransactionSignaturesVerified() || !block.HasAlpenglowBlockID || !block.HasAlpenglowParentBlockID || !block.HasAlpenglowLastChainedRoot || !block.HasExpectedBankhash || !block.HasAlpenglowFooter {
		return nil, fmt.Errorf("genesis replay requires the next consecutive signed block with a complete footer/identity")
	}
	if solana.Hash(block.AlpenglowParentBlockID) != s.identity.AlpenglowBlockID {
		return nil, fmt.Errorf("genesis replay parent identity mismatch")
	}
	if workers < 1 || workers > len(sealevel.BorrowedAccountArenas) {
		return nil, fmt.Errorf("genesis replay requires initialized account arenas for %d workers", workers)
	}
	if block.AlpenglowShredVersion != turbine.ShredVersionFromGenesisHash(solana.MustHashFromBase58(s.seed.metadata.GenesisHash)) || block.AlpenglowBlockID == ([32]byte{}) || block.AlpenglowLastChainedRoot == ([32]byte{}) {
		return nil, fmt.Errorf("genesis replay shred version or block identity mismatch")
	}
	var parent *sealevel.BankSysvars
	var genesisParent []*GenesisReplayBootstrap
	if s.tip == nil {
		var err error
		parent, err = s.seed.ConfigureFirstBlock(block)
		if err != nil {
			return nil, err
		}
		genesisParent = []*GenesisReplayBootstrap{s.seed}
	} else {
		p := s.tip
		parent = p.BankSysvars()
		block.ParentSlot, block.ParentBankhash, block.BlockHeight = p.Slot, solana.HashFromBytes(p.FinalBankhash), s.height+1
		block.Features, block.AcctsLtHash = p.Features.Clone(), p.AcctsLtHash.Clone()
		block.LastBlockhash, block.PrevNumSignatures = p.Blockhash, p.NumSignatures
		gov := *p.FeeRateGovernor
		block.PrevFeeRateGovernor = &gov
		block.EpochStakesPerVoteAcct, block.TotalEpochStake, block.VoteTimestamps = p.VoteAccts, p.TotalEpochStake, p.VoteTimestamps
	}
	schedule, _ := parent.EpochSchedule()
	block.Epoch = schedule.GetEpoch(block.Slot)
	if block.Epoch != schedule.GetEpoch(s.height) {
		if err := requireDurableEpochBoundaryParent(block.Slot, s.height, s.durable); err != nil {
			return nil, err
		}
	}
	if err := validateGenesisReplayEntries(block); err != nil {
		return nil, err
	}
	// Existing epoch/reward helpers panic on execution failures. Fence this
	// session so partially staged metadata can only be discarded by reopening.
	defer func() {
		if v := recover(); v != nil {
			retErr = fmt.Errorf("genesis epoch/replay failed: %v", v)
			s.failed = retErr
		}
	}()
	block.EpochUpdatedAccts, block.ParentEpochUpdatedAccts = nil, nil
	if err := s.prepareEpoch(ctx, block, &schedule); err != nil {
		s.failed = err
		return nil, err
	}
	block.FromLiveStream = true
	block.BlockReward = &b.BlockRewardsInfo{Leader: block.Leader}
	global.SetTransactionCount(s.transactionCount)
	tip, err := ProcessBlock(s.db, block, &schedule, workers, nil, new(persistedTracker), s.tail, s.statuses, true, parent, genesisParent...)
	if err != nil {
		s.failed = err
		return nil, err
	}
	s.runtime.Capitalization -= tip.LamportsBurnt
	if s.tip == nil {
		// SlotHashes is first created in the child of genesis.
		stats, _, err := scanGenesisBank(ctx, s.db, s.tail, tip.Slot)
		if err != nil {
			s.failed = err
			return nil, err
		}
		s.runtime.Capitalization = stats.cap
	}
	s.tip, s.parentHash, s.parentSlot, s.height = tip, block.ParentBankhash, block.ParentSlot, block.BlockHeight
	s.transactionCount = global.TransactionCount()
	s.identity = ChainTipIdentity{AlpenglowBlockID: solana.Hash(block.AlpenglowBlockID), HasAlpenglowBlockID: true,
		AlpenglowChainedMerkleRoot: solana.Hash(block.AlpenglowLastChainedRoot), HasAlpenglowChainedMerkleRoot: true}
	return tip, nil
}

// Check the native Alpenglow entry chain before treating the bank as complete.
// A signed footer alone does not prove that its transactions extend this parent
// or that the canonical ending tick was supplied.
func validateGenesisReplayEntries(block *b.Block) error {
	if len(block.Entries) == 0 {
		return fmt.Errorf("genesis replay has no ending tick")
	}
	last := solana.Hash(block.LastBlockhash)
	index := uint64(0)
	for i, entry := range block.Entries {
		if entry == nil || entry.NumHashes != 1 || len(entry.Hash) != 32 || (len(entry.Indices) == 0) != (i == len(block.Entries)-1) {
			return fmt.Errorf("genesis replay requires native entries and one final Alpenglow tick")
		}
		txs := make([]solana.Transaction, 0, len(entry.Indices))
		for _, at := range entry.Indices {
			if at != index || at >= uint64(len(block.Transactions)) || block.Transactions[at] == nil {
				return fmt.Errorf("genesis replay entry transaction coverage mismatch")
			}
			txs = append(txs, *block.Transactions[at])
			index++
		}
		last = turbine.NextAlpenglowEntryHash(last, entry.NumHashes, txs)
		if !bytes.Equal(last[:], entry.Hash) {
			return fmt.Errorf("genesis replay entry hash does not extend the parent")
		}
	}
	if index != uint64(len(block.Transactions)) || last != solana.Hash(block.Blockhash) {
		return fmt.Errorf("genesis replay ending blockhash or transaction coverage mismatch")
	}
	return nil
}

// CheckpointTrusted atomically persists the completed offline replay tip. Calling
// this explicitly trusts this local branch; it does not certify it for a cluster.
// Cancellation is checked before AccountsDB's commit decision. Once CommitBatch starts,
// it completes/reports its decision without cancelling fsync halfway through.
func (s *GenesisReplay) CheckpointTrusted(ctx context.Context) error { return s.checkpoint(ctx, nil) }

func (s *GenesisReplay) checkpoint(ctx context.Context, hook func(string) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil || s.failed != nil {
		return fmt.Errorf("genesis replay is closed or failed: %v", s.failed)
	}
	if s.tip == nil || s.tip.Slot == s.durable {
		return fmt.Errorf("no new completed child bank to checkpoint")
	}
	if s.partitionedRewards != nil && s.partitionedRewards.NumRewardPartitionsRemaining > 0 {
		return fmt.Errorf("checkpoint held until epoch reward distribution completes; restart will replay the boundary from slot %d", s.durable+1)
	}
	check := func(stage string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if hook != nil {
			return hook(stage)
		}
		return nil
	}
	if err := check("before-capture"); err != nil {
		return err
	}
	cp, err := s.capture(ctx)
	if err != nil {
		return err
	}
	payload, err := s.statuses.SnapshotThrough(s.tip.Slot)
	if err != nil {
		return err
	}
	cp.Context.TransactionStatusCheckpoint, err = PrepareTransactionStatusCheckpoint(s.root, s.tip.Slot, payload)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	if _, _, err := decodeGenesisCheckpoint(s.root, raw, s.tip.Slot, s.seed); err != nil {
		return err
	}
	if err := check("sidecar"); err != nil {
		return err
	}
	// Fence older binaries before any child account/index mutation. The marker
	// remains an immutable genesis anchor, even if the first fold is interrupted.
	if s.marker.StateSchemaVersion != state.GenesisReplayStateSchemaVersion {
		s.marker.StateSchemaVersion = state.GenesisReplayStateSchemaVersion
		if err := s.marker.Save(s.root); err != nil {
			s.failed = err
			return err
		}
	}
	if err := check("before-commit"); err != nil {
		return err
	}
	deltas := s.tail.overlay.PromotionPrefix(s.tip.Slot)
	if len(deltas) == 0 || deltas[0].Slot != s.durable+1 || deltas[len(deltas)-1].Slot != s.tip.Slot {
		return fmt.Errorf("incomplete checkpoint account lineage")
	}
	if _, err := global.FlushPendingStakePubkeysThrough(s.root, s.tip.Slot); err != nil {
		s.failed = err // the legacy appender may have consumed pending entries
		return err
	}
	if _, err := s.db.CommitBatch(deltas, s.tip.Slot, s.tail.bankhashes, raw); err != nil {
		s.failed = err
		return err
	}
	if hook != nil {
		if err := hook("committed"); err != nil {
			s.failed = err
			return err
		}
	}
	s.runtime.Capitalization = cp.Context.Capitalization
	s.durable = s.tip.Slot
	s.statuses.Root(s.durable)
	s.tail = newUnrootedTail(s.db, s.db, unrootedTailHaltCap, defaultFoldBatchSlots, s.root)
	return nil
}

func (s *GenesisReplay) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return nil
	}
	global.DropPendingStakePubkeysFrom(s.durable + 1)
	err := s.db.Shutdown(context.Background())
	s.db = nil
	return err
}

func (s *GenesisReplay) capture(ctx context.Context) (*GenesisCheckpoint, error) {
	p := s.tip
	stats, _, err := scanGenesisBank(ctx, s.db, s.tail, p.Slot)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(stats.lt.Hash(), p.AcctsLtHash.Hash()) {
		return nil, fmt.Errorf("checkpoint account state differs from frozen bank LtHash")
	}
	view := p.BankSysvars()
	if err := view.ValidateForExecution(); err != nil {
		return nil, err
	}
	recent, _ := view.RecentBlockhashes()
	hashes, _ := view.SlotHashes()
	clock, _ := view.RawView(sealevel.SysvarClockAddr)
	count := s.transactionCount
	cp := &GenesisCheckpoint{Version: 2, EpochState: s.epochs, Policy: genesisCheckpointPolicy, GenesisBank: s.seed.metadata,
		ParentSlot: s.parentSlot, ParentBankhash: s.parentHash, Fees: *p.FeeRateGovernor,
		TickHeight:    (p.Slot + 1) * s.seed.metadata.TicksPerSlot,
		AccountsCount: stats.count, AccountsDataLen: stats.dataLen, AccountsSHA256: stats.digest,
		VoteTimestamps: make(map[solana.PublicKey]sealevel.BlockTimestamp)}
	for key, value := range p.VoteTimestamps {
		cp.VoteTimestamps[key] = value
	}
	m := s.seed.metadata
	cp.Context = state.ResumeContext{Slot: p.Slot, Epoch: s.seedEpoch(p.Slot), Bankhash: solana.HashFromBytes(p.FinalBankhash).String(), BlockHeight: s.height,
		AlpenglowBlockID: s.identity.AlpenglowBlockID.String(), AlpenglowChainedMerkleRoot: s.identity.AlpenglowChainedMerkleRoot.String(),
		AcctsLtHash: base64.StdEncoding.EncodeToString(p.AcctsLtHash.Hash()), NumSignatures: p.NumSignatures,
		Blockhash: solana.Hash(p.Blockhash).String(), EvictedBlockhash: solana.Hash(p.LatestEvictedBlockhash).String(),
		LamportsPerSignature: p.FeeRateGovernor.LamportsPerSignature, PrevLamportsPerSig: p.FeeRateGovernor.PrevLamportsPerSignature,
		RecentBlockhashes: EncodeRecentBlockhashes(&recent), SlotHashes: EncodeSlotHashes(&hashes), Clock: base64.StdEncoding.EncodeToString(clock),
		Capitalization: stats.cap, TransactionCount: &count, SlotsPerYear: m.SlotsPerYear,
		InflationInitial: m.Inflation.Initial, InflationTerminal: m.Inflation.Terminal, InflationTaper: m.Inflation.Taper,
		InflationFoundation: m.Inflation.Foundation, InflationFoundationTerm: m.Inflation.FoundationTerm}
	return cp, nil
}

func decodeGenesisCheckpoint(root string, raw []byte, slot uint64, seed *GenesisReplayBootstrap) (*GenesisCheckpoint, *TransactionStatusCache, error) {
	var cp GenesisCheckpoint
	if err := json.Unmarshal(raw, &cp); err != nil {
		return nil, nil, err
	}
	r := &cp.Context
	if (cp.Version != 1 && cp.Version != 2) || cp.Policy != genesisCheckpointPolicy || !reflect.DeepEqual(cp.GenesisBank, seed.metadata) || slot == 0 || slot >= math.MaxUint64/seed.metadata.TicksPerSlot-1 || r.Slot != slot || cp.ParentSlot != slot-1 || r.BlockHeight != slot || r.TransactionCount == nil || cp.TickHeight != (slot+1)*seed.metadata.TicksPerSlot {
		return nil, nil, fmt.Errorf("unsupported or inconsistent genesis replay checkpoint")
	}
	schedule, _ := seed.sysvars.EpochSchedule()
	if r.Epoch != schedule.GetEpoch(slot) || (cp.Version == 1 && r.Epoch != 0) {
		return nil, nil, fmt.Errorf("genesis checkpoint epoch mismatch")
	}
	if cp.Version >= 2 {
		if err := cp.EpochState.validate(r.Epoch, schedule.LeaderScheduleEpoch(slot)); err != nil {
			return nil, nil, err
		}
	}
	for _, hash := range []string{r.Bankhash, r.Blockhash, r.AlpenglowBlockID, r.AlpenglowChainedMerkleRoot} {
		v, err := solana.HashFromBase58(hash)
		if err != nil || v.IsZero() {
			return nil, nil, fmt.Errorf("invalid checkpoint hash %q", hash)
		}
	}
	if _, err := solana.HashFromBase58(r.EvictedBlockhash); err != nil {
		return nil, nil, fmt.Errorf("invalid evicted blockhash: %w", err)
	}
	lt, err := base64.StdEncoding.DecodeString(r.AcctsLtHash)
	if err != nil || len(lt) != 2048 {
		return nil, nil, fmt.Errorf("invalid checkpoint AccountsLtHash")
	}
	digest, err := hex.DecodeString(cp.AccountsSHA256)
	if err != nil || len(digest) != 32 {
		return nil, nil, fmt.Errorf("invalid checkpoint account digest")
	}
	// Pinned profile uses AccountsLtHash and has removed the old delta hash.
	h := sha256.New()
	h.Write(cp.ParentBankhash[:])
	_ = binary.Write(h, binary.LittleEndian, r.NumSignatures)
	last := solana.MustHashFromBase58(r.Blockhash)
	h.Write(last[:])
	outer := sha256.New()
	outer.Write(h.Sum(nil))
	outer.Write(lt)
	if solana.HashFromBytes(outer.Sum(nil)).String() != r.Bankhash {
		return nil, nil, fmt.Errorf("checkpoint frozen bank hash mismatch")
	}
	if cp.ParentBankhash.IsZero() || cp.Fees.LamportsPerSignature != r.LamportsPerSignature || cp.Fees.PrevLamportsPerSignature != r.PrevLamportsPerSig {
		return nil, nil, fmt.Errorf("checkpoint parent/fee metadata mismatch")
	}
	m := seed.metadata
	if cp.Fees.TargetLamportsPerSignature != m.Fees.TargetLamportsPerSig || cp.Fees.TargetSignaturesPerSlot != m.Fees.TargetSigsPerSlot || cp.Fees.BurnPercent != m.Fees.BurnPercent || cp.VoteTimestamps == nil || r.SlotsPerYear != m.SlotsPerYear || r.InflationInitial != m.Inflation.Initial || r.InflationTerminal != m.Inflation.Terminal || r.InflationTaper != m.Inflation.Taper || r.InflationFoundation != m.Inflation.Foundation || r.InflationFoundationTerm != m.Inflation.FoundationTerm {
		return nil, nil, fmt.Errorf("checkpoint runtime profile mismatch")
	}
	if err := ValidateTransactionStatusCheckpointRef(r.TransactionStatusCheckpoint, slot); err != nil {
		return nil, nil, err
	}
	payload, err := ReadTransactionStatusCheckpoint(root, r.TransactionStatusCheckpoint)
	if err != nil {
		return nil, nil, err
	}
	statuses, err := NewTransactionStatusCacheFromSnapshot(payload)
	if err != nil {
		return nil, nil, err
	}
	if err := validateRestoredTransactionStatusCache(statuses, slot, solana.MustHashFromBase58(r.AlpenglowBlockID), true); err != nil {
		return nil, nil, err
	}
	return &cp, statuses, nil
}

type genesisAccountSummary struct {
	count, cap, dataLen uint64
	digest              string
	lt                  *lthash.LtHash
}

// Merge the account-index streaming enumeration with only the bounded unrooted write set.
// Hash every live account field, including rent epoch (not covered by LtHash).
// No second account index or complete in-memory bank copy is required.
func scanGenesisBank(ctx context.Context, db *accountsdb.AccountsDb, tail *unrootedTail, slot uint64) (genesisAccountSummary, []*accounts.Account, error) {
	out := genesisAccountSummary{lt: new(lthash.LtHash)}
	pending := make(map[solana.PublicKey]*accounts.Account)
	for _, delta := range tail.overlay.PromotionPrefix(slot) {
		for _, a := range delta.Delta {
			pending[a.Key] = a
		}
	}
	keys := make([]solana.PublicKey, 0, len(pending))
	for key := range pending {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i][:], keys[j][:]) < 0 })
	h := sha256.New()
	var sysvars []*accounts.Account
	visit := func(key solana.PublicKey) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		a, ok := pending[key]
		if !ok {
			var err error
			a, err = db.GetAccount(slot, key)
			if err != nil {
				return err
			}
		}
		if a == nil || a.Key != key {
			return fmt.Errorf("invalid account during checkpoint scan: %s", key)
		}
		if a.Lamports == 0 {
			return nil
		}
		if math.MaxUint64-out.cap < a.Lamports || math.MaxUint64-out.dataLen < uint64(len(a.Data)) {
			return fmt.Errorf("checkpoint account totals overflow")
		}
		out.count++
		out.cap += a.Lamports
		out.dataLen += uint64(len(a.Data))
		out.lt.MixIn(new(lthash.LtHash).InitWithAcct(a))
		h.Write(key[:])
		h.Write(a.Owner[:])
		_ = binary.Write(h, binary.LittleEndian, a.Lamports)
		_ = binary.Write(h, binary.LittleEndian, a.RentEpoch)
		_ = binary.Write(h, binary.LittleEndian, a.Executable)
		_ = binary.Write(h, binary.LittleEndian, uint64(len(a.Data)))
		h.Write(a.Data)
		if sealevel.IsBankSysvarAccount(key) {
			sysvars = append(sysvars, a.Clone())
		}
		return nil
	}
	i := 0
	err := db.ScanKeysBetweenPrefixes(ctx, 0, math.MaxUint64, func(key solana.PublicKey) error {
		for i < len(keys) && bytes.Compare(keys[i][:], key[:]) < 0 {
			if err := visit(keys[i]); err != nil {
				return err
			}
			i++
		}
		if i < len(keys) && keys[i] == key {
			i++
		}
		return visit(key)
	})
	if err != nil {
		return out, nil, err
	}
	for ; i < len(keys); i++ {
		if err := visit(keys[i]); err != nil {
			return out, nil, err
		}
	}
	out.digest = hex.EncodeToString(h.Sum(nil))
	return out, sysvars, nil
}

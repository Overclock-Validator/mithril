package block

import (
	"fmt"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/txstatus"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
)

// TurbineIngressTimings carries per-slot observations only on a
// trusted in-memory Turbine block. Durations never serialize with Block.
type TurbineIngressTimings struct {
	ShredCollection      time.Duration
	CompletionQueueDelay time.Duration
	BlockDecode          time.Duration
	// Completion-only parse and outstanding-signature join/verification time.
	TransactionParse     time.Duration
	TransactionSigverify time.Duration
	ReplayAdmission      time.Duration
	// Early durations sum completed prefetched component work, including an
	// optimistic prefix later discarded, and overlap reception and each other.
	// EarlyTransactionSigverify includes queueing through future completion;
	// neither early duration is CPU time or an additive pipeline wall stage.
	EarlyTransactionParse     time.Duration
	EarlyTransactionSigverify time.Duration
	// Completion wait for already-claimed background parsing/submission.
	// Recorded separately from BlockDecode's active completion work.
	EarlyPreparationWait time.Duration
	// Only retained transactions whose verification finished by ShredFullNanos.
	EarlyVerifiedTransactions uint64
	// FullToReady is wall time from full shred assembly to replay-ready completion.
	// It contains completion queueing, decode and outstanding verification waits.
	FullToReady time.Duration
}

var transactionDerivedStateInitMu sync.Mutex

// transactionDerivedState is shared by shallow in-memory Block copies and is
// never serialized. Its mutex makes repeated preparation safe when status-cache
// validation of the same immutable block overlaps.
type transactionDerivedState struct {
	mu                 sync.Mutex
	signaturesVerified bool
	messageIdentities  *PreparedTransactionMessageIdentities
}

// PreparedTransactionMessageIdentities is an opaque, immutable set of message
// identities bound to one ordered transaction slice.
type PreparedTransactionMessageIdentities struct {
	transactions []*solana.Transaction
	versions     []solana.MessageVersion
	identities   []txstatus.TransactionMessageIdentity
}

type Block struct {
	Slot                                uint64
	ParentSlot                          uint64
	SourceParentSlot                    uint64 // Ingress parent slot from the block source; replay may later rewrite ParentSlot.
	BlockHeight                         uint64
	Epoch                               uint64
	Transactions                        []*solana.Transaction
	Versions                            []uint8
	Entries                             []*TxEntry
	BankHash                            [32]byte
	EahWorkaroundBankhash               []byte
	HasEahWorkaround                    bool
	ParentBankhash                      [32]byte
	AcctsLtHash                         *lthash.LtHash
	NumSignatures                       uint64 // signatures carried by this block; replay rejects duplicate transaction messages
	PrevNumSignatures                   uint64 // signatures processed in the parent bank (fee-governor input)
	InitialPreviousLamportsPerSignature uint64
	Blockhash                           [32]byte
	AlpenglowBlockID                    [32]byte // Turbine Merkle-root block id used by Alpenglow/Votor.
	HasAlpenglowBlockID                 bool
	AlpenglowParentBlockID              [32]byte // parent's Merkle-root block id (header/update-parent marker)
	HasAlpenglowParentBlockID           bool
	AlpenglowLastChainedRoot            [32]byte // Last data-shred Merkle root, chained into child slots.
	HasAlpenglowLastChainedRoot         bool
	AlpenglowFinalCert                  []byte // raw footer final_cert bytes (finalization cert for an earlier slot), decoded in replay
	ExpectedBankhash                    [32]byte
	HasExpectedBankhash                 bool
	TxMetas                             []*rpc.TransactionMeta
	Leader                              solana.PublicKey
	BlockReward                         *BlockRewardsInfo
	LastBlockhash                       [32]byte
	UnixTimestamp                       int64
	EpochStakesPerVoteAcct              map[solana.PublicKey]uint64
	VoteTimestamps                      map[solana.PublicKey]sealevel.BlockTimestamp
	TotalEpochStake                     uint64
	Features                            *features.Features
	UpdatedAccts                        []solana.PublicKey
	ParentEpochUpdatedAccts             []*accounts.Account
	EpochUpdatedAccts                   []*accounts.Account
	Rewards                             []rpc.BlockReward
	NumRewardPartitions                 uint64
	LatestEvictedBlockhash              [32]byte
	PrevFeeRateGovernor                 *sealevel.FeeRateGovernor
	FeeRateGovernor                     *sealevel.FeeRateGovernor
	FromLiveStream                      bool
	FromLocalProduction                 bool
	IsSkipped                           bool // True for slots that were skipped by the leader
	// transactionDerivedState contains only trusted, nonserialized data derived
	// from this exact in-memory block (and is shared by its shallow copies).
	transactionDerivedState *transactionDerivedState
	// turbineReplayAdmissionStart is deliberately private and monotonic. It
	// exists only on the exact in-memory block handed from Turbine to replay,
	// so serialized/RPC blocks cannot inject or preserve an admission clock.
	turbineReplayAdmissionStart time.Time
	turbineIngressTimings       TurbineIngressTimings
	hasTurbineIngressTimings    bool
	SkipRewardCert              []byte
	NotarRewardCert             []byte
	BlockFinalCert              []byte
	FooterProducerTimeNanos     uint64
	HasAlpenglowFooter          bool
	AlpenglowShredVersion       uint16

	// Shred-path observability (zero when the block did not come from shreds —
	// RPC/file blocks must not fabricate these). "Full" follows Agave's
	// SlotMeta/is_full language: all data shreds present, block reconstructable
	// — NOT finalized/consensus-safe.
	ShredFirstNanos int64 // wall clock (unix nanos) of the first accepted shred for the slot
	ShredFullNanos  int64 // wall clock (unix nanos) when the slot became full
	RepairedShreds  int   // data shreds obtained via repair rather than turbine
}

// MarkTransactionSignaturesVerified records that every transaction signature
// in this exact in-memory block has already been verified. Callers must set it
// only after successful verification and must not subsequently mutate signed
// transaction data. Signature-preserving transformations such as resolving
// address-table lookups remain safe.
func (b *Block) MarkTransactionSignaturesVerified() {
	if b == nil {
		return
	}
	state := b.transactionState()
	state.mu.Lock()
	state.signaturesVerified = true
	state.mu.Unlock()
}

// TransactionSignaturesVerified reports whether this exact in-memory block
// crossed a trusted transaction-signature verification boundary. The marker is
// intentionally not serialized; replay re-verifies after any serialization.
func (b *Block) TransactionSignaturesVerified() bool {
	if b == nil {
		return false
	}
	state := b.transactionState()
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.signaturesVerified
}

// PrepareTransactionMessageIdentities serializes and hashes every transaction
// message at most once for an immutable in-memory block. The returned opaque
// handle exposes identities only by value, so callers cannot mutate the cache.
//
// As with MarkTransactionSignaturesVerified, callers must not mutate signed
// message contents after preparation. Address-table resolution is safe because
// it does not change the canonical serialized message.
func (b *Block) PrepareTransactionMessageIdentities() (*PreparedTransactionMessageIdentities, error) {
	if b == nil {
		return nil, fmt.Errorf("nil block")
	}
	state := b.transactionState()
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.messageIdentities.matches(b.Transactions) {
		return state.messageIdentities, nil
	}
	if state.messageIdentities != nil {
		state.messageIdentities = nil
		state.signaturesVerified = false
	}

	prepared := &PreparedTransactionMessageIdentities{
		transactions: append([]*solana.Transaction(nil), b.Transactions...),
		versions:     make([]solana.MessageVersion, len(b.Transactions)),
		identities:   make([]txstatus.TransactionMessageIdentity, len(b.Transactions)),
	}
	for index, tx := range b.Transactions {
		if tx == nil {
			state.signaturesVerified = false
			return nil, fmt.Errorf("transaction %d is nil", index)
		}
		identity, err := txstatus.IdentityForTransaction(tx)
		if err != nil {
			state.signaturesVerified = false
			return nil, fmt.Errorf("transaction %d message identity: %w", index, err)
		}
		prepared.versions[index] = tx.Message.GetVersion()
		prepared.identities[index] = identity
	}
	state.messageIdentities = prepared
	return prepared, nil
}

func (cache *PreparedTransactionMessageIdentities) matches(transactions []*solana.Transaction) bool {
	if cache == nil || len(cache.transactions) != len(transactions) ||
		len(cache.versions) != len(transactions) || len(cache.identities) != len(transactions) {
		return false
	}
	for index, tx := range transactions {
		if tx == nil || cache.transactions[index] != tx ||
			cache.versions[index] != tx.Message.GetVersion() ||
			cache.identities[index].RecentBlockhash != tx.Message.RecentBlockhash {
			return false
		}
	}
	return true
}

func (b *Block) invalidateTransactionDerivedState() {
	state := b.transactionState()
	state.mu.Lock()
	state.messageIdentities = nil
	state.signaturesVerified = false
	state.mu.Unlock()
}

// MarkTurbineReplayAdmissionStart starts the interval from successful
// assembler validation to replay taking ownership of this exact block.
func (b *Block) MarkTurbineReplayAdmissionStart(at time.Time) {
	if b != nil {
		b.turbineReplayAdmissionStart = at
	}
}

// TurbineReplayAdmissionStart returns the in-process admission clock. The
// bool is false for RPC/file/local-production blocks and after serialization.
func (b *Block) TurbineReplayAdmissionStart() (time.Time, bool) {
	if b == nil || b.turbineReplayAdmissionStart.IsZero() {
		return time.Time{}, false
	}
	return b.turbineReplayAdmissionStart, true
}

// MarkTurbineIngressTimings attaches phase durations to the trusted in-memory
// block. It does not start or end the replay-admission interval.
func (b *Block) MarkTurbineIngressTimings(timings TurbineIngressTimings) {
	if b != nil {
		b.turbineIngressTimings = timings
		b.hasTurbineIngressTimings = true
	}
}

// TurbineIngressTimings reports trusted per-slot phase durations. Serialized,
// RPC, file, and local-production blocks return false.
func (b *Block) TurbineIngressTimings() (TurbineIngressTimings, bool) {
	if b == nil || !b.hasTurbineIngressTimings {
		return TurbineIngressTimings{}, false
	}
	return b.turbineIngressTimings, true
}

// CompleteTurbineReplayAdmission ends the admission interval once. Using the
// stored time.Time preserves its monotonic clock across the in-process queues.
func (b *Block) CompleteTurbineReplayAdmission(at time.Time) (TurbineIngressTimings, bool) {
	if b == nil || b.turbineReplayAdmissionStart.IsZero() || !b.hasTurbineIngressTimings {
		return TurbineIngressTimings{}, false
	}
	duration := at.Sub(b.turbineReplayAdmissionStart)
	if duration < 0 {
		return TurbineIngressTimings{}, false
	}
	b.turbineIngressTimings.ReplayAdmission = duration
	b.turbineReplayAdmissionStart = time.Time{}
	return b.turbineIngressTimings, true
}
func (b *Block) FixupTxVersions() error {
	if b == nil || len(b.Versions) == 0 {
		return nil
	}
	if len(b.Versions) != len(b.Transactions) {
		return fmt.Errorf("restore transaction versions: have %d versions for %d transactions", len(b.Versions), len(b.Transactions))
	}

	// Validate into copies first so malformed persisted metadata cannot leave a
	// partially updated block or invalidate otherwise reusable derived state.
	messages := make([]solana.Message, len(b.Transactions))
	for idx, tx := range b.Transactions {
		if tx == nil {
			return fmt.Errorf("restore transaction version %d: nil transaction", idx)
		}
		messages[idx] = tx.Message
		version := solana.MessageVersion(b.Versions[idx])
		if _, err := messages[idx].SetVersion(version); err != nil {
			return fmt.Errorf("restore transaction version %d as message version %d: %w", idx, version, err)
		}
	}

	b.invalidateTransactionDerivedState()
	for idx := range b.Transactions {
		b.Transactions[idx].Message = messages[idx]
	}
	return nil
}

type TxEntry struct {
	NumHashes uint64
	Hash      []byte
	Indices   []uint64
}

type BlockRewardsInfo struct {
	Leader      solana.PublicKey
	Lamports    uint64
	PostBalance uint64
}

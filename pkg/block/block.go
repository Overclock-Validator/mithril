package block

import (
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
)

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
	NumSignatures                       uint64
	PrevNumSignatures                   uint64
	InitialPreviousLamportsPerSignature uint64
	Blockhash                           [32]byte
	AlpenglowBlockID                    [32]byte // Turbine Merkle-root block id used by Alpenglow/Votor.
	HasAlpenglowBlockID                 bool
	AlpenglowParentBlockID              [32]byte // parent's Merkle-root block id (header/update-parent marker)
	HasAlpenglowParentBlockID           bool
	AlpenglowFinalCert                  []byte // raw footer final_cert bytes (finalization cert for an earlier slot), decoded in replay
	ExpectedBankhash                    [32]byte
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
	FromLightbringer                    bool
	IsSkipped                           bool // True for slots that were skipped by the leader

	// Shred-path observability (zero when the block did not come from shreds —
	// RPC/file blocks must not fabricate these). "Full" follows Agave's
	// SlotMeta/is_full language: all data shreds present, block reconstructable
	// — NOT finalized/consensus-safe.
	ShredFirstNanos int64 // wall clock (unix nanos) of the first accepted shred for the slot
	ShredFullNanos  int64 // wall clock (unix nanos) when the slot became full
	RepairedShreds  int   // data shreds obtained via repair rather than turbine
}

func (b *Block) FixupTxVersions() {
	if len(b.Versions) == 0 {
		return
	}
	for idx, tx := range b.Transactions {
		tx.Message.SetVersion(solana.MessageVersion(b.Versions[idx]))
	}
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

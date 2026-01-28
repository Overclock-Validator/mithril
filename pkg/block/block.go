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
	BlockHeight                         uint64
	Epoch                               uint64
	Transactions                        []*solana.Transaction
	Versions                            []uint8
	Entries                             []*TxEntry
	BankHash                            [32]byte
	EpochAcctsHash                      []byte
	EahWorkaroundBankhash               []byte
	HasEahWorkaround                    bool
	ParentBankhash                      [32]byte
	AcctsLtHash                         *lthash.LtHash
	NumSignatures                       uint64
	PrevNumSignatures                   uint64
	InitialPreviousLamportsPerSignature uint64
	Blockhash                           [32]byte
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

func (b *Block) UniqueAccounts() map[solana.PublicKey]struct{} {
	numPubkeys := 0
	for _, tx := range b.Transactions {
		numPubkeys += len(tx.Message.AccountKeys)
	}

	numPubkeys += len(b.UpdatedAccts)

	pubkeyMap := make(map[solana.PublicKey]struct{}, numPubkeys)

	for _, tx := range b.Transactions {
		for _, pk := range tx.Message.AccountKeys {
			pubkeyMap[pk] = struct{}{}
		}
	}

	for _, pk := range b.UpdatedAccts {
		pubkeyMap[pk] = struct{}{}
	}
	return pubkeyMap
}

package replay

import (
	"errors"
	"fmt"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// blockMetadata is an executed candidate, not a finality or durability claim.
type blockMetadata struct {
	Slot              uint64
	ParentSlot        uint64
	Blockhash         [32]byte
	PreviousBlockhash [32]byte
	ParentBankhash    [32]byte
	Bankhash          [32]byte
	Transactions      []*transactionMetadata
	// Watched is set when a watch set selected Transactions: they are then a
	// subset of the block, in block order.
	Watched bool
}

// blockMetadataCapture collects one candidate's bounded execution metadata.
// The replay goroutine owns it; workers write only their indexed records.
type blockMetadataCapture struct {
	Record  *blockMetadata
	Err     error
	records []*transactionMetadata
	budget  metadataBudget
	// watch selects the transactions to capture; nil captures all. seen and
	// expected count the block's transactions and the selected ones.
	watch          *TransactionWatchSet
	seen, expected int
}

type transactionMetadataGroup struct {
	captures []transactionMetadataCapture
	tokens   *transactionTokenGroup
	// selected marks the captured transactions; nil means all.
	selected []bool
}

func (capture *blockMetadataCapture) beginGroup(ctx *sealevel.SlotCtx, txs []*solana.Transaction, workers int) *transactionMetadataGroup {
	if capture == nil || capture.Err != nil {
		return nil
	}
	if workers > metadataWorkerLimit {
		capture.fail(errMetadataBudget)
		return nil
	}
	// Unselected transactions cost nothing below: no charge, no token
	// hydration, and no capture slot handed to execution.
	selected, captured := capture.watch.selection(txs)
	if err := metadataGroupCharge(captured, len(capture.records)); err != nil {
		capture.fail(err)
		return nil
	}
	tokens, err := prepareTransactionTokenGroup(ctx, captured)
	if err != nil {
		capture.fail(err)
		return nil
	}
	capture.seen += len(txs)
	capture.expected += len(captured)
	group := &transactionMetadataGroup{captures: make([]transactionMetadataCapture, len(txs)), tokens: tokens, selected: selected}
	for i := range group.captures {
		group.captures[i].budget = &capture.budget
	}
	return group
}

func (group *transactionMetadataGroup) transaction(index int) *transactionMetadataCapture {
	if group == nil || (group.selected != nil && !group.selected[index]) {
		return nil
	}
	return &group.captures[index]
}

func (capture *blockMetadataCapture) finishGroup(group *transactionMetadataGroup) {
	if capture == nil || capture.Err != nil || group == nil {
		return
	}
	for i := range group.captures {
		if group.selected != nil && !group.selected[i] {
			continue
		}
		result := &group.captures[i]
		if result.Err != nil {
			capture.fail(fmt.Errorf("transaction %d metadata: %w", len(capture.records), result.Err))
			return
		}
		if result.Record == nil {
			capture.fail(fmt.Errorf("transaction %d metadata is missing", len(capture.records)))
			return
		}
		record := result.Record
		pre, post, err := group.tokens.resolve(record.preTokens, record.postTokens)
		if err != nil {
			capture.fail(fmt.Errorf("transaction %d token balances: %w", len(capture.records), err))
			return
		}
		record.PreTokenBalances, record.PostTokenBalances = pre, post
		record.preTokens, record.postTokens = transactionTokenSnapshot{}, transactionTokenSnapshot{}
		capture.records = append(capture.records, record)
	}
}

func (capture *blockMetadataCapture) seal(block *b.Block, ctx *sealevel.SlotCtx) {
	if capture == nil || capture.Err != nil {
		return
	}
	if capture.seen != len(block.Transactions) || len(capture.records) != capture.expected || len(ctx.FinalBankhash) != 32 {
		capture.fail(errors.New("block metadata is incomplete"))
		return
	}
	var bankhash [32]byte
	copy(bankhash[:], ctx.FinalBankhash)
	capture.Record = &blockMetadata{
		Slot: block.Slot, ParentSlot: block.ParentSlot, Blockhash: block.Blockhash,
		PreviousBlockhash: block.LastBlockhash,
		ParentBankhash:    block.ParentBankhash, Bankhash: bankhash, Transactions: capture.records,
		Watched: capture.watch != nil,
	}
	capture.records = nil
}

func (capture *blockMetadataCapture) fail(err error) {
	if capture == nil {
		return
	}
	capture.Record, capture.records = nil, nil
	if capture.Err == nil {
		capture.Err = err
	}
}

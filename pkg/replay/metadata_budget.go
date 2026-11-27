package replay

import (
	"errors"
	"sync/atomic"

	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// These limits govern optional metadata availability, not valid block sizes.
// Charges include conservative slice/map overhead, not allocator or process RSS.
const (
	metadataRecordLimit      = 1 << 20
	metadataBlockLimit       = 64 << 20
	metadataGroupLimit       = 32 << 20
	metadataTransactionLimit = 16_384
	metadataGroupKeyLimit    = 131_072
	metadataWorkerLimit      = 16
	metadataAccountDataLimit = 64 << 10
	metadataHydrationLimit   = 16 << 20
	metadataRecordBaseCharge = 8192
	metadataRecordKeyCharge  = 1024
	metadataErrorTextLimit   = 256
)

var errMetadataBudget = errors.New("transaction metadata capture budget exceeded")

// Reservations are monotonic for the block, including failed captures. Workers
// only reserve bytes; the replay owner discards the candidate after joining them.
type metadataBudget struct {
	used atomic.Uint64
}

func (budget *metadataBudget) reserve(bytes uint64) bool {
	if budget == nil {
		return true
	}
	for {
		used := budget.used.Load()
		if used > metadataBlockLimit || bytes > metadataBlockLimit-used {
			return false
		}
		if budget.used.CompareAndSwap(used, used+bytes) {
			return true
		}
	}
}

func (metadata *transactionMetadata) reserve(budget *metadataBudget, bytes uint64) error {
	if metadata.charge > metadataRecordLimit || bytes > metadataRecordLimit-metadata.charge || !budget.reserve(bytes) {
		return errMetadataBudget
	}
	metadata.charge += bytes
	return nil
}

// Check before allocating the group's capture array or hydrating token maps.
// Two mint-state entries and one reference entry per declared key are possible.
func metadataGroupCharge(txs []*solana.Transaction, previous int) error {
	if previous > metadataTransactionLimit || len(txs) > metadataTransactionLimit-previous {
		return errMetadataBudget
	}
	keys := 0
	for _, tx := range txs {
		if tx == nil || len(tx.Message.AccountKeys) > metadataGroupKeyLimit-keys {
			return errMetadataBudget
		}
		keys += len(tx.Message.AccountKeys)
	}
	if uint64(keys)*384+uint64(len(txs))*128 > metadataGroupLimit {
		return errMetadataBudget
	}
	return nil
}

// Runtime collectors are separately bounded and may overlap across at most
// metadataWorkerLimit workers. Reserve final copies before assembling them.
func (metadata *transactionMetadata) reserveExecution(budget *metadataBudget, execCtx *sealevel.ExecutionCtx) error {
	if execCtx == nil {
		return nil
	}
	bytes := uint64(0)
	add := func(n uint64) bool {
		if n > metadataRecordLimit-bytes {
			return false
		}
		bytes += n
		return true
	}
	for _, instruction := range execCtx.InnerInstrs {
		// Assembly grows byte slices; allow spare capacity without changing the
		// shared simulation encoder merely to tighten this optional budget.
		if !add(256) || !add(uint64(len(instruction.Data))) || !add(uint64(len(instruction.Data))) ||
			!add(uint64(len(instruction.Accounts))) || !add(uint64(len(instruction.Accounts))) {
			return errMetadataBudget
		}
	}
	if recorder, ok := execCtx.Log.(*sealevel.LogRecorder); ok {
		if !add(uint64(len(recorder.Logs)) * 32) {
			return errMetadataBudget
		}
		for _, entry := range recorder.Logs {
			if !add(uint64(len(entry))) {
				return errMetadataBudget
			}
		}
	}
	_, data := execCtx.TransactionContext.ReturnData()
	if !add(uint64(len(data))) {
		return errMetadataBudget
	}
	return metadata.reserve(budget, bytes)
}

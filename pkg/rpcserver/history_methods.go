package rpcserver

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/bankview"
	"github.com/Overclock-Validator/mithril/pkg/rpcdb"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
)

const maxSignaturesForAddress = 1000

// historyCommitment parses a history method's commitment; Solana serves
// history at confirmed or finalized only.
func historyCommitment(params []interface{}, idx int) (commitmentConfig, map[string]interface{}, error) {
	conf, m, err := parseCommitmentConfig(params, idx)
	if err != nil {
		return conf, m, err
	}
	if conf.level == bankview.Processed {
		return conf, m, &InvalidParamsError{Message: "Method does not support commitment below `confirmed`"}
	}
	return conf, m, nil
}

func optionalSignature(m map[string]interface{}, key string) (*solana.Signature, error) {
	raw, ok := m[key]
	if !ok || raw == nil {
		return nil, nil
	}
	s, ok := raw.(string)
	if !ok {
		return nil, &InvalidParamsError{Message: fmt.Sprintf("invalid %s", key)}
	}
	sig, err := solana.SignatureFromBase58(s)
	if err != nil {
		return nil, &InvalidParamsError{Message: fmt.Sprintf("invalid %s: %v", key, err)}
	}
	return &sig, nil
}

type SignatureForAddress struct {
	Signature          string          `json:"signature"`
	Slot               uint64          `json:"slot"`
	Err                json.RawMessage `json:"err"`
	Memo               *string         `json:"memo"`
	BlockTime          *int64          `json:"blockTime"`
	ConfirmationStatus string          `json:"confirmationStatus"`
}

// GetSignaturesForAddress serves the history of a watched address, newest
// first. Unfinalized entries come from the indexer, finalized ones from rpcdb.
func (rpcServer *RpcServer) GetSignaturesForAddress(ctx context.Context, p jsonrpc.RawParams) ([]SignatureForAddress, error) {
	params, err := decodeParams(p)
	if err != nil {
		return nil, err
	}
	addrStr, ok := firstString(params)
	if !ok {
		return nil, &InvalidParamsError{Message: "getSignaturesForAddress requires an address"}
	}
	addr, err := solana.PublicKeyFromBase58(addrStr)
	if err != nil {
		return nil, &InvalidParamsError{Message: "Invalid param: Invalid"}
	}
	commitment, m, err := historyCommitment(params, 1)
	if err != nil {
		return nil, err
	}
	limit := maxSignaturesForAddress
	if raw, ok := m["limit"]; ok && raw != nil {
		f, ok := raw.(float64)
		if !ok || f < 1 || f > maxSignaturesForAddress || f != float64(int(f)) {
			return nil, &InvalidParamsError{Message: "Invalid limit; max 1000"}
		}
		limit = int(f)
	}
	before, err := optionalSignature(m, "before")
	if err != nil {
		return nil, err
	}
	until, err := optionalSignature(m, "until")
	if err != nil {
		return nil, err
	}
	ix, err := rpcServer.historyAvailable()
	if err != nil {
		return nil, err
	}
	if ix.Registry().Kinds(addr) == 0 {
		return nil, &InvalidParamsError{Message: fmt.Sprintf("address %s is not watched by this node", addr)}
	}
	view, err := rpcServer.currentView(ctx, commitment)
	if err != nil {
		return nil, err
	}
	finalizedSlot := uint64(0)
	if fin, _, err := rpcServer.bankViews.Current(bankview.Finalized); err == nil {
		finalizedSlot = fin.Slot
	}

	out := make([]SignatureForAddress, 0, limit)
	seen := map[solana.Signature]bool{}
	pending := ix.Pending(addr, view.Slot)
	start, storeBefore, reachedUntil := 0, before, false
	if before != nil {
		for i, rec := range pending {
			if rec.Signature == *before {
				start, storeBefore = i+1, nil
				break
			}
		}
		if storeBefore != nil {
			start = len(pending) // before is older than every pending entry
		}
	}
	for _, rec := range pending[start:] {
		if len(out) == limit {
			break
		}
		if until != nil && rec.Signature == *until {
			reachedUntil = true
			break
		}
		status := "confirmed"
		if rec.Slot <= finalizedSlot {
			status = "finalized"
		}
		out = append(out, signatureEntry(rec, status))
		seen[rec.Signature] = true
	}
	if reachedUntil || len(out) == limit {
		return out, nil
	}
	store := ix.Store()
	entries, err := store.SignaturesForAddress(addr, storeBefore, until, limit-len(out)+len(seen))
	if err != nil {
		return nil, &InvalidParamsError{Message: err.Error()}
	}
	for _, e := range entries {
		if len(out) == limit {
			break
		}
		if seen[e.Signature] {
			continue
		}
		rec, ok, err := store.Transaction(e.Signature)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		out = append(out, signatureEntry(rec, "finalized"))
	}
	return out, nil
}

func signatureEntry(rec *rpcdb.TxRecord, status string) SignatureForAddress {
	e := SignatureForAddress{
		Signature:          rec.Signature.String(),
		Slot:               rec.Slot,
		Err:                metaErr(rec.Meta),
		ConfirmationStatus: status,
	}
	if rec.BlockTime != 0 {
		bt := rec.BlockTime
		e.BlockTime = &bt
	}
	return e
}

func metaErr(meta []byte) json.RawMessage {
	var m struct {
		Err json.RawMessage `json:"err"`
	}
	if len(meta) == 0 || json.Unmarshal(meta, &m) != nil || len(m.Err) == 0 {
		return json.RawMessage("null")
	}
	return m.Err
}

// findRecord returns a transaction record visible at maxSlot, unfinalized
// first, and whether it is finalized.
func findRecord(ctx context.Context, rpcServer *RpcServer, sig solana.Signature, maxSlot uint64) (*rpcdb.TxRecord, bool, error) {
	ix, err := rpcServer.historyAvailable()
	if err != nil {
		return nil, false, err
	}
	if rec, ok := ix.PendingTransaction(sig, maxSlot); ok {
		return rec, false, nil
	}
	rec, ok, err := ix.Store().Transaction(sig)
	if err != nil || !ok {
		return nil, false, err
	}
	return rec, true, nil
}

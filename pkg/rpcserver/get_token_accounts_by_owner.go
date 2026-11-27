package rpcserver

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/bankview"
	"github.com/Overclock-Validator/mithril/pkg/rpcindex"
	"github.com/Overclock-Validator/mithril/pkg/watch"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
	"github.com/mr-tron/base58"
)

var nativeMint = solana.MustPublicKeyFromBase58("So11111111111111111111111111111111111111112")

// TokenEnumerationNotReadyError reports that the node cannot yet list every
// token account of an owner: accounts that existed before the wallet was
// registered and stayed idle are found only by scanning the account store,
// and recent discoveries may still be queued for the indexer.
type TokenEnumerationNotReadyError struct {
	Owner solana.PublicKey
}

func (e *TokenEnumerationNotReadyError) Error() string {
	return fmt.Sprintf("token accounts of %s are still being enumerated on this node; retry shortly, or pass allowPartial to list those found so far", e.Owner)
}

// indexerWait bounds how long a request waits for the indexer to handle
// blocks replay has already handed over.
const indexerWait = 200 * time.Millisecond

func (e *TokenEnumerationNotReadyError) ToJSONRPCError() (jsonrpc.JSONRPCError, error) {
	return jsonrpc.JSONRPCError{Code: rpcCodeNodeUnhealthy, Message: e.Error()}, nil
}

type KeyedAccount struct {
	Pubkey  string `json:"pubkey"`
	Account any    `json:"account"`
}

type GetTokenAccountsByOwnerResp struct {
	Context GetAccountInfoRespContext `json:"context"`
	Value   []KeyedAccount            `json:"value"`
}

// GetTokenAccountsByOwner lists a watched wallet's token accounts at the
// selected bank. Candidates are every account the node has seen owned by the
// wallet: from the enumeration scan, finalized discoveries, and unfinalized
// discoveries up to the bank. Each is checked against the bank, so the result
// is exactly the accounts the wallet owns there.
func (rpcServer *RpcServer) GetTokenAccountsByOwner(ctx context.Context, p jsonrpc.RawParams) (GetTokenAccountsByOwnerResp, error) {
	params, err := decodeParams(p)
	if err != nil {
		return GetTokenAccountsByOwnerResp{}, err
	}
	ownerStr, ok := firstString(params)
	if !ok {
		return GetTokenAccountsByOwnerResp{}, &InvalidParamsError{Message: "getTokenAccountsByOwner requires an owner"}
	}
	owner, err := solana.PublicKeyFromBase58(ownerStr)
	if err != nil {
		return GetTokenAccountsByOwnerResp{}, &InvalidParamsError{Message: "Invalid param: Invalid"}
	}
	var mintFilter, programFilter *solana.PublicKey
	if len(params) < 2 {
		return GetTokenAccountsByOwnerResp{}, &InvalidParamsError{Message: "getTokenAccountsByOwner requires a mint or programId filter"}
	}
	filter, ok := params[1].(map[string]interface{})
	if !ok {
		return GetTokenAccountsByOwnerResp{}, &InvalidParamsError{Message: "invalid filter"}
	}
	for key, target := range map[string]**solana.PublicKey{"mint": &mintFilter, "programId": &programFilter} {
		if raw, ok := filter[key].(string); ok {
			pk, err := solana.PublicKeyFromBase58(raw)
			if err != nil {
				return GetTokenAccountsByOwnerResp{}, &InvalidParamsError{Message: fmt.Sprintf("invalid %s", key)}
			}
			*target = &pk
		}
	}
	if (mintFilter == nil) == (programFilter == nil) {
		return GetTokenAccountsByOwnerResp{}, &InvalidParamsError{Message: "filter must contain exactly one of mint or programId"}
	}
	if programFilter != nil && *programFilter != rpcindex.SPLTokenProgram && *programFilter != rpcindex.Token2022Program {
		return GetTokenAccountsByOwnerResp{}, &InvalidParamsError{Message: "Invalid param: unrecognized Token program id"}
	}
	commitment, m, err := parseCommitmentConfig(params, 2)
	if err != nil {
		return GetTokenAccountsByOwnerResp{}, err
	}
	encoding := "base58"
	if raw, ok := m["encoding"].(string); ok {
		encoding = raw
	}
	allowPartial, _ := m["allowPartial"].(bool)
	conf, err := parseGetAccountInfoConfMap(append([]interface{}{nil}, params[2:]...))
	if err != nil {
		return GetTokenAccountsByOwnerResp{}, &InvalidParamsError{Message: err.Error()}
	}

	ix, err := rpcServer.historyAvailable()
	if err != nil {
		return GetTokenAccountsByOwnerResp{}, err
	}
	if !allowPartial && ix.Registry().Kinds(owner)&watch.Wallet == 0 {
		return GetTokenAccountsByOwnerResp{}, &InvalidParamsError{Message: fmt.Sprintf("token accounts are indexed only for watched wallets; add %s to rpc.watch", owner)}
	}

	var resp GetTokenAccountsByOwnerResp
	err = rpcServer.readView(ctx, commitment, func(r *bankview.Read) error {
		// Replay hands a block to the indexer before publishing its bank, so
		// once the queue drains, every discovery up to this bank is pending
		// or stored.
		if err := ix.WaitCompleted(ctx, indexerWait); err != nil && !allowPartial {
			return &TokenEnumerationNotReadyError{Owner: owner}
		}
		if !allowPartial && !rpcServer.enumerator.Ready(owner) {
			return &TokenEnumerationNotReadyError{Owner: owner}
		}
		candidates, err := tokenAccountCandidates(ix, owner, r.View.Slot)
		if err != nil {
			return err
		}
		values := make([]KeyedAccount, 0, len(candidates))
		accts, err := r.Accounts(ctx, candidates)
		if err != nil {
			return err
		}
		for i, a := range accts {
			mint, acctOwner, amount, program, ok := rpcindex.TokenAccountState(a)
			if !ok || acctOwner != owner {
				continue // closed or reassigned at this bank
			}
			if (mintFilter != nil && mint != *mintFilter) || (programFilter != nil && program != *programFilter) {
				continue
			}
			var account any
			if encoding == "jsonParsed" {
				decimals := uint8(0)
				if mintAcct, err := r.Account(mint); err == nil {
					decimals, _ = rpcindex.MintDecimals(mintAcct)
				}
				account = parsedTokenAccount(a, mint, acctOwner, amount, decimals, program)
			} else {
				if account, err = accountInfoValue(a, conf); err != nil {
					return err
				}
			}
			values = append(values, KeyedAccount{Pubkey: candidates[i].String(), Account: account})
		}
		resp = GetTokenAccountsByOwnerResp{Context: accountContext(r.View.Slot), Value: values}
		return nil
	})
	return resp, err
}

// tokenAccountCandidates returns the finalized token accounts recorded for
// owner plus unfinalized discoveries at or below maxSlot. The caller checks
// each against the bank, which also removes closed ones.
func tokenAccountCandidates(ix *rpcindex.Indexer, owner solana.PublicKey, maxSlot uint64) ([]solana.PublicKey, error) {
	rows, err := ix.Store().TokenAccounts(owner)
	if err != nil {
		return nil, err
	}
	set := map[solana.PublicKey]bool{}
	for _, row := range rows {
		set[row.Account] = true
	}
	for _, ch := range ix.PendingTokenAccounts(maxSlot) {
		if !ch.Closed && ch.Owner == owner {
			set[ch.Account] = true
		}
	}
	out := make([]solana.PublicKey, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return string(out[i][:]) < string(out[j][:]) })
	return out, nil
}

func parsedTokenAccount(a *accounts.Account, mint, owner solana.PublicKey, amount uint64, decimals uint8, program solana.PublicKey) map[string]any {
	state := "initialized"
	if a.Data[108] == 2 {
		state = "frozen"
	}
	info := map[string]any{
		"isNative":    mint == nativeMint,
		"mint":        mint.String(),
		"owner":       owner.String(),
		"state":       state,
		"tokenAmount": uiTokenAmount(amount, decimals),
	}
	programName := "spl-token"
	if program == rpcindex.Token2022Program {
		programName = "spl-token-2022"
	}
	return map[string]any{
		"data": map[string]any{
			"parsed":  map[string]any{"info": info, "type": "account"},
			"program": programName,
			"space":   len(a.Data),
		},
		"executable": a.Executable,
		"lamports":   a.Lamports,
		"owner":      base58.Encode(a.Owner[:]),
		"rentEpoch":  a.RentEpoch,
		"space":      len(a.Data),
	}
}

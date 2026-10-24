package replay

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/txverify"
	"github.com/gagliardetto/solana-go"
)

type transactionTokenFact struct {
	AccountIndex uint16
	Mint         solana.PublicKey
	Owner        solana.PublicKey
	ProgramID    solana.PublicKey
	Amount       uint64
}

// transactionTokenBalance retains raw units and mint classification. Historical
// RPC balances use raw decimals even when Token-2022 extensions are present.
type transactionTokenBalance struct {
	transactionTokenFact
	Decimals      uint8
	HasExtensions bool
}

type tokenMintState struct {
	ProgramID     solana.PublicKey
	Decimals      uint8
	HasExtensions bool
	Valid         bool
}

// transactionTokenSnapshot owns scalar observations, never account data. Mints
// includes every message-writable key, including non-mints and tombstones, so a
// committed owner change or closure cannot leave an old mint generation alive.
type transactionTokenSnapshot struct {
	Balances []transactionTokenFact
	Mints    map[solana.PublicKey]tokenMintState
}

// transactionTokenGroup is hydrated while execution is quiescent. Workers do not
// access it; after they join, resolve consumes their snapshots in transaction
// order. A new group reads the current overlay, including earlier group commits.
type transactionTokenGroup struct {
	mints map[solana.PublicKey]tokenMintState
}

func prepareTransactionTokenGroup(slotCtx *sealevel.SlotCtx, txs []*solana.Transaction) (*transactionTokenGroup, error) {
	group := &transactionTokenGroup{mints: make(map[solana.PublicKey]tokenMintState)}
	remaining := uint64(metadataHydrationLimit)
	referencedMints := make(map[solana.PublicKey]struct{})
	for _, tx := range txs {
		if err := validateTransactionTokenCapture(tx); err != nil {
			return nil, err
		}
		for _, key := range tx.Message.AccountKeys {
			if _, seen := group.mints[key]; seen {
				continue
			}
			account, err := transactionMetadataAccount(slotCtx, key, &remaining)
			if err != nil {
				return nil, fmt.Errorf("read token group account %s: %w", key, err)
			}
			group.mints[key] = decodeTokenMint(account)
			if fact, ok := decodeTokenAccount(account); ok {
				referencedMints[fact.Mint] = struct{}{}
			}
		}
	}
	for key := range referencedMints {
		if _, seen := group.mints[key]; seen {
			continue
		}
		account, err := transactionMetadataAccount(slotCtx, key, &remaining)
		if err != nil {
			return nil, fmt.Errorf("read token group mint %s: %w", key, err)
		}
		group.mints[key] = decodeTokenMint(account)
	}
	return group, nil
}

func captureTransactionTokenSnapshot(slotCtx *sealevel.SlotCtx, tx *solana.Transaction) (transactionTokenSnapshot, error) {
	if err := validateTransactionTokenCapture(tx); err != nil {
		return transactionTokenSnapshot{}, err
	}
	snapshot := transactionTokenSnapshot{Mints: make(map[solana.PublicKey]tokenMintState)}
	invoked := make([]bool, len(tx.Message.AccountKeys))
	for _, instruction := range tx.Message.Instructions {
		invoked[instruction.ProgramIDIndex] = true
	}
	hasTokenProgram := false
	for _, key := range tx.Message.AccountKeys {
		hasTokenProgram = hasTokenProgram || isTokenProgram(key)
	}
	static, writableLookups := messageAccountLayout(&tx.Message)
	for i, key := range tx.Message.AccountKeys {
		account, err := transactionMetadataAccount(slotCtx, key, nil)
		if err != nil {
			return transactionTokenSnapshot{}, fmt.Errorf("read token snapshot account %s: %w", key, err)
		}
		if messageAccountIsWritable(&tx.Message, i, static, writableLookups) {
			snapshot.Mints[key] = decodeTokenMint(account)
		}
		if !hasTokenProgram || invoked[i] || isTokenProgram(key) {
			continue
		}
		if fact, ok := decodeTokenAccount(account); ok {
			fact.AccountIndex = uint16(i)
			snapshot.Balances = append(snapshot.Balances, fact)
		}
	}
	return snapshot, nil
}

// resolve applies only committed post-state. It does not read live accounts and
// leaves the group unchanged on error. The caller must discard incomplete group
// results rather than publish balances with unavailable mint data.
func (group *transactionTokenGroup) resolve(pre, post transactionTokenSnapshot) ([]transactionTokenBalance, []transactionTokenBalance, error) {
	if len(pre.Mints) != len(post.Mints) {
		return nil, nil, errors.New("token snapshots have different writable keys")
	}
	for key, state := range pre.Mints {
		if _, ok := post.Mints[key]; !ok {
			return nil, nil, errors.New("token snapshots have different writable keys")
		}
		if current, ok := group.mints[key]; !ok || current != state {
			return nil, nil, fmt.Errorf("token mint state is out of order for %s", key)
		}
	}
	preBalances, err := group.resolveBalances(pre.Balances, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve pre-token balances: %w", err)
	}
	postBalances, err := group.resolveBalances(post.Balances, post.Mints)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve post-token balances: %w", err)
	}
	for key, state := range post.Mints {
		group.mints[key] = state
	}
	return preBalances, postBalances, nil
}

func (group *transactionTokenGroup) resolveBalances(facts []transactionTokenFact, updates map[solana.PublicKey]tokenMintState) ([]transactionTokenBalance, error) {
	balances := make([]transactionTokenBalance, 0, len(facts))
	for _, fact := range facts {
		mint, ok := updates[fact.Mint]
		if !ok {
			mint, ok = group.mints[fact.Mint]
		}
		if !ok {
			return nil, fmt.Errorf("token mint %s is unavailable for program %s", fact.Mint, fact.ProgramID)
		}
		if !mint.Valid || mint.ProgramID != fact.ProgramID {
			continue
		}
		balances = append(balances, transactionTokenBalance{
			transactionTokenFact: fact,
			Decimals:             mint.Decimals,
			HasExtensions:        mint.HasExtensions,
		})
	}
	return balances, nil
}

func validateTransactionTokenCapture(tx *solana.Transaction) error {
	if err := txverify.SanitizeTransaction(tx); err != nil {
		return fmt.Errorf("validate token capture transaction: %w", err)
	}
	if tx.Message.NumLookups() > 0 && !tx.Message.IsResolved() {
		return errors.New("token capture requires resolved address lookups")
	}
	return nil
}

func isTokenProgram(key solana.PublicKey) bool {
	return key == solana.TokenProgramID || key == solana.Token2022ProgramID
}

// These base-layout checks match spl-generic-token 2.0.1, used by the pinned
// Agave balance collector. They classify initialized token accounts, not their
// extension payloads; UI conversion is deliberately outside this raw capture.
func decodeTokenAccount(account *accounts.Account) (transactionTokenFact, bool) {
	if account == nil || account.Lamports == 0 || !isTokenProgram(account.Owner) {
		return transactionTokenFact{}, false
	}
	data := account.Data
	validSize := len(data) == 165 || (account.Owner == solana.Token2022ProgramID && len(data) > 165 && len(data) != 355 && data[165] == 2)
	if !validSize || data[108] == 0 {
		return transactionTokenFact{}, false
	}
	return transactionTokenFact{
		Mint:      solana.PublicKey(data[:32]),
		Owner:     solana.PublicKey(data[32:64]),
		ProgramID: account.Owner,
		Amount:    binary.LittleEndian.Uint64(data[64:72]),
	}, true
}

func decodeTokenMint(account *accounts.Account) tokenMintState {
	if account == nil || account.Lamports == 0 || !isTokenProgram(account.Owner) {
		return tokenMintState{}
	}
	data := account.Data
	validSize := len(data) == 82 || (account.Owner == solana.Token2022ProgramID && len(data) > 165 && len(data) != 355 && data[165] == 1)
	if !validSize || data[45] == 0 {
		return tokenMintState{}
	}
	return tokenMintState{
		ProgramID:     account.Owner,
		Decimals:      data[44],
		HasExtensions: len(data) > 82,
		Valid:         true,
	}
}

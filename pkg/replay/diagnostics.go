package replay

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/base58"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
)

type consensusTxDiagnostic struct {
	Index                  int      `json:"index"`
	Signatures             []string `json:"signatures"`
	WireBase64             string   `json:"wire_base64,omitempty"`
	IsVote                 bool     `json:"is_vote"`
	Version                string   `json:"version"`
	RecentBlockhash        string   `json:"recent_blockhash"`
	AccountKeys            []string `json:"account_keys,omitempty"`
	AccountKeyCount        int      `json:"account_key_count"`
	WritableAccountCount   int      `json:"writable_account_count,omitempty"`
	ReadonlyAccountCount   int      `json:"readonly_account_count,omitempty"`
	AddressTableCount      int      `json:"address_table_count,omitempty"`
	AddressLookupCount     int      `json:"address_lookup_count,omitempty"`
	AddressTableLookupKeys []string `json:"address_table_lookup_keys,omitempty"`
	InstructionCount       int      `json:"instruction_count"`
	ProgramIDs             []string `json:"program_ids,omitempty"`
	Instructions           []any    `json:"instructions,omitempty"`
	Meta                   any      `json:"meta"`
}

type consensusEntryDiagnostic struct {
	Index        int      `json:"index"`
	NumHashes    uint64   `json:"num_hashes"`
	Hash         string   `json:"hash,omitempty"`
	TxCount      int      `json:"tx_count"`
	FirstTxIndex uint64   `json:"first_tx_index,omitempty"`
	LastTxIndex  uint64   `json:"last_tx_index,omitempty"`
	TxIndices    []uint64 `json:"tx_indices,omitempty"`
}

type consensusAccountDiagnostic struct {
	Key        string `json:"key"`
	Slot       uint64 `json:"slot"`
	Lamports   uint64 `json:"lamports"`
	Owner      string `json:"owner"`
	Executable bool   `json:"executable"`
	RentEpoch  uint64 `json:"rent_epoch"`
	DataLen    int    `json:"data_len"`
	DataSHA256 string `json:"data_sha256"`
	IsDummy    bool   `json:"is_dummy"`
}

type consensusBlobDiagnostic struct {
	Length int    `json:"length"`
	SHA256 string `json:"sha256,omitempty"`
}

func consensusHashString(hash [32]byte) string {
	if hash == ([32]byte{}) {
		return ""
	}
	return base58.Encode(hash[:])
}

func consensusByteHashString(hash []byte) string {
	if len(hash) == 0 {
		return ""
	}
	if len(hash) == 32 {
		return base58.Encode(hash)
	}
	return hex.EncodeToString(hash)
}

func consensusLtHashChecksum(ltHash *lthash.LtHash) string {
	if ltHash == nil {
		return ""
	}
	return consensusByteHashString(ltHash.Checksum())
}

func consensusBlob(data []byte) consensusBlobDiagnostic {
	out := consensusBlobDiagnostic{Length: len(data)}
	if len(data) != 0 {
		sum := sha256.Sum256(data)
		out.SHA256 = hex.EncodeToString(sum[:])
	}
	return out
}

func consensusAccountDiagnostics(accts []*accounts.Account) []consensusAccountDiagnostic {
	out := make([]consensusAccountDiagnostic, 0, len(accts))
	for _, acct := range accts {
		if acct == nil {
			continue
		}
		dataSum := sha256.Sum256(acct.Data)
		out = append(out, consensusAccountDiagnostic{
			Key:        acct.Key.String(),
			Slot:       acct.Slot,
			Lamports:   acct.Lamports,
			Owner:      solana.PublicKey(acct.Owner).String(),
			Executable: acct.Executable,
			RentEpoch:  acct.RentEpoch,
			DataLen:    len(acct.Data),
			DataSHA256: hex.EncodeToString(dataSum[:]),
			IsDummy:    acct.IsDummy,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

func consensusSignatureStrings(tx *solana.Transaction) []string {
	sigs := make([]string, 0, len(tx.Signatures))
	for _, sig := range tx.Signatures {
		sigs = append(sigs, sig.String())
	}
	return sigs
}

func consensusTxVersion(tx *solana.Transaction) string {
	switch tx.Message.GetVersion() {
	case solana.MessageVersionLegacy:
		return "legacy"
	case solana.MessageVersionV0:
		return "v0"
	case solana.MessageVersionV1:
		return "v1"
	default:
		return fmt.Sprintf("unknown(%d)", tx.Message.GetVersion())
	}
}

func consensusProgramIDs(tx *solana.Transaction) []string {
	out := make([]string, 0, len(tx.Message.Instructions))
	seen := make(map[solana.PublicKey]struct{}, len(tx.Message.Instructions))
	for _, instr := range tx.Message.Instructions {
		idx := int(instr.ProgramIDIndex)
		if idx < 0 || idx >= len(tx.Message.AccountKeys) {
			out = append(out, fmt.Sprintf("invalid_program_index:%d", idx))
			continue
		}
		programID := tx.Message.AccountKeys[idx]
		if _, ok := seen[programID]; ok {
			continue
		}
		seen[programID] = struct{}{}
		out = append(out, programID.String())
	}
	return out
}

func consensusLookupTableKeys(tx *solana.Transaction) []string {
	if tx.Message.GetVersion() != solana.MessageVersionV0 || tx.Message.AddressTableLookups.NumLookups() == 0 {
		return nil
	}
	tableIDs := tx.Message.GetAddressTableLookups().GetTableIDs()
	out := make([]string, 0, len(tableIDs))
	for _, tableID := range tableIDs {
		out = append(out, tableID.String())
	}
	return out
}

func consensusTxMetaDiagnostic(txMeta *rpc.TransactionMeta) any {
	if txMeta == nil {
		return map[string]any{"present": false}
	}
	out := map[string]any{
		"present":                       true,
		"fee":                           txMeta.Fee,
		"pre_balance_count":             len(txMeta.PreBalances),
		"post_balance_count":            len(txMeta.PostBalances),
		"loaded_writable_address_count": len(txMeta.LoadedAddresses.Writable),
		"loaded_readonly_address_count": len(txMeta.LoadedAddresses.ReadOnly),
	}
	if txMeta.Err != nil {
		out["err"] = fmt.Sprintf("%v", txMeta.Err)
	}
	if txMeta.ComputeUnitsConsumed != nil {
		out["compute_units_consumed"] = *txMeta.ComputeUnitsConsumed
	}
	return out
}

func consensusTxDiagnostics(block *b.Block) []consensusTxDiagnostic {
	out := make([]consensusTxDiagnostic, 0, len(block.Transactions))
	for idx, tx := range block.Transactions {
		var txMeta *rpc.TransactionMeta
		if idx < len(block.TxMetas) {
			txMeta = block.TxMetas[idx]
		}

		txDiag := consensusTxDiagnostic{
			Index:              idx,
			Signatures:         consensusSignatureStrings(tx),
			IsVote:             tx.IsVote(),
			Version:            consensusTxVersion(tx),
			RecentBlockhash:    tx.Message.RecentBlockhash.String(),
			AccountKeyCount:    len(tx.Message.AccountKeys),
			AddressTableCount:  len(tx.Message.AddressTableLookups),
			AddressLookupCount: tx.Message.AddressTableLookups.NumLookups(),
			InstructionCount:   len(tx.Message.Instructions),
			ProgramIDs:         consensusProgramIDs(tx),
			Meta:               consensusTxMetaDiagnostic(txMeta),
		}
		if wire, err := tx.ToBase64(); err == nil {
			txDiag.WireBase64 = wire
		}
		for _, key := range tx.Message.AccountKeys {
			txDiag.AccountKeys = append(txDiag.AccountKeys, key.String())
		}
		for _, instr := range tx.Message.Instructions {
			txDiag.Instructions = append(txDiag.Instructions, map[string]any{
				"program_id_index": instr.ProgramIDIndex,
				"account_indices":  append([]uint16(nil), instr.Accounts...),
				"data_base58":      instr.Data.String(),
				"data_hex":         hex.EncodeToString(instr.Data),
			})
		}
		if canDeriveAccountsFromMessage(tx) {
			txDiag.WritableAccountCount = len(messageWritableAccounts(&tx.Message))
			txDiag.ReadonlyAccountCount = len(messageReadonlyAccounts(&tx.Message))
		}
		txDiag.AddressTableLookupKeys = consensusLookupTableKeys(tx)
		out = append(out, txDiag)
	}
	return out
}

func consensusEntryDiagnostics(block *b.Block) []consensusEntryDiagnostic {
	out := make([]consensusEntryDiagnostic, 0, len(block.Entries))
	for idx, entry := range block.Entries {
		entryDiag := consensusEntryDiagnostic{
			Index:     idx,
			NumHashes: entry.NumHashes,
			Hash:      consensusByteHashString(entry.Hash),
			TxCount:   len(entry.Indices),
			TxIndices: append([]uint64(nil), entry.Indices...),
		}
		if len(entry.Indices) > 0 {
			entryDiag.FirstTxIndex = entry.Indices[0]
			entryDiag.LastTxIndex = entry.Indices[len(entry.Indices)-1]
		}
		out = append(out, entryDiag)
	}
	return out
}

func consensusBlockDiagnostic(block *b.Block) map[string]any {
	voteTxCount := 0
	for _, tx := range block.Transactions {
		if tx.IsVote() {
			voteTxCount++
		}
	}

	return map[string]any{
		"slot":                        block.Slot,
		"parent_slot":                 block.ParentSlot,
		"source_parent_slot":          block.SourceParentSlot,
		"block_height":                block.BlockHeight,
		"epoch":                       block.Epoch,
		"from_lightbringer":           block.FromLiveStream,
		"is_skipped":                  block.IsSkipped,
		"leader":                      block.Leader.String(),
		"blockhash":                   consensusHashString(block.Blockhash),
		"last_blockhash":              consensusHashString(block.LastBlockhash),
		"parent_bankhash":             consensusHashString(block.ParentBankhash),
		"expected_bankhash":           consensusHashString(block.ExpectedBankhash),
		"accts_lthash_checksum":       consensusLtHashChecksum(block.AcctsLtHash),
		"num_signatures":              block.NumSignatures,
		"prev_num_signatures":         block.PrevNumSignatures,
		"initial_lamports_per_sig":    block.InitialPreviousLamportsPerSignature,
		"tx_count":                    len(block.Transactions),
		"vote_tx_count":               voteTxCount,
		"non_vote_tx_count":           len(block.Transactions) - voteTxCount,
		"tx_meta_count":               len(block.TxMetas),
		"entry_count":                 len(block.Entries),
		"entries":                     consensusEntryDiagnostics(block),
		"transactions":                consensusTxDiagnostics(block),
		"latest_evicted_blockhash":    consensusHashString(block.LatestEvictedBlockhash),
		"has_eah_workaround":          block.HasEahWorkaround,
		"eah_workaround_bankhash":     consensusByteHashString(block.EahWorkaroundBankhash),
		"num_reward_partitions":       block.NumRewardPartitions,
		"reward_count":                len(block.Rewards),
		"updated_account_count":       len(block.UpdatedAccts),
		"epoch_updated_account_count": len(block.EpochUpdatedAccts),
	}
}

// writeFooterBankhashMismatchArtifact captures the exact locally computed end-of-slot
// state when a certified Alpenglow footer disagrees with replay. It deliberately stores
// hashes rather than raw account or certificate data so the artifact remains bounded.
// Like all consensus diagnostics, failure to write it must not affect replay behavior.
func writeFooterBankhashMismatchArtifact(
	verifyErr error,
	block *b.Block,
	slotCtx *sealevel.SlotCtx,
	writableAccts []*accounts.Account,
	modifiedAccts []*accounts.Account,
) {
	if block == nil || slotCtx == nil {
		return
	}

	artifact := consensusBlockDiagnostic(block)
	artifact["error"] = verifyErr.Error()
	artifact["computed_bankhash"] = consensusByteHashString(slotCtx.FinalBankhash)
	artifact["computed_accts_lthash_checksum"] = consensusLtHashChecksum(slotCtx.AcctsLtHash)
	artifact["total_compute_units_consumed"] = slotCtx.TotalComputeUnitsConsumed
	artifact["lamports_burnt"] = slotCtx.LamportsBurnt
	artifact["processed_num_signatures"] = slotCtx.NumSignatures
	artifact["footer_producer_time_nanos"] = block.FooterProducerTimeNanos
	artifact["has_alpenglow_footer"] = block.HasAlpenglowFooter
	artifact["has_expected_bankhash"] = block.HasExpectedBankhash
	artifact["skip_reward_cert"] = consensusBlob(block.SkipRewardCert)
	artifact["notar_reward_cert"] = consensusBlob(block.NotarRewardCert)
	artifact["block_final_cert"] = consensusBlob(block.BlockFinalCert)
	artifact["alpenglow_final_cert"] = consensusBlob(block.AlpenglowFinalCert)
	artifact["writable_accounts"] = consensusAccountDiagnostics(writableAccts)
	artifact["modified_accounts"] = consensusAccountDiagnostics(modifiedAccts)

	writeConsensusArtifact(fmt.Sprintf("footer-bankhash-mismatch-slot-%d.json", block.Slot), artifact)
	writeFooterAccountDataArtifact(block, slotCtx, modifiedAccts)
}

const (
	footerAccountDataMaxBytes     = 2 << 20
	footerAccountArtifactMaxBytes = 32 << 20
)

type footerRawAccountDiagnostic struct {
	consensusAccountDiagnostic
	DataBase64  string `json:"data_base64,omitempty"`
	DataOmitted string `json:"data_omitted,omitempty"`
}

type footerRawAccountPair struct {
	Key               string                      `json:"key"`
	Parent            *footerRawAccountDiagnostic `json:"parent,omitempty"`
	Post              *footerRawAccountDiagnostic `json:"post,omitempty"`
	ParentUnavailable bool                        `json:"parent_unavailable,omitempty"`
}

func footerRawAccount(acct *accounts.Account) *footerRawAccountDiagnostic {
	if acct == nil {
		return nil
	}
	diagnostic := consensusAccountDiagnostics([]*accounts.Account{acct})[0]
	image := &footerRawAccountDiagnostic{consensusAccountDiagnostic: diagnostic}
	if len(acct.Data) > footerAccountDataMaxBytes {
		image.DataOmitted = "account_data_limit"
	} else {
		image.DataBase64 = base64.StdEncoding.EncodeToString(acct.Data)
	}
	return image
}

// Capture bounded public account bytes only after verification has failed. The
// exact parent images were pinned before hashing; never substitute a later
// database version. Encoding copies bytes and leaves account state untouched.
func writeFooterAccountDataArtifact(block *b.Block, slotCtx *sealevel.SlotCtx, modified []*accounts.Account) {
	const metadataReserve = 16384
	remaining := footerAccountArtifactMaxBytes - metadataReserve
	images := make([]json.RawMessage, 0, len(modified))
	omittedPairs, oversizedImages := 0, 0
	for _, post := range modified {
		if post == nil {
			continue
		}
		parent, err := slotCtx.GetParentAccount(post.Key)
		pair := footerRawAccountPair{
			Key: post.Key.String(), Parent: footerRawAccount(parent), Post: footerRawAccount(post),
			ParentUnavailable: err != nil || parent == nil,
		}
		for _, image := range []*footerRawAccountDiagnostic{pair.Parent, pair.Post} {
			if image != nil && image.DataOmitted != "" {
				oversizedImages++
			}
		}
		encoded, err := json.Marshal(pair)
		if err != nil {
			omittedPairs++
			continue
		}
		if len(encoded)+1 > remaining {
			omittedPairs++
			continue
		}
		remaining -= len(encoded) + 1
		images = append(images, encoded)
	}
	artifact := map[string]any{
		"slot": block.Slot, "parent_slot": block.ParentSlot,
		"computed_bankhash":          consensusByteHashString(slotCtx.FinalBankhash),
		"footer_producer_time_nanos": block.FooterProducerTimeNanos,
		"skip_reward_cert_base64":    base64.StdEncoding.EncodeToString(block.SkipRewardCert),
		"notar_reward_cert_base64":   base64.StdEncoding.EncodeToString(block.NotarRewardCert),
		"final_cert_base64":          base64.StdEncoding.EncodeToString(block.BlockFinalCert),
		"account_data_limit_bytes":   footerAccountDataMaxBytes,
		"artifact_limit_bytes":       footerAccountArtifactMaxBytes,
		"modified_account_count":     len(modified), "included_account_pairs": len(images),
		"omitted_account_pairs": omittedPairs, "oversized_account_images": oversizedImages,
		"truncated": omittedPairs > 0 || oversizedImages > 0, "accounts": images,
	}
	if slotCtx.AcctsLtHash != nil {
		artifact["computed_accts_lthash_base64"] = base64.StdEncoding.EncodeToString(slotCtx.AcctsLtHash.Hash())
	}
	encoded, err := json.Marshal(artifact)
	if err != nil || len(encoded) > footerAccountArtifactMaxBytes {
		mlog.Log.Warnf("consensus account data artifact: cannot encode within byte limit at slot %d", block.Slot)
		return
	}
	writeConsensusArtifactBytes(fmt.Sprintf("footer-account-data-slot-%d.json", block.Slot), encoded)
}

// writeConsensusArtifact writes a best-effort JSON diagnostic artifact to the
// per-run consensus subdirectory. If the log dir is empty or any step fails,
// it logs a warning and continues; artifact failure must not crash replay.
func writeConsensusArtifact(filename string, data map[string]interface{}) {
	artifactJSON, jsonErr := json.MarshalIndent(data, "", "  ")
	if jsonErr != nil {
		mlog.Log.Warnf("consensus artifact: failed to marshal JSON for %s: %v", filename, jsonErr)
		return
	}
	writeConsensusArtifactBytes(filename, artifactJSON)
}

func writeConsensusArtifactBytes(filename string, artifactJSON []byte) {
	logDir := mlog.GetLogDir()
	if logDir == "" {
		return
	}
	dir := filepath.Join(logDir, "consensus")
	if err := os.MkdirAll(dir, 0755); err != nil {
		mlog.Log.Warnf("consensus artifact: failed to create directory %s: %v", dir, err)
		return
	}
	artifactPath := filepath.Join(dir, filename)
	if writeErr := os.WriteFile(artifactPath, artifactJSON, 0644); writeErr != nil {
		mlog.Log.Warnf("consensus artifact: failed to write %s: %v", artifactPath, writeErr)
		return
	}
	mlog.Log.FileOnlyf("consensus artifact written: %s", artifactPath)
}

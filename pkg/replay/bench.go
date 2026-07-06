package replay

import (
	"bufio"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/base58"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
)

// A bench bundle is a self-contained directory for replaying blocks B+1..B+n
// on a machine with no accountsdb: the instance's state file at slot B, every
// account those blocks touch (at their slot-B values), and the blocks
// themselves as fetched from RPC.
//
//	bundle/
//	  bench_manifest.json   bundle metadata (slot range, source)
//	  mithril_state.json    verbatim copy of the instance state file
//	  accounts.bin          account records at slot B
//	  blocks/<slot>.json    block.Block JSON, unresolved ALTs, with TxMetas

const benchManifestName = "bench_manifest.json"
const benchAccountsName = "accounts.bin"
const benchBlocksDir = "blocks"

type BenchManifest struct {
	SchemaVersion int      `json:"schema_version"`
	ParentSlot    uint64   `json:"parent_slot"` // B: last slot replayed by the source instance
	FirstSlot     uint64   `json:"first_slot"`  // first bundled block (B+1 unless skipped)
	LastSlot      uint64   `json:"last_slot"`   // last bundled block
	NumBlocks     int      `json:"num_blocks"`
	NumAccounts   int      `json:"num_accounts"`
	SkippedSlots  []uint64 `json:"skipped_slots,omitempty"`
	RpcEndpoint   string   `json:"rpc_endpoint"`
	SourceCommit  string   `json:"source_commit,omitempty"`
}

func writeBenchManifest(dir string, m *BenchManifest) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, benchManifestName), data, 0644)
}

func ReadBenchManifest(dir string) (*BenchManifest, error) {
	data, err := os.ReadFile(filepath.Join(dir, benchManifestName))
	if err != nil {
		return nil, err
	}
	m := new(BenchManifest)
	if err := json.Unmarshal(data, m); err != nil {
		return nil, err
	}
	return m, nil
}

// accounts.bin holds length-prefixed account records:
// key[32] owner[32] lamports[8] rentEpoch[8] executable[1] dataLen[4] data.
var benchAccountsMagic = [8]byte{'M', 'B', 'E', 'N', 'C', 'H', '0', '1'}

func writeBenchAccounts(dir string, accts []*accounts.Account) error {
	f, err := os.Create(filepath.Join(dir, benchAccountsName))
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriterSize(f, 1<<20)

	if _, err := w.Write(benchAccountsMagic[:]); err != nil {
		return err
	}
	var hdr [85]byte
	for _, acct := range accts {
		copy(hdr[0:32], acct.Key[:])
		copy(hdr[32:64], acct.Owner[:])
		binary.LittleEndian.PutUint64(hdr[64:72], acct.Lamports)
		binary.LittleEndian.PutUint64(hdr[72:80], acct.RentEpoch)
		hdr[80] = 0
		if acct.Executable {
			hdr[80] = 1
		}
		binary.LittleEndian.PutUint32(hdr[81:85], uint32(len(acct.Data)))
		if _, err := w.Write(hdr[:]); err != nil {
			return err
		}
		if _, err := w.Write(acct.Data); err != nil {
			return err
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}
	return f.Sync()
}

func readBenchAccounts(dir string) ([]*accounts.Account, error) {
	f, err := os.Open(filepath.Join(dir, benchAccountsName))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)

	var magic [8]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return nil, fmt.Errorf("reading accounts.bin magic: %w", err)
	}
	if magic != benchAccountsMagic {
		return nil, fmt.Errorf("accounts.bin: bad magic %q", magic)
	}

	var accts []*accounts.Account
	var hdr [85]byte
	for {
		if _, err := io.ReadFull(r, hdr[:]); err == io.EOF {
			return accts, nil
		} else if err != nil {
			return nil, fmt.Errorf("reading account header: %w", err)
		}
		acct := new(accounts.Account)
		copy(acct.Key[:], hdr[0:32])
		copy(acct.Owner[:], hdr[32:64])
		acct.Lamports = binary.LittleEndian.Uint64(hdr[64:72])
		acct.RentEpoch = binary.LittleEndian.Uint64(hdr[72:80])
		acct.Executable = hdr[80] == 1
		dataLen := binary.LittleEndian.Uint32(hdr[81:85])
		acct.Data = make([]byte, dataLen)
		if _, err := io.ReadFull(r, acct.Data); err != nil {
			return nil, fmt.Errorf("reading account data for %s: %w", solana.PublicKey(acct.Key), err)
		}
		accts = append(accts, acct)
	}
}

// BuildResumeState reconstructs the ResumeState the node builds on startup
// from the last_* fields of a state file. Mirrors the logic in node.go.
func BuildResumeState(s *state.MithrilState) (*ResumeState, error) {
	if !s.HasResumeData() {
		return nil, fmt.Errorf("state file has no resume data (last_slot/last_bankhash missing)")
	}

	parentBankhash, err := base58.DecodeFromString(s.LastBankhash)
	if err != nil {
		return nil, fmt.Errorf("decoding last_bankhash: %w", err)
	}
	ltHashBytes, err := base64.StdEncoding.DecodeString(s.LastAcctsLtHash)
	if err != nil {
		return nil, fmt.Errorf("decoding last_accts_lt_hash: %w", err)
	}

	rs := &ResumeState{
		ParentSlot:               s.LastSlot,
		ParentBlockHeight:        s.LastBlockHeight,
		ParentBankhash:           parentBankhash[:],
		AcctsLtHash:              new(lthash.LtHash).InitWithHash(ltHashBytes),
		LamportsPerSignature:     s.LastLamportsPerSignature,
		PrevLamportsPerSignature: s.LastPrevLamportsPerSig,
		NumSignatures:            s.LastNumSignatures,
		Capitalization:           s.LastCapitalization,
		SlotsPerYear:             s.LastSlotsPerYear,
		InflationInitial:         s.LastInflationInitial,
		InflationTerminal:        s.LastInflationTerminal,
		InflationTaper:           s.LastInflationTaper,
		InflationFoundation:      s.LastInflationFoundation,
		InflationFoundationTerm:  s.LastInflationFoundationTerm,
	}

	if len(s.LastRecentBlockhashes) > 0 {
		rbh := make(sealevel.SysvarRecentBlockhashes, 0, len(s.LastRecentBlockhashes))
		for _, entry := range s.LastRecentBlockhashes {
			hash, err := base58.DecodeFromString(entry.Blockhash)
			if err != nil {
				return nil, fmt.Errorf("decoding recent blockhash %q: %w", entry.Blockhash, err)
			}
			rbh = append(rbh, sealevel.RecentBlockHashesEntry{
				Blockhash:     hash,
				FeeCalculator: sealevel.FeeCalculator{LamportsPerSignature: entry.LamportsPerSignature},
			})
		}
		rs.RecentBlockhashes = &rbh

		if s.LastEvictedBlockhash != "" {
			evicted, err := base58.DecodeFromString(s.LastEvictedBlockhash)
			if err != nil {
				return nil, fmt.Errorf("decoding last_evicted_blockhash: %w", err)
			}
			rs.EvictedBlockhash = evicted
		}
		if s.LastBlockhash != "" {
			bh, err := base58.DecodeFromString(s.LastBlockhash)
			if err != nil {
				return nil, fmt.Errorf("decoding last_blockhash: %w", err)
			}
			rs.LastBlockhash = bh
		}
	}

	if len(s.LastSlotHashes) > 0 {
		sh := make(sealevel.SysvarSlotHashes, 0, len(s.LastSlotHashes))
		for _, entry := range s.LastSlotHashes {
			hash, err := base58.DecodeFromString(entry.Hash)
			if err != nil {
				return nil, fmt.Errorf("decoding slot hash %q: %w", entry.Hash, err)
			}
			sh = append(sh, sealevel.SlotHash{Slot: entry.Slot, Hash: hash})
		}
		rs.SlotHashes = &sh
	}

	if len(s.ComputedEpochStakes) > 0 {
		rs.ComputedEpochStakes = make(map[uint64][]byte, len(s.ComputedEpochStakes))
		for epoch, data := range s.ComputedEpochStakes {
			rs.ComputedEpochStakes[epoch] = []byte(data)
		}
	}

	return rs, nil
}

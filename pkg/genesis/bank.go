package genesis

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"sort"

	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/runtime"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

var builtinAccounts = map[solana.PublicKey]string{
	solana.SystemProgramID: "system_program", solana.VoteProgramID: "vote_program",
	addresses.BpfLoaderDeprecatedAddr: "solana_bpf_loader_deprecated_program", addresses.BpfLoader2Addr: "solana_bpf_loader_program", addresses.BpfLoaderUpgradeableAddr: "solana_bpf_loader_upgradeable_program",
	solana.ComputeBudget: "compute_budget_program", addresses.ZkTokenProofProgramAddr: "zk_token_proof_program", addresses.LoaderV4Addr: "loader_v4", addresses.ZkElgamalProofProgramAddr: "zk_elgamal_proof_program",
	addresses.Ed25519PrecompileAddr: "", addresses.Secp256kPrecompileAddr: "", addresses.Secp256r1PrecompileAddr: "",
}

func bankReservedAccounts() map[solana.PublicKey]bool {
	out := map[solana.PublicKey]bool{}
	var reserved []string
	if err := json.Unmarshal(reservedBytes, &reserved); err != nil {
		panic(err)
	}
	for _, key := range reserved {
		out[solana.MustPublicKeyFromBase58(key)] = true
	}
	for key := range builtinAccounts {
		out[key] = true
	}
	for _, key := range []solana.PublicKey{addresses.NativeLoaderAddr, addresses.StakeProgramAddr, addresses.ConfigProgramAddr, sealevel.SysvarClockAddr, sealevel.SysvarRentAddr, sealevel.SysvarEpochScheduleAddr, sealevel.SysvarLastRestartSlotAddr, sealevel.SysvarStakeHistoryAddr, sealevel.SysvarRecentBlockHashesAddr, sealevel.SysvarSlotHistoryAddr, sealevel.SysvarSlotHashesAddr, sealevel.SysvarFeesAddr, sealevel.SysvarInstructionsAddr, addresses.SysvarOwnerAddr, solana.SysVarRewardsPubkey, addresses.IncineratorAddr} {
		out[key] = true
	}
	return out
}

type EpochVote struct {
	VoteAccount     string `json:"vote_account"`
	Node            string `json:"node"`
	AuthorizedVoter string `json:"authorized_voter"`
	Stake           uint64 `json:"stake"`
	Lamports        uint64 `json:"lamports"`
	BLSPublicKey    string `json:"bls_public_key"`
}
type EpochStake struct {
	Epoch      uint64      `json:"epoch"`
	TotalStake uint64      `json:"total_stake"`
	Votes      []EpochVote `json:"votes"`
}
type Blockhash struct {
	Hash                 string `json:"hash"`
	HashIndex            uint64 `json:"hash_index"`
	LamportsPerSignature uint64 `json:"lamports_per_signature"`
}

// BankMetadata is a complete genesis seed, independent of snapshot manifests.
// Absence of ConsensusBlockID is intentional: the genesis certificate carries
// Genesis(0, zero), while the bank has not received a post-genesis block ID.
type BankMetadata struct {
	Version              uint32                  `json:"version"`
	Profile              string                  `json:"profile"`
	AgaveRevision        string                  `json:"agave_revision"`
	GenesisHash          string                  `json:"genesis_hash"`
	BankHash             string                  `json:"bank_hash"`
	ConsensusBlockID     *string                 `json:"consensus_block_id"`
	Slot                 uint64                  `json:"slot"`
	ParentSlot           uint64                  `json:"parent_slot"`
	ParentBankHash       string                  `json:"parent_bank_hash"`
	NextReplaySlot       uint64                  `json:"next_replay_slot"`
	Frozen               bool                    `json:"frozen"`
	TickHeight           uint64                  `json:"tick_height"`
	MaxTickHeight        uint64                  `json:"max_tick_height"`
	BlockHeight          uint64                  `json:"block_height"`
	SignatureCount       uint64                  `json:"signature_count"`
	TransactionCount     uint64                  `json:"transaction_count"`
	Capitalization       uint64                  `json:"capitalization"`
	AccountsDataLen      uint64                  `json:"accounts_data_len"`
	AccountsLtHash       []byte                  `json:"lthash"`
	LastBlockhash        string                  `json:"last_blockhash"`
	RecentBlockhashes    []Blockhash             `json:"recent_blockhashes"`
	BlockhashQueueMaxAge uint64                  `json:"blockhash_queue_max_age"`
	Leader               string                  `json:"leader"`
	EpochStakes          []EpochStake            `json:"epoch_stakes"`
	CreationTime         int64                   `json:"creation_time"`
	TicksPerSlot         uint64                  `json:"ticks_per_slot"`
	NanosecondsPerSlot   uint64                  `json:"nanoseconds_per_slot"`
	SlotsPerYear         float64                 `json:"slots_per_year"`
	Poh                  runtime.PohParams       `json:"poh"`
	Rent                 runtime.RentParams      `json:"rent"`
	Fees                 runtime.FeeParams       `json:"fees"`
	LamportsPerSignature uint64                  `json:"lamports_per_signature"`
	Inflation            runtime.InflationParams `json:"inflation"`
	EpochSchedule        runtime.EpochSchedule   `json:"epoch_schedule"`
	Features             []Feature               `json:"features"`
}
type Bank struct {
	Accounts []AccountEntry `json:"accounts"`
	Metadata BankMetadata   `json:"metadata"`
}
type InitialBank struct {
	Initialized Bank `json:"initialized"`
	Frozen      Bank `json:"frozen"`
}

func encodeValue(v any) []byte {
	var b bytes.Buffer
	if err := bin.NewBinEncoder(&b).Encode(v); err != nil {
		panic(err)
	}
	return b.Bytes()
}
func le64(v uint64) []byte { b := make([]byte, 8); binary.LittleEndian.PutUint64(b, v); return b }
func cloneEntries(a []AccountEntry) []AccountEntry {
	out := append([]AccountEntry(nil), a...)
	for i := range out {
		out[i].Data = bytes.Clone(out[i].Data)
		out[i].Key = out[i].Pubkey
		out[i].Slot = 0
	}
	return out
}

func cloneMetadata(m BankMetadata) BankMetadata {
	m.AccountsLtHash = bytes.Clone(m.AccountsLtHash)
	m.RecentBlockhashes = append([]Blockhash(nil), m.RecentBlockhashes...)
	m.Features = append([]Feature(nil), m.Features...)
	m.EpochStakes = append([]EpochStake(nil), m.EpochStakes...)
	for i := range m.EpochStakes {
		m.EpochStakes[i].Votes = append([]EpochVote(nil), m.EpochStakes[i].Votes...)
	}
	if m.ConsensusBlockID != nil {
		id := *m.ConsensusBlockID
		m.ConsensusBlockID = &id
	}
	return m
}

// ConstructInitialBank is the common future startup / genesis-init constructor.
// Both outputs own their account buffers; no database or global replay state is used.
func ConstructInitialBank(ctx context.Context, g *Genesis) (*InitialBank, error) {
	c, err := ValidateProfile(ctx, g)
	if err != nil {
		return nil, err
	}
	raw, err := Encode(g)
	if err != nil {
		return nil, err
	}
	// Derive all runtime state from the wire representation, including the
	// absent optional values and internal account identity/slot fields.
	g, _, err = Decode(raw)
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(raw)
	rent := g.Rent
	// SIMD-0194 doubles the rate while lowering the threshold, preserving balances.
	rent.LamportsPerByteYear *= 2
	rent.ExemptionThreshold = 1
	m := BankMetadata{Version: 1, Profile: Profile, AgaveRevision: AgaveRevision, GenesisHash: solana.Hash(hash).String(), BankHash: solana.Hash{}.String(), ParentBankHash: solana.Hash{}.String(), NextReplaySlot: 1, MaxTickHeight: g.TicksPerSlot, LastBlockhash: solana.Hash(hash).String(), BlockhashQueueMaxAge: 300, CreationTime: g.CreationTime.Unix(), TicksPerSlot: g.TicksPerSlot, NanosecondsPerSlot: uint64(g.PohParams.TickDuration) * g.TicksPerSlot, SlotsPerYear: 365.242199 * 24 * 60 * 60 / g.PohParams.TickDuration.Seconds() / float64(g.TicksPerSlot), Poh: g.PohParams, Rent: rent, Fees: g.Fees, Inflation: g.Inflation, EpochSchedule: g.EpochSchedule, Features: ProfileFeatures(), RecentBlockhashes: []Blockhash{{Hash: solana.Hash(hash).String()}}}
	var votes []EpochVote
	var total, maxStake uint64
	for _, v := range c.Validators {
		stake := v.StakeLamports - uint64(328*6960)
		total += stake
		votes = append(votes, EpochVote{VoteAccount: v.VoteAccount, Node: v.Identity, AuthorizedVoter: v.AuthorizedVoter, Stake: stake, Lamports: v.VoteLamports, BLSPublicKey: v.BLSPublicKey})
		if stake > maxStake {
			maxStake = stake
			m.Leader = v.Identity
		}
	}
	sort.Slice(votes, func(i, j int) bool {
		a := solana.MustPublicKeyFromBase58(votes[i].VoteAccount)
		b := solana.MustPublicKeyFromBase58(votes[j].VoteAccount)
		return bytes.Compare(a[:], b[:]) < 0
	})
	for epoch := uint64(0); epoch <= 1; epoch++ {
		m.EpochStakes = append(m.EpochStakes, EpochStake{Epoch: epoch, TotalStake: total, Votes: append([]EpochVote(nil), votes...)})
	}
	rows := make(map[solana.PublicKey]AccountEntry)
	for _, a := range cloneEntries(g.Accounts) {
		rows[a.Pubkey] = a
	}
	for key, name := range builtinAccounts {
		a := entry(key, addresses.NativeLoaderAddr, 1, []byte(name))
		a.Executable = true
		rows[key] = a
	}
	sysvar := func(key solana.PublicKey, data []byte, size int) {
		data = append(bytes.Clone(data), make([]byte, size-len(data))...)
		a := entry(key, addresses.SysvarOwnerAddr, uint64(128+size)*rent.LamportsPerByteYear, data)
		rows[key] = a
	}
	sysvar(sealevel.SysvarStakeHistoryAddr, le64(0), 16392)
	clock := sealevel.SysvarClock{EpochStartTimestamp: g.CreationTime.Unix(), LeaderScheduleEpoch: 1, UnixTimestamp: g.CreationTime.Unix()}
	sysvar(sealevel.SysvarClockAddr, clock.MustMarshal(), 40)
	sysvar(sealevel.SysvarRentAddr, encodeValue(rent), 17)
	sysvar(sealevel.SysvarEpochScheduleAddr, encodeValue(g.EpochSchedule), 33)
	recent := func(hashes []Blockhash) {
		data := le64(uint64(len(hashes)))
		for _, h := range hashes {
			hash := solana.MustHashFromBase58(h.Hash)
			data = append(data, hash[:]...)
			data = append(data, le64(h.LamportsPerSignature)...)
		}
		sysvar(sealevel.SysvarRecentBlockHashesAddr, data, 6008)
	}
	recent(m.RecentBlockhashes)
	sysvar(sealevel.SysvarLastRestartSlotAddr, le64(0), 8)
	materialize := func(metadata BankMetadata) (Bank, error) {
		b := Bank{Metadata: cloneMetadata(metadata)}
		var lt lthash.LtHash
		for _, a := range rows {
			if err := ctx.Err(); err != nil {
				return b, err
			}
			if math.MaxUint64-b.Metadata.Capitalization < a.Lamports {
				return b, fmt.Errorf("initial-bank capitalization overflows uint64")
			}
			b.Metadata.Capitalization += a.Lamports
			b.Metadata.AccountsDataLen += uint64(len(a.Data))
			var part lthash.LtHash
			part.InitWithAcct(&a.Account)
			lt.MixIn(&part)
			b.Accounts = append(b.Accounts, a)
		}
		b.Metadata.AccountsLtHash = bytes.Clone(lt.Hash())
		sortAccounts(b.Accounts)
		b.Accounts = cloneEntries(b.Accounts)
		return b, nil
	}
	initialized, err := materialize(m)
	if err != nil {
		return nil, err
	}
	// Agave create_ticks with hashes_per_tick=None performs zero hashes.
	// The terminal tick re-registers the genesis hash at index 1, replacing
	// its index-0 queue entry rather than adding a duplicate recent hash.
	last := hash
	for i := uint64(0); i < g.TicksPerSlot; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// Hashing is disabled in this pinned sleep-mode profile.
	}
	m.LastBlockhash = solana.Hash(last).String()
	m.TickHeight = g.TicksPerSlot
	m.Frozen = true
	m.RecentBlockhashes = []Blockhash{{Hash: m.LastBlockhash, HashIndex: 1}}
	recent(m.RecentBlockhashes)
	slotHistory := append([]byte{1}, le64(16384)...)
	slotHistory = append(slotHistory, le64(1)...)
	slotHistory = append(slotHistory, make([]byte, 131064)...)
	slotHistory = append(slotHistory, le64(1048576)...)
	slotHistory = append(slotHistory, le64(1)...)
	sysvar(sealevel.SysvarSlotHistoryAddr, slotHistory, 131097)
	frozen, err := materialize(m)
	if err != nil {
		return nil, err
	}
	inner := sha256.New()
	inner.Write(make([]byte, 32))
	inner.Write(le64(0))
	inner.Write(last[:])
	outer := sha256.New()
	outer.Write(inner.Sum(nil))
	outer.Write(frozen.Metadata.AccountsLtHash)
	frozen.Metadata.BankHash = solana.HashFromBytes(outer.Sum(nil)).String()
	return &InitialBank{Initialized: initialized, Frozen: frozen}, nil
}

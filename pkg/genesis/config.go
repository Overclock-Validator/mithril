package genesis

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"time"

	bls "github.com/Overclock-Validator/gnark-crypto/ecc/bls12-381"
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/pelletier/go-toml/v2"
)

const Profile = "agave-alpenglow-development-v1"
const AgaveRevision = "7e51da963aee49622a395f562386a6bd8ba0e717"
const MinimumVAT = uint64(1_600_000_000)

// The immutable profile contains only the fixed feature/config/certificate
// accounts and runtime parameters. It contains no validator or private keys.
// Regeneration requires an explicit interoperability-profile version change.
//
//go:embed profile/alpenglow-v1.bin
var profileBytes []byte

//go:embed profile/features.json
var featureBytes []byte

//go:embed profile/reserved.json
var reservedBytes []byte

type Feature struct {
	Address string `json:"address"`
	Name    string `json:"name"`
}

func ProfileFeatures() []Feature {
	var f []Feature
	if err := json.Unmarshal(featureBytes, &f); err != nil {
		panic(err)
	}
	return f
}

type Config struct {
	Profile      string `toml:"profile" json:"profile"`
	CreationTime string `toml:"creation_time" json:"creation_time"`
	// TestSlotsPerEpoch shortens the pinned non-warmup development schedule.
	// Zero preserves the oracle's default; features and slot duration stay pinned.
	TestSlotsPerEpoch uint64          `toml:"test_slots_per_epoch,omitempty" json:"test_slots_per_epoch,omitempty"`
	Accounts          []FundedAccount `toml:"accounts" json:"accounts"`
	Validators        []Validator     `toml:"validators" json:"validators"`
}
type FundedAccount struct {
	Address  string `toml:"address" json:"address"`
	Lamports uint64 `toml:"lamports" json:"lamports"`
}
type Validator struct {
	Identity                  string `toml:"identity" json:"identity"`
	VoteAccount               string `toml:"vote_account" json:"vote_account"`
	StakeAccount              string `toml:"stake_account" json:"stake_account"`
	AuthorizedVoter           string `toml:"authorized_voter" json:"authorized_voter"`
	AuthorizedWithdrawer      string `toml:"authorized_withdrawer" json:"authorized_withdrawer"`
	StakeStaker               string `toml:"stake_staker" json:"stake_staker"`
	StakeWithdrawer           string `toml:"stake_withdrawer" json:"stake_withdrawer"`
	InflationRewardsCollector string `toml:"inflation_rewards_collector" json:"inflation_rewards_collector"`
	BlockRevenueCollector     string `toml:"block_revenue_collector" json:"block_revenue_collector"`
	BLSPublicKey              string `toml:"bls_public_key" json:"bls_public_key"`
	IdentityLamports          uint64 `toml:"identity_lamports" json:"identity_lamports"`
	VoteLamports              uint64 `toml:"vote_lamports" json:"vote_lamports"`
	StakeLamports             uint64 `toml:"stake_lamports" json:"stake_lamports"`
	InflationCommissionBPS    uint16 `toml:"inflation_commission_bps" json:"inflation_commission_bps"`
	BlockCommissionBPS        uint16 `toml:"block_commission_bps" json:"block_commission_bps"`
}

func ParseConfig(r io.Reader) (Config, error) {
	var c Config
	data, err := io.ReadAll(io.LimitReader(r, MaxGenesisSize+1))
	if err != nil {
		return c, err
	}
	if len(data) > MaxGenesisSize {
		return c, fmt.Errorf("genesis configuration too large")
	}
	err = toml.NewDecoder(bytes.NewReader(data)).DisallowUnknownFields().Decode(&c)
	return c, err
}
func fixedProfile() (*Genesis, error) { g, _, err := Decode(profileBytes); return g, err }
func publicKey(s string) (solana.PublicKey, error) {
	key, err := solana.PublicKeyFromBase58(s)
	if err != nil || key.String() != s || key.IsZero() {
		return key, fmt.Errorf("invalid public key %q", s)
	}
	return key, nil
}
func decodeBLS(s string) ([48]byte, error) {
	var out [48]byte
	raw, err := hex.DecodeString(s)
	if err != nil || len(raw) != 48 {
		return out, fmt.Errorf("BLS public key must be 96 hexadecimal characters")
	}
	var point bls.G1Affine
	if _, err = point.SetBytes(raw); err != nil || point.IsInfinity() {
		return out, fmt.Errorf("invalid BLS public key")
	}
	copy(out[:], raw)
	return out, nil
}
func entry(key solana.PublicKey, owner solana.PublicKey, lamports uint64, data []byte) AccountEntry {
	return AccountEntry{Pubkey: key, Account: accounts.Account{Key: key, Owner: owner, Lamports: lamports, Data: data}}
}

// Build resolves the sole v1 profile and mints the explicit allocations. It has
// no wall-clock, network, storage or private-key generation dependencies.
func Build(ctx context.Context, in Config) (*Genesis, Config, error) {
	c := in
	c.Accounts = append([]FundedAccount(nil), in.Accounts...)
	c.Validators = append([]Validator(nil), in.Validators...)
	if c.Profile == "" {
		c.Profile = Profile
	}
	if c.Profile != Profile {
		return nil, c, fmt.Errorf("unsupported genesis profile %q", c.Profile)
	}
	when, err := time.Parse(time.RFC3339, c.CreationTime)
	if err != nil || when.Nanosecond() != 0 || when.Unix() < 0 {
		return nil, c, fmt.Errorf("creation_time must be an explicit nonnegative RFC3339 time with whole-second precision")
	}
	c.CreationTime = when.UTC().Format(time.RFC3339)
	if len(c.Validators) == 0 || len(c.Validators) > 2000 {
		return nil, c, fmt.Errorf("genesis requires 1..2000 bootstrap validators")
	}
	g, err := fixedProfile()
	if err != nil {
		return nil, c, err
	}
	g.CreationTime = when.UTC()
	if c.TestSlotsPerEpoch != 0 {
		if c.TestSlotsPerEpoch < sealevel.MinimumSlotsPerEpoch || c.TestSlotsPerEpoch > g.EpochSchedule.SlotPerEpoch {
			return nil, c, fmt.Errorf("test_slots_per_epoch must be between %d and %d", sealevel.MinimumSlotsPerEpoch, g.EpochSchedule.SlotPerEpoch)
		}
		g.EpochSchedule.SlotPerEpoch = c.TestSlotsPerEpoch
		g.EpochSchedule.LeaderScheduleSlotOffset = c.TestSlotsPerEpoch
	}
	seen := make(map[solana.PublicKey]bool)
	for _, a := range g.Accounts {
		seen[a.Pubkey] = true
	}
	for key := range bankReservedAccounts() {
		seen[key] = true
	}
	add := func(a AccountEntry) error {
		if seen[a.Pubkey] {
			return fmt.Errorf("duplicate or reserved account %s", a.Key)
		}
		seen[a.Pubkey] = true
		g.Accounts = append(g.Accounts, a)
		return nil
	}
	for _, a := range c.Accounts {
		if err := ctx.Err(); err != nil {
			return nil, c, err
		}
		key, err := publicKey(a.Address)
		if err != nil {
			return nil, c, err
		}
		if a.Lamports == 0 {
			return nil, c, fmt.Errorf("account %s has zero allocation", a.Address)
		}
		if err = add(entry(key, solana.SystemProgramID, a.Lamports, nil)); err != nil {
			return nil, c, err
		}
	}
	blsSeen := map[[48]byte]bool{}
	for i := range c.Validators {
		if err := ctx.Err(); err != nil {
			return nil, c, err
		}
		v := &c.Validators[i]
		for _, field := range []*string{&v.AuthorizedVoter, &v.AuthorizedWithdrawer, &v.StakeStaker, &v.StakeWithdrawer, &v.InflationRewardsCollector, &v.BlockRevenueCollector} {
			if *field == "" {
				*field = v.Identity
			}
		}
		keys := make([]solana.PublicKey, 9)
		for j, s := range []string{v.Identity, v.VoteAccount, v.StakeAccount, v.AuthorizedVoter, v.AuthorizedWithdrawer, v.StakeStaker, v.StakeWithdrawer, v.InflationRewardsCollector, v.BlockRevenueCollector} {
			keys[j], err = publicKey(s)
			if err != nil {
				return nil, c, err
			}
		}
		blskey, err := decodeBLS(v.BLSPublicKey)
		if err != nil {
			return nil, c, err
		}
		if blsSeen[blskey] {
			return nil, c, fmt.Errorf("duplicate validator BLS public key")
		}
		blsSeen[blskey] = true
		v.BLSPublicKey = hex.EncodeToString(blskey[:])
		if v.InflationCommissionBPS > 10000 || v.BlockCommissionBPS > 10000 {
			return nil, c, fmt.Errorf("commission exceeds 10000 basis points")
		}
		voteRent := uint64(128+sealevel.VoteStateV4Size) * 6960
		stakeRent := uint64(128+200) * 6960
		if v.IdentityLamports == 0 || v.VoteLamports < voteRent+MinimumVAT || v.StakeLamports < stakeRent+1_000_000_000 {
			return nil, c, fmt.Errorf("validator requires positive identity allocation, vote_lamports >= %d and stake_lamports >= %d", voteRent+MinimumVAT, stakeRent+1_000_000_000)
		}
		vote := sealevel.VoteStateVersions{Type: sealevel.VoteStateVersionV4, V4: sealevel.VoteState4{NodePubkey: keys[0], AuthorizedWithdrawer: keys[4], InflationRewardsCollector: keys[7], BlockRevenueCollector: keys[8], InflationRewardsCommissionBps: v.InflationCommissionBPS, BlockRevenueCommissionBps: v.BlockCommissionBPS, BlsPubkeyCompressed: &blskey}}
		vote.V4.AuthorizedVoters.AuthorizedVoters.Set(0, keys[3])
		voteData, err := sealevel.MarshalVersionedVoteState(&vote)
		if err != nil {
			return nil, c, err
		}
		stake := sealevel.StakeStateV2{Status: sealevel.StakeStateV2StatusStake, Stake: sealevel.StakeStateV2Stake{Meta: sealevel.Meta{RentExemptReserve: stakeRent, Authorized: sealevel.Authorized{Staker: keys[5], Withdrawer: keys[6]}}, Stake: sealevel.Stake{Delegation: sealevel.Delegation{VoterPubkey: keys[1], StakeLamports: v.StakeLamports - stakeRent, ActivationEpoch: math.MaxUint64, DeactivationEpoch: math.MaxUint64, WarmupCooldownRate: 0.25}}}}
		stakeData, err := sealevel.MarshalStakeStake(&stake)
		if err != nil {
			return nil, c, err
		}
		voteData = append(voteData, make([]byte, sealevel.VoteStateV4Size-len(voteData))...)
		stakeData = append(stakeData, make([]byte, 200-len(stakeData))...)
		for _, a := range []AccountEntry{entry(keys[0], solana.SystemProgramID, v.IdentityLamports, nil), entry(keys[1], solana.VoteProgramID, v.VoteLamports, voteData), entry(keys[2], solana.StakeProgramID, v.StakeLamports, stakeData)} {
			if err = add(a); err != nil {
				return nil, c, err
			}
		}
	}
	// Agave chooses a HashMap maximum here, so tied top stakes can select
	// different initial leaders. V1 requires a unique highest-staked validator.
	var highest uint64
	leaders := 0
	for _, v := range c.Validators {
		if v.StakeLamports > highest {
			highest = v.StakeLamports
			leaders = 1
		} else if v.StakeLamports == highest {
			leaders++
		}
	}
	if leaders != 1 {
		return nil, c, fmt.Errorf("v1 requires a unique highest-staked bootstrap validator (Agave does not define a tie-break)")
	}
	var total uint64
	for _, a := range g.Accounts {
		if math.MaxUint64-total < a.Lamports {
			return nil, c, fmt.Errorf("genesis capitalization overflows uint64")
		}
		total += a.Lamports
	}
	sort.Slice(c.Accounts, func(i, j int) bool { return c.Accounts[i].Address < c.Accounts[j].Address })
	sort.Slice(c.Validators, func(i, j int) bool { return c.Validators[i].Identity < c.Validators[j].Identity })
	sortAccounts(g.Accounts)
	return g, c, nil
}
func sortAccounts(a []AccountEntry) {
	sort.Slice(a, func(i, j int) bool { return bytes.Compare(a[i].Pubkey[:], a[j].Pubkey[:]) < 0 })
}

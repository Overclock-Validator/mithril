package genesis

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	bls "github.com/Overclock-Validator/gnark-crypto/ecc/bls12-381"
	"github.com/Overclock-Validator/mithril/pkg/runtime"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type oracleAccount struct {
	Pubkey     string `json:"pubkey"`
	Owner      string `json:"owner"`
	Lamports   uint64 `json:"lamports"`
	RentEpoch  uint64 `json:"rent_epoch"`
	Executable bool   `json:"executable"`
	Data       []byte `json:"data"`
}
type oracleBank struct {
	BankMetadata
	Accounts []oracleAccount `json:"accounts"`
	Rent     struct {
		Lamports  uint64  `json:"lamports_per_byte_year"`
		Threshold float64 `json:"exemption_threshold"`
		Burn      uint8   `json:"burn_percent"`
	} `json:"rent"`
	Fees struct {
		Target     uint64 `json:"targetLamportsPerSignature"`
		Signatures uint64 `json:"targetSignaturesPerSlot"`
		Min        uint64 `json:"minLamportsPerSignature"`
		Max        uint64 `json:"maxLamportsPerSignature"`
		Burn       uint8  `json:"burnPercent"`
	} `json:"fees"`
	Inflation struct {
		Initial, Terminal, Taper, Foundation float64
		FoundationTerm                       float64 `json:"foundationTerm"`
	} `json:"inflation"`
	Schedule struct {
		Slots      uint64 `json:"slotsPerEpoch"`
		Offset     uint64 `json:"leaderScheduleSlotOffset"`
		Warmup     bool   `json:"warmup"`
		FirstEpoch uint64 `json:"firstNormalEpoch"`
		FirstSlot  uint64 `json:"firstNormalSlot"`
	} `json:"epoch_schedule"`
}

func multiValidatorConfig() Config {
	c := testConfig()
	c.CreationTime = "2027-04-05T06:07:08Z"
	_, _, generator, _ := bls.Generators()
	key := generator.Bytes()
	c.Validators = append(c.Validators, Validator{Identity: testKey(10), VoteAccount: testKey(11), StakeAccount: testKey(12), BLSPublicKey: hex.EncodeToString(key[:]), AuthorizedVoter: testKey(13), AuthorizedWithdrawer: testKey(14), StakeStaker: testKey(15), StakeWithdrawer: testKey(16), InflationRewardsCollector: testKey(17), BlockRevenueCollector: testKey(18), IdentityLamports: 42_000_000_000, VoteLamports: 3_000_000_000, StakeLamports: 20_000_000_000, InflationCommissionBPS: 1234, BlockCommissionBPS: 4321})
	c.Accounts = append(c.Accounts, FundedAccount{Address: testKey(25), Lamports: 234567890})
	return c
}

// Committed oracle fixtures keep ordinary tests independent of Rust/Agave.
// Set MITHRIL_AGAVE_ORACLE to the pinned helper to independently recheck them.
func TestAgaveOracle(t *testing.T) {
	for name, c := range map[string]Config{"single": testConfig(), "multiple": multiValidatorConfig()} {
		t.Run(name, func(t *testing.T) {
			g, _, err := Build(context.Background(), c)
			require.NoError(t, err)
			bank, err := ConstructInitialBank(context.Background(), g)
			require.NoError(t, err)
			fixture := filepath.Join("testdata", name+".json.gz")
			var result struct {
				Revision    string     `json:"agave_revision"`
				GenesisHash string     `json:"genesis_hash"`
				Initialized oracleBank `json:"initialized"`
				Frozen      oracleBank `json:"frozen"`
			}
			if helper := os.Getenv("MITHRIL_AGAVE_ORACLE"); helper != "" {
				dir := t.TempDir()
				raw, err := Encode(g)
				require.NoError(t, err)
				input := filepath.Join(dir, "genesis.bin")
				output := filepath.Join(dir, "oracle.json")
				require.NoError(t, os.WriteFile(input, raw, 0644))
				cmd := exec.CommandContext(t.Context(), helper, input, output)
				log, err := cmd.CombinedOutput()
				require.NoError(t, err, string(log))
				data, err := os.ReadFile(output)
				require.NoError(t, err)
				require.NoError(t, json.Unmarshal(data, &result))
				if os.Getenv("MITHRIL_UPDATE_ORACLE_FIXTURES") == "1" {
					require.NoError(t, os.MkdirAll("testdata", 0755))
					f, err := os.Create(fixture)
					require.NoError(t, err)
					z := gzip.NewWriter(f)
					_, err = z.Write(data)
					require.NoError(t, err)
					require.NoError(t, z.Close())
					require.NoError(t, f.Close())
				}
			} else {
				f, err := os.Open(fixture)
				require.NoError(t, err)
				defer f.Close()
				z, err := gzip.NewReader(f)
				require.NoError(t, err)
				defer z.Close()
				require.NoError(t, json.NewDecoder(z).Decode(&result))
			}
			require.Equal(t, AgaveRevision, result.Revision)
			require.Equal(t, bank.Frozen.Metadata.GenesisHash, result.GenesisHash)
			for phase, pair := range map[string]struct {
				native Bank
				oracle oracleBank
			}{"initialized": {bank.Initialized, result.Initialized}, "frozen": {bank.Frozen, result.Frozen}} {
				t.Run(phase, func(t *testing.T) {
					n, o := pair.native, pair.oracle
					m := n.Metadata
					require.Len(t, n.Accounts, len(o.Accounts))
					for i, a := range o.Accounts {
						got := n.Accounts[i]
						require.Equal(t, a.Pubkey, got.Key.String())
						require.Equal(t, a.Owner, solana.PublicKey(got.Owner).String(), a.Pubkey)
						require.Equal(t, a.Lamports, got.Lamports, a.Pubkey)
						require.Equal(t, a.RentEpoch, got.RentEpoch, a.Pubkey)
						require.Equal(t, a.Executable, got.Executable, a.Pubkey)
						require.True(t, bytes.Equal(a.Data, got.Data), a.Pubkey)
					}
					require.Equal(t, o.BankHash, m.BankHash)
					require.Equal(t, o.AccountsLtHash, m.AccountsLtHash)
					require.Equal(t, o.Capitalization, m.Capitalization)
					require.Equal(t, o.AccountsDataLen, m.AccountsDataLen)
					require.Equal(t, o.LastBlockhash, m.LastBlockhash)
					require.Equal(t, o.RecentBlockhashes, m.RecentBlockhashes)
					require.Equal(t, o.BlockhashQueueMaxAge, m.BlockhashQueueMaxAge)
					require.Equal(t, o.EpochStakes, m.EpochStakes)
					require.Equal(t, o.TickHeight, m.TickHeight)
					require.Equal(t, o.MaxTickHeight, m.MaxTickHeight)
					require.Equal(t, o.TicksPerSlot, m.TicksPerSlot)
					require.Equal(t, o.NanosecondsPerSlot, m.NanosecondsPerSlot)
					require.Equal(t, o.Slot, m.Slot)
					require.Equal(t, o.SignatureCount, m.SignatureCount)
					require.Equal(t, o.TransactionCount, m.TransactionCount)
					require.Equal(t, o.BlockHeight, m.BlockHeight)
					require.Equal(t, o.ConsensusBlockID, m.ConsensusBlockID)
					require.Equal(t, o.Leader, m.Leader)
					require.Equal(t, o.SlotsPerYear, m.SlotsPerYear)
					require.Equal(t, o.LamportsPerSignature, m.LamportsPerSignature)
					require.Equal(t, runtime.RentParams{LamportsPerByteYear: o.Rent.Lamports, ExemptionThreshold: o.Rent.Threshold, BurnPercent: o.Rent.Burn}, m.Rent)
					require.Equal(t, runtime.FeeParams{TargetLamportsPerSig: o.Fees.Target, TargetSigsPerSlot: o.Fees.Signatures, MinLamportsPerSig: o.Fees.Min, MaxLamportsPerSig: o.Fees.Max, BurnPercent: o.Fees.Burn}, m.Fees)
					require.Equal(t, runtime.InflationParams{Initial: o.Inflation.Initial, Terminal: o.Inflation.Terminal, Taper: o.Inflation.Taper, Foundation: o.Inflation.Foundation, FoundationTerm: o.Inflation.FoundationTerm}, m.Inflation)
					require.Equal(t, runtime.EpochSchedule{SlotPerEpoch: o.Schedule.Slots, LeaderScheduleSlotOffset: o.Schedule.Offset, Warmup: o.Schedule.Warmup, FirstNormalEpoch: o.Schedule.FirstEpoch, FirstNormalSlot: o.Schedule.FirstSlot}, m.EpochSchedule)
				})
			}
		})
	}
}

package genesis

import (
	"context"
	"os"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func testKey(n byte) string {
	var k solana.PublicKey
	for i := range k {
		k[i] = n
	}
	return k.String()
}
func testConfig() Config {
	return Config{CreationTime: "2026-01-01T00:00:00Z", Accounts: []FundedAccount{{Address: testKey(4), Lamports: 1_000_000_000}}, Validators: []Validator{{Identity: testKey(1), VoteAccount: testKey(2), StakeAccount: testKey(3), BLSPublicKey: "a55384b86c60c0e2a13ffdf9087c94495455083acf06ff23d8e7416ee1df1cafccfaad802b527ade7dfe1b02a6bfd79a", IdentityLamports: 500_000_000_000, VoteLamports: 2_000_000_000, StakeLamports: 10_000_000_000}}}
}
func TestNativeBank(t *testing.T) {
	g, _, err := Build(context.Background(), testConfig())
	require.NoError(t, err)
	raw, err := Encode(g)
	require.NoError(t, err)
	bank, err := ConstructInitialBank(context.Background(), g)
	require.NoError(t, err)
	require.True(t, bank.Frozen.Metadata.Frozen)
	if path := os.Getenv("MITHRIL_GENESIS_TEST_OUTPUT"); path != "" {
		require.NoError(t, os.WriteFile(path, raw, 0644))
	}
	t.Logf("genesis %s bank %s cap %d data %d accounts %d", bank.Frozen.Metadata.GenesisHash, bank.Frozen.Metadata.BankHash, bank.Frozen.Metadata.Capitalization, bank.Frozen.Metadata.AccountsDataLen, len(bank.Frozen.Accounts))
}

func TestBankUsesCanonicalAccountIdentity(t *testing.T) {
	g, _, err := Build(t.Context(), testConfig())
	require.NoError(t, err)
	expected, err := ConstructInitialBank(t.Context(), g)
	require.NoError(t, err)
	// Internal replay fields have no genesis-wire representation.
	for i := range g.Accounts {
		g.Accounts[i].Key = solana.MustPublicKeyFromBase58(testKey(99))
		g.Accounts[i].Slot = 999
	}
	got, err := ConstructInitialBank(t.Context(), g)
	require.NoError(t, err)
	require.Equal(t, expected, got)
	// Returned stages do not share mutable account data.
	for i := range got.Initialized.Accounts {
		if len(got.Initialized.Accounts[i].Data) > 0 {
			got.Initialized.Accounts[i].Data[0] ^= 0xff
			break
		}
	}
	got.Initialized.Metadata.Features[0].Name = "changed"
	got.Initialized.Metadata.EpochStakes[0].Votes[0].Stake++
	got.Initialized.Metadata.AccountsLtHash[0] ^= 0xff
	got.Initialized.Metadata.RecentBlockhashes[0].HashIndex++
	require.Equal(t, expected.Frozen, got.Frozen)
}

func TestFastEpochScheduleIsExplicitAndValidated(t *testing.T) {
	defaultGenesis, _, err := Build(t.Context(), testConfig())
	require.NoError(t, err)
	for _, slots := range []uint64{0, 32, 64, defaultGenesis.EpochSchedule.SlotPerEpoch} {
		c := testConfig()
		c.TestSlotsPerEpoch = slots
		g, _, err := Build(t.Context(), c)
		require.NoError(t, err)
		_, err = ValidateProfile(t.Context(), g)
		require.NoError(t, err)
		want := slots
		if want == 0 {
			want = defaultGenesis.EpochSchedule.SlotPerEpoch
		}
		require.Equal(t, want, g.EpochSchedule.SlotPerEpoch)
		require.Equal(t, want, g.EpochSchedule.LeaderScheduleSlotOffset)
		require.False(t, g.EpochSchedule.Warmup)
		require.Equal(t, defaultGenesis.PohParams, g.PohParams)
		bank, err := ConstructInitialBank(t.Context(), g)
		require.NoError(t, err)
		require.Equal(t, ProfileFeatures(), bank.Frozen.Metadata.Features)
		g.EpochSchedule.LeaderScheduleSlotOffset++
		_, err = ValidateProfile(t.Context(), g)
		require.Error(t, err)
	}
	for _, slots := range []uint64{1, 31, defaultGenesis.EpochSchedule.SlotPerEpoch + 1, ^uint64(0)} {
		c := testConfig()
		c.TestSlotsPerEpoch = slots
		_, _, err := Build(t.Context(), c)
		require.ErrorContains(t, err, "test_slots_per_epoch")
	}
}

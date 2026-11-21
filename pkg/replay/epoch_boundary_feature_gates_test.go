package replay

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"os"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// Testnet epochs 1093 and 1094: the first executed bank of each epoch failed
// its footer check because the boundary activated a feature gate mithril did
// not know, so it neither wrote the activation into the feature account nor
// applied its effect:
//
//   - slot 466652256: set_lamports_per_byte_to_2575 (SIMD-0437). Agave also
//     rewrites Rent, and the epoch inflation and reward-epoch delegated stakes
//     accounts staged later in that bank are sized by the new Rent.
//   - slot 467084256: loader_v3_set_program_data_to_elf_length (SIMD-0433),
//     which changes only instruction behaviour; the block had no transactions.
//
// Starting from the captured broken accounts lt-hash, applying exactly those
// changes through the production helpers must recover each certified footer.
func TestTestnetEpochBoundaryFeatureGates(t *testing.T) {
	var fixture struct {
		Cases []struct {
			Slot                             uint64
			ParentBankhash                   solana.Hash       `json:"parent_bankhash"`
			Blockhash                        solana.Hash       `json:"blockhash"`
			Signatures                       uint64            `json:"num_signatures"`
			BrokenHash                       solana.Hash       `json:"broken_bankhash"`
			ExpectedHash                     solana.Hash       `json:"expected_bankhash"`
			BrokenLtHash                     []byte            `json:"broken_lthash"`
			FeatureParent                    *accounts.Account `json:"feature_parent"`
			RentParent                       *accounts.Account `json:"rent_parent"`
			BrokenEpochInflation             *accounts.Account `json:"broken_epoch_inflation"`
			BrokenRewardEpochDelegatedStakes *accounts.Account `json:"broken_reward_epoch_delegated_stakes"`
		}
	}
	raw, err := os.ReadFile("testdata/testnet_epoch_boundary_feature_gates.json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &fixture))
	require.Len(t, fixture.Cases, 2)

	for _, c := range fixture.Cases {
		lt := new(lthash.LtHash).InitWithHash(c.BrokenLtHash)
		bankHash := func() solana.Hash {
			input := append([]byte(nil), c.ParentBankhash[:]...)
			input = binary.LittleEndian.AppendUint64(input, c.Signatures)
			input = append(input, c.Blockhash[:]...)
			inner := sha256.Sum256(input)
			return solana.Hash(sha256.Sum256(append(inner[:], lt.Hash()...)))
		}
		replace := func(old, updated *accounts.Account) {
			lt.Sub(new(lthash.LtHash).InitWithAcct(old))
			lt.Add(new(lthash.LtHash).InitWithAcct(updated))
		}
		require.Equal(t, c.BrokenHash, bankHash(), "slot %d: fixture must reproduce the broken hash", c.Slot)

		// The gate must now be known, or scanAndEnableFeatures skips it.
		var gate *features.FeatureGate
		for i := range features.AllFeatureGates {
			if solana.PublicKey(features.AllFeatureGates[i].Address) == c.FeatureParent.Key {
				gate = &features.AllFeatureGates[i]
			}
		}
		require.NotNil(t, gate, "slot %d: feature %s is not registered", c.Slot, c.FeatureParent.Key)
		require.Nil(t, features.UnmarshalFeatureAcct(c.FeatureParent.Data).ActivatedAt)

		activated := c.FeatureParent.Clone()
		activated.Data, err = features.MarshalFeatureAcct(&features.FeatureAcct{ActivatedAt: &c.Slot})
		require.NoError(t, err)
		replace(c.FeatureParent, activated)

		lamportsPerByte, changesRent := rentLamportsPerByteForActivations(map[solana.PublicKey]struct{}{c.FeatureParent.Key: {}})
		require.Equal(t, c.RentParent != nil, changesRent, "slot %d", c.Slot)
		if changesRent {
			var parentRent sealevel.SysvarRent
			require.NoError(t, parentRent.UnmarshalWithDecoder(bin.NewBinDecoder(c.RentParent.Data)))
			updatedRent, parentRentAcct, previous, err := rentAccountWithLamportsPerByte(c.RentParent, parentRent, lamportsPerByte)
			require.NoError(t, err)
			require.Equal(t, uint64(5080), previous)
			require.Equal(t, uint64(2575), lamportsPerByte)
			replace(parentRentAcct, updatedRent)

			// stageEpochInflationAccount and stageRewardEpochDelegatedStakes size
			// these accounts with the cached Rent, which activation now updates
			// before handleEpochTransition runs.
			newRent := parentRent
			newRent.LamportsPerUint8Year = lamportsPerByte
			inflation := c.BrokenEpochInflation.Clone()
			inflation.Lamports = newRent.MinimumBalance(uint64(len(inflation.Data)))
			replace(c.BrokenEpochInflation, inflation)
			delegated := c.BrokenRewardEpochDelegatedStakes.Clone()
			delegated.Lamports = newRent.MinimumBalance(rewardEpochDelegatedStakesMaxDataLen())
			replace(c.BrokenRewardEpochDelegatedStakes, delegated)
		}

		require.Equal(t, c.ExpectedHash, bankHash(),
			"slot %d: activating %s must recover the certified footer", c.Slot, gate.Name)
	}
}

func TestRentLamportsPerByteForActivations(t *testing.T) {
	set := func(gates ...features.FeatureGate) map[solana.PublicKey]struct{} {
		m := make(map[solana.PublicKey]struct{}, len(gates))
		for _, g := range gates {
			m[g.Address] = struct{}{}
		}
		return m
	}
	for _, tc := range []struct {
		name    string
		active  map[solana.PublicKey]struct{}
		want    uint64
		changed bool
	}{
		{"none", set(), 0, false},
		{"unrelated gate", set(features.LoaderV3SetProgramDataToElfLength), 0, false},
		{"single", set(features.SetLamportsPerByteTo2575), 2575, true},
		{"several: lowest wins", set(features.SetLamportsPerByteTo6333, features.SetLamportsPerByteTo1322, features.SetLamportsPerByteTo5080), 1322, true},
		{"all SIMD-0437", set(features.SetLamportsPerByteTo6333, features.SetLamportsPerByteTo5080, features.SetLamportsPerByteTo2575, features.SetLamportsPerByteTo1322, features.SetLamportsPerByteTo696), 696, true},
		{"SIMD-0438 safeguard overrides", set(features.SetLamportsPerByteTo696, features.SetLamportsPerByteTo6960), 6960, true},
	} {
		got, changed := rentLamportsPerByteForActivations(tc.active)
		require.Equal(t, tc.changed, changed, tc.name)
		require.Equal(t, tc.want, got, tc.name)
	}
}

func TestRentAccountWithLamportsPerByteKeepsOtherFields(t *testing.T) {
	current := sealevel.SysvarRent{LamportsPerUint8Year: 5080, ExemptionThreshold: 1, BurnPercent: 50}
	acct := &accounts.Account{Key: solana.PublicKey(sealevel.SysvarRentAddr), Lamports: 1009200, Data: current.MustMarshal()}
	updated, parent, previous, err := rentAccountWithLamportsPerByte(acct, current, 2575)
	require.NoError(t, err)
	require.Equal(t, uint64(5080), previous)
	require.Equal(t, acct.Data, parent.Data, "parent image must be the unmodified account")
	require.Equal(t, acct.Lamports, updated.Lamports, "Agave inherits the sysvar's lamports")

	var got sealevel.SysvarRent
	require.NoError(t, got.UnmarshalWithDecoder(bin.NewBinDecoder(updated.Data)))
	require.Equal(t, sealevel.SysvarRent{LamportsPerUint8Year: 2575, ExemptionThreshold: 1, BurnPercent: 50}, got)

	_, _, _, err = rentAccountWithLamportsPerByte(&accounts.Account{Data: []byte{1, 2}}, current, 2575)
	require.Error(t, err, "an undecodable parent must not be overwritten silently")
}

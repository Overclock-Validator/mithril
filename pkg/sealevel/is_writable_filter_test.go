package sealevel

import (
	"math/rand"
	"testing"

	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/gagliardetto/solana-go"
)

// referenceIsWritable is IsWritable as it was before the key filter.
func referenceIsWritable(am *AccountMeta, f *features.Features) bool {
	if !am.IsWritable {
		return false
	}
	k := am.Pubkey
	if k == a.SystemProgramAddr || k == a.BpfLoaderUpgradeableAddr || k == a.BpfLoader2Addr || k == a.BpfLoaderDeprecatedAddr ||
		k == a.VoteProgramAddr || k == a.StakeProgramAddr || k == a.ConfigProgramAddr || k == a.StakeProgramConfigAddr ||
		k == a.NativeLoaderAddr {
		return false
	}
	if k == SysvarClockAddr || k == SysvarEpochScheduleAddr || k == SysvarFeesAddr || k == SysvarInstructionsAddr ||
		k == SysvarRecentBlockHashesAddr || k == SysvarRentAddr || k == a.SysvarRewardsAddr || k == SysvarSlotHashesAddr ||
		k == SysvarSlotHistoryAddr || k == SysvarStakeHistoryAddr {
		return false
	}
	if f.IsActive(features.AddNewReservedAccountKeys) {
		if _, ok := NewReservedAcctsSet[k]; ok {
			return false
		}
	}
	if f.IsActive(features.EnableSecp256r1Precompile) && k == a.Secp256r1PrecompileAddr {
		return false
	}
	return true
}

func TestIsWritableMatchesReference(t *testing.T) {
	special := []solana.PublicKey{a.Secp256r1PrecompileAddr}
	special = append(special, sysvarAddrs[:]...)
	special = append(special, nativeProgramAddrs[:]...)
	for k := range NewReservedAcctsSet {
		special = append(special, k)
	}
	rng := rand.New(rand.NewSource(11))
	keys := append([]solana.PublicKey(nil), special...)
	for i := 0; i < 200_000; i++ {
		var k solana.PublicKey
		rng.Read(k[:])
		if i%4 == 0 {
			// Share the filter bits of a special key so the full check runs.
			s := special[rng.Intn(len(special))]
			k[0], k[1] = s[0], s[1]
		}
		keys = append(keys, k)
	}
	for _, reserved := range []bool{false, true} {
		for _, r1 := range []bool{false, true} {
			f := features.NewFeaturesDefault()
			if reserved {
				f.EnableFeature(features.AddNewReservedAccountKeys, 0)
			}
			if r1 {
				f.EnableFeature(features.EnableSecp256r1Precompile, 0)
			}
			for _, k := range keys {
				for _, w := range []bool{false, true} {
					am := &AccountMeta{Pubkey: k, IsWritable: w}
					if got, want := IsWritable(am, f), referenceIsWritable(am, f); got != want {
						t.Fatalf("IsWritable(%s, writable=%v, reserved=%v, r1=%v) = %v, want %v", k, w, reserved, r1, got, want)
					}
				}
			}
		}
	}
	for _, k := range special {
		if !IsNativeProgram(k) && !IsSysvar(k) {
			if _, ok := NewReservedAcctsSet[k]; !ok && k != a.Secp256r1PrecompileAddr {
				t.Fatalf("special key %s not recognised", k)
			}
		}
	}
}

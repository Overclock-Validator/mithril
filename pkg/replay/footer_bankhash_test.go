package replay

import (
	"testing"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/stretchr/testify/require"
)

func TestVerifyFooterBankHashMatch(t *testing.T) {
	var hash [32]byte
	hash[0] = 0xab
	block := &b.Block{Slot: 42, HasExpectedBankhash: true, ExpectedBankhash: hash}
	slotCtx := &sealevel.SlotCtx{Slot: 42, FinalBankhash: hash[:]}
	require.NoError(t, verifyFooterBankHash(slotCtx, block))
}

func TestVerifyFooterBankHashMismatch(t *testing.T) {
	var expected, computed [32]byte
	expected[0] = 0x01
	computed[0] = 0x02
	block := &b.Block{Slot: 42, HasExpectedBankhash: true, ExpectedBankhash: expected}
	slotCtx := &sealevel.SlotCtx{Slot: 42, FinalBankhash: computed[:]}
	err := verifyFooterBankHash(slotCtx, block)
	require.Error(t, err)
	require.Contains(t, err.Error(), "footer bank hash mismatch")
}

func TestVerifyFooterBankHashSkippedWhenUnset(t *testing.T) {
	block := &b.Block{Slot: 42}
	slotCtx := &sealevel.SlotCtx{Slot: 42, FinalBankhash: []byte{1}}
	require.NoError(t, verifyFooterBankHash(slotCtx, block))
	require.NoError(t, verifyFooterBankHash(nil, block))
}

func TestRequireAlpenglowBlockFooter(t *testing.T) {
	ft := features.NewFeaturesDefault()
	ft.EnableFeature(features.Alpenglow, 1)
	slotCtx := &sealevel.SlotCtx{Features: ft}

	require.False(t, requireAlpenglowBlockFooter(&b.Block{Slot: 1, FromLiveStream: true}, slotCtx, false))
	require.False(t, requireAlpenglowBlockFooter(&b.Block{Slot: 1, FromLiveStream: true, IsSkipped: true}, slotCtx, true))
	require.False(t, requireAlpenglowBlockFooter(&b.Block{Slot: 1}, slotCtx, true))
	require.True(t, requireAlpenglowBlockFooter(&b.Block{Slot: 1, FromLiveStream: true}, slotCtx, true))
}

func TestVerifyAlpenglowBlockFooterMissing(t *testing.T) {
	ft := features.NewFeaturesDefault()
	ft.EnableFeature(features.Alpenglow, 1)
	slotCtx := &sealevel.SlotCtx{Slot: 99, Features: ft, FinalBankhash: []byte{1}}
	block := &b.Block{Slot: 99, FromLiveStream: true}

	err := verifyAlpenglowBlockFooter(slotCtx, block, true, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing block footer")
}

func TestVerifyAlpenglowBlockFooterRequiresBankHash(t *testing.T) {
	ft := features.NewFeaturesDefault()
	ft.EnableFeature(features.Alpenglow, 1)
	slotCtx := &sealevel.SlotCtx{Slot: 99, Features: ft, FinalBankhash: []byte{1}}
	block := &b.Block{Slot: 99, FromLiveStream: true, HasAlpenglowFooter: true}

	err := verifyAlpenglowBlockFooter(slotCtx, block, true, false)
	require.ErrorContains(t, err, "block footer is missing its bank hash")
}

func TestVerifyAlpenglowBlockFooterAllowsLeaderBlocks(t *testing.T) {
	ft := features.NewFeaturesDefault()
	ft.EnableFeature(features.Alpenglow, 1)
	slotCtx := &sealevel.SlotCtx{Slot: 99, Features: ft, FinalBankhash: []byte{1}}
	block := &b.Block{Slot: 99}

	require.NoError(t, verifyAlpenglowBlockFooter(slotCtx, block, true, false))
}

// The selected certified-footer policy must fail closed even if feature metadata
// is absent, or an unexpected non-shred block reaches replay.
func TestCertifiedFooterPolicyRequiresShredIdentityAndBankHash(t *testing.T) {
	good := b.Block{Slot: 99, FromLiveStream: true, HasAlpenglowBlockID: true,
		AlpenglowBlockID: [32]byte{7}, HasAlpenglowFooter: true,
		HasExpectedBankhash: true, ExpectedBankhash: [32]byte{1}}
	ctx := &sealevel.SlotCtx{Slot: 99, FinalBankhash: good.ExpectedBankhash[:]}
	for _, tc := range []struct {
		name   string
		change func(*b.Block)
		want   string
	}{
		{"valid without feature metadata", func(*b.Block) {}, ""},
		{"not shred sourced", func(b *b.Block) { b.FromLiveStream = false }, "shred-derived block identity"},
		{"missing identity", func(b *b.Block) { b.HasAlpenglowBlockID = false }, "shred-derived block identity"},
		{"zero identity", func(b *b.Block) { b.AlpenglowBlockID = [32]byte{} }, "shred-derived block identity"},
		{"missing footer", func(b *b.Block) { b.HasAlpenglowFooter = false }, "missing block footer"},
		{"missing bank hash", func(b *b.Block) { b.HasExpectedBankhash = false }, "missing its bank hash"},
		{"wrong bank hash", func(b *b.Block) { b.ExpectedBankhash[0]++ }, "footer bank hash mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			block := good
			tc.change(&block)
			err := verifyAlpenglowBlockFooter(ctx, &block, true, true)
			if tc.want == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.want)
			}
		})
	}
	for _, block := range []*b.Block{
		{Slot: 99, IsSkipped: true},
		{Slot: 99, FromLocalProduction: true},
	} {
		require.NoError(t, verifyAlpenglowBlockFooter(ctx, block, true, true))
	}
}

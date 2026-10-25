package replay

import (
	"compress/gzip"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"testing"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/blockstream"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestMissingFooterPreflightScope(t *testing.T) {
	for _, tc := range []struct {
		name         string
		block        *b.Block
		wantRejected bool
	}{
		{"missing footer", &b.Block{Slot: 99, FromLiveStream: true}, true},
		{"present footer", &b.Block{Slot: 99, FromLiveStream: true, HasAlpenglowFooter: true}, false},
		{"skipped slot", &b.Block{Slot: 99, FromLiveStream: true, IsSkipped: true}, false},
		{"local leader", &b.Block{Slot: 99, FromLiveStream: true, FromLocalProduction: true}, false},
		{"non-live block", &b.Block{Slot: 99}, false},
		{"nil", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			rejected, err := quarantineMissingAlpenglowFooter(tc.block, nil, func(got *b.Block) error {
				calls++
				require.Same(t, tc.block, got)
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, tc.wantRejected, rejected)
			if tc.wantRejected {
				require.Equal(t, 1, calls)
			} else {
				require.Zero(t, calls)
			}
		})
	}
}

func TestMissingFooterPreflightDiscardsStreamBeforeQuarantine(t *testing.T) {
	StreamingExecutionCfg = StreamingExecutionConfig{Enabled: true}
	defer func() { StreamingExecutionCfg = StreamingExecutionConfig{} }()
	h := newFinalizeFailureHarness(t)
	block := h.matchingBlock(t, h.txs)
	called := false
	rejected, err := quarantineMissingAlpenglowFooter(block, h.exec, func(got *b.Block) error {
		called = true
		require.Same(t, block, got)
		h.assertUndone(t, "missing_footer")
		return nil
	})
	require.NoError(t, err)
	require.True(t, rejected)
	require.True(t, called)
}

func TestMissingFooterPreflightFailsClosedWhenQuarantineFails(t *testing.T) {
	for _, cause := range []error{blockstream.ErrBlockNotQuarantinable, errors.New("candidate conflicts with certified state")} {
		block := &b.Block{Slot: 99, FromLiveStream: true}
		rejected, err := quarantineMissingAlpenglowFooter(block, nil, func(*b.Block) error { return cause })
		require.True(t, rejected)
		require.ErrorIs(t, err, cause)
		require.ErrorContains(t, err, "slot 99 missing block footer")
	}
}

// Captured on 2026-10-26 from mainnet's verified shred spool. The candidate at
// 461653846 ended without a footer; finalized slot 461653848 instead builds on
// 461653843. Preserve the original packet order, CRCs, coding shreds and mixed
// FEC variants so this exercises the real assembler, including FEC recovery.
func TestMissingFooterPreflightCapturedMainnetCandidate(t *testing.T) {
	f, err := os.Open("testdata/mainnet_slot_461653846.shreds.gz")
	require.NoError(t, err)
	defer f.Close()
	gz, err := gzip.NewReader(f)
	require.NoError(t, err)
	defer gz.Close()
	raw, err := io.ReadAll(gz)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(raw), 8)
	require.Equal(t, "MITHSP\x02\x00", string(raw[:8]))
	assembler := turbine.NewSlotAssembler()
	var candidate *b.Block
	for off := 8; off < len(raw); {
		require.GreaterOrEqual(t, len(raw)-off, 8)
		n := int(binary.LittleEndian.Uint32(raw[off:]))
		crc := binary.LittleEndian.Uint32(raw[off+4:])
		off += 8
		require.GreaterOrEqual(t, len(raw)-off, n)
		packet := raw[off : off+n]
		off += n
		require.Equal(t, crc, crc32.ChecksumIEEE(packet))
		block, err := assembler.AddPacket(packet)
		if err != nil {
			require.ErrorContains(t, err, "mixed FEC signatures")
		}
		if block != nil {
			require.Nil(t, candidate, "only one candidate should assemble")
			candidate = block
		}
	}
	require.NotNil(t, candidate)
	require.Equal(t, uint64(461653846), candidate.Slot)
	require.Equal(t, uint64(461653845), candidate.SourceParentSlot)
	require.Equal(t, "GZKU7fvTgAV9gjjaUnwPsZnfxWYaeoMUtdU7mvYBPdu6", solana.Hash(candidate.AlpenglowBlockID).String())
	require.False(t, candidate.HasAlpenglowFooter)
	require.Len(t, candidate.Transactions, 3)
	// Empty reward-certificate bytes do not reject a missing footer.
	require.NoError(t, validatePreConsensusRewardCertificates(candidate, &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 432000}, 50093))
	var quarantined *b.Block
	rejected, err := quarantineMissingAlpenglowFooter(candidate, nil, func(got *b.Block) error {
		quarantined = got
		return nil
	})
	require.NoError(t, err)
	require.True(t, rejected)
	require.Same(t, candidate, quarantined)
	// The execution-time safety checks still reject it if preflight is bypassed.
	ft := features.NewFeaturesDefault()
	ft.EnableFeature(features.Alpenglow, 1)
	slotCtx := &sealevel.SlotCtx{Slot: candidate.Slot, Features: ft}
	require.ErrorContains(t, validateAlpenglowFooterNanosecondClock(slotCtx, candidate), "missing block footer")
	require.ErrorContains(t, verifyAlpenglowBlockFooter(slotCtx, candidate, true), "missing block footer")
}

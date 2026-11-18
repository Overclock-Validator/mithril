package sealevel

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func voteMigrationClock() *accounts.Account {
	return &accounts.Account{Key: voteAlpenglowClockAddr, Owner: a.SystemProgramAddr, Lamports: 1, Data: make([]byte, 8)}
}

func TestVoteAlpenglowMigrationRequiresParentMarker(t *testing.T) {
	require.Equal(t, "GaGQ2vyb3xuUwjKiq9tQQWoqhzWYh7LbiLXvqG2sUzzG", voteAlpenglowClockAddr.String())
	for _, tt := range []struct {
		name       string
		gate       features.FeatureGate
		parent     *accounts.Account
		current    *accounts.Account
		parentView bool
		want       bool
	}{
		{name: "before feature activation", parent: voteMigrationClock(), parentView: true},
		{name: "feature enabled before migration", gate: features.Alpenglow, parentView: true},
		{name: "prefunded empty PDA", gate: features.Alpenglow, parent: &accounts.Account{Key: voteAlpenglowClockAddr, Lamports: 1}, parentView: true},
		{name: "post migration", gate: features.Alpenglow, parent: voteMigrationClock(), parentView: true, want: true},
		{name: "dev context post migration", gate: features.AlpenglowDevContext, parent: voteMigrationClock(), parentView: true, want: true},
		{name: "child marker does not override parent absence", gate: features.Alpenglow, current: voteMigrationClock(), parentView: true},
		{name: "parent marker survives child tombstone", gate: features.Alpenglow, parent: voteMigrationClock(), current: &accounts.Account{Key: voteAlpenglowClockAddr}, parentView: true, want: true},
		{name: "isolated bank marker", gate: features.Alpenglow, current: voteMigrationClock(), want: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			vb := newVoteBench(false)
			if tt.gate != (features.FeatureGate{}) {
				vb.execCtx.Features.EnableFeature(tt.gate, 0)
			}
			vb.execCtx.SlotCtx = &SlotCtx{Accounts: vb.execCtx.Accounts}
			if tt.parentView {
				vb.execCtx.SlotCtx.ParentAccts = accounts.NewMemAccounts()
			}
			if tt.parent != nil {
				require.NoError(t, vb.execCtx.SlotCtx.ParentAccts.SetAccountWithoutLock(tt.parent.Key, tt.parent))
			}
			if tt.current != nil {
				require.NoError(t, vb.execCtx.SlotCtx.Accounts.SetAccountWithoutLock(tt.current.Key, tt.current))
			}
			require.Equal(t, tt.want, voteAlpenglowMigrationSucceeded(vb.execCtx))
			err := vb.step()
			if tt.want {
				require.ErrorIs(t, err, InstrErrInvalidInstructionData)
				require.Equal(t, vb.initial, vb.txAccts.Accounts[1].Data)
			} else {
				require.NoError(t, err, "valid historical TowerSync must still execute before migration")
				require.NotEqual(t, vb.initial, vb.txAccts.Accounts[1].Data)
			}
		})
	}
}

func TestVoteRejectsLegacyInstructionsAfterAlpenglow(t *testing.T) {
	// These are all the variants Agave rejects after Alpenglow migration.
	// Their payloads deserialize successfully, but need no Clock/SlotHashes
	// because the migration check must precede semantic vote processing.
	for _, kind := range []uint32{
		VoteProgramInstrTypeVote, VoteProgramInstrTypeVoteSwitch,
		VoteProgramInstrTypeUpdateVoteState, VoteProgramInstrTypeUpdateVoteStateSwitch,
		VoteProgramInstrTypeCompactUpdateVoteState, VoteProgramInstrTypeCompactUpdateVoteStateSwitch,
		VoteProgramInstrTypeTowerSync, VoteProgramInstrTypeTowerSyncSwitch,
	} {
		vb := newVoteBench(false)
		vb.execCtx.Features.EnableFeature(features.Alpenglow, 0)
		parents := accounts.NewMemAccounts()
		require.NoError(t, parents.SetAccountWithoutLock(voteAlpenglowClockAddr, voteMigrationClock()))
		vb.execCtx.SlotCtx = &SlotCtx{Accounts: accounts.NewMemAccounts(), ParentAccts: parents}
		data := binary.LittleEndian.AppendUint32(nil, kind)
		switch kind {
		case VoteProgramInstrTypeVote, VoteProgramInstrTypeVoteSwitch:
			data = binary.LittleEndian.AppendUint64(data, 0) // empty slots
			data = append(data, make([]byte, 32)...)
			data = append(data, 0) // no timestamp
		case VoteProgramInstrTypeUpdateVoteState, VoteProgramInstrTypeUpdateVoteStateSwitch:
			data = binary.LittleEndian.AppendUint64(data, 0) // empty lockouts
			data = append(data, 0)                           // no root
			data = append(data, make([]byte, 32)...)
			data = append(data, 0) // no timestamp
		default:
			data = binary.LittleEndian.AppendUint64(data, math.MaxUint64) // no root
			data = append(data, 0)                                        // empty compact lockouts
			data = append(data, make([]byte, 32)...)
			data = append(data, 0) // no timestamp
			if kind == VoteProgramInstrTypeTowerSync || kind == VoteProgramInstrTypeTowerSyncSwitch {
				data = append(data, make([]byte, 32)...) // block ID
			}
		}
		switch kind {
		case VoteProgramInstrTypeVoteSwitch, VoteProgramInstrTypeUpdateVoteStateSwitch,
			VoteProgramInstrTypeCompactUpdateVoteStateSwitch, VoteProgramInstrTypeTowerSyncSwitch:
			data = append(data, make([]byte, 32)...) // switch proof
		}
		vb.instr = data
		require.ErrorIs(t, vb.step(), InstrErrInvalidInstructionData, "variant %d", kind)
		require.Equal(t, vb.initial, vb.txAccts.Accounts[1].Data, "variant %d", kind)
	}
}

func TestVoteMigrationDoesNotRejectAccountManagement(t *testing.T) {
	vb := newVoteBench(false)
	vb.execCtx.Features.EnableFeature(features.Alpenglow, 0)
	parents := accounts.NewMemAccounts()
	require.NoError(t, parents.SetAccountWithoutLock(voteAlpenglowClockAddr, voteMigrationClock()))
	vb.execCtx.SlotCtx = &SlotCtx{Accounts: vb.execCtx.Accounts, ParentAccts: parents}
	// The benchmark's authorized voter is also its authorized withdrawer.
	// Updating the validator identity remains permitted after migration.
	vb.instr = binary.LittleEndian.AppendUint32(nil, VoteProgramInstrTypeUpdateValidatorIdentity)
	require.NoError(t, vb.step())
	state, err := UnmarshalVersionedVoteState(vb.txAccts.Accounts[1].Data)
	require.NoError(t, err)
	require.Equal(t, solana.PublicKey(vb.txAccts.Accounts[2].Key), state.ConvertToCurrent().NodePubkey)
}

func TestTowerSyncTimestampOptionIsStrict(t *testing.T) {
	for _, kind := range []uint32{VoteProgramInstrTypeTowerSync, VoteProgramInstrTypeTowerSyncSwitch} {
		for _, marker := range []byte{0, 1, 2, 255} {
			vb := newVoteBench(false)
			binary.LittleEndian.PutUint32(vb.instr, kind)
			optionOffset := len(vb.instr) - 32 - 8 - 1
			vb.instr[optionOffset] = marker
			if marker == 0 {
				vb.instr = append(vb.instr[:optionOffset+1], vb.instr[optionOffset+9:]...)
			}
			if kind == VoteProgramInstrTypeTowerSyncSwitch {
				vb.instr = append(vb.instr, make([]byte, 32)...)
			}
			err := vb.step()
			if marker > 1 {
				require.ErrorIs(t, err, InstrErrInvalidInstructionData, "variant %d marker %d", kind, marker)
				require.Equal(t, vb.initial, vb.txAccts.Accounts[1].Data)
			} else {
				require.NoError(t, err, "variant %d marker %d", kind, marker)
			}
		}
	}
}

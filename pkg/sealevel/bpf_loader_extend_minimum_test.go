package sealevel

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/Overclock-Validator/mithril/fixtures"
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/features"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// Match Agave v4.2's extend_program checks, including the maximum-size
// exception and error precedence. Reference: anza-xyz/agave at
// 961d762037f1e92a022e8f369cf7ca79ca703b88, programs/bpf_loader/src/lib.rs.
func TestExtendProgramMinimumSize(t *testing.T) {
	const minimum = 10 * 1024
	const ordinarySize = 64 * 1024
	for _, tt := range []struct {
		name       string
		enabled    bool
		oldLen     int
		additional uint32
		wantErr    error
	}{
		{"legacy small extension", false, ordinarySize, 1, nil},
		{"zero extension", true, ordinarySize, 0, InstrErrInvalidInstructionData},
		{"one byte", true, ordinarySize, 1, InstrErrInvalidArgument},
		{"decimal 10KB", true, ordinarySize, 10000, InstrErrInvalidArgument},
		{"below minimum", true, ordinarySize, minimum - 1, InstrErrInvalidArgument},
		{"minimum", true, ordinarySize, minimum, nil},
		{"above minimum", true, ordinarySize, minimum + 1, nil},
		{"exact minimum headroom rejected", true, MaxPermittedDataLength - minimum, minimum - 1, InstrErrInvalidArgument},
		{"exact minimum headroom filled", true, MaxPermittedDataLength - minimum, minimum, nil},
		{"partial headroom", true, MaxPermittedDataLength - 100, 99, InstrErrInvalidArgument},
		{"fill headroom", true, MaxPermittedDataLength - 100, 100, nil},
		{"fill last byte", true, MaxPermittedDataLength - 1, 1, nil},
		{"exceed headroom", true, MaxPermittedDataLength - 100, 101, InstrErrInvalidRealloc},
		{"minimum exceeds headroom", true, MaxPermittedDataLength - 100, minimum, InstrErrInvalidRealloc},
		{"already at maximum", true, MaxPermittedDataLength, 1, InstrErrInvalidRealloc},
	} {
		t.Run(tt.name, func(t *testing.T) {
			authority := solana.PublicKey{1}
			programDataKey := solana.PublicKey{2}
			programKey := solana.PublicKey{3}
			marshalState := func(state UpgradeableLoaderState) []byte {
				var buf bytes.Buffer
				require.NoError(t, state.MarshalWithEncoder(bin.NewBinEncoder(&buf)))
				return buf.Bytes()
			}
			data := make([]byte, tt.oldLen)
			copy(data, marshalState(UpgradeableLoaderState{
				Type:        UpgradeableLoaderStateTypeProgramData,
				ProgramData: UpgradeableLoaderStateProgramData{Slot: 1, UpgradeAuthorityAddress: &authority},
			}))
			elf := fixtures.Load(t, "sbpf", "noop_aligned.so")
			require.LessOrEqual(t, len(elf)+upgradeableLoaderSizeOfProgramDataMetaData, len(data))
			copy(data[upgradeableLoaderSizeOfProgramDataMetaData:], elf)
			programData := accounts.Account{Key: programDataKey, Owner: a.BpfLoaderUpgradeableAddr, Data: data, Lamports: 1_000_000_000_000}
			program := accounts.Account{Key: programKey, Owner: a.BpfLoaderUpgradeableAddr, Executable: true, Data: marshalState(UpgradeableLoaderState{
				Type:    UpgradeableLoaderStateTypeProgram,
				Program: UpgradeableLoaderStateProgram{ProgramDataAddress: programDataKey},
			})}
			loader := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Owner: a.NativeLoaderAddr, Executable: true}
			txAccounts := NewTransactionAccounts([]accounts.Account{loader, programData, program})
			txCtx := NewTransactionCtx(*txAccounts, 5, 64)
			execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault(), Features: *features.NewFeaturesDefault(), Accounts: accounts.NewMemAccounts()}
			if tt.enabled {
				execCtx.Features.EnableFeature(features.LoaderV3MinimumExtendProgramSize, 0)
			}
			require.NoError(t, execCtx.Accounts.SetAccount(&SysvarClockAddr, &accounts.Account{Lamports: 1}))
			WriteClockSysvar(&execCtx.Accounts, SysvarClock{Slot: 2})
			require.NoError(t, execCtx.Accounts.SetAccount(&SysvarRentAddr, &accounts.Account{Lamports: 1}))
			WriteRentSysvar(&execCtx.Accounts, SysvarRent{LamportsPerUint8Year: 1, ExemptionThreshold: 1})
			initializeLegacyBankFixture(t, &execCtx)
			instructionAccounts := InstructionAcctsFromAccountMetas([]AccountMeta{
				{Pubkey: programDataKey, IsWritable: true},
				{Pubkey: programKey, IsWritable: true},
			}, *txAccounts)
			instruction := binary.LittleEndian.AppendUint32(nil, UpgradeableLoaderInstrTypeExtendProgram)
			instruction = binary.LittleEndian.AppendUint32(instruction, tt.additional)
			err := execCtx.ProcessInstruction(instruction, instructionAccounts, []uint64{0})
			require.Equal(t, tt.wantErr, err)
			post, err := txCtx.Accounts.GetAccount(1)
			require.NoError(t, err)
			if tt.wantErr != nil {
				require.Equal(t, data, post.Data, "rejected extension must not change program data")
				require.Equal(t, programData.Lamports, post.Lamports)
				return
			}
			require.Len(t, post.Data, tt.oldLen+int(tt.additional))
			require.Equal(t, data[upgradeableLoaderSizeOfProgramDataMetaData:], post.Data[upgradeableLoaderSizeOfProgramDataMetaData:tt.oldLen])
			require.Equal(t, make([]byte, tt.additional), post.Data[tt.oldLen:])
			state, err := UnmarshalUpgradeableLoaderState(post.Data)
			require.NoError(t, err)
			require.Equal(t, uint64(2), state.ProgramData.Slot)
			require.Equal(t, authority, *state.ProgramData.UpgradeAuthorityAddress)
		})
	}
}

package sealevel

import (
	"bytes"
	"encoding/binary"
	"math"
	"strconv"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/features"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// Loader v4 uses the same bincode consumed-byte budget as the upgradeable loader:
// https://github.com/anza-xyz/agave/blob/v2.2.20/programs/loader-v4/src/lib.rs
func TestLoaderV4WriteDecodeLimit(t *testing.T) {
	for _, tt := range []struct {
		name     string
		bytesLen uint64
		payload  int
		trailing int
		wantErr  error
	}{
		{name: "empty"},
		{name: "exact limit", bytesLen: 1216, payload: 1216},
		{name: "over limit", bytesLen: 1217, payload: 1217, wantErr: InstrErrInvalidInstructionData},
		{name: "v1 oversized write", bytesLen: 3758, payload: 3758, wantErr: InstrErrInvalidInstructionData},
		{name: "empty with oversized trailing bytes", trailing: 4096},
		{name: "exact limit with trailing bytes", bytesLen: 1216, payload: 1216, trailing: 4096},
		{name: "overflowing vector length", bytesLen: math.MaxUint64, wantErr: InstrErrInvalidInstructionData},
	} {
		t.Run(tt.name, func(t *testing.T) {
			data := binary.LittleEndian.AppendUint32(nil, LoaderV4InstrTypeWrite)
			data = binary.LittleEndian.AppendUint32(data, 9)
			data = binary.LittleEndian.AppendUint64(data, tt.bytesLen)
			data = append(data, bytes.Repeat([]byte{0xAB}, tt.payload)...)
			data = append(data, bytes.Repeat([]byte{0xCD}, tt.trailing)...)
			decoder := bin.NewBinDecoder(data)
			_, err := decoder.ReadUint32(bin.LE)
			require.NoError(t, err)
			var write LoaderV4Write
			err = write.UnmarshalWithDecoder(decoder)
			require.ErrorIs(t, err, tt.wantErr)
			if err == nil {
				require.Equal(t, uint32(9), write.Offset)
				require.Equal(t, bytes.Repeat([]byte{0xAB}, tt.payload), write.Bytes)
				require.Equal(t, uint(16+tt.payload), decoder.Position())
			}
		})
	}
	data := binary.LittleEndian.AppendUint32(nil, 0)
	data = binary.LittleEndian.AppendUint64(data, 3)
	data = append(data, 1, 2)
	var write LoaderV4Write
	require.Error(t, write.UnmarshalWithDecoder(bin.NewBinDecoder(data)), "a truncated vector must not decode")
}

func loaderV4WriteTestContext(t *testing.T, payloadLen int) (*ExecutionCtx, []InstructionAccount, []byte) {
	t.Helper()
	authority := solana.PublicKey{1}
	programKey := solana.PublicKey{2}
	state := LoaderV4State{AuthorityAddrOrNextVersion: authority, Status: LoaderV4StatusRetracted}
	programData := append(state.Marshal(), make([]byte, payloadLen+9)...)
	loader := accounts.Account{Key: a.LoaderV4Addr, Owner: a.NativeLoaderAddr, Executable: true}
	program := accounts.Account{Key: programKey, Owner: a.LoaderV4Addr, Lamports: 1_000_000_000, Data: append([]byte(nil), programData...)}
	signer := accounts.Account{Key: authority, Lamports: 1_000_000_000}
	txCtx := NewTransactionCtx(*NewTransactionAccounts([]accounts.Account{loader, program, signer}), 5, 64)
	feats := features.NewFeaturesDefault()
	feats.EnableFeature(features.EnableLoaderV4, 0)
	execCtx := &ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault(), Features: *feats}
	instructionAccounts := InstructionAcctsFromAccountMetas([]AccountMeta{
		{Pubkey: programKey, IsWritable: true},
		{Pubkey: authority, IsSigner: true},
	}, txCtx.Accounts)
	return execCtx, instructionAccounts, programData
}

func TestLoaderV4WriteLimitBeforeMutation(t *testing.T) {
	for _, payloadLen := range []int{1216, 1217, 3758} {
		t.Run(strconv.Itoa(payloadLen), func(t *testing.T) {
			execCtx, instructionAccounts, originalData := loaderV4WriteTestContext(t, payloadLen)
			data := binary.LittleEndian.AppendUint32(nil, LoaderV4InstrTypeWrite)
			data = binary.LittleEndian.AppendUint32(data, 9)
			data = binary.LittleEndian.AppendUint64(data, uint64(payloadLen))
			payload := bytes.Repeat([]byte{0xAB}, payloadLen)
			data = append(data, payload...)
			err := execCtx.ProcessInstruction(data, instructionAccounts, []uint64{0})
			post, acctErr := execCtx.TransactionContext.Accounts.GetAccount(1)
			require.NoError(t, acctErr)
			require.Equal(t, uint64(cu.CULoaderV4ComputeUnits), execCtx.ComputeMeter.Used())
			if payloadLen > 1216 {
				require.ErrorIs(t, err, InstrErrInvalidInstructionData)
				require.Equal(t, originalData, post.Data, "rejected write must preserve the entire program")
			} else {
				require.NoError(t, err)
				require.Equal(t, originalData[:loaderV4ProgramDataOffset+9], post.Data[:loaderV4ProgramDataOffset+9])
				require.Equal(t, payload, post.Data[loaderV4ProgramDataOffset+9:])
			}
		})
	}
}

func TestLoaderV4RejectsMalformedDiscriminants(t *testing.T) {
	for _, data := range [][]byte{nil, {LoaderV4InstrTypeDeploy}, {0, 1, 0, 0}, {255, 0, 0, 0}} {
		execCtx, instructionAccounts, originalData := loaderV4WriteTestContext(t, 0)
		require.ErrorIs(t, execCtx.ProcessInstruction(data, instructionAccounts, []uint64{0}), InstrErrInvalidInstructionData)
		post, err := execCtx.TransactionContext.Accounts.GetAccount(1)
		require.NoError(t, err)
		require.Equal(t, originalData, post.Data)
	}
}

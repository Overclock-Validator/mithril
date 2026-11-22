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

// Agave v4.4.0-alpha.5 uses limited_deserialize(..., PACKET_DATA_SIZE)
// before inspecting loader accounts. The limit counts consumed bytes and
// includes the 16-byte Write header; trailing bytes are explicitly allowed.
// https://github.com/anza-xyz/agave/blob/v4.4.0-alpha.5/programs/bpf_loader/src/lib.rs#L147
func TestUpgradeableLoaderWriteDecodeLimit(t *testing.T) {
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
			data := binary.LittleEndian.AppendUint32(nil, UpgradeableLoaderInstrTypeWrite)
			data = binary.LittleEndian.AppendUint32(data, 9)
			data = binary.LittleEndian.AppendUint64(data, tt.bytesLen)
			data = append(data, bytes.Repeat([]byte{0xAB}, tt.payload)...)
			data = append(data, bytes.Repeat([]byte{0xCD}, tt.trailing)...)
			decoder := bin.NewBinDecoder(data)
			_, err := decoder.ReadUint32(bin.LE)
			require.NoError(t, err)
			var write UpgradeableLoaderInstrWrite
			err = write.UnmarshalWithDecoder(decoder)
			require.ErrorIs(t, err, tt.wantErr)
			if err == nil {
				require.Equal(t, uint32(9), write.Offset)
				require.Equal(t, bytes.Repeat([]byte{0xAB}, tt.payload), write.Bytes)
				require.Equal(t, uint(16+tt.payload), decoder.Position())
			}
		})
	}

	data := binary.LittleEndian.AppendUint32(nil, UpgradeableLoaderInstrTypeWrite)
	data = binary.LittleEndian.AppendUint32(data, 0)
	data = binary.LittleEndian.AppendUint64(data, 3)
	data = append(data, 1, 2)
	decoder := bin.NewBinDecoder(data)
	_, err := decoder.ReadUint32(bin.LE)
	require.NoError(t, err)
	var write UpgradeableLoaderInstrWrite
	require.Error(t, write.UnmarshalWithDecoder(decoder), "a truncated vector must not decode")
}

func TestUpgradeableLoaderWriteLimitBeforeMutation(t *testing.T) {
	for _, payloadLen := range []int{1216, 1217, 3758} {
		t.Run(strconv.Itoa(payloadLen), func(t *testing.T) {
			authority := solana.PublicKey{1}
			bufferKey := solana.PublicKey{2}
			var state bytes.Buffer
			require.NoError(t, (&UpgradeableLoaderState{
				Type:   UpgradeableLoaderStateTypeBuffer,
				Buffer: UpgradeableLoaderStateBuffer{AuthorityAddress: &authority},
			}).MarshalWithEncoder(bin.NewBinEncoder(&state)))
			bufferData := make([]byte, upgradeableLoaderSizeOfBufferMetaData+payloadLen)
			copy(bufferData, state.Bytes())
			loader := accounts.Account{Key: a.BpfLoaderUpgradeableAddr, Owner: a.NativeLoaderAddr, Executable: true}
			buffer := accounts.Account{Key: bufferKey, Owner: a.BpfLoaderUpgradeableAddr, Lamports: 1_000_000_000, Data: bufferData}
			signer := accounts.Account{Key: authority, Lamports: 1_000_000_000}
			txCtx := NewTransactionCtx(*NewTransactionAccounts([]accounts.Account{loader, buffer, signer}), 5, 64)
			execCtx := ExecutionCtx{TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeterDefault(), Features: *features.NewFeaturesDefault()}
			instructionAccounts := InstructionAcctsFromAccountMetas([]AccountMeta{
				{Pubkey: bufferKey, IsWritable: true},
				{Pubkey: authority, IsSigner: true},
			}, txCtx.Accounts)
			data := binary.LittleEndian.AppendUint32(nil, UpgradeableLoaderInstrTypeWrite)
			data = binary.LittleEndian.AppendUint32(data, 0)
			data = binary.LittleEndian.AppendUint64(data, uint64(payloadLen))
			payload := bytes.Repeat([]byte{0xAB}, payloadLen)
			data = append(data, payload...)
			err := execCtx.ProcessInstruction(data, instructionAccounts, []uint64{0})
			post, acctErr := txCtx.Accounts.GetAccount(1)
			require.NoError(t, acctErr)
			require.Equal(t, uint64(2370), execCtx.ComputeMeter.Used())
			if payloadLen > 1216 {
				require.ErrorIs(t, err, InstrErrInvalidInstructionData)
				require.Equal(t, bufferData, post.Data, "rejected write must preserve the entire buffer")
			} else {
				require.NoError(t, err)
				require.Equal(t, payload, post.Data[upgradeableLoaderSizeOfBufferMetaData:])
			}
		})
	}
}

package sealevel

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/base58"
	"github.com/Overclock-Validator/mithril/pkg/solana"
)

const SysvarInstructionsAddrStr = "Sysvar1nstructions1111111111111111111111111"

var SysvarInstructionsAddr = base58.MustDecodeFromString(SysvarInstructionsAddrStr)

var instructionSysvarAcctMetaIsSigner = byte(0b00000001)
var instructionSysvarAcctMetaIsWritable = byte(0b00000010)

var ErrInstructionsSysvarInstructionIndexOutOfBounds = errors.New("instructions sysvar instruction index out of bounds")

func instructionsMarshaledSize(instructions []Instruction) (uint64, error) {
	var marshaledSize uint64

	marshaledSize += 2                             // num_instructions
	marshaledSize += uint64(2 * len(instructions)) // instruction offsets

	for _, instr := range instructions {
		// The instructions sysvar stores every instruction's start offset as a
		// u16. Match Agave's construct_instructions_data behavior: the final
		// instruction may extend beyond 64 KiB, but no instruction may start
		// beyond the largest representable offset.
		if marshaledSize > math.MaxUint16 {
			return 0, ErrInstructionsSysvarInstructionIndexOutOfBounds
		}

		marshaledSize += 2                                                          // num_accounts
		marshaledSize += uint64(len(instr.Accounts) * (1 + solana.PublicKeyLength)) // flags (i.e. is_signer, is_writeable) + pubkey len

		marshaledSize += uint64(32 + // program_id pubkey
			2 + // instr_data_len
			len(instr.Data))
	}

	marshaledSize += 2 // current_instr_id

	return marshaledSize, nil
}

func marshalInstructions(instructions []Instruction) ([]byte, error) {
	serializedLen, err := instructionsMarshaledSize(instructions)
	if err != nil {
		return nil, err
	}
	data := make([]byte, serializedLen)

	var offset uint64

	// num_instructions
	binary.LittleEndian.PutUint16(data[offset:], uint16(len(instructions)))
	offset += 2

	serializedInstrOffset := offset

	// instruction offsets
	offset += 2 * uint64(len(instructions))

	for _, instr := range instructions {
		binary.LittleEndian.PutUint16(data[serializedInstrOffset:], uint16(offset))
		serializedInstrOffset += 2

		binary.LittleEndian.PutUint16(data[offset:], uint16(len(instr.Accounts)))
		offset += 2

		for _, acctMeta := range instr.Accounts {
			// flags
			var acctMetaFlags byte
			if acctMeta.IsSigner {
				acctMetaFlags = acctMetaFlags | instructionSysvarAcctMetaIsSigner
			}
			if acctMeta.IsWritable {
				acctMetaFlags = acctMetaFlags | instructionSysvarAcctMetaIsWritable
			}
			data[offset] = acctMetaFlags
			offset += 1

			// pubkey
			copy(data[offset:], acctMeta.Pubkey[:])
			offset += solana.PublicKeyLength
		}

		// program_id pubkey
		copy(data[offset:], instr.ProgramId[:])
		offset += solana.PublicKeyLength

		// instr data len
		binary.LittleEndian.PutUint16(data[offset:], uint16(len(instr.Data)))
		offset += 2

		// instr data
		copy(data[offset:], instr.Data)
		offset += uint64(len(instr.Data))
	}

	binary.LittleEndian.PutUint16(data[offset:], 0)

	return data, nil
}

func MakeInstructionsSysvarAccount(instructions []Instruction) (*accounts.Account, error) {
	serializedData, err := marshalInstructions(instructions)
	if err != nil {
		return nil, err
	}

	instructionsAcct := accounts.Account{}
	instructionsAcct.Key = SysvarInstructionsAddr
	instructionsAcct.Lamports = 0
	instructionsAcct.Data = serializedData
	instructionsAcct.RentEpoch = 0
	instructionsAcct.Executable = false
	instructionsAcct.Owner = a.SysvarOwnerAddr

	return &instructionsAcct, nil
}

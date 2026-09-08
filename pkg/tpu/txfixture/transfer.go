package txfixture

import (
	"encoding/binary"
	"fmt"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/programs/system"
)

var (
	payerWallet = solana.NewWallet()
	destWallet  = solana.NewWallet()
)

// PayerPubkey returns the funded signer used by transfer fixtures.
func PayerPubkey() solana.PublicKey {
	return payerWallet.PublicKey()
}

// SignedV1Wire returns a structurally valid, signed SIMD-0385 transaction.
// dataLen can be used to exercise the larger v1 QUIC admission boundary; seq
// is encoded into the payload so fixtures have distinct signatures.
func SignedV1Wire(seq uint64, dataLen int) ([]byte, error) {
	if dataLen < 0 {
		return nil, fmt.Errorf("negative instruction data length %d", dataLen)
	}
	data := make([]byte, dataLen)
	if len(data) >= 8 {
		binary.LittleEndian.PutUint64(data, seq)
	} else {
		for i := range data {
			data[i] = byte(seq >> (8 * i))
		}
	}

	msg := solana.Message{
		Header: solana.MessageHeader{
			NumRequiredSignatures:       1,
			NumReadonlyUnsignedAccounts: 1,
		},
		AccountKeys:     solana.PublicKeySlice{payerWallet.PublicKey(), solana.SystemProgramID},
		RecentBlockhash: solana.Hash{},
		Instructions: []solana.CompiledInstruction{{
			ProgramIDIndex: 1,
			Accounts:       []uint16{0},
			Data:           data,
		}},
	}
	if _, err := msg.SetVersion(solana.MessageVersionV1); err != nil {
		return nil, fmt.Errorf("set v1 message version: %w", err)
	}
	tx := &solana.Transaction{Message: msg}
	if _, err := tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
		if payerWallet.PublicKey().Equals(key) {
			return &payerWallet.PrivateKey
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("sign v1 transaction: %w", err)
	}
	return tx.MarshalBinary()
}

// MustSignedV1Wire panics if SignedV1Wire fails.
func MustSignedV1Wire(seq uint64, dataLen int) []byte {
	wire, err := SignedV1Wire(seq, dataLen)
	if err != nil {
		panic(err)
	}
	return wire
}

// DestPubkey returns the transfer destination used by transfer fixtures.
func DestPubkey() solana.PublicKey {
	return destWallet.PublicKey()
}

// TestBlockhash is the recent blockhash used by SignedTransferWire fixtures.
func TestBlockhash() solana.Hash {
	return solana.Hash{}
}

// PayerPrivateKey returns the signer used by transfer fixtures.
func PayerPrivateKey() solana.PrivateKey {
	return payerWallet.PrivateKey
}

// SignedTransferWire returns a valid signed system-transfer transaction wire.
// seq varies lamports so each transaction has a distinct signature.
func SignedTransferWire(seq uint64) ([]byte, error) {
	tx, err := solana.NewTransaction(
		[]solana.Instruction{
			system.NewTransferInstruction(1+(seq%1_000_000), payerWallet.PublicKey(), destWallet.PublicKey()).Build(),
		},
		solana.Hash{},
		solana.TransactionPayer(payerWallet.PublicKey()),
	)
	if err != nil {
		return nil, fmt.Errorf("build transfer tx: %w", err)
	}

	_, err = tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
		if payerWallet.PublicKey().Equals(key) {
			return &payerWallet.PrivateKey
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("sign transfer tx: %w", err)
	}

	return tx.MarshalBinary()
}

// MustSignedTransferWire panics if SignedTransferWire fails.
func MustSignedTransferWire(seq uint64) []byte {
	wire, err := SignedTransferWire(seq)
	if err != nil {
		panic(err)
	}
	return wire
}

// PrecomputeTransferPool builds n distinct signed transfer wire transactions.
func PrecomputeTransferPool(n int) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		out[i] = MustSignedTransferWire(uint64(i))
	}
	return out
}

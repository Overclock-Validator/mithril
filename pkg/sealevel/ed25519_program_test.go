package sealevel

import (
	stded25519 "crypto/ed25519"

	"crypto/rand"
	"encoding/binary"
	"testing"

	"filippo.io/edwards25519"
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// These drive the whole precompile -- offsets parsing and predicate together --
// through ProcessInstruction, rather than calling the verifier underneath it.
// The 3479 Firedancer fixtures in conformance/ cover the same path far more
// broadly, but they need a ~7 GB external corpus, so they skip in an ordinary
// "go test ./...". These cost microseconds and always run.
//
// On the case that is deliberately absent: verify_strict accepts a
// non-canonical A and hashes its original bytes, and it would be natural to
// pin that here. It cannot be done. A non-canonical encoding requires y < 19,
// and every curve point with such a y whose discrete log is computable is
// already small-order, so it is rejected by the small-order gate before
// canonicality is ever consulted. Every one of the 152 non-canonical-A fixtures
// in the Firedancer corpus expects an error for exactly that reason. A test
// written for this bullet would pass identically whether or not the
// implementation handles non-canonical A correctly, so it would look like
// coverage while proving nothing.

// buildEd25519Instruction lays out precompile instruction data for one
// signature. Field order matches Ed25519SignatureOffsets: signature_offset,
// signature_instruction_index, public_key_offset, public_key_instruction_index,
// message_data_offset, message_data_size, message_instruction_index. Index
// 0xffff means "this instruction's own data".
func buildEd25519Instruction(pubkey, signature, message []byte) []byte {
	const currentInstruction = 0xFFFF
	base := SignatureOffsetStarts + SignatureOffsetsSerializedSize

	data := make([]byte, 0, base+len(pubkey)+len(signature)+len(message))
	data = append(data, 1, 0) // one signature, then one padding byte

	put := func(v int) { data = binary.LittleEndian.AppendUint16(data, uint16(v)) }
	put(base + len(pubkey))                  // signature_offset
	put(currentInstruction)                  //
	put(base)                                // public_key_offset
	put(currentInstruction)                  //
	put(base + len(pubkey) + len(signature)) // message_data_offset
	put(len(message))                        // message_data_size
	put(currentInstruction)                  //

	data = append(data, pubkey...)
	data = append(data, signature...)
	return append(data, message...)
}

// runEd25519Precompile dispatches instruction data the same way the runtime
// does, so program-id routing and the instruction stack are exercised too.
func runEd25519Precompile(t *testing.T, data []byte) error {
	t.Helper()
	return runEd25519PrecompileWithStrict(t, data, true)
}

// runEd25519PrecompileWithStrict lets a caller drive the pre-activation branch.
// Ed25519PrecompileVerifyStrict gates the predicate, and blocks from before it
// activated were validated without it, so historical replay has to be able to
// reach the non-strict path.
func runEd25519PrecompileWithStrict(t *testing.T, data []byte, strict bool) error {
	t.Helper()

	programAcct := accounts.Account{
		Key:        solana.PublicKeyFromBytes(a.Ed25519PrecompileAddr[:]),
		Lamports:   1,
		Data:       []byte{},
		Owner:      a.NativeLoaderAddr,
		Executable: true,
	}
	txAccts := NewTransactionAccounts([]accounts.Account{programAcct})
	txCtx := NewTransactionCtx(*txAccts, 5, 64)
	txCtx.AllInstructions = append(txCtx.AllInstructions, Instruction{Data: data})

	execCtx := ExecutionCtx{
		TransactionContext: txCtx,
		ComputeMeter:       cu.NewComputeMeter(200000),
		Log:                &LogRecorder{},
	}
	execCtx.Accounts = accounts.NewMemAccounts()
	execCtx.Features = *features.NewFeaturesDefault()
	if strict {
		execCtx.Features.EnableFeature(features.Ed25519PrecompileVerifyStrict, 0)
	}

	return execCtx.ProcessInstruction(data, []InstructionAccount{}, []uint64{0})
}

func TestEd25519PrecompileAcceptsAValidSignature(t *testing.T) {
	pub, priv, err := stded25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	message := []byte("ed25519 precompile end to end")
	sig := stded25519.Sign(priv, message)

	require.NoError(t, runEd25519Precompile(t, buildEd25519Instruction(pub, sig, message)),
		"a valid signature must be accepted")
}

// Both encodings below are order-4 points, taken from the fourteen strings a
// permissive decoder maps into the 8-torsion subgroup. verify_strict rejects
// them before evaluating the equation, which is the entire difference between
// the strict predicate and a plain stdlib verify.
func TestEd25519PrecompileRejectsSmallOrderPoints(t *testing.T) {
	pub, priv, err := stded25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	message := []byte("ed25519 precompile end to end")
	sig := stded25519.Sign(priv, message)

	smallOrder := make([]byte, PubkeySerializedSize) // y = 0, canonical spelling

	t.Run("small-order A", func(t *testing.T) {
		err := runEd25519Precompile(t, buildEd25519Instruction(smallOrder, sig, message))
		require.ErrorIs(t, err, PrecompileErrSignature,
			"a small-order public key must be rejected")
	})

	t.Run("small-order R", func(t *testing.T) {
		spliced := make([]byte, SignatureSerializedSize)
		copy(spliced, smallOrder)
		copy(spliced[32:], sig[32:])

		err := runEd25519Precompile(t, buildEd25519Instruction(pub, spliced, message))
		require.ErrorIs(t, err, PrecompileErrSignature,
			"a small-order R must be rejected")
	})
}

// A tampered signature is well-formed and decodes cleanly, so it fails on the
// equation rather than on any byte-level gate. This separates "the predicate
// rejects malformed input" from "the arithmetic actually runs".
func TestEd25519PrecompileRejectsATamperedSignature(t *testing.T) {
	pub, priv, err := stded25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	message := []byte("ed25519 precompile end to end")
	sig := stded25519.Sign(priv, message)
	sig[40] ^= 0x01

	err = runEd25519Precompile(t, buildEd25519Instruction(pub, sig, message))
	require.ErrorIs(t, err, PrecompileErrSignature,
		"a tampered signature must fail the equation")
}

// smallOrderForgery builds a signature the standard library accepts and the
// strict predicate rejects: A is the identity, so [s]B - [k]A collapses to
// [s]B, which is exactly the R the signature carries. Nothing about it is
// forged in the usual sense -- it satisfies the equation. It is rejected only
// because strict verification refuses a small-order public key.
func smallOrderForgery(t *testing.T) (pub [32]byte, sig []byte) {
	t.Helper()

	pub[0] = 1 // canonical identity: y = 1, sign bit clear

	uniform := make([]byte, 64)
	_, err := rand.Read(uniform)
	require.NoError(t, err)
	s, err := edwards25519.NewScalar().SetUniformBytes(uniform)
	require.NoError(t, err)

	r := (&edwards25519.Point{}).ScalarBaseMult(s)

	sig = make([]byte, SignatureSerializedSize)
	copy(sig[:32], r.Bytes())
	copy(sig[32:], s.Bytes())
	return pub, sig
}

// The feature gate is the whole difference between the two branches, so pin it
// with one input and both feature states. Historical replay depends on the
// inactive branch staying non-strict: blocks from before activation were
// validated that way, and re-verifying them strictly would reject transactions
// the network accepted, producing a different bank hash.
//
// This also guards the failure mode that actually bit us. When the conformance
// fixtures parsed with an empty feature set, the gate read inactive, the
// non-strict branch ran, and 163 signatures were accepted that should have been
// rejected. A feature-plumbing bug became an acceptance change. Pinning both
// directions means that shows up as a test failure rather than a quiet fork.
func TestEd25519PrecompileFeatureGateSelectsThePredicate(t *testing.T) {
	message := []byte("historical replay")
	pub, sig := smallOrderForgery(t)
	data := buildEd25519Instruction(pub[:], sig, message)

	require.True(t, stded25519.Verify(pub[:], message, sig),
		"premise: this signature satisfies the stdlib equation")

	t.Run("feature active rejects it", func(t *testing.T) {
		require.ErrorIs(t, runEd25519PrecompileWithStrict(t, data, true), PrecompileErrSignature,
			"strict verification must refuse a small-order public key")
	})

	t.Run("feature inactive accepts it", func(t *testing.T) {
		require.NoError(t, runEd25519PrecompileWithStrict(t, data, false),
			"pre-activation replay must reproduce the non-strict predicate")
	})
}

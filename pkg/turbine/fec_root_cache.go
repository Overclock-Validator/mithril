package turbine

import (
	"bytes"

	"github.com/gagliardetto/solana-go"
)

// One snapshot per FEC generation binds an authenticated root to its source.
// It is written only during assembly, before the state is frozen. Completion
// never trusts pointer identity alone or changes the deterministic root choice.
type authenticatedFECRoot struct {
	source  *Shred
	input   shredRootInput
	payload []byte
	root    solana.Hash
}

// These are every parsed field read by MerkleRoot. The exact payload comparison
// covers headers, data, proof, and trailers, even for noncanonical test input.
type shredRootInput struct {
	variant             byte
	kind                ShredType
	index, fecSetIndex  uint32
	dataCount, position uint16
}

func rootInput(s *Shred) shredRootInput {
	return shredRootInput{s.Variant, s.Type, s.Index, s.FECSetIndex, s.NumDataShreds, s.Position}
}

func hasMerkleRootProof(s *Shred) bool {
	return s != nil && isMerkleVariant(s.Variant) && (s.Type == ShredTypeData || s.Type == ShredTypeCode)
}

func (c *authenticatedFECRoot) matches(s *Shred) bool {
	return s == c.source && rootInput(s) == c.input && bytes.Equal(s.Payload, c.payload)
}

func (f *fecState) rememberAuthenticatedRoot(s *Shred, root solana.Hash) {
	if s.Recovered || !isMerkleVariant(s.Variant) {
		return
	}
	if cached := f.rootCache; cached != nil {
		// Data proofs precede coding proofs, and the lowest index wins. An
		// unauthenticated earlier arrival can still force fallback at completion.
		old := cached.input
		if old.kind == ShredTypeData && (s.Type != ShredTypeData || s.Index >= old.index) {
			return
		}
		if old.kind == ShredTypeCode && s.Type == ShredTypeCode && s.Position >= old.position {
			return
		}
	}
	f.rootCache = &authenticatedFECRoot{source: s, input: rootInput(s), payload: bytes.Clone(s.Payload), root: root}
}

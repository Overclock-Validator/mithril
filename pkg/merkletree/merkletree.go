// Package merkletree is a binary SHA256 Merkle tree.
//
// The binary Merkle tree contains two types of nodes:
// leaves (type 0x00) and intermediate nodes (type 0x01).
//
// The leaf hash is a hash of its data, prefixed by a zero byte:
// `SHA256(0x00 || leaf)`.
//
// The intermediate hash is a hash of the child hashes, prefixed by a one byte:
// `SHA256(0x01 || left || right)`
//
// # Construction
//
// Solana consensus relies on deterministic construction of Merkle trees.
//
// The "canoical" construction method arranges the tree into "layers" (identified by node distance to root),
// with the lowest layer always consisting of leaf nodes.
//
// If the lowest layer contains more than one leaf, recursively construct upper layers
// of intermediate nodes that each hash a pair of two of the lower layer's nodes in order.
//
// When any upper layer hashes an uneven amount of nodes,
// the last intermediate node shall hash the same lower node twice.
package merkletree

import "crypto/sha256"

// One byte type prefix used as a hash domain.
const (
	TypeLeaf         = 0x00
	TypeIntermediate = 0x01
)

// Nodes stores the hashes of all nodes, excluding node type and content.
type Nodes struct {
	NumLeaves uint
	Nodes     [][32]byte
}

// GetRoot returns the root hash.
// Returns zero if tree is empty.
func (n *Nodes) GetRoot() (out *[32]byte) {
	if len(n.Nodes) == 0 {
		return nil
	}
	return &n.Nodes[len(n.Nodes)-1]
}

// HashRoot computes the same root as HashNodes without retaining proof nodes.
// Empty input returns zero. Each level overwrites the preceding level, so only
// one hash per leaf is allocated. Leaves are never modified.
func HashRoot(leaves [][]byte) (root [32]byte) {
	if len(leaves) == 0 {
		return root
	}
	if len(leaves) == 1 {
		return HashLeaf(leaves[0])
	}
	nodes := make([][32]byte, len(leaves))
	for i, leaf := range leaves {
		nodes[i] = HashLeaf(leaf)
	}
	for len(nodes) > 1 {
		for i := 0; i < len(nodes); i += 2 {
			right := min(i+1, len(nodes)-1)
			nodes[i/2] = HashIntermediate(&nodes[i], &nodes[right])
		}
		nodes = nodes[:(len(nodes)+1)/2]
	}
	return nodes[0]
}

// HashNodes constructs proof data from a set of leaves.
//
// Port of solana_merkle_tree::MerkleTree::new
func HashNodes(leaves [][]byte) (out Nodes) {
	// Construct lowest layer by hashing every leaf.
	out.NumLeaves = uint(len(leaves))
	for _, leaf := range leaves {
		out.Nodes = append(out.Nodes, HashLeaf(leaf))
	}

	// Iteratively hash into upper layers until we reach the root.
	levelLen := nextLevelLen(out.NumLeaves)
	levelOff := out.NumLeaves // offset in node list of cur level
	prevLevelLen := out.NumLeaves
	prevLevelOff := uint(0) // offset in node list of prev level
	for levelLen > 0 {
		for i := uint(0); i < levelLen; i++ {
			prevLevelIdx := i * 2
			prevLevelNodeIdx := prevLevelOff + prevLevelIdx

			// Read back two nodes from previous layer.
			var left, right *[32]byte
			left = &out.Nodes[prevLevelNodeIdx]
			if prevLevelIdx+1 < prevLevelLen {
				right = &out.Nodes[prevLevelNodeIdx+1]
			} else {
				// Only one node left in the lower layer,
				// therefore hash remaining node twice.
				right = left
			}

			// Construct intermediate node.
			node := HashIntermediate(left, right)
			out.Nodes = append(out.Nodes, node)
		}

		// Move on to next layer.
		prevLevelOff = levelOff
		prevLevelLen = levelLen
		levelOff += levelLen
		levelLen = nextLevelLen(levelLen)
	}

	return
}

// HashLeaf returns the hash of a leaf node.
func HashLeaf(data []byte) (out [32]byte) {
	if len(data) == 64 {
		// Transaction signatures are fixed-width leaves. A single buffer avoids
		// incremental hash writes while retaining the leaf domain separator.
		var input [65]byte
		input[0] = TypeLeaf
		copy(input[1:], data)
		return sha256.Sum256(input[:])
	}
	h := sha256.New()
	h.Write([]byte{TypeLeaf})
	h.Write(data)
	h.Sum(out[:0])
	return
}

// HashIntermediate returns the hash of an intermediate node.
func HashIntermediate(left *[32]byte, right *[32]byte) (out [32]byte) {
	var input [65]byte
	input[0] = TypeIntermediate
	copy(input[1:33], left[:])
	copy(input[33:], right[:])
	return sha256.Sum256(input[:])
}

// nextLevelLen returns the amount of nodes in the layer above the current one,
// given the number of nodes in the current layer.
func nextLevelLen(levelLen uint) uint {
	if levelLen <= 1 {
		return 0
	}
	return (levelLen + 1) / 2
}

package lthash

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
)

func accountTreeFixture(rng *rand.Rand, dataLen int) *accounts.Account {
	acct := &accounts.Account{Lamports: 1 + uint64(rng.Int63()), Executable: rng.Intn(2) == 0, Data: randomBytes(rng, dataLen)}
	rng.Read(acct.Key[:])
	rng.Read(acct.Owner[:])
	return acct
}

func accountHasherBytes(acct *accounts.Account) []byte {
	var h AccountHasher
	var out LtHash
	h.HashInto(&out, acct)
	return append([]byte(nil), out.bytes()...)
}

// A tree built over an account must give the bytes AccountHasher gives, for
// data lengths that put the trailing fields on each side of a chunk boundary.
func TestAccountTreeMatchesAccountHasher(t *testing.T) {
	rng := rand.New(rand.NewSource(5))
	var sizes []int
	for chunks := 1; chunks <= 6; chunks++ {
		for delta := -accountInputPrefix - accountInputSuffix - 2; delta <= 2; delta++ {
			sizes = append(sizes, chunks*treeChunkLen+delta)
		}
	}
	sizes = append(sizes, 952, 1016, 1017, 100_000, 1_622_064)
	for _, size := range sizes {
		acct := accountTreeFixture(rng, size)
		in := accountInput{acct}
		if in.chunkCount() < 2 {
			continue
		}
		tree := buildHashTree(in, 4)
		if !bytes.Equal(treeXOF(tree, 2*numElements), accountHasherBytes(acct)) {
			t.Fatalf("data length %d: account tree differs from AccountHasher", size)
		}
	}
}

// Updating only the chunks accountDirtyChunks reports must track every kind of
// change an account can undergo at a fixed data length.
func TestAccountTreeIncrementalUpdateMatchesAccountHasher(t *testing.T) {
	rng := rand.New(rand.NewSource(6))
	for trial := 0; trial < 300; trial++ {
		size := 1100 + rng.Intn(60_000)
		prev := accountTreeFixture(rng, size)
		tree := buildHashTree(accountInput{prev}, 1)
		for round := 0; round < 5; round++ {
			next := prev.Clone()
			switch rng.Intn(6) {
			case 0:
				next.Lamports += 1 + uint64(rng.Intn(1000))
			case 1:
				next.Executable = !next.Executable
			case 2:
				next.Owner[rng.Intn(32)] ^= 0x80
			case 3: // nothing changed
			default:
				for edits := 1 + rng.Intn(40); edits > 0; edits-- {
					next.Data[rng.Intn(size)] ^= byte(1 + rng.Intn(255))
				}
				if rng.Intn(2) == 0 {
					next.Data[0] ^= 1
					next.Data[size-1] ^= 1
				}
			}
			dirty := accountDirtyChunks(prev, next)
			tree.update(accountInput{next}, dirty)
			if !bytes.Equal(treeXOF(tree, 2*numElements), accountHasherBytes(next)) {
				t.Fatalf("trial %d round %d: incremental account tree differs from AccountHasher", trial, round)
			}
			prev = next
		}
	}
}

// The dirty list is exact about the data: one changed byte marks one chunk
// beyond the always-listed first and trailing chunks.
func TestAccountDirtyChunksAreMinimal(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	prev := accountTreeFixture(rng, 50*treeChunkLen)
	next := prev.Clone()
	base := len(accountDirtyChunks(prev, next))
	if base > 3 {
		t.Fatalf("unchanged account lists %d chunks", base)
	}
	next.Data[20*treeChunkLen] ^= 1
	if got := len(accountDirtyChunks(prev, next)); got != base+1 {
		t.Fatalf("one changed byte listed %d extra chunks", got-base)
	}
	if dirty, total := AccountDirtyChunkCount(prev, next); dirty != base+1 || total != (accountInput{next}).chunkCount() {
		t.Fatalf("AccountDirtyChunkCount = %d of %d", dirty, total)
	}
	grown := prev.Clone()
	grown.Data = append(grown.Data, 1)
	if dirty, _ := AccountDirtyChunkCount(prev, grown); dirty != -1 {
		t.Fatal("a length change must not be reported as an incremental update")
	}
}

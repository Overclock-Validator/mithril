package turbine

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// Exercise exact FEC boundaries and the smaller resigned final-set capacity.
// The receiver must see one DATA_COMPLETE per serialized component, regardless
// of how many FEC sets carry it, with proofs authenticating the final flags.
func TestGeneratedComponentHasOneCompletionBoundary(t *testing.T) {
	unsigned := dataShredsPerFECBlock * dataCapacity(proofEntriesFor32x32, false)
	signed := dataShredsPerFECBlock * dataCapacity(proofEntriesFor32x32, true)
	for _, last := range []bool{false, true} {
		for _, size := range []int{0, 1, signed, signed + 1, unsigned, unsigned + 1, 2 * unsigned, 2*unsigned + signed} {
			t.Run(fmt.Sprintf("last=%t/bytes=%d", last, size), func(t *testing.T) {
				leader := testShredLeader(t)
				gen := ShredGenerator{Slot: 100, ParentSlot: 99, Version: 7}
				payload := bytes.Repeat([]byte{0x5a}, size)
				packets, _, nextData, _, err := gen.MakeShredsFromData(leader, payload, last, solana.Hash{}, 0, 0)
				require.NoError(t, err)
				var decoded []byte
				var completed int
				for _, packet := range packets {
					shred, err := ParseShred(packet)
					require.NoError(t, err)
					require.NoError(t, shred.VerifySignature(leader.PublicKey()))
					if shred.Type != ShredTypeData {
						continue
					}
					decoded = append(decoded, shred.Data...)
					if shred.DataComplete() {
						completed++
						require.Equal(t, nextData-1, shred.Index)
					}
					require.Equal(t, last && shred.Index == nextData-1, shred.LastInSlot())
				}
				require.Equal(t, 1, completed)
				require.True(t, bytes.Equal(payload, decoded))
			})
		}
	}
}

package snapshot

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func appendManifestUint64(dst []byte, value uint64) []byte {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], value)
	return append(dst, encoded[:]...)
}

func appendManifestUint32(dst []byte, value uint32) []byte {
	var encoded [4]byte
	binary.LittleEndian.PutUint32(encoded[:], value)
	return append(dst, encoded[:]...)
}

func TestManifestVoteAccountDecoderPreservesV4BLSPubkey(t *testing.T) {
	var node solana.PublicKey
	node[0] = 7
	var owner solana.PublicKey
	owner[0] = 9
	var bls [48]byte
	for i := range bls {
		bls[i] = byte(i + 1)
	}

	voteState := &sealevel.VoteStateVersions{
		Type: sealevel.VoteStateVersionV4,
		V4: sealevel.VoteState4{
			NodePubkey:          node,
			BlsPubkeyCompressed: &bls,
			LastTimestamp: sealevel.BlockTimestamp{
				Timestamp: 123,
				Slot:      456,
			},
		},
	}

	var voteStateBuf bytes.Buffer
	require.NoError(t, voteState.MarshalWithEncoder(bin.NewBinEncoder(&voteStateBuf)))

	var accountBuf bytes.Buffer
	encoder := bin.NewBinEncoder(&accountBuf)
	require.NoError(t, encoder.WriteUint64(42, bin.LE))
	require.NoError(t, encoder.WriteUint64(uint64(voteStateBuf.Len()), bin.LE))
	require.NoError(t, encoder.WriteBytes(voteStateBuf.Bytes(), false))
	require.NoError(t, encoder.WriteBytes(owner[:], false))
	require.NoError(t, encoder.WriteByte(0))
	require.NoError(t, encoder.WriteUint64(18446744073709551615, bin.LE))

	var decoded VoteAccount
	require.NoError(t, decoded.UnmarshalWithDecoder(bin.NewBinDecoder(accountBuf.Bytes())))
	require.Equal(t, node, decoded.NodePubkey)
	require.NotNil(t, decoded.BlsPubkeyCompressed)
	require.Equal(t, bls, *decoded.BlsPubkeyCompressed)
	require.Equal(t, int64(123), decoded.LastTimestampTs)
	require.Equal(t, uint64(456), decoded.LastTimestampSlot)
}

func TestManifestDecoderRejectsImpossibleCollectionCountBeforeAllocation(t *testing.T) {
	data := appendManifestUint64(nil, 0) // last hash index
	data = append(data, 0)               // no last hash
	data = appendManifestUint64(data, math.MaxUint64)

	var queue BlockHashVec
	err := queue.UnmarshalWithDecoder(bin.NewBinDecoder(data))
	require.ErrorContains(t, err, "blockhash queue ages count")
	require.Nil(t, queue.HashAndAge)
}

func TestManifestBankHashInfoRejectsTruncation(t *testing.T) {
	var info BankHashInfo
	err := info.UnmarshalWithDecoder(bin.NewBinDecoder(make([]byte, 63)))
	require.ErrorContains(t, err, "snapshot hash")
}

func TestManifestAccountsDbFieldsRejectsTruncationAndDuplicateSlots(t *testing.T) {
	t.Run("missing version", func(t *testing.T) {
		var fields AccountsDbFields
		err := fields.UnmarshalWithDecoder(bin.NewBinDecoder(appendManifestUint64(nil, 0)))
		require.Error(t, err)
	})

	t.Run("missing historical roots", func(t *testing.T) {
		data := appendManifestUint64(nil, 0) // storages
		data = appendManifestUint64(data, 1) // version
		data = appendManifestUint64(data, 2) // slot
		data = append(data, make([]byte, 64+5*8)...)

		var fields AccountsDbFields
		err := fields.UnmarshalWithDecoder(bin.NewBinDecoder(data))
		require.Error(t, err)
	})

	t.Run("duplicate storage slot", func(t *testing.T) {
		data := appendManifestUint64(nil, 2)
		for range 2 {
			data = appendManifestUint64(data, 7) // slot
			data = appendManifestUint64(data, 0) // account vectors
		}

		var fields AccountsDbFields
		err := fields.UnmarshalWithDecoder(bin.NewBinDecoder(data))
		require.ErrorContains(t, err, "duplicate AccountsDB storage slot 7")
	})
}

func TestManifestSlotAccountVectorsRejectDuplicateFileID(t *testing.T) {
	data := appendManifestUint64(nil, 7)
	data = appendManifestUint64(data, 2)
	for range 2 {
		data = appendManifestUint64(data, 9)
		data = appendManifestUint64(data, 123)
	}

	var vectors SlotAcctVecs
	err := vectors.UnmarshalWithDecoder(bin.NewBinDecoder(data))
	require.ErrorContains(t, err, "duplicate appendvec file ID 9")
}

func TestManifestOptionalIncrementalPersistencePropagatesTruncation(t *testing.T) {
	var manifest SnapshotManifest
	err := manifest.unmarshalOptionalFields(bin.NewBinDecoder([]byte{1}))
	require.ErrorContains(t, err, "incremental snapshot persistence")
}

func TestManifestRejectsNonCanonicalBooleanEncoding(t *testing.T) {
	var manifest SnapshotManifest
	err := manifest.unmarshalOptionalFields(bin.NewBinDecoder([]byte{2}))
	require.ErrorContains(t, err, "invalid boolean encoding 2")
}

func TestManifestOptionalFieldsDecodeAlpenglowBlockIDAndRejectTrailingBytes(t *testing.T) {
	data := []byte{0, 0}                 // no persistence, no epoch account hash
	data = appendManifestUint64(data, 0) // no versioned epoch stakes
	data = append(data, 1)               // AccountsLtHash present
	data = append(data, make([]byte, 2048)...)
	data = append(data, 1) // block ID present
	blockID := bytes.Repeat([]byte{0x5a}, 32)
	data = append(data, blockID...)

	var manifest SnapshotManifest
	require.NoError(t, manifest.unmarshalOptionalFields(bin.NewBinDecoder(data)))
	require.NotNil(t, manifest.LtHash)
	require.NotNil(t, manifest.BlockID)
	require.Equal(t, blockID, manifest.BlockID[:])

	var withTrailing SnapshotManifest
	err := withTrailing.unmarshalOptionalFields(bin.NewBinDecoder(append(data, 0xff)))
	require.ErrorContains(t, err, "unsupported trailing bytes")

	var truncated SnapshotManifest
	err = truncated.unmarshalOptionalFields(bin.NewBinDecoder(data[:len(data)-1]))
	require.ErrorContains(t, err, "Alpenglow block ID")
}

func TestManifestDecoderRejectsUnknownVariantsWithoutPanic(t *testing.T) {
	t.Run("versioned epoch stakes", func(t *testing.T) {
		var stakes VersionedEpochStakes
		err := stakes.UnmarshalWithDecoder(bin.NewBinDecoder(appendManifestUint32(nil, 99)))
		require.ErrorContains(t, err, "unsupported versioned epoch stakes version 99")
	})

	t.Run("epoch reward status", func(t *testing.T) {
		var status SerializableEpochRewardStatus
		err := status.UnmarshalWithDecoder(bin.NewBinDecoder(appendManifestUint32(nil, 99)))
		require.ErrorContains(t, err, "invalid epoch reward status type 99")
	})
}

func TestManifestVoteAccountRejectsOversizedDataLength(t *testing.T) {
	data := appendManifestUint64(nil, 1)
	data = appendManifestUint64(data, math.MaxUint64)
	var account VoteAccount
	err := account.UnmarshalWithDecoder(bin.NewBinDecoder(data))
	require.ErrorContains(t, err, "vote account data length")
}

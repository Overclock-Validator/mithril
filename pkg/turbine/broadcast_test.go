package turbine

import (
	"crypto/ed25519"
	"net"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func testBroadcastLeader(t *testing.T) solana.PrivateKey {
	t.Helper()
	var seed [32]byte
	for i := range seed {
		seed[i] = byte(i + 3)
	}
	return solana.PrivateKey(ed25519.NewKeyFromSeed(seed[:]))
}

func TestBroadcastSessionHeaderAndFooter(t *testing.T) {
	capture := &packetCapture{}
	leader := testBroadcastLeader(t)
	parentBlockID := solana.Hash{0xaa}
	parentChainedRoot := solana.Hash{0xbb}
	session := NewBroadcastSession(BroadcastSessionConfig{
		Leader:                  leader,
		Slot:                    100,
		ParentSlot:              99,
		ParentBlockID:           parentBlockID,
		ParentChainedMerkleRoot: parentChainedRoot,
		Version:                 7,
		Broadcaster:             capture,
	})
	require.NoError(t, session.BroadcastHeader(parentBlockID))
	require.NoError(t, session.BroadcastFooter(solana.Hash{0xcc}, 1234, nil, nil))
	require.Greater(t, capture.len(), 0)

	var firstData *Shred
	for _, packet := range capture.packets {
		shred, err := ParseShred(packet)
		require.NoError(t, err)
		if shred.Type == ShredTypeData && shred.Index == 0 {
			firstData = shred
			break
		}
	}
	require.NotNil(t, firstData)
	chainedRoot, err := firstData.EmbeddedChainedMerkleRoot()
	require.NoError(t, err)
	require.Equal(t, parentChainedRoot, chainedRoot)
	require.NotEqual(t, parentBlockID, chainedRoot)
}

func TestBroadcastSessionMatchesParsedShreds(t *testing.T) {
	leader := testBroadcastLeader(t)
	parentID, parentRoot := solana.Hash{0xaa}, solana.Hash{0xbb}
	capture := &packetCapture{}
	session := NewBroadcastSession(BroadcastSessionConfig{
		Leader: leader, Slot: 100, ParentSlot: 99, Version: 7,
		ParentChainedMerkleRoot: parentRoot, Broadcaster: capture,
	})
	shredder := Shredder{Slot: 100, ParentSlot: 99, Version: 7}
	txns := make([]solana.Transaction, 400)
	for i := range txns {
		txns[i] = mustParseTransferTx(t, uint64(i))
	}
	entries, err := NewEntryBatch([]Entry{{NumHashes: 1, Hash: solana.Hash{1}, Txns: txns}})
	require.NoError(t, err)
	tick, err := NewEntryBatch([]Entry{{NumHashes: 1, Hash: solana.Hash{2}}})
	require.NoError(t, err)
	components := []BlockComponent{
		NewBlockHeader(99, parentID),
		entries, // More than two FEC sets: every root must enter the block ID.
		NewUpdateParent(98, solana.Hash{3}),
		NewBlockFooter(BlockFooter{BankHash: solana.Hash{4}}),
		tick,
	}
	var nextData, nextCode uint32
	root := parentRoot
	var roots []solana.Hash
	for i, component := range components {
		last := i == len(components)-1
		batch, data, code, err := shredder.MakeMerkleShredsFromComponent(
			leader, component, last, root, nextData, nextCode,
		)
		require.NoError(t, err)
		if i == 1 {
			require.Greater(t, len(batch.DataShreds), 2*dataShredsPerFECBlock)
		}
		for j, shred := range batch.DataShreds {
			if j == 0 || shred.FECSetIndex != batch.DataShreds[j-1].FECSetIndex {
				fecRoot, err := shred.MerkleRoot()
				require.NoError(t, err)
				roots = append(roots, fecRoot)
			}
		}
		capture.packets = nil
		require.NoError(t, session.BroadcastComponent(component, last))
		require.Equal(t, batch.Packets, capture.packets)
		require.Equal(t, batch.ChainedMerkleRoot, session.ChainedMerkleRoot())
		require.Equal(t, data, session.nextDataIndex)
		require.Equal(t, code, session.nextCodeIndex)
		require.Equal(t, roots, session.fecSetRoots)
		require.Equal(t, DoubleMerkleBlockID(99, parentID, roots), session.BlockID(99, parentID))
		root, nextData, nextCode = batch.ChainedMerkleRoot, data, code
	}

	// Invalid components must not publish packets or advance the commitment.
	capture.packets = nil
	require.Error(t, session.BroadcastComponent(BlockComponent{Marker: &BlockMarker{Kind: 255}}, false))
	require.Empty(t, capture.packets)
	require.Equal(t, root, session.ChainedMerkleRoot())
	require.Equal(t, nextData, session.nextDataIndex)
	require.Equal(t, nextCode, session.nextCodeIndex)
	require.Equal(t, roots, session.fecSetRoots)
}

func TestUDPBroadcasterLoopback(t *testing.T) {
	recvAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	require.NoError(t, err)
	recvConn, err := net.ListenUDP("udp", recvAddr)
	require.NoError(t, err)
	defer recvConn.Close()

	bc, err := NewUDPBroadcaster("")
	require.NoError(t, err)
	bc.AddPeer(recvConn.LocalAddr().(*net.UDPAddr))
	require.NoError(t, bc.Broadcast([][]byte{{1, 2, 3}}))

	buf := make([]byte, 16)
	n, _, err := recvConn.ReadFromUDP(buf)
	require.NoError(t, err)
	require.Equal(t, 3, n)
}

type packetCapture struct {
	packets [][]byte
}

func (p *packetCapture) Broadcast(packets [][]byte) error {
	for _, pkt := range packets {
		p.packets = append(p.packets, append([]byte(nil), pkt...))
	}
	return nil
}

func (p *packetCapture) len() int {
	return len(p.packets)
}

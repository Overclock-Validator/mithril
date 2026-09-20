package turbine

import (
	"crypto/ed25519"
	"errors"
	"net"
	"os"
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

func TestBroadcastSessionStoresGeneratedShredsBeforeBroadcast(t *testing.T) {
	spool, err := OpenShredSpool(t.TempDir(), 0)
	require.NoError(t, err)
	t.Cleanup(spool.Close)
	var expected [][]byte
	capture := &packetCapture{beforeBroadcast: func(packets [][]byte) error {
		expected = append(expected, packets...)
		stored, err := spool.ReadSlot(100)
		require.NoError(t, err)
		require.ElementsMatch(t, expected, stored)
		return nil
	}}
	parentID := solana.Hash{0xaa}
	bankHash := solana.Hash{0xcc}
	endingTick := solana.Hash{0xdd}
	session := NewBroadcastSession(BroadcastSessionConfig{
		Leader: testBroadcastLeader(t), Slot: 100, ParentSlot: 99,
		Broadcaster: capture, ShredSpool: spool, Version: 7,
	})
	require.NoError(t, session.BroadcastHeader(parentID))
	require.NoError(t, session.BroadcastEntryBatch([]Entry{{NumHashes: 1, Hash: solana.Hash{0xee}}}))
	require.NoError(t, session.BroadcastFooter(bankHash, 1234, nil, nil))
	require.NoError(t, session.BroadcastEndingTickLast(endingTick))

	stored, err := spool.ReadSlot(100)
	require.NoError(t, err)
	require.ElementsMatch(t, capture.packets, stored)
	var data []*Shred
	for _, packet := range stored {
		shred, err := ParseShred(packet)
		require.NoError(t, err)
		require.False(t, spool.AppendShred(shred, packet), "loopback duplicate was stored again")
		if shred.Type == ShredTypeData {
			data = append(data, shred)
		}
	}
	components, err := DecodeComponentsFromDataShreds(data)
	require.NoError(t, err)
	require.Len(t, components, 4)
	require.NotNil(t, components[0].Marker)
	require.NotNil(t, components[0].Marker.Header)
	require.Equal(t, parentID, components[0].Marker.Header.ParentBlockID)
	require.NotNil(t, components[2].Marker)
	require.NotNil(t, components[2].Marker.Footer)
	require.Equal(t, bankHash, components[2].Marker.Footer.BankHash)
	require.Len(t, components[3].EntryBatch, 1)
	require.Equal(t, uint64(1), components[3].EntryBatch[0].NumHashes)
	require.Equal(t, endingTick, components[3].EntryBatch[0].Hash)
	require.Empty(t, components[3].EntryBatch[0].Txns)
	require.True(t, data[len(data)-1].LastInSlot())
	_, complete := spool.IsComplete(100)
	require.False(t, complete, "recording generated shreds must not mark a slot assembled")
}

func TestBroadcastSessionFlushesCompletedSlot(t *testing.T) {
	spool, err := OpenShredSpool(t.TempDir(), 0)
	require.NoError(t, err)
	t.Cleanup(spool.Close)
	capture := &packetCapture{}
	session := NewBroadcastSession(BroadcastSessionConfig{
		Leader: testBroadcastLeader(t), Slot: 100, ParentSlot: 99,
		Broadcaster: capture, ShredSpool: spool, Version: 7,
	})
	require.NoError(t, session.BroadcastHeader(solana.Hash{0xaa}))
	require.NoError(t, session.BroadcastFooter(solana.Hash{0xcc}, 1234, nil, nil))
	require.NoError(t, session.BroadcastEndingTickLast(solana.Hash{0xdd}))
	want := int64(len(spoolFileMagic))
	for _, packet := range capture.packets {
		want += int64(spoolRecordHeaderSize + len(packet))
	}
	// Stat does not flush the writer, unlike ReadSlot or a repair lookup.
	info, err := os.Stat(spool.pathFor(100))
	require.NoError(t, err)
	require.Equal(t, want, info.Size(), "completed block still has process-local buffered repair data")
}

func TestBroadcastSessionCacheRejectionDoesNotChangeBroadcast(t *testing.T) {
	broadcast := func(t *testing.T, spool *ShredSpool) *packetCapture {
		t.Helper()
		capture := &packetCapture{}
		session := NewBroadcastSession(BroadcastSessionConfig{
			Leader: testBroadcastLeader(t), Slot: 100, ParentSlot: 99,
			Broadcaster: capture, ShredSpool: spool, Version: 7,
		})
		require.NoError(t, session.BroadcastHeader(solana.Hash{0xaa}))
		require.NoError(t, session.BroadcastFooter(solana.Hash{0xcc}, 1234, nil, nil))
		require.NoError(t, session.BroadcastEndingTickLast(solana.Hash{0xdd}))
		return capture
	}
	expected := broadcast(t, nil).packets
	for _, tc := range []struct {
		name     string
		disabled bool
		maxBytes int64
		floor    uint64
		closed   bool
	}{
		{name: "disabled", disabled: true},
		{name: "limited", maxBytes: int64(len(spoolFileMagic) + spoolRecordHeaderSize + dataPayloadSize)},
		{name: "below floor", floor: 101},
		{name: "closed", closed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var spool *ShredSpool
			if !tc.disabled {
				var err error
				spool, err = OpenShredSpool(t.TempDir(), tc.maxBytes)
				require.NoError(t, err)
				t.Cleanup(spool.Close)
				spool.SetFloor(tc.floor)
				if tc.closed {
					spool.Close()
				}
			}
			capture := broadcast(t, spool)
			require.Equal(t, expected, capture.packets)
			if spool != nil {
				_, complete := spool.IsComplete(100)
				require.False(t, complete)
				if tc.maxBytes > 0 {
					_, size := spool.Stats()
					require.LessOrEqual(t, size, tc.maxBytes)
					stored, err := spool.ReadSlot(100)
					require.NoError(t, err)
					require.Len(t, stored, 1)
				} else {
					require.False(t, spool.HasSlot(100))
				}
			}
		})
	}
}

func TestBroadcastSessionRetainsShredsOnBroadcastError(t *testing.T) {
	spool, err := OpenShredSpool(t.TempDir(), 0)
	require.NoError(t, err)
	t.Cleanup(spool.Close)
	wantErr := errors.New("broadcast unavailable")
	capture := &packetCapture{beforeBroadcast: func(packets [][]byte) error {
		stored, err := spool.ReadSlot(100)
		require.NoError(t, err)
		require.ElementsMatch(t, packets, stored)
		return wantErr
	}}
	session := NewBroadcastSession(BroadcastSessionConfig{
		Leader: testBroadcastLeader(t), Slot: 100, ParentSlot: 99,
		Broadcaster: capture, ShredSpool: spool, Version: 7,
	})
	require.ErrorIs(t, session.BroadcastHeader(solana.Hash{0xaa}), wantErr)
	require.True(t, spool.HasSlot(100))
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
	packets         [][]byte
	beforeBroadcast func([][]byte) error
}

func (p *packetCapture) Broadcast(packets [][]byte) error {
	if p.beforeBroadcast != nil {
		if err := p.beforeBroadcast(packets); err != nil {
			return err
		}
	}
	for _, pkt := range packets {
		p.packets = append(p.packets, append([]byte(nil), pkt...))
	}
	return nil
}

func (p *packetCapture) len() int {
	return len(p.packets)
}

package turbine

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
	narya "github.com/Overclock-Validator/narya-ed25519/ed25519"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type capturedRetransmit struct {
	packet []byte
	peers  []*net.UDPAddr
}

type captureBatchSender struct {
	mu   sync.Mutex
	sent []capturedRetransmit
	wake chan struct{}
}

type scriptedSendResult struct {
	sent int
	err  error
}

type scriptedBatchSender struct {
	results  []scriptedSendResult
	requests [][]*net.UDPAddr
}

type repairAwareTVUPeers struct {
	*mutableTVUPeers
	repairPeers []gossip.RepairPeer
}

func (p *repairAwareTVUPeers) RepairPeers() []gossip.RepairPeer { return p.repairPeers }

func (s *scriptedBatchSender) Send(_ []byte, peers []*net.UDPAddr) (int, error) {
	s.requests = append(s.requests, append([]*net.UDPAddr(nil), peers...))
	if len(s.results) == 0 {
		return len(peers), nil
	}
	result := s.results[0]
	s.results = s.results[1:]
	return result.sent, result.err
}

func (s *scriptedBatchSender) Close() error { return nil }

func newCaptureBatchSender() *captureBatchSender {
	return &captureBatchSender{wake: make(chan struct{}, 8)}
}

func (s *captureBatchSender) Send(packet []byte, peers []*net.UDPAddr) (int, error) {
	s.mu.Lock()
	s.sent = append(s.sent, capturedRetransmit{
		packet: append([]byte(nil), packet...),
		peers:  append([]*net.UDPAddr(nil), peers...),
	})
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
	return len(peers), nil
}

func (s *captureBatchSender) Close() error { return nil }

func (s *captureBatchSender) packets() []capturedRetransmit {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]capturedRetransmit(nil), s.sent...)
}

func testIdentity(t *testing.T) (solana.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	var key solana.PublicKey
	copy(key[:], pub)
	return key, private
}

func TestRetransmitTreeCoversClusterAndParentsAgree(t *testing.T) {
	leader, _ := testIdentity(t)
	const nodeCount = 31
	pubkeys := make([]solana.PublicKey, nodeCount)
	peers := make([]gossip.TVUPeer, nodeCount)
	stakes := map[solana.PublicKey]uint64{leader: 10_000}
	byAddr := make(map[string]solana.PublicKey, nodeCount)
	for i := range nodeCount {
		pubkeys[i], _ = testIdentity(t)
		addr := &net.UDPAddr{IP: net.IPv4(127, 1, byte(i/250), byte(i%250+1)), Port: 8001 + i}
		peers[i] = gossip.TVUPeer{Pubkey: gossip.Pubkey(pubkeys[i]), TVUAddr: addr}
		stakes[pubkeys[i]] = uint64(nodeCount - i)
		byAddr[addr.String()] = pubkeys[i]
	}

	nodes := NewRetransmitClusterNodes(ClusterNodesConfig{
		Self:       pubkeys[0],
		TVUPeers:   peers,
		Stakes:     stakes,
		UseChaCha8: true,
	})
	shred := ShredID{Slot: 812, Index: 17, Type: ShredTypeData}
	shuffle := nodes.retransmitShuffle(leader, shred)
	require.Len(t, shuffle, nodeCount, "slot leader must be excluded from the relay tree")

	parentOf := make(map[solana.PublicKey]solana.PublicKey, nodeCount-1)
	childrenOf := make(map[solana.PublicKey][]solana.PublicKey, nodeCount)
	for _, index := range shuffle {
		self := nodes.nodes[index].pubkey
		nodes.selfPubkey = self
		nodes.selfIndex = index
		parent, ok, err := nodes.RetransmitParent(leader, shred, 3)
		require.NoError(t, err)
		if ok {
			parentOf[self] = parent
		}
		_, addrs, err := nodes.RetransmitPeers(leader, shred, 3)
		require.NoError(t, err)
		for _, addr := range addrs {
			child, found := byAddr[addr.String()]
			require.True(t, found)
			childrenOf[self] = append(childrenOf[self], child)
		}
	}

	root := nodes.nodes[shuffle[0]].pubkey
	_, hasRootParent := parentOf[root]
	require.False(t, hasRootParent)
	for _, index := range shuffle[1:] {
		child := nodes.nodes[index].pubkey
		parent, ok := parentOf[child]
		require.True(t, ok, "every non-root node needs one deterministic parent")
		require.Contains(t, childrenOf[parent], child, "parent and child must derive the same tree edge")
	}
}

func TestRetransmitterVerifiesParentResignsAndDeduplicates(t *testing.T) {
	leaderPub, leaderKey := testIdentity(t)
	const identityCount = dataPlaneFanout + 5
	identities := make(map[solana.PublicKey]ed25519.PrivateKey)
	pubkeys := make([]solana.PublicKey, identityCount)
	allPeers := make([]gossip.TVUPeer, identityCount)
	stakes := map[solana.PublicKey]uint64{leaderPub: 1_000}
	for i := range pubkeys {
		pubkey, private := testIdentity(t)
		pubkeys[i] = pubkey
		identities[pubkey] = private
		allPeers[i] = gossip.TVUPeer{
			Pubkey:  gossip.Pubkey(pubkey),
			TVUAddr: &net.UDPAddr{IP: net.IPv4(127, 2, byte(i/250), byte(i%250+1)), Port: 9000 + i},
		}
		stakes[pubkey] = uint64(identityCount - i)
	}

	gen := ShredGenerator{Slot: 90, ParentSlot: 89, Version: 44, ReferenceTick: 63}
	packets, _, _, _, err := gen.MakeShredsFromData(solana.PrivateKey(leaderKey), []byte("terminal component"), true, solana.Hash{9}, 0, 0)
	require.NoError(t, err)
	packet := packets[0]
	shred, err := ParseShred(packet)
	require.NoError(t, err)
	id := ShredID{Slot: shred.Slot, Index: shred.Index, Type: shred.Type}

	nodes := NewRetransmitClusterNodes(ClusterNodesConfig{
		Self: pubkeys[0], TVUPeers: allPeers, Stakes: stakes, UseChaCha8: true,
	})
	shuffle := nodes.retransmitShuffle(leaderPub, id)
	// Position 1 has the root as its parent and, with fanout+5 nodes, also has
	// a child at position fanout+1. This exercises both hop verification and send.
	self := nodes.nodes[shuffle[1]].pubkey
	nodes.selfPubkey = self
	nodes.selfIndex = shuffle[1]
	parent, ok, err := nodes.RetransmitParent(leaderPub, id, dataPlaneFanout)
	require.NoError(t, err)
	require.True(t, ok)
	parentKey, ok := identities[parent]
	require.True(t, ok)

	root, err := shred.MerkleRoot()
	require.NoError(t, err)
	offset, err := shred.retransmitterSignatureOffset()
	require.NoError(t, err)
	copy(packet[offset:offset+ed25519.SignatureSize], ed25519.Sign(parentKey, root[:]))
	shred, err = ParseShred(packet)
	require.NoError(t, err)

	peerSource := &mutableTVUPeers{peers: allPeers}
	sender := newCaptureBatchSender()
	retransmitter, err := newRetransmitterWithSenders(RetransmitConfig{
		Identity: identityHolder(t, identities[self]),
		Peers:    peerSource,
		Stakes: func(uint64) map[solana.PublicKey]uint64 {
			return stakes
		},
		UseChaCha8: true,
	}, []packetBatchSender{sender})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		retransmitter.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		var payloadBytes uint64
		for _, call := range sender.packets() {
			payloadBytes += uint64(len(call.packet)) * uint64(len(call.peers))
		}
		require.Equal(t, payloadBytes, retransmitter.Stats().SentPayloadBytes)
	})

	wire := append(append([]byte(nil), packet...), 1, 2, 3, 4)
	require.NoError(t, retransmitter.Submit(wire, shred, leaderPub, false))
	select {
	case <-sender.wake:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for retransmit")
	}
	sent := sender.packets()
	require.Len(t, sent, 1)
	require.NotEmpty(t, sent[0].peers)
	require.Len(t, sent[0].packet, len(shred.Payload), "only the canonical payload is relayed and counted")
	forwarded, err := ParseShred(sent[0].packet)
	require.NoError(t, err)
	forwardedSignature, err := forwarded.RetransmitterSignature()
	require.NoError(t, err)
	require.True(t, narya.VerifyStrict(self[:], root[:], forwardedSignature[:]), "next hop must carry this relay's signature")

	// The exact same common header is forwarded once. This mirrors Agave's
	// retransmit-stage deduper while still allowing two conflicting shreds per ID.
	require.NoError(t, retransmitter.Submit(packet, shred, leaderPub, false))
	time.Sleep(20 * time.Millisecond)
	require.Len(t, sender.packets(), 1)
	require.Equal(t, uint64(1), retransmitter.Stats().DuplicateShreds)

	require.NoError(t, retransmitter.Submit(packet, shred, leaderPub, true))
	require.Equal(t, uint64(1), retransmitter.Stats().RepairShreds)
}

func TestRetransmitterRejectsInvalidParentSignature(t *testing.T) {
	leaderPub, leaderKey := testIdentity(t)
	pubA, keyA := testIdentity(t)
	pubB, keyB := testIdentity(t)
	keys := map[solana.PublicKey]ed25519.PrivateKey{pubA: keyA, pubB: keyB}
	peers := &mutableTVUPeers{peers: []gossip.TVUPeer{
		{Pubkey: gossip.Pubkey(pubA), TVUAddr: &net.UDPAddr{IP: net.IPv4(127, 3, 0, 1), Port: 8001}},
		{Pubkey: gossip.Pubkey(pubB), TVUAddr: &net.UDPAddr{IP: net.IPv4(127, 3, 0, 2), Port: 8002}},
	}}
	stakes := map[solana.PublicKey]uint64{leaderPub: 100, pubA: 10, pubB: 20}
	gen := ShredGenerator{Slot: 91, ParentSlot: 90, Version: 44}
	packets, _, _, _, err := gen.MakeShredsFromData(solana.PrivateKey(leaderKey), nil, true, solana.Hash{7}, 0, 0)
	require.NoError(t, err)
	shred, err := ParseShred(packets[0])
	require.NoError(t, err)

	id := ShredID{Slot: shred.Slot, Index: shred.Index, Type: shred.Type}
	nodes := NewRetransmitClusterNodes(ClusterNodesConfig{Self: pubA, TVUPeers: peers.peers, Stakes: stakes, UseChaCha8: true})
	shuffle := nodes.retransmitShuffle(leaderPub, id)
	selfPub := nodes.nodes[shuffle[1]].pubkey
	selfKey := keys[selfPub]
	nodes.selfPubkey = selfPub
	nodes.selfIndex = shuffle[1]
	parent, hasParent, err := nodes.RetransmitParent(leaderPub, id, dataPlaneFanout)
	require.NoError(t, err)
	require.True(t, hasParent)
	root, err := shred.MerkleRoot()
	require.NoError(t, err)
	offset, err := shred.retransmitterSignatureOffset()
	require.NoError(t, err)
	copy(packets[0][offset:offset+ed25519.SignatureSize], ed25519.Sign(selfKey, root[:]))
	shred, err = ParseShred(packets[0])
	require.NoError(t, err)

	sender := newCaptureBatchSender()
	retransmitter, err := newRetransmitterWithSenders(RetransmitConfig{
		Identity:   identityHolder(t, selfKey),
		Peers:      peers,
		Stakes:     func(uint64) map[solana.PublicKey]uint64 { return stakes },
		UseChaCha8: true,
	}, []packetBatchSender{sender})
	require.NoError(t, err)
	var source *net.UDPAddr
	for _, peer := range peers.peers {
		if solana.PublicKey(peer.Pubkey) == selfPub {
			source = peer.TVUAddr
			break
		}
	}
	require.NotNil(t, source)
	err = retransmitter.SubmitFrom(packets[0], shred, leaderPub, false, source)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrInvalidRetransmitterSignature))
	stats := retransmitter.Stats()
	require.Equal(t, uint64(1), stats.InvalidParentSignatures)
	require.Zero(t, stats.SentPayloadBytes)
	require.Equal(t, uint64(1), stats.ParentDiagnosticSamples)
	require.Equal(t, uint64(1), stats.ParentSignerFound)
	require.Equal(t, uint64(0), stats.ParentSignerNotFound)
	require.Equal(t, uint64(0), stats.ParentSourceDirectLeader)
	require.Equal(t, uint64(0), stats.ParentSourceRepairSocket)
	require.Equal(t, uint64(1), stats.ParentSourceUnexplained)
	require.Contains(t, stats.LastParentDiagnostic, "source="+source.String())
	require.Contains(t, stats.LastParentDiagnostic, "source_identity="+selfPub.String())
	require.Contains(t, stats.LastParentDiagnostic, "source_role=unexplained")
	require.Contains(t, stats.LastParentDiagnostic, "expected_parent="+parent.String())
	require.Contains(t, stats.LastParentDiagnostic, "actual_signer="+selfPub.String())
	require.Empty(t, sender.packets())
}

func TestRetransmitterClassifiesInvalidParentSources(t *testing.T) {
	leader, _ := testIdentity(t)
	peer, _ := testIdentity(t)
	leaderTVU := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 10), Port: 8001}
	peerTVU := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 20), Port: 8001}
	repairAddr := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 20), Port: 8008}
	peers := &repairAwareTVUPeers{
		mutableTVUPeers: &mutableTVUPeers{peers: []gossip.TVUPeer{
			{Pubkey: gossip.Pubkey(leader), TVUAddr: leaderTVU},
			{Pubkey: gossip.Pubkey(peer), TVUAddr: peerTVU},
		}},
		repairPeers: []gossip.RepairPeer{{Pubkey: gossip.Pubkey(peer), Addr: repairAddr}},
	}
	nodes := NewRetransmitClusterNodes(ClusterNodesConfig{
		Self: peer, TVUPeers: peers.peers, Stakes: map[solana.PublicKey]uint64{leader: 20, peer: 10},
	})
	retransmitter := &Retransmitter{cfg: RetransmitConfig{Peers: peers}}

	role, identity := retransmitter.classifyInvalidParentSource(nodes, leader,
		&net.UDPAddr{IP: append(net.IP(nil), leaderTVU.IP...), Port: 8006})
	require.Equal(t, parentSourceDirectLeader, role)
	require.Equal(t, leader, identity)

	role, identity = retransmitter.classifyInvalidParentSource(nodes, leader, repairAddr)
	require.Equal(t, parentSourceRepairSocket, role)
	require.Equal(t, peer, identity)

	role, identity = retransmitter.classifyInvalidParentSource(nodes, leader,
		&net.UDPAddr{IP: net.IPv4(198, 51, 100, 1), Port: 9006})
	require.Equal(t, parentSourceUnexplained, role)
	require.Equal(t, solana.PublicKey{}, identity)
}

func TestRetransmitSendRetriesUnsentSuffix(t *testing.T) {
	peers := []*net.UDPAddr{
		{IP: net.IPv4(192, 0, 2, 1), Port: 8001},
		{IP: net.IPv4(192, 0, 2, 2), Port: 8002},
		{IP: net.IPv4(192, 0, 2, 3), Port: 8003},
		{IP: net.IPv4(192, 0, 2, 4), Port: 8004},
	}
	sender := &scriptedBatchSender{results: []scriptedSendResult{
		{sent: 2},
		{sent: 1, err: syscall.ENOBUFS},
		{sent: 1},
	}}
	retransmitter := &Retransmitter{}
	retransmitter.sendToPeers([]byte{1}, peers, sender)

	require.Len(t, sender.requests, 3)
	require.Equal(t, peers, sender.requests[0])
	require.Equal(t, peers[2:], sender.requests[1], "first retry must start at the first unsent destination")
	require.Equal(t, peers[3:], sender.requests[2], "second retry must continue from the remaining suffix")
	stats := retransmitter.Stats()
	require.Equal(t, uint64(4), stats.TargetPackets)
	require.Equal(t, uint64(7), stats.PacketAttempts)
	require.Equal(t, uint64(4), stats.SentPackets)
	require.Equal(t, uint64(0), stats.UnsentPackets)
	require.Equal(t, uint64(2), stats.ShortSendBatches)
	require.Equal(t, uint64(1), stats.SendSyscallErrors)
	require.Equal(t, uint64(2), stats.RetryBatches)
	require.Equal(t, uint64(3), stats.RetryPackets)
	require.Equal(t, uint64(2), stats.RetrySentPackets)
	require.Equal(t, uint64(0), stats.ExhaustedSendBatches)
	require.Equal(t, uint64(1), stats.SendErrors)
	require.Equal(t, uint64(1), stats.SentShreds)
}

func TestRetransmitSendRecordsErrnoAndExhaustion(t *testing.T) {
	peers := []*net.UDPAddr{
		{IP: net.IPv4(192, 0, 2, 10), Port: 8010},
		{IP: net.IPv4(192, 0, 2, 11), Port: 8011},
	}
	sender := &scriptedBatchSender{results: []scriptedSendResult{
		{err: syscall.ENOBUFS},
		{err: syscall.ENOBUFS},
		{err: syscall.ENOBUFS},
	}}
	retransmitter := &Retransmitter{}
	retransmitter.sendToPeers([]byte{1}, peers, sender)

	stats := retransmitter.Stats()
	require.Equal(t, uint64(2), stats.TargetPackets)
	require.Equal(t, uint64(6), stats.PacketAttempts)
	require.Equal(t, uint64(0), stats.SentPackets)
	require.Equal(t, uint64(2), stats.UnsentPackets)
	require.Equal(t, uint64(3), stats.ShortSendBatches)
	require.Equal(t, uint64(3), stats.SendSyscallErrors)
	require.Equal(t, uint64(2), stats.RetryBatches)
	require.Equal(t, uint64(4), stats.RetryPackets)
	require.Equal(t, uint64(1), stats.ExhaustedSendBatches)
	require.Equal(t, uint64(1), stats.SendDiagnosticSamples)
	require.Contains(t, stats.LastSendDiagnostic, "first_unsent="+peers[0].String())
	require.Contains(t, stats.LastSendDiagnostic, "errno=no buffer space available")
}

func TestReceiverRejectsWrongShredVersion(t *testing.T) {
	leaderPub, leaderKey := testIdentity(t)
	gen := ShredGenerator{Slot: 92, ParentSlot: 91, Version: 7}
	packets, _, _, _, err := gen.MakeShredsFromData(solana.PrivateKey(leaderKey), []byte("payload"), false, solana.Hash{3}, 0, 0)
	require.NoError(t, err)
	receiver := NewUDPReceiver("127.0.0.1:0")
	receiver.SetShredVersion(8)
	receiver.SetLeaderForSlot(func(uint64) (solana.PublicKey, bool) { return leaderPub, true })
	require.True(t, receiver.processPacket(context.Background(), nil, packets[0], nil, false))
	require.Equal(t, uint64(1), receiver.Stats().ShredVersionMismatch)
	require.Equal(t, uint64(0), receiver.Stats().DataShreds)
}

func TestRetransmitSentPayloadBytes(t *testing.T) {
	peers := []*net.UDPAddr{{Port: 8001}, {Port: 8002}, {Port: 8003}, {Port: 8004}}
	for _, test := range []struct {
		name    string
		results []scriptedSendResult
		payload []byte
		peers   []*net.UDPAddr
		bytes   uint64
		packets uint64
	}{
		{name: "full", payload: []byte{1, 2, 3}, peers: peers, bytes: 12, packets: 4},
		{name: "full_with_error", results: []scriptedSendResult{{sent: 4, err: syscall.ENOBUFS}}, payload: []byte{1, 2, 3}, peers: peers, bytes: 12, packets: 4},
		{name: "retry_prefixes", results: []scriptedSendResult{{sent: 2}, {sent: 1, err: syscall.ENOBUFS}, {sent: 1}}, payload: []byte{1, 2, 3}, peers: peers, bytes: 12, packets: 4},
		{name: "exhausted", results: []scriptedSendResult{{err: syscall.ENOBUFS}, {err: syscall.ENOBUFS}, {err: syscall.ENOBUFS}}, payload: []byte{1, 2, 3}, peers: peers},
		{name: "negative_then_retry", results: []scriptedSendResult{{sent: -1, err: syscall.ENOBUFS}, {sent: 2}, {sent: 2}}, payload: []byte{1, 2, 3}, peers: peers, bytes: 12, packets: 4},
		{name: "over_report", results: []scriptedSendResult{{sent: 5}}, payload: []byte{1, 2, 3}, peers: peers, packets: 4},
		{name: "over_report_with_error", results: []scriptedSendResult{{sent: 5, err: syscall.ENOBUFS}}, payload: []byte{1, 2, 3}, peers: peers, packets: 4},
		{name: "valid_prefix_then_over_report", results: []scriptedSendResult{{sent: 2}, {sent: 3}}, payload: []byte{1, 2, 3}, peers: peers, bytes: 6, packets: 4},
		{name: "empty_payload", peers: peers, packets: 4},
		{name: "no_peers", payload: []byte{1, 2, 3}},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := &Retransmitter{}
			sender := &scriptedBatchSender{results: test.results}
			r.sendToPeers(test.payload, test.peers, sender)
			stats := r.Stats()
			require.Equal(t, test.bytes, stats.SentPayloadBytes)
			require.Equal(t, test.packets, stats.SentPackets, "legacy packet accounting is unchanged")
		})
	}

	r := &Retransmitter{cfg: RetransmitConfig{ShredReceivers: peers}}
	sender := &scriptedBatchSender{}
	r.sendToShredReceivers([]byte{1, 2, 3}, sender)
	require.Equal(t, uint64(4), r.Stats().ShredReceiverSent)
	require.Zero(t, r.Stats().SentPayloadBytes, "auxiliary copies are not peer traffic")
	r.sendToPeers([]byte{1, 2, 3}, peers[:2], sender)
	r.sendToPeers([]byte{1, 2, 3, 4, 5}, peers[:2], sender)
	require.Equal(t, uint64(16), r.Stats().SentPayloadBytes, "payload lengths are counted separately")
	require.Zero(t, (&Retransmitter{}).Stats().SentPayloadBytes)
	require.Zero(t, (*Retransmitter)(nil).Stats().SentPayloadBytes)
}

func TestRetransmitSentPayloadBytesConcurrentStats(t *testing.T) {
	r := &Retransmitter{}
	peers := []*net.UDPAddr{{Port: 8001}, {Port: 8002}}
	var writers sync.WaitGroup
	t.Cleanup(writers.Wait)
	for range 2 {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for range 100 {
				r.sendToPeers([]byte{1, 2, 3}, peers, &discardRelaySender{})
			}
		}()
	}
	for range 100 {
		require.LessOrEqual(t, r.Stats().SentPayloadBytes, uint64(1200))
	}
	writers.Wait()
	require.Equal(t, uint64(1200), r.Stats().SentPayloadBytes)
}

func TestReceiverRetransmitPayloadPresence(t *testing.T) {
	r := NewUDPReceiver("127.0.0.1:0")
	require.False(t, r.Stats().RetransmitEnabled)
	r.retransmitter = &Retransmitter{}
	stats := r.Stats()
	require.True(t, stats.RetransmitEnabled)
	require.Zero(t, stats.Retransmit.SentPayloadBytes)
	r.retransmitter.sendToPeers([]byte{1, 2, 3}, []*net.UDPAddr{{Port: 8001}, {Port: 8002}}, &discardRelaySender{})
	stats = r.Stats()
	require.True(t, stats.RetransmitEnabled)
	require.Equal(t, uint64(6), stats.Retransmit.SentPayloadBytes)
}

func TestReceiverNeverRetransmitsDedicatedRepairSocketPackets(t *testing.T) {
	leaderPub, leaderKey := testIdentity(t)
	gen := ShredGenerator{Slot: 93, ParentSlot: 92, Version: 7}
	packets, _, _, _, err := gen.MakeShredsFromData(solana.PrivateKey(leaderKey), []byte("repair payload"), false, solana.Hash{4}, 0, 0)
	require.NoError(t, err)

	receiver := NewUDPReceiver("127.0.0.1:0")
	receiver.SetShredVersion(7)
	receiver.SetLeaderForSlot(func(uint64) (solana.PublicKey, bool) { return leaderPub, true })
	receiver.retransmitter = &Retransmitter{}
	require.True(t, receiver.processPacket(context.Background(), nil, packets[0],
		&net.UDPAddr{IP: net.IPv4(192, 0, 2, 30), Port: 9008}, true))

	stats := receiver.Stats()
	require.Equal(t, uint64(1), stats.RepairSocketPackets)
	require.Equal(t, uint64(1), stats.RepairSocketUnmatched)
	require.Equal(t, uint64(1), stats.Retransmit.RepairShreds)
	require.Equal(t, uint64(0), stats.Retransmit.Submitted)
	require.Equal(t, uint64(0), stats.Retransmit.QueueDrops)
	require.Zero(t, stats.Retransmit.SentPayloadBytes)
}

func TestRetransmitterSignatureOffsetForDataAndCodingShreds(t *testing.T) {
	_, leaderKey := testIdentity(t)
	gen := ShredGenerator{Slot: 94, ParentSlot: 93, Version: 44}
	packets, _, _, _, err := gen.MakeShredsFromData(solana.PrivateKey(leaderKey), nil, true, solana.Hash{5}, 0, 0)
	require.NoError(t, err)
	require.Greater(t, len(packets), dataShredsPerFECBlock)

	for _, packetIndex := range []int{0, dataShredsPerFECBlock} {
		packet := packets[packetIndex]
		shred, err := ParseShred(packet)
		require.NoError(t, err)
		offset, err := shred.retransmitterSignatureOffset()
		require.NoError(t, err)
		var want solana.Signature
		for i := range want {
			want[i] = byte(i + packetIndex)
		}
		copy(packet[offset:offset+ed25519.SignatureSize], want[:])
		shred, err = ParseShred(packet)
		require.NoError(t, err)
		got, err := shred.RetransmitterSignature()
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
}

func TestRetransmitDeduperAllowsTwoConflictingShredsPerID(t *testing.T) {
	var deduper retransmitDeduper
	id := ShredID{Slot: 93, Index: 4, Type: ShredTypeData}
	now := time.Now()
	first := make([]byte, commonShredHeaderSize)
	second := make([]byte, commonShredHeaderSize)
	third := make([]byte, commonShredHeaderSize)
	first[0], second[0], third[0] = 1, 2, 3

	require.True(t, deduper.accept(first, id, now))
	require.False(t, deduper.accept(first, id, now), "exact duplicate must be suppressed")
	require.True(t, deduper.accept(second, id, now), "a second leader-signed variant exposes duplicate blocks cluster-wide")
	require.False(t, deduper.accept(third, id, now), "Agave caps retransmit at two variants per shred ID")
}

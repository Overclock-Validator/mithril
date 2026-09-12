package replay

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/genesis"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// This is a non-voting receiver: no consensus engine, voter, RPC, repair peer or
// finality callback is configured. Only local UDP connects producer and replay.
func TestGenesisSignedShredIngressAgave(t *testing.T) {
	for _, workers := range []int{0, 2} {
		t.Run(fmt.Sprintf("workers-%d", workers), func(t *testing.T) {
			testGenesisFirstBlocksAgave(t, workers, true)
		})
	}
}

func TestGenesisIngressRejectsInvalidTransactionAndIncompleteSlot(t *testing.T) {
	g, _, err := genesis.Build(t.Context(), replayGenesisConfig())
	require.NoError(t, err)
	seed, err := NewGenesisReplayBootstrap(t.Context(), g)
	require.NoError(t, err)
	oracle := loadReplayOracle(t, g)
	for _, incomplete := range []bool{false, true} {
		t.Run(fmt.Sprintf("incomplete-%t", incomplete), func(t *testing.T) {
			f := newGenesisIngressFixture(t, g, seed)
			component, err := turbine.UnmarshalBlockComponent(oracle.Blocks[1].Entries)
			require.NoError(t, err)
			if !incomplete {
				// The scheduled leader signs valid Merkle shreds containing an
				// invalid transaction. Shred authentication alone is insufficient.
				component.EntryBatch[0].Txns[0].Signatures[0][0] ^= 1
			}
			capture := new(recordedShredBatches)
			session := turbine.NewBroadcastSession(turbine.BroadcastSessionConfig{
				Leader: f.leader, Slot: 2, ParentSlot: 1, Version: f.version, Broadcaster: capture,
			})
			require.NoError(t, session.BroadcastComponent(component, true))
			var packets [][]byte
			for _, packet := range capture.batches[0] {
				s, err := turbine.ParseShred(packet)
				require.NoError(t, err)
				if s.Type == turbine.ShredTypeData && (!incomplete || s.Index != 0) {
					packets = append(packets, packet)
				}
			}
			f.send(t, packets)
			if incomplete {
				// One missing data shred, zero parity: retain an incomplete slot
				// until cancellation, with no replay delivery or false completion.
				require.Eventually(t, func() bool { return f.receiver.Stats().ActiveSlots == 1 }, time.Second, time.Millisecond)
			} else {
				select {
				case err := <-f.receiver.Errors():
					require.ErrorContains(t, err, "failed signature verification")
				case <-time.After(10 * time.Second):
					t.Fatal("invalid transaction was not rejected")
				}
				require.Positive(t, f.receiver.Stats().AssemblyErrors)
			}
			require.Zero(t, f.receiver.Stats().SignatureErrors, "the leader's shred signatures are valid")
			require.Zero(t, f.receiver.Stats().BlocksEmitted)
			select {
			case block := <-f.receiver.Blocks():
				t.Fatalf("invalid/incomplete slot reached replay: %v", block)
			default:
			}
		})
	}
}

type recordedShredBatches struct{ batches [][][]byte }

func (r *recordedShredBatches) Broadcast(packets [][]byte) error {
	batch := make([][]byte, len(packets))
	for i := range packets {
		batch[i] = bytes.Clone(packets[i])
	}
	r.batches = append(r.batches, batch)
	return nil
}

type genesisIngressFixture struct {
	receiver             *turbine.UDPReceiver
	sender               *turbine.UDPBroadcaster
	leader               solana.PrivateKey
	version              uint16
	parentID, parentRoot solana.Hash
	genesisRoot          solana.Hash
	wire                 bytes.Buffer
	batches              uint32
	sent                 uint64
}

func startGenesisIngress(t *testing.T, seed *GenesisReplayBootstrap, version uint16) (*turbine.UDPReceiver, *turbine.UDPBroadcaster) {
	t.Helper()
	r := turbine.NewUDPReceiver("127.0.0.1:0")
	r.SetLeaderForSlot(seed.LeaderForSlot)
	r.SetShredVersion(version)
	require.Nil(t, r.LocalAddr())
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Error("UDP receiver did not stop after cancellation")
		}
		require.Nil(t, r.LocalAddr())
	})
	select {
	case err := <-r.Ready():
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("UDP receiver did not become ready")
	}
	addr := r.LocalAddr()
	require.NotNil(t, addr)
	require.NotZero(t, addr.Port)
	sender, err := turbine.NewUDPBroadcaster("127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sender.Close()) })
	sender.SetPeers([]*net.UDPAddr{addr})
	// Callers cannot mutate the receiver's bound address through this accessor.
	r.LocalAddr().Port = 0
	require.NotZero(t, r.LocalAddr().Port)
	return r, sender
}

func newGenesisIngressFixture(t *testing.T, g *genesis.Genesis, seed *GenesisReplayBootstrap) *genesisIngressFixture {
	t.Helper()
	hash := solana.MustHashFromBase58(seed.metadata.GenesisHash)
	f := &genesisIngressFixture{
		leader:  solana.PrivateKey(ed25519.NewKeyFromSeed(bytes.Repeat([]byte{1}, 32))),
		version: turbine.ShredVersionFromGenesisHash(hash),
	}
	f.receiver, f.sender = startGenesisIngress(t, seed, f.version)
	// Reproduce Agave create_new_ledger's slot-0 tick stream and chained root.
	// This is a transport anchor, not a fabricated slot-0 consensus block ID.
	ticks := make([]turbine.Entry, seed.metadata.TicksPerSlot)
	for i := range ticks {
		ticks[i].Hash = hash
	}
	component, err := turbine.NewEntryBatch(ticks)
	require.NoError(t, err)
	shredder := turbine.Shredder{Version: f.version}
	batch, _, _, err := shredder.MakeMerkleShredsFromComponent(f.leader, component, true, hash, 0, 0)
	require.NoError(t, err)
	f.parentRoot, f.genesisRoot = batch.ChainedMerkleRoot, batch.ChainedMerkleRoot
	f.record(t, component, true, batch.Packets)
	return f
}

// record encodes the exact inputs and output packets for independent Agave
// regeneration. The helper decodes components, generates its own data/parity
// shreds, compares every byte, and recovers a missing data shred in every batch.
func (f *genesisIngressFixture) record(t *testing.T, component turbine.BlockComponent, last bool, packets [][]byte) {
	t.Helper()
	var data, code *turbine.Shred
	for _, packet := range packets {
		s, err := turbine.ParseShred(packet)
		require.NoError(t, err)
		if s.Type == turbine.ShredTypeData && (data == nil || s.Index < data.Index) {
			data = s
		}
		if s.Type == turbine.ShredTypeCode && (code == nil || s.Index < code.Index) {
			code = s
		}
	}
	require.NotNil(t, data)
	require.NotNil(t, code)
	write := func(value any) { require.NoError(t, binary.Write(&f.wire, binary.LittleEndian, value)) }
	blob := func(value []byte) { write(uint32(len(value))); f.wire.Write(value) }
	write(data.Slot)
	write(data.Slot - uint64(data.ParentOffset))
	write(data.Index)
	write(code.Index)
	if last {
		write(uint8(1))
	} else {
		write(uint8(0))
	}
	chain, err := data.EmbeddedChainedMerkleRoot()
	require.NoError(t, err)
	f.wire.Write(chain[:])
	raw, err := turbine.MarshalBlockComponent(component)
	require.NoError(t, err)
	blob(raw)
	write(uint32(len(packets)))
	for _, packet := range packets {
		blob(packet)
	}
	f.batches++
}

func (f *genesisIngressFixture) send(t *testing.T, packets [][]byte) {
	t.Helper()
	// Bound bursts so this tests intentional FEC loss, not OS receive-buffer
	// saturation. Wait for observed packet counts, not arbitrary sleeps.
	for len(packets) > 0 {
		n := min(16, len(packets))
		require.NoError(t, f.sender.Broadcast(packets[:n]))
		f.sent += uint64(n)
		require.Eventually(t, func() bool { return f.receiver.Stats().Packets >= f.sent }, 10*time.Second, time.Millisecond)
		packets = packets[n:]
	}
}

func (f *genesisIngressFixture) receive(t *testing.T, fixture replayOracleBlock, entries []turbine.Entry) *b.Block {
	t.Helper()
	capture := new(recordedShredBatches)
	session := turbine.NewBroadcastSession(turbine.BroadcastSessionConfig{
		Leader: f.leader, Slot: fixture.Slot, ParentSlot: fixture.ParentSlot,
		ParentChainedMerkleRoot: f.parentRoot, Version: f.version, Broadcaster: capture,
	})
	bankHash := solana.MustHashFromBase58(fixture.Frozen.BankHash)
	footer := turbine.BlockFooter{BankHash: bankHash, BlockProducerTimeNanos: fixture.ProducerTimeNanos}
	body, err := turbine.NewEntryBatch(entries[:len(entries)-1])
	require.NoError(t, err)
	ending, err := turbine.NewEntryBatch(entries[len(entries)-1:])
	require.NoError(t, err)
	components := []turbine.BlockComponent{turbine.NewBlockHeader(fixture.ParentSlot, f.parentID), body, turbine.NewBlockFooter(footer), ending}
	require.NoError(t, session.BroadcastHeader(f.parentID))
	require.NoError(t, session.BroadcastEntryBatch(body.EntryBatch))
	require.NoError(t, session.BroadcastFooter(bankHash, fixture.ProducerTimeNanos, nil, nil))
	// Preserve the pinned sleep-mode oracle's num_hashes=0 ending tick. The
	// native producer's convenience method emits a num_hashes=1 tick instead.
	require.NoError(t, session.BroadcastComponent(ending, true))
	require.Len(t, capture.batches, len(components))
	for i, packets := range capture.batches {
		f.record(t, components[i], i == len(components)-1, packets)
	}
	if fixture.Slot == 1 {
		f.rejectUnauthenticated(t, capture.batches[0][0])
	}
	previousRoot := f.parentRoot
	for i := len(capture.batches) - 1; i >= 0; i-- {
		var delivered [][]byte
		for _, packet := range capture.batches[i] {
			shred, err := turbine.ParseShred(packet)
			require.NoError(t, err)
			chain, err := shred.EmbeddedChainedMerkleRoot()
			require.NoError(t, err)
			if i == 0 {
				require.Equal(t, previousRoot, chain)
			}
			// Drop first and last data shreds (including slot-complete flags),
			// and retain enough coding shreds to recover both.
			if shred.Type == turbine.ShredTypeData && (shred.Index%32 == 0 || shred.Index%32 == 31) {
				continue
			}
			if shred.Type == turbine.ShredTypeCode && shred.Position >= 4 {
				continue
			}
			delivered = append(delivered, packet)
		}
		for l, r := 0, len(delivered)-1; l < r; l, r = l+1, r-1 {
			delivered[l], delivered[r] = delivered[r], delivered[l]
		}
		delivered = append(delivered, delivered[0]) // duplicate
		f.send(t, delivered)
	}
	var got *b.Block
	select {
	case got = <-f.receiver.Blocks():
		require.NotNil(t, got)
	case err := <-f.receiver.Errors():
		t.Fatalf("ingress rejected slot %d: %v", fixture.Slot, err)
	case <-time.After(10 * time.Second):
		t.Fatalf("no reconstructed slot %d: %+v", fixture.Slot, f.receiver.Stats())
	}
	f.receiver.AcknowledgeBlockDelivery(got.Slot)
	require.Equal(t, fixture.Slot, got.Slot)
	require.Equal(t, fixture.ParentSlot, got.SourceParentSlot)
	require.True(t, got.HasAlpenglowParentBlockID)
	require.Equal(t, f.parentID, solana.Hash(got.AlpenglowParentBlockID))
	require.True(t, got.HasAlpenglowBlockID)
	require.Equal(t, session.BlockID(fixture.ParentSlot, f.parentID), solana.Hash(got.AlpenglowBlockID))
	require.True(t, got.HasAlpenglowLastChainedRoot)
	require.Equal(t, session.ChainedMerkleRoot(), solana.Hash(got.AlpenglowLastChainedRoot))
	require.NotEqual(t, got.AlpenglowBlockID, got.AlpenglowLastChainedRoot)
	require.NotEqual(t, bankHash, solana.Hash(got.AlpenglowBlockID))
	require.Equal(t, f.version, got.AlpenglowShredVersion)
	require.True(t, got.HasAlpenglowFooter)
	require.True(t, got.HasExpectedBankhash)
	require.Equal(t, bankHash, solana.Hash(got.ExpectedBankhash))
	require.Equal(t, fixture.ProducerTimeNanos, got.FooterProducerTimeNanos)
	require.True(t, got.TransactionSignaturesVerified())
	expected := turbine.BlockFromEntries(fixture.Slot, fixture.ParentSlot, entries)
	require.Equal(t, expected.Entries, got.Entries)
	require.Equal(t, expected.Blockhash, got.Blockhash)
	stats := f.receiver.Stats()
	require.Equal(t, fixture.Slot, stats.BlocksEmitted)
	require.GreaterOrEqual(t, stats.RecoveredData, fixture.Slot*8)
	require.Positive(t, stats.SigVerifies)
	require.Positive(t, stats.SigVerifyCached)
	f.parentID, f.parentRoot = solana.Hash(got.AlpenglowBlockID), solana.Hash(got.AlpenglowLastChainedRoot)
	return got
}

func (f *genesisIngressFixture) rejectUnauthenticated(t *testing.T, packet []byte) {
	t.Helper()
	badSig, badPayload, wrongLeader, wrongVersion := bytes.Clone(packet), bytes.Clone(packet), bytes.Clone(packet), bytes.Clone(packet)
	badSig[0] ^= 1
	badPayload[90] ^= 1
	shred, err := turbine.ParseShred(packet)
	require.NoError(t, err)
	root, err := shred.MerkleRoot()
	require.NoError(t, err)
	other := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{9}, 32))
	copy(wrongLeader[:64], ed25519.Sign(other, root[:]))
	binary.LittleEndian.PutUint16(wrongVersion[77:79], f.version^1)
	f.send(t, [][]byte{badSig, badPayload, wrongLeader, wrongVersion})
	require.Eventually(t, func() bool {
		stats := f.receiver.Stats()
		return stats.SignatureErrors == 3 && stats.ShredVersionMismatch == 1
	}, 10*time.Second, time.Millisecond)
	for range 3 {
		select {
		case err := <-f.receiver.Errors():
			require.ErrorIs(t, err, turbine.ErrInvalidSignature)
		case <-time.After(10 * time.Second):
			t.Fatal("missing shred signature rejection")
		}
	}
	select {
	case err := <-f.receiver.Errors():
		require.ErrorContains(t, err, "shred version mismatch")
	case <-time.After(10 * time.Second):
		t.Fatal("missing shred version rejection")
	}
	require.Zero(t, f.receiver.Stats().BlocksEmitted)
}

func (f *genesisIngressFixture) verifyOracle(t *testing.T, g *genesis.Genesis) {
	t.Helper()
	var request bytes.Buffer
	request.WriteString("MSHRED01")
	require.NoError(t, binary.Write(&request, binary.LittleEndian, f.version))
	require.NoError(t, binary.Write(&request, binary.LittleEndian, f.batches))
	request.Write(f.wire.Bytes())
	digest := fmt.Sprintf("%x", sha256.Sum256(request.Bytes()))
	fixturePath := filepath.Join("testdata", "genesis-shreds-agave.txt")
	var report []byte
	if helper := os.Getenv("MITHRIL_AGAVE_SHRED_ORACLE"); helper != "" {
		dir := t.TempDir()
		raw, err := genesis.Encode(g)
		require.NoError(t, err)
		genesisPath, wirePath := filepath.Join(dir, "genesis.bin"), filepath.Join(dir, "shreds.bin")
		require.NoError(t, os.WriteFile(genesisPath, raw, 0644))
		require.NoError(t, os.WriteFile(wirePath, request.Bytes(), 0644))
		output, err := exec.CommandContext(t.Context(), helper, genesisPath, wirePath).CombinedOutput()
		require.NoError(t, err, string(output))
		report = append([]byte("request_sha256="+digest+"\n"), output...)
		if os.Getenv("MITHRIL_UPDATE_ORACLE_FIXTURES") == "1" {
			require.NoError(t, os.WriteFile(fixturePath, report, 0644))
		}
	} else {
		var err error
		report, err = os.ReadFile(fixturePath)
		require.NoError(t, err)
	}
	require.Contains(t, string(report), "request_sha256="+digest+"\n", "signed wire fixture changed: re-run the pinned Agave shred oracle")
	require.Contains(t, string(report), "agave_revision="+genesis.AgaveRevision+"\n")
	require.Contains(t, string(report), fmt.Sprintf("components=%d\n", f.batches))
	require.Contains(t, string(report), fmt.Sprintf("version=%d\n", f.version))
	require.Contains(t, string(report), "genesis_chained_root="+f.genesisRoot.String()+"\n")
	t.Logf("signed UDP ingress: %+v", f.receiver.Stats())
}

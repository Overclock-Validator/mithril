package quicserver_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/tpu"
	"github.com/Overclock-Validator/mithril/pkg/tpu/pipeline"
	"github.com/Overclock-Validator/mithril/pkg/tpu/quicserver"
	"github.com/Overclock-Validator/mithril/pkg/tpu/quicserver/testutils"
	"github.com/Overclock-Validator/mithril/pkg/tpu/sink"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/stretchr/testify/require"
)

const signedTestTx = `
01fd 740a 299c 8140 4158 2e9e d4cc 7c96
0c11 ead6 eefc 2681 ae9e df71 cb31 d2ca
608b 62a1 3555 f268 82fb 622c 9c60 132c
a362 d64d 6906 ed47 012d af73 f1b6 d805
0f01 0002 06ed 341d e66d 6788 6d10 97e4
ac2b cfd5 00c7 9411 872b 0f47 ed52 5786
ce23 f750 0a16 8b21 8f1e 3359 a2be 9952
02b4 d07f 1978 eecf e602 2fed dd83 3509
9f9e 25af 0031 9098 8c12 0f69 b745 18e1
5183 8682 b982 2fa5 7d3f 529d 87d2 0b05
0ceb c02b 1a02 39ac 5042 f7fd afc3 f269
19a7 9644 8de8 142c d2ee d20d c08e cf80
8b79 b09d 6985 0f2d 6e02 a47a f824 d09a
b69d c42d 70cb 28cb fa24 9fb7 ee57 b9d2
56c1 2762 ef00 0000 0000 0000 0000 0000
0000 0000 0000 0000 0000 0000 0000 0000
0000 0000 00ce 9121 49be dcfc c6d4 82a1
7a1a 6ae3 d823 35a8 57e4 d86e 2c3f b730
e519 cca5 a802 0405 0102 0302 0207 0003
0000 000b 0005 0200 000c 0200 0000 0f0b
0000 0000 0000
`

func parseHexdump(s string) []byte {
	s = strings.ReplaceAll(s, " ", "")
	s = strings.ReplaceAll(s, "\n", "")
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}

func spawnWithPipeline(
	t *testing.T,
	ctx context.Context,
	name string,
	udpConn *net.UDPConn,
	cfg quicserver.ServerConfig,
) (*quicserver.SpawnServerResult, *pipeline.Pipeline, *sink.Noop) {
	t.Helper()

	_, identity, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	noop := &sink.Noop{}
	pl, ingress := pipeline.Start(ctx, pipeline.Config{
		SigverifyWorkers: 2,
		TxV1Enabled:      func() bool { return true },
		Sink:             noop,
	})
	cfg.Ingress = ingress

	result, err := quicserver.Spawn(
		ctx,
		name,
		[]quicserver.QuicSocket{quicserver.QuicSocketFromUDP(udpConn)},
		identity,
		cfg,
	)
	require.NoError(t, err)
	return result, pl, noop
}

func TestServerReceivesTransactionOverQUIC(t *testing.T) {
	payload := parseHexdump(signedTestTx)
	require.True(t, tpu.VerifyPacket(payload))

	udpConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	require.NoError(t, err)
	defer udpConn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result, pl, noop := spawnWithPipeline(t, ctx, "test-tpu", udpConn, quicserver.DefaultServerConfig())
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()
		_ = result.Close(shutdownCtx)
		pl.Stop()
	}()

	addr := result.Listeners[0].Addr().String()

	client, err := testutils.NewClient()
	require.NoError(t, err)

	dialCtx, dialCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer dialCancel()

	conn, err := client.Dial(dialCtx, addr)
	require.NoError(t, err)
	defer conn.CloseWithError(0, "done")

	require.NoError(t, client.Send(dialCtx, conn, payload))

	require.Eventually(t, func() bool {
		stats := noop.Snapshot()
		return stats.InPackets == 1 && stats.InBytes == uint64(len(payload))
	}, time.Second, 10*time.Millisecond)
}

func TestServerReceivesMaxSizeV1TransactionOverQUIC(t *testing.T) {
	// This fixture's fixed envelope is 175 bytes, leaving 3921 instruction
	// data bytes at the 4096-byte SIMD-0385 limit.
	payload := txfixture.MustSignedV1Wire(17, 3921)
	require.Len(t, payload, quicserver.PacketDataSize)

	udpConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	require.NoError(t, err)
	defer udpConn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result, pl, noop := spawnWithPipeline(t, ctx, "test-tpu-v1-max", udpConn, quicserver.DefaultServerConfig())
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()
		_ = result.Close(shutdownCtx)
		pl.Stop()
	}()

	client, err := testutils.NewClient()
	require.NoError(t, err)
	conn, err := client.Dial(ctx, result.Listeners[0].Addr().String())
	require.NoError(t, err)
	defer conn.CloseWithError(0, "done")
	require.NoError(t, client.Send(ctx, conn, payload))

	require.Eventually(t, func() bool {
		stats := noop.Snapshot()
		return stats.InPackets == 1 && stats.InBytes == uint64(len(payload))
	}, 2*time.Second, 10*time.Millisecond)
}

func TestServerRejectsTransactionAboveV1Limit(t *testing.T) {
	payload := txfixture.MustSignedV1Wire(18, 3922)
	require.Len(t, payload, quicserver.PacketDataSize+1)

	udpConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	require.NoError(t, err)
	defer udpConn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result, pl, noop := spawnWithPipeline(t, ctx, "test-tpu-v1-over", udpConn, quicserver.DefaultServerConfig())
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()
		_ = result.Close(shutdownCtx)
		pl.Stop()
	}()

	client, err := testutils.NewClient()
	require.NoError(t, err)
	conn, err := client.Dial(ctx, result.Listeners[0].Addr().String())
	require.NoError(t, err)
	defer conn.CloseWithError(0, "done")
	_ = client.Send(ctx, conn, payload)

	require.Eventually(t, func() bool {
		return result.Stats.InvalidStreamSize.Load() == 1
	}, 2*time.Second, 10*time.Millisecond)
	require.Zero(t, noop.Snapshot().InPackets)
}

func TestServerRejectsTransportLimitsThatExceedPool(t *testing.T) {
	_, identity, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	udpConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	require.NoError(t, err)
	defer udpConn.Close()

	cfg := quicserver.DefaultServerConfig()
	cfg.MaxStreamDataBytes = quicserver.PacketDataSize + 1
	cfg.StreamReceiveWindowSize = cfg.MaxStreamDataBytes
	_, err = quicserver.Spawn(context.Background(), "invalid-pool-limit", []quicserver.QuicSocket{
		quicserver.QuicSocketFromUDP(udpConn),
	}, identity, cfg)
	require.ErrorContains(t, err, "exceeds packet buffer size")

	cfg = quicserver.DefaultServerConfig()
	cfg.StreamReceiveWindowSize = cfg.MaxStreamDataBytes - 1
	_, err = quicserver.Spawn(context.Background(), "invalid-window", []quicserver.QuicSocket{
		quicserver.QuicSocketFromUDP(udpConn),
	}, identity, cfg)
	require.ErrorContains(t, err, "smaller than max stream data bytes")
}

func TestServerDiscardsInvalidTransaction(t *testing.T) {
	udpConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	require.NoError(t, err)
	defer udpConn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result, pl, noop := spawnWithPipeline(t, ctx, "test-tpu-discard", udpConn, quicserver.DefaultServerConfig())
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()
		_ = result.Close(shutdownCtx)
		pl.Stop()
	}()

	addr := result.Listeners[0].Addr().String()
	payload := []byte("not a transaction")

	client, err := testutils.NewClient()
	require.NoError(t, err)

	dialCtx, dialCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer dialCancel()

	conn, err := client.Dial(dialCtx, addr)
	require.NoError(t, err)
	defer conn.CloseWithError(0, "done")

	require.NoError(t, client.Send(dialCtx, conn, payload))

	require.Eventually(t, func() bool {
		stats := pl.Stats()
		return noop.Snapshot().InPackets == 0 &&
			stats.Dedup.DroppedSanitize+stats.Sigverify.DroppedSigverify > 0
	}, time.Second, 10*time.Millisecond)
}

func TestServerRefusesConnectionsBeyondCap(t *testing.T) {
	_, identity, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	udpConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	require.NoError(t, err)
	defer udpConn.Close()

	cfg := quicserver.DefaultServerConfig()
	cfg.MaxConnections = 1

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result, err := quicserver.Spawn(
		ctx,
		"test-tpu-cap",
		[]quicserver.QuicSocket{quicserver.QuicSocketFromUDP(udpConn)},
		identity,
		cfg,
	)
	require.NoError(t, err)
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()
		_ = result.Close(shutdownCtx)
	}()

	addr := result.Listeners[0].Addr().String()
	client, err := testutils.NewClient()
	require.NoError(t, err)

	dialCtx, dialCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer dialCancel()

	conn1, err := client.Dial(dialCtx, addr)
	require.NoError(t, err)
	defer conn1.CloseWithError(0, "done")

	stream1, err := conn1.OpenUniStreamSync(dialCtx)
	require.NoError(t, err)
	_, err = stream1.Write([]byte("hold"))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return result.Stats.OpenConnections.Load() == 1
	}, time.Second, 10*time.Millisecond)

	time.Sleep(2 * cfg.WaitForChunkTimeout)

	conn2, err := client.Dial(dialCtx, addr)
	if err == nil {
		defer conn2.CloseWithError(0, "done")
	}

	require.Eventually(t, func() bool {
		return result.Stats.RefusedTooManyOpen.Load() >= 1 || err != nil
	}, time.Second, 10*time.Millisecond, "expected connection cap enforcement, refused=%d err=%v",
		result.Stats.RefusedTooManyOpen.Load(), err)
}

func TestBenchClientEndToEnd(t *testing.T) {
	_, identity, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	udpConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	require.NoError(t, err)
	defer udpConn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result, err := quicserver.Spawn(
		ctx,
		"test-tpu-bench",
		[]quicserver.QuicSocket{quicserver.QuicSocketFromUDP(udpConn)},
		identity,
		quicserver.DefaultServerConfig(),
	)
	require.NoError(t, err)
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()
		_ = result.Close(shutdownCtx)
	}()

	benchResult, err := testutils.RunBench(ctx, testutils.BenchOptions{
		Addr:          result.Listeners[0].Addr().String(),
		Duration:      200 * time.Millisecond,
		Connections:   2,
		PayloadSize:   128,
		ProgressEvery: 0,
		DialTimeout:   time.Second,
		StreamTimeout: time.Second,
	})
	require.NoError(t, err)
	require.Greater(t, benchResult.Sent, uint64(0))
}

func TestServerRefusesConnectionsBeyondPerIPCap(t *testing.T) {
	_, identity, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	udpConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	require.NoError(t, err)
	defer udpConn.Close()

	cfg := quicserver.DefaultServerConfig()
	cfg.MaxConnections = 16
	cfg.MaxConnectionsPerIP = 1

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result, err := quicserver.Spawn(
		ctx,
		"test-tpu-per-ip",
		[]quicserver.QuicSocket{quicserver.QuicSocketFromUDP(udpConn)},
		identity,
		cfg,
	)
	require.NoError(t, err)
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()
		_ = result.Close(shutdownCtx)
	}()

	addr := result.Listeners[0].Addr().String()
	client, err := testutils.NewClient()
	require.NoError(t, err)

	dialCtx, dialCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer dialCancel()

	conn1, err := client.Dial(dialCtx, addr)
	require.NoError(t, err)
	defer conn1.CloseWithError(0, "done")

	stream1, err := conn1.OpenUniStreamSync(dialCtx)
	require.NoError(t, err)
	_, err = stream1.Write([]byte("hold"))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return result.Stats.OpenConnections.Load() == 1
	}, time.Second, 10*time.Millisecond)

	time.Sleep(2 * cfg.WaitForChunkTimeout)

	conn2, err := client.Dial(dialCtx, addr)
	if err == nil {
		defer conn2.CloseWithError(0, "done")
	}

	require.Eventually(t, func() bool {
		return result.Stats.RefusedPerIP.Load() >= 1 || err != nil
	}, time.Second, 10*time.Millisecond, "expected per-IP cap enforcement, refused_per_ip=%d err=%v",
		result.Stats.RefusedPerIP.Load(), err)
}

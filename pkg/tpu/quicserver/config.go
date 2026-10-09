package quicserver

import (
	"time"

	"github.com/Overclock-Validator/mithril/pkg/tpu/packet"
	"github.com/Overclock-Validator/mithril/pkg/tpu/pipeline"
)

const (
	ALPNTPUProtocolID = "solana-tpu"

	PacketDataSize = packet.DataSize

	DefaultMaxConnections       = 1024
	DefaultMaxStreamsPerConn    = 16
	DefaultMaxQuicEndpoints     = 1
	DefaultMaxStreamDataBytes   = PacketDataSize
	DefaultStreamReceiveWindow  = PacketDataSize
	DefaultConnectionRecvWindow = 8 * 1024 * 1024

	QuicMaxTimeout             = 30 * time.Second
	DefaultWaitForChunkTimeout = 100 * time.Millisecond
	DefaultHandshakeTimeout    = 3 * time.Second
	DefaultMaxConnectionsPerIP = 8
)

// ServerConfig mirrors agave's streamer::quic::QuicStreamerConfig.
type ServerConfig struct {
	MaxConnectionsPerIP     int
	WaitForChunkTimeout     time.Duration
	StreamReceiveWindowSize uint32
	MaxStreamDataBytes      uint32
	MaxConnections          int
	MaxStreamsPerConnection int

	// Ingress receives completed transaction packets for the TPU pipeline.
	// When nil, packets are released after read.
	Ingress      chan<- packet.Packet
	IngressStats *pipeline.IngressStats
}

func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		MaxConnectionsPerIP:     DefaultMaxConnectionsPerIP,
		WaitForChunkTimeout:     DefaultWaitForChunkTimeout,
		StreamReceiveWindowSize: DefaultStreamReceiveWindow,
		MaxStreamDataBytes:      DefaultMaxStreamDataBytes,
		MaxConnections:          DefaultMaxConnections,
		MaxStreamsPerConnection: DefaultMaxStreamsPerConn,
	}
}

func (c ServerConfig) normalized() ServerConfig {
	out := c
	if out.MaxConnections <= 0 {
		out.MaxConnections = DefaultMaxConnections
	}
	if out.MaxStreamsPerConnection <= 0 {
		out.MaxStreamsPerConnection = DefaultMaxStreamsPerConn
	}
	if out.MaxConnectionsPerIP <= 0 {
		out.MaxConnectionsPerIP = DefaultMaxConnectionsPerIP
	}
	if out.WaitForChunkTimeout <= 0 {
		out.WaitForChunkTimeout = DefaultWaitForChunkTimeout
	}
	if out.StreamReceiveWindowSize == 0 {
		out.StreamReceiveWindowSize = DefaultStreamReceiveWindow
	}
	if out.MaxStreamDataBytes == 0 {
		out.MaxStreamDataBytes = DefaultMaxStreamDataBytes
	}
	return out
}

func (c ServerConfig) maxStreamSlots() int {
	n := c.normalized()
	return n.MaxConnections * n.MaxStreamsPerConnection
}

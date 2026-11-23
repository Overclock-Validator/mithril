package gossip

import (
	"math"
	"net"
	"sync/atomic"
	"time"
)

// DecodeWireKind is only the decoded protocol header, not authenticated body
// acceptance. Pull requests and prune packets are not fully parsed here.
type DecodeWireKind uint8

const (
	WirePullRequest DecodeWireKind = iota
	WirePullResponse
	WirePush
	WirePrune
	WirePing
	WirePong
	WireUnknownVariant
	WireMalformedHeader
)

// PacketDecodeObservation is an optional fixed-size result of one actual decode.
// Contact counts describe only visited CRDS values; a verified prefix may already
// have reached its callback even when an unsupported or malformed value follows.
// UnparsedSuffix excludes that offending value and counts only later declared
// values. It never labels their contents invalid. Per-packet counts are <=128;
// callers aggregating them must use checked/saturating counters independently.
type PacketDecodeObservation struct {
	WireKind           DecodeWireKind
	AttemptedContactV2 uint64
	SignatureValid     uint64
	SignatureInvalid   uint64
	UnsupportedType    uint64
	MalformedValue     uint64
	UnparsedSuffix     uint64
}

// DecodeCounter keeps saturated counts unknown instead of presenting zero.
type DecodeCounter struct {
	Value uint64
	Known bool
}

// DecodeMonitoringObservation belongs to one opted-in Run/socket generation.
// Its independently read counters are published only on the existing diagnostics
// cadence; SampledAt must not be refreshed when a reader repeats this cache copy.
type DecodeMonitoringObservation struct {
	Generation         uint64
	StartedAt          time.Time
	SampledAt          time.Time
	WireKinds          [8]DecodeCounter
	AttemptedContactV2 DecodeCounter
	SignatureValid     DecodeCounter
	SignatureInvalid   DecodeCounter
	UnsupportedType    DecodeCounter
	MalformedValue     DecodeCounter
	UnparsedSuffix     DecodeCounter
}

type decodeMonitoringRecorder struct {
	conn       *net.UDPConn
	generation uint64
	startedAt  time.Time
	wireKinds  [8]atomic.Uint64
	contacts   [6]atomic.Uint64
}

func (r *decodeMonitoringRecorder) record(o PacketDecodeObservation) {
	if o.WireKind <= WireMalformedHeader {
		addUDPPayloadBytes(&r.wireKinds[o.WireKind], 1, 1)
	}
	for i, count := range [...]uint64{o.AttemptedContactV2, o.SignatureValid, o.SignatureInvalid, o.UnsupportedType, o.MalformedValue, o.UnparsedSuffix} {
		// Values come from the same bounded parser, never packet-declared counts
		// before its 128-value admission check.
		if count <= 128 {
			addUDPPayloadBytes(&r.contacts[i], int(count), 128)
		}
	}
}

func (r *decodeMonitoringRecorder) snapshot(now time.Time) DecodeMonitoringObservation {
	read := func(counter *atomic.Uint64) DecodeCounter {
		value := counter.Load()
		if value == math.MaxUint64 {
			return DecodeCounter{}
		}
		return DecodeCounter{Value: value, Known: true}
	}
	o := DecodeMonitoringObservation{Generation: r.generation, StartedAt: r.startedAt, SampledAt: now}
	for i := range o.WireKinds {
		o.WireKinds[i] = read(&r.wireKinds[i])
	}
	for i, field := range [...]*DecodeCounter{&o.AttemptedContactV2, &o.SignatureValid, &o.SignatureInvalid, &o.UnsupportedType, &o.MalformedValue, &o.UnparsedSuffix} {
		*field = read(&r.contacts[i])
	}
	return o
}

// EnableDecodeMonitoring selects recording before the next Run. Like payload
// recording, enabling mid-Run does not invent coverage of its earlier packets.
func (c *Client) EnableDecodeMonitoring() {
	if c != nil {
		c.decodeMonitoringEnabled.Store(true)
	}
}

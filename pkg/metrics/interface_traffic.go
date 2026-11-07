package metrics

import (
	"errors"
	"strconv"
	"strings"
	"time"
)

// MaxInterfaceTraffic bounds one visible-interface sample, ordered by index.
const MaxInterfaceTraffic = 8

// InterfaceTrafficCounters are kernel interface counters, not Mithril traffic
// or proof of remote delivery. Loopback, tunnel and physical counters overlap.
type InterfaceTrafficCounters struct {
	Index                           uint32
	Loopback                        bool
	MTU                             uint32
	ReceiveBytes, TransmitBytes     uint64
	ReceivePackets, TransmitPackets uint64
	FailuresAvailable               bool
	ReceiveErrors, ReceiveDropped   uint64
	TransmitErrors, TransmitDropped uint64
}

// InterfaceTrafficObservation keeps original capture dates and bounded coverage.
// State is available, read_failed, invalid_record, binding_changed,
// capture_expired or clock_invalid. A failed sample has no interface counters.
// Counters are sampled, not an atomic transaction across interfaces.
type InterfaceTrafficObservation struct {
	StartedAt, SampledAt time.Time
	State                string
	TotalInterfaces      uint32
	Count                int
	Interfaces           [MaxInterfaceTraffic]InterfaceTrafficCounters
}

type interfaceTrafficIdentity struct {
	name     string
	index    uint32
	loopback bool
	mtu      uint32
}

// InterfaceTrafficSampler is confined to the existing diagnostics owner.
// Construction performs no reads and a nil sampler performs no work.
type InterfaceTrafficSampler struct{}

// NewInterfaceTrafficSampler selects observation without starting a worker.
func NewInterfaceTrafficSampler(enabled bool) *InterfaceTrafficSampler {
	if !enabled {
		return nil
	}
	return &InterfaceTrafficSampler{}
}

// Sample returns false for disabled or unsupported platforms. Failed supported
// reads retain their failure category. No interface-lifetime or rate is inferred.
func (s *InterfaceTrafficSampler) Sample() (InterfaceTrafficObservation, bool) {
	if s == nil {
		return InterfaceTrafficObservation{}, false
	}
	o, supported := readInterfaceTraffic()
	if !supported {
		return InterfaceTrafficObservation{}, false
	}
	return o, true
}

var errInterfaceTraffic = errors.New("invalid interface traffic record")

func validInterfaceName(name string) bool {
	return len(name) > 0 && len(name) < 16 && name != "." && name != ".." && !strings.ContainsAny(name, "/\x00\n\r")
}

func parseInterfaceTraffic(data []byte) (map[string]InterfaceTrafficCounters, error) {
	if len(data) == 0 || len(data) > 65536 || data[len(data)-1] != '\n' {
		return nil, errInterfaceTraffic
	}
	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	if len(lines) < 2 || len(lines) > 258 || strings.Join(strings.Fields(lines[0]), " ") != "Inter-| Receive | Transmit" ||
		strings.Join(strings.Fields(lines[1]), " ") != "face |bytes packets errs drop fifo frame compressed multicast|bytes packets errs drop fifo colls carrier compressed" {
		return nil, errInterfaceTraffic
	}
	result := make(map[string]InterfaceTrafficCounters, len(lines)-2)
	for _, line := range lines[2:] {
		// Interface aliases can contain a colon; the final colon starts counters.
		separator := strings.LastIndexByte(line, ':')
		if separator < 0 {
			return nil, errInterfaceTraffic
		}
		name, fields := strings.TrimSpace(line[:separator]), strings.Fields(line[separator+1:])
		if !validInterfaceName(name) || len(fields) != 16 {
			return nil, errInterfaceTraffic
		}
		if _, duplicate := result[name]; duplicate {
			return nil, errInterfaceTraffic
		}
		var counters [16]uint64
		for i, text := range fields {
			value, err := strconv.ParseUint(text, 10, 64)
			if err != nil || strconv.FormatUint(value, 10) != text {
				return nil, errInterfaceTraffic
			}
			counters[i] = value
		}
		result[name] = InterfaceTrafficCounters{ReceiveBytes: counters[0], ReceivePackets: counters[1], ReceiveErrors: counters[2], ReceiveDropped: counters[3],
			TransmitBytes: counters[8], TransmitPackets: counters[9], TransmitErrors: counters[10], TransmitDropped: counters[11], FailuresAvailable: true}
	}
	return result, nil
}

func readInterfaceTrafficRecord(read func(string, int, bool) ([]byte, error), inventory func() ([]interfaceTrafficIdentity, error), namespace func() ([2]uint64, error), now func() time.Time) (o InterfaceTrafficObservation) {
	o.StartedAt, o.State = now(), "read_failed"
	last, expired, clockInvalid := o.StartedAt, false, false
	withinBudget := func() bool {
		at := now()
		clockInvalid = clockInvalid || at.Before(last)
		expired = expired || at.Sub(o.StartedAt) > time.Second
		last = at
		return !clockInvalid && !expired
	}
	defer func() {
		withinBudget()
		o.SampledAt = last
		if clockInvalid || expired {
			state := "capture_expired"
			if clockInvalid {
				state = "clock_invalid"
			}
			o = InterfaceTrafficObservation{StartedAt: o.StartedAt, SampledAt: last, State: state}
		}
		if o.State != "available" {
			o = InterfaceTrafficObservation{StartedAt: o.StartedAt, SampledAt: last, State: o.State}
		}
	}()
	beforeNamespace, err := namespace()
	if err != nil || !withinBudget() || beforeNamespace == ([2]uint64{}) {
		return o
	}
	before, err := inventory()
	if err != nil || !withinBudget() || len(before) > 256 {
		return o
	}
	data, err := read("/proc/thread-self/net/dev", 65536, false)
	if err != nil || !withinBudget() {
		return o
	}
	counters, err := parseInterfaceTraffic(data)
	if err != nil {
		o.State = "invalid_record"
		return o
	}
	if !withinBudget() {
		return o
	}
	after, err := inventory()
	if err != nil || !withinBudget() {
		return o
	}
	afterNamespace, err := namespace()
	if err != nil || !withinBudget() {
		return o
	}
	if beforeNamespace != afterNamespace || len(before) != len(after) || len(before) != len(counters) {
		o.State = "binding_changed"
		return o
	}
	for i, identity := range before {
		if !validInterfaceName(identity.name) || identity.index == 0 || i > 0 && identity.index <= before[i-1].index || identity != after[i] {
			o.State = "binding_changed"
			return o
		}
		value, ok := counters[identity.name]
		if !ok {
			o.State = "binding_changed"
			return o
		}
		delete(counters, identity.name)
		if i < MaxInterfaceTraffic {
			value.Index, value.Loopback, value.MTU = identity.index, identity.loopback, identity.mtu
			o.Interfaces[i] = value
		}
	}
	o.State, o.TotalInterfaces, o.Count = "available", uint32(len(before)), min(len(before), MaxInterfaceTraffic)
	return o
}

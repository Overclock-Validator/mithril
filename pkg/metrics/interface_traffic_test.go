package metrics

import (
	"errors"
	"fmt"
	"math"
	"runtime"
	"strings"
	"testing"
	"time"
)

const interfaceTrafficHeader = "Inter-|   Receive                                                |  Transmit\n face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed\n"

func interfaceTrafficFixture(names ...string) string {
	result := interfaceTrafficHeader
	for _, name := range names {
		result += name + ": 100 2 3 4 5 6 7 8 200 10 11 12 13 14 15 16\n"
	}
	return result
}

func TestInterfaceTrafficParser(t *testing.T) {
	valid := interfaceTrafficFixture("lo", "eth0:1")
	got, err := parseInterfaceTraffic([]byte(valid))
	if err != nil || len(got) != 2 || got["eth0:1"] != (InterfaceTrafficCounters{ReceiveBytes: 100, ReceivePackets: 2, ReceiveErrors: 3, ReceiveDropped: 4,
		TransmitBytes: 200, TransmitPackets: 10, TransmitErrors: 11, TransmitDropped: 12, FailuresAvailable: true}) {
		t.Fatalf("field positions or alias changed: %+v, %v", got, err)
	}
	for _, value := range []string{"0", "18446744073709551615"} {
		body := interfaceTrafficHeader + "lo: " + strings.TrimSpace(strings.Repeat(value+" ", 16)) + "\n"
		got, err := parseInterfaceTraffic([]byte(body))
		if err != nil || !got["lo"].FailuresAvailable || value == "0" && got["lo"].TransmitDropped != 0 ||
			value != "0" && (got["lo"].ReceiveBytes != math.MaxUint64 || got["lo"].ReceiveDropped != math.MaxUint64 || got["lo"].TransmitErrors != math.MaxUint64) {
			t.Fatal("zero or full-width counters were lost")
		}
	}
	for name, body := range map[string]string{
		"empty": "", "truncated": strings.TrimSuffix(valid, "\n"),
		"header":         strings.Replace(valid, "Receive", "Unknown", 1),
		"duplicate":      interfaceTrafficFixture("lo", "lo"),
		"missing":        strings.Replace(valid, " 15 16", " 15", 1),
		"extra":          strings.Replace(valid, " 15 16", " 15 16 17", 1),
		"negative":       strings.Replace(valid, "100", "-1", 1),
		"overflow":       strings.Replace(valid, "100", "18446744073709551616", 1),
		"unused_invalid": strings.Replace(valid, " 15 ", " bad ", 1),
		"path":           interfaceTrafficFixture("../private"), "oversize": strings.Repeat("x", 65537),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseInterfaceTraffic([]byte(body)); err == nil {
				t.Fatal("invalid record accepted")
			}
		})
	}
}

func TestInterfaceTrafficCaptureAndCoverage(t *testing.T) {
	at := time.Now()
	var identities []interfaceTrafficIdentity
	var names []string
	for i := range MaxInterfaceTraffic + 1 {
		name := fmt.Sprintf("if%d", i)
		names = append(names, name)
		identities = append(identities, interfaceTrafficIdentity{name: name, index: uint32(i + 1), loopback: i == 0, mtu: 1500})
	}
	read := func(path string, limit int, first bool) ([]byte, error) {
		if path != "/proc/thread-self/net/dev" || limit != 65536 || first {
			t.Fatal("unexpected source read")
		}
		return []byte(interfaceTrafficFixture(names...)), nil
	}
	namespace := func() ([2]uint64, error) { return [2]uint64{1, 2}, nil }
	inventory := func() ([]interfaceTrafficIdentity, error) {
		return append([]interfaceTrafficIdentity(nil), identities...), nil
	}
	got := readInterfaceTrafficRecord(read, inventory, namespace, func() time.Time { return at })
	if got.State != "available" || got.Count != MaxInterfaceTraffic || got.TotalInterfaces != MaxInterfaceTraffic+1 || got.SampledAt != at || got.StartedAt != at {
		t.Fatalf("coverage = %+v", got)
	}
	for i := range got.Count {
		if got.Interfaces[i].Index != uint32(i+1) || got.Interfaces[i].Loopback != (i == 0) || got.Interfaces[i].MTU != 1500 || got.Interfaces[i].ReceivePackets != 2 {
			t.Fatal("ordered numeric identity or counters lost")
		}
	}
	ticks := 0
	boundary := readInterfaceTrafficRecord(read, inventory, namespace, func() time.Time {
		ticks++
		if ticks == 1 {
			return at
		}
		return at.Add(time.Second)
	})
	if boundary.State != "available" || boundary.StartedAt != at || boundary.SampledAt != at.Add(time.Second) {
		t.Fatal("the exact one-second capture boundary was rejected or redated")
	}
	empty := readInterfaceTrafficRecord(func(string, int, bool) ([]byte, error) {
		return []byte(interfaceTrafficHeader), nil
	}, func() ([]interfaceTrafficIdentity, error) { return nil, nil }, namespace, func() time.Time { return at })
	if empty.State != "available" || empty.Count != 0 || empty.TotalInterfaces != 0 {
		t.Fatal("a measured empty inventory became a read failure")
	}
	for _, failure := range []string{"read", "invalid", "namespace", "index", "mtu_changed", "duplicate_name", "zero_index", "late", "clock"} {
		t.Run(failure, func(t *testing.T) {
			calls, ticks, nsCalls := 0, 0, 0
			inv := func() ([]interfaceTrafficIdentity, error) {
				calls++
				v, _ := inventory()
				if failure == "zero_index" {
					v[0].index = 0
				}
				if calls == 2 {
					switch failure {
					case "index":
						v[0].index++
					case "mtu_changed":
						v[0].mtu++
					case "duplicate_name":
						v[1].name = v[0].name
					}
				}
				return v, nil
			}
			got := readInterfaceTrafficRecord(func(path string, limit int, first bool) ([]byte, error) {
				if failure == "read" {
					return nil, errors.New("private source")
				}
				if failure == "invalid" {
					return []byte("bad"), nil
				}
				return read(path, limit, first)
			}, inv, func() ([2]uint64, error) {
				nsCalls++
				if failure == "namespace" && nsCalls == 2 {
					return [2]uint64{1, 3}, nil
				}
				return namespace()
			}, func() time.Time {
				ticks++
				if ticks > 1 && failure == "late" {
					return at.Add(time.Second + 1)
				}
				if ticks > 1 && failure == "clock" {
					return at.Add(-1)
				}
				return at
			})
			if got.State == "available" || got.Count != 0 || got.TotalInterfaces != 0 || got.Interfaces != ([MaxInterfaceTraffic]InterfaceTrafficCounters{}) {
				t.Fatalf("failed sample retained partial counters: %+v", got)
			}
			if failure == "late" && got.State != "capture_expired" || failure == "clock" && got.State != "clock_invalid" {
				t.Fatal("capture failure category changed")
			}
		})
	}
}

func TestInterfaceTrafficDisabledAndUnsupported(t *testing.T) {
	if NewInterfaceTrafficSampler(false) != nil {
		t.Fatal("disabled sampler allocated")
	}
	var disabled *InterfaceTrafficSampler
	if got, supported := disabled.Sample(); supported || got != (InterfaceTrafficObservation{}) {
		t.Fatal("disabled sampler read a source")
	}
	if testing.AllocsPerRun(100, func() { _, _ = disabled.Sample() }) != 0 {
		t.Fatal("disabled path allocated")
	}
	if runtime.GOOS != "linux" {
		if got, supported := NewInterfaceTrafficSampler(true).Sample(); supported || got != (InterfaceTrafficObservation{}) {
			t.Fatal("unsupported platform invented data")
		}
	}
}

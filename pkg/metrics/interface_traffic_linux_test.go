//go:build linux

package metrics

import (
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestInterfaceTrafficNativeRead(t *testing.T) {
	before := time.Now()
	o, supported := NewInterfaceTrafficSampler(true).Sample()
	after := time.Now()
	if !supported || o.StartedAt.Before(before) || o.SampledAt.After(after) {
		t.Fatalf("native interface capture: supported=%t state=%s count=%d total=%d", supported, o.State, o.Count, o.TotalInterfaces)
	}
	// Sandboxes may deny netlink and busy hosts may exceed the acquisition
	// budget. Those are unknown observations, not fabricated zero counters.
	if o.State != "available" {
		switch o.State {
		case "read_failed", "invalid_record", "binding_changed", "capture_expired", "clock_invalid":
		default:
			t.Fatal("unknown native failure category")
		}
		if o.Count != 0 || o.TotalInterfaces != 0 || o.Interfaces != ([MaxInterfaceTraffic]InterfaceTrafficCounters{}) {
			t.Fatal("failed native capture retained counters")
		}
		t.Logf("native interface counters unavailable: %s", o.State)
		return
	}
	if o.SampledAt.Before(o.StartedAt) || o.SampledAt.Sub(o.StartedAt) > time.Second || o.Count != min(int(o.TotalInterfaces), MaxInterfaceTraffic) {
		t.Fatal("available native capture violated its date or coverage contract")
	}
	for i := range o.Count {
		if o.Interfaces[i].Index == 0 || i > 0 && o.Interfaces[i].Index <= o.Interfaces[i-1].Index {
			t.Fatal("native interface indices were not uniquely ordered")
		}
	}
}

func TestInterfaceTrafficNativeMTU(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	before, err := interfaceTrafficInventory()
	if err != nil {
		t.Skip("interface inventory unavailable")
	}
	o, supported := NewInterfaceTrafficSampler(true).Sample()
	after, err := interfaceTrafficInventory()
	if err != nil || !supported || o.State != "available" || len(before) != len(after) || o.Count == 0 || o.Count > len(before) {
		t.Skip("stable interface capture unavailable")
	}
	for i := range o.Count {
		if before[i] != after[i] {
			t.Skip("interface inventory changed")
		}
		if o.Interfaces[i].Index != before[i].index || o.Interfaces[i].MTU != before[i].mtu {
			t.Fatal("sample did not retain the interface MTU")
		}
	}
}

func TestInterfaceTrafficNativeFailureCountersMatchProc(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	read := func() map[string][4]uint64 {
		body, err := readHostFile("/proc/thread-self/net/dev", 65536, false)
		if err != nil {
			t.Skip("proc interface counters unavailable")
		}
		values := make(map[string][4]uint64)
		lines := strings.Split(string(body), "\n")
		if len(lines) < 2 {
			t.Fatal("invalid proc interface header")
		}
		for _, line := range lines[2:] {
			if line == "" {
				continue
			}
			separator := strings.LastIndexByte(line, ':')
			if separator < 0 {
				t.Fatal("invalid proc interface row")
			}
			fields := strings.Fields(line[separator+1:])
			if len(fields) != 16 {
				t.Fatal("invalid proc interface field count")
			}
			var counters [4]uint64
			for i, index := range []int{2, 3, 10, 11} {
				counters[i], err = strconv.ParseUint(fields[index], 10, 64)
				if err != nil {
					t.Fatal("invalid proc interface counter")
				}
			}
			values[strings.TrimSpace(line[:separator])] = counters
		}
		return values
	}
	before := read()
	identities, err := interfaceTrafficInventory()
	if err != nil {
		t.Skip("interface inventory unavailable")
	}
	o, supported := NewInterfaceTrafficSampler(true).Sample()
	after := read()
	if !supported || o.State != "available" {
		t.Skipf("interface sample unavailable: %s", o.State)
	}
	if o.Count == 0 {
		t.Skip("no interfaces to compare")
	}
	if len(identities) < o.Count {
		t.Skip("interface inventory changed")
	}
	for i := range o.Count {
		identity, got := identities[i], o.Interfaces[i]
		first, foundFirst := before[identity.name]
		last, foundLast := after[identity.name]
		if !foundFirst || !foundLast || identity.index != got.Index {
			t.Skip("interface changed during comparison")
		}
		if !got.FailuresAvailable {
			t.Fatal("available native sample omitted failure counters")
		}
		for _, v := range [][3]uint64{{first[0], got.ReceiveErrors, last[0]},
			{first[1], got.ReceiveDropped, last[1]}, {first[2], got.TransmitErrors, last[2]},
			{first[3], got.TransmitDropped, last[3]}} {
			if v[0] > v[2] {
				t.Skip("counter reset during comparison")
			}
			if v[1] < v[0] || v[1] > v[2] {
				t.Fatal("sample differs from surrounding kernel counters")
			}
		}
	}
	t.Logf("verified four failure columns for %d interface(s) against procfs", o.Count)
}

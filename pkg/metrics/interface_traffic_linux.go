//go:build linux

package metrics

import (
	"math"
	"net"
	"os"
	"runtime"
	"sort"
	"syscall"
	"time"
)

func readInterfaceTraffic() (InterfaceTrafficObservation, bool) {
	// Namespace probes, netlink inventory and procfs counters must use one thread.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	return readInterfaceTrafficRecord(readHostFile, interfaceTrafficInventory, interfaceTrafficNamespace, time.Now), true
}

func interfaceTrafficNamespace() ([2]uint64, error) {
	info, err := os.Stat("/proc/thread-self/ns/net")
	if err != nil {
		return [2]uint64{}, err
	}
	value, ok := info.Sys().(*syscall.Stat_t)
	if !ok || value.Ino == 0 {
		return [2]uint64{}, errInterfaceTraffic
	}
	return [2]uint64{uint64(value.Dev), value.Ino}, nil
}

func interfaceTrafficInventory() ([]interfaceTrafficIdentity, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	if len(interfaces) > 256 {
		return nil, errInterfaceTraffic
	}
	result := make([]interfaceTrafficIdentity, len(interfaces))
	for i, value := range interfaces {
		if value.Index <= 0 || uint64(value.Index) > math.MaxUint32 || !validInterfaceName(value.Name) {
			return nil, errInterfaceTraffic
		}
		var mtu uint32
		if value.MTU > 0 && uint64(value.MTU) <= math.MaxUint32 {
			mtu = uint32(value.MTU)
		}
		result[i] = interfaceTrafficIdentity{name: value.Name, index: uint32(value.Index), loopback: value.Flags&net.FlagLoopback != 0, mtu: mtu}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].index < result[j].index })
	return result, nil
}

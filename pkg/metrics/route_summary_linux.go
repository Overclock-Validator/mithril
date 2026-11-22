//go:build linux

package metrics

import (
	"runtime"
	"time"
)

func readRouteSummary(topology TopologyObservation, affinity ThreadAffinityObservation, links LinkTopologyObservation) RouteSummaryObservation {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	return readRouteSummaryCapture(topology, affinity, links, interfaceTrafficNamespace, openLinkTopologySocket, time.Now)
}

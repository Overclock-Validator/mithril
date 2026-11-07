//go:build linux

package metrics

import "time"

func readTopology() TopologyObservation {
	return readTopologyFiles(readHostFile, time.Now)
}

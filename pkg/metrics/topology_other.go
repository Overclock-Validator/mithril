//go:build !linux

package metrics

import "time"

func readTopology() TopologyObservation {
	started := time.Now()
	return TopologyObservation{StartedAt: started, SampledAt: time.Now(), CPUState: TopologyUnsupported, NUMAState: TopologyUnsupported}
}

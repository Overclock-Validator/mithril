//go:build !linux

package metrics

import "time"

func readThreadAffinity(topology TopologyObservation) ThreadAffinityObservation {
	c, state := affinityBudget(topology, time.Now)
	o := ThreadAffinityObservation{TopologyGeneration: topology.Generation, CumulativeReads: c.reads, CumulativeBytes: c.bytes}
	if state != "" {
		o.State = state
		return o
	}
	if !affinityCheck(&c) {
		o.State = c.failure
		return o
	}
	o.StartedAt = c.last
	if !affinityCheck(&c) {
		o.State = c.failure
	} else {
		o.State = TopologyUnsupported
	}
	o.SampledAt = c.last
	return o
}

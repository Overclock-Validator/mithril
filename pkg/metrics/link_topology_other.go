//go:build !linux

package metrics

import "time"

func readLinkTopology(topology TopologyObservation, affinity ThreadAffinityObservation) LinkTopologyObservation {
	c, state := linkTopologyBudget(topology, affinity, time.Now)
	o := LinkTopologyObservation{TopologyGeneration: topology.Generation, AffinityGeneration: affinity.Generation, MetadataStartedAt: topology.StartedAt, CumulativeReads: c.base.reads, CumulativeBytes: c.base.bytes, State: state}
	if state != "" {
		return o
	}
	if !c.check() {
		o.State = c.base.failure
		return o
	}
	c.started, o.StartedAt = c.base.last, c.base.last
	if !c.check() {
		o.State = c.base.failure
	} else {
		o.State = TopologyUnsupported
	}
	o.SampledAt = c.base.last
	return o
}

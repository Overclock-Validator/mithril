//go:build !linux

package metrics

import "time"

func readRouteSummary(topology TopologyObservation, affinity ThreadAffinityObservation, links LinkTopologyObservation) RouteSummaryObservation {
	// No socket is opened on unsupported systems, even with valid fixture inputs.
	c, state := routeSummaryBudget(topology, affinity, links, time.Now)
	o := RouteSummaryObservation{TopologyGeneration: topology.Generation, AffinityGeneration: affinity.Generation, LinkGeneration: links.Generation, MetadataStartedAt: topology.StartedAt, NetworkStartedAt: links.StartedAt, CumulativeReads: c.base.reads, CumulativeBytes: c.base.bytes, LinkDatagrams: c.datagrams, LinkBytes: c.inputBytes, State: state}
	if state != "" {
		return o
	}
	if !c.check() {
		o.State = c.base.failure
		return o
	}
	o.StartedAt = c.base.last
	c.check()
	o.SampledAt, o.State = c.base.last, TopologyUnsupported
	if c.base.failure != "" {
		o.State = c.base.failure
	}
	return o
}

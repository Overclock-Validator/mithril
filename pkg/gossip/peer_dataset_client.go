package gossip

import "net"

// peerDatasetSession binds an optional owner to the socket of one Run.
type peerDatasetSession struct {
	conn    *net.UDPConn
	dataset *peerDataset
}

// EnablePeerDataset opts in before Run. Enabling during a Run takes effect on
// the next Run; it never starts a worker or publishes an invented sample.
func (c *Client) EnablePeerDataset() {
	if c != nil {
		c.peerDatasetEnabled.Store(true)
		c.EnableMonitoring()
	}
}

// PeerDatasetSnapshot returns a detached cached observation for the current
// Run. Callers must still verify the original sample age and Run binding.
func (c *Client) PeerDatasetSnapshot() (PeerDatasetSnapshot, bool) {
	if c == nil {
		return PeerDatasetSnapshot{}, false
	}
	session := c.peerDataset.Load()
	if session == nil {
		return PeerDatasetSnapshot{}, false
	}
	snapshot, ok := session.dataset.snapshot()
	if !ok || c.peerDataset.Load() != session {
		return PeerDatasetSnapshot{}, false
	}
	return snapshot, true
}

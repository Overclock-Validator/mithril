package replay

import (
	"net"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/stretchr/testify/assert"
)

func TestRepairPeerHintsRoundTrip(t *testing.T) {
	var key gossip.Pubkey
	key[0], key[31] = 7, 9
	peers := []gossip.RepairPeer{{Pubkey: key, Addr: &net.UDPAddr{IP: net.IPv4(192, 0, 2, 4), Port: 8008}}, {Pubkey: key}}
	hints := repairPeerHints(peers)
	assert.Len(t, hints, 1, "a peer without an address is not remembered")
	seeds := repairPeerSeeds(append(hints,
		state.RepairPeerHint{Pubkey: "not-base58!", Addr: "192.0.2.4:8008"},
		state.RepairPeerHint{Pubkey: hints[0].Pubkey, Addr: "no-port"},
	))
	assert.Len(t, seeds, 1, "unparsable hints are skipped")
	assert.Equal(t, key, seeds[0].Pubkey)
	assert.Equal(t, "192.0.2.4:8008", seeds[0].Addr.String())
}

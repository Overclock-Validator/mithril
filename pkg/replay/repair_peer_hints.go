package replay

import (
	"net"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/mr-tron/base58"
)

// rememberedRepairPeers is the most repair peers saved in the state file.
const rememberedRepairPeers = 64

// repairPeerSeeds converts the state file's repair peer hints to repair
// peers, skipping any that do not parse.
func repairPeerSeeds(hints []state.RepairPeerHint) []gossip.RepairPeer {
	seeds := make([]gossip.RepairPeer, 0, len(hints))
	for _, hint := range hints {
		key, err := base58.Decode(hint.Pubkey)
		if err != nil || len(key) != len(gossip.Pubkey{}) {
			continue
		}
		addr, err := net.ResolveUDPAddr("udp", hint.Addr)
		if err != nil || addr.Port == 0 || addr.IP == nil {
			continue
		}
		seeds = append(seeds, gossip.RepairPeer{Pubkey: gossip.Pubkey(key), Addr: addr})
	}
	return seeds
}

// repairPeerHints converts repair peers to state file hints.
func repairPeerHints(peers []gossip.RepairPeer) []state.RepairPeerHint {
	hints := make([]state.RepairPeerHint, 0, len(peers))
	for _, peer := range peers {
		if peer.Addr == nil {
			continue
		}
		hints = append(hints, state.RepairPeerHint{Pubkey: base58.Encode(peer.Pubkey[:]), Addr: peer.Addr.String()})
	}
	return hints
}

func repairPeerSeedsFromState(mithrilState *state.MithrilState) []gossip.RepairPeer {
	if mithrilState == nil {
		return nil
	}
	return repairPeerSeeds(mithrilState.RepairPeerHints)
}

package node

import (
	"fmt"
	"strings"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/replay"
)

// offlineReplay (--offline, network.offline) replays only blocks already on
// local disk: the shred spool a corpus capture restores. The node starts no
// gossip client, Turbine tree, repair, shred-version query or Votor listener,
// so a benchmark replay sends nothing to the cluster and spends none of its
// pinned cores on network traffic. Rooting follows block footer certificates
// only, and no RPC call is made: network.rpc is ignored, the startup genesis
// check uses the cluster setting, and no tip is polled.
var offlineReplay bool

// applyOfflineReplay checks that the configuration can run without the
// network and turns off every component that would reach it. It runs after the
// block source is resolved and before the source defaults its gossip
// entrypoint.
func applyOfflineReplay() error {
	if !offlineReplay {
		return nil
	}
	var missing []string
	if blockSource != "turbine" {
		missing = append(missing, fmt.Sprintf("block.source=turbine (offline blocks come from the shred spool, not %q)", blockSource))
	}
	if lightbringerEnabled {
		missing = append(missing, "lightbringer.enabled=false")
	}
	if consensusMode == "validator" {
		missing = append(missing, "consensus.mode=verifying (a validator has to reach the cluster)")
	}
	// Local snapshot files (--snapshot) build without the network; anything
	// that could discover or download a snapshot cannot run offline.
	if bootstrapMode != "accountsdb" && snapshotArchivePath == "" {
		missing = append(missing, "bootstrap.mode=accountsdb or local snapshot files via --snapshot (an offline node cannot download a snapshot)")
	}
	if endSlot < 0 && numReplaySlots <= 0 {
		missing = append(missing, "replay.end_slot or replay.num_slots (with no network the node would wait at the end of the spool forever)")
	}
	if turbineShredVersion == 0 {
		missing = append(missing, "turbine.shred_version (it cannot be discovered from gossip)")
	}
	if blockRPCFallback {
		missing = append(missing, "block.rpc_fallback=false")
	}
	if len(missing) > 0 {
		return fmt.Errorf("network.offline=true requires:\n  - %s", strings.Join(missing, "\n  - "))
	}

	// No entrypoint means no gossip client, so no repair or retransmit, and
	// startup skips the Turbine prewarm. The receiver still runs, because it
	// is what serves the spool, but on a loopback port nobody sends to.
	turbineGossipEntrypoint = ""
	turbineGossipBindAddr = ""
	turbineAdvertisedIP = ""
	turbineBindAddr = "127.0.0.1:0"
	alpenglowObserverBindAddr = ""
	// Rooting follows block footer certificates only, never an RPC endpoint's
	// view of finality, and the replay loop makes no blocking RPC call.
	replay.DelegatedFinalityEnabled = false
	mlog.Log.Infof("offline replay: no gossip, Turbine tree, repair or Votor listener; blocks come only from the local shred spool")
	return nil
}

package node

import "github.com/Overclock-Validator/mithril/pkg/rpcserver"

func enableRPCBlockHistory(server *rpcserver.RpcServer, alpenglowMode bool, dir string, retentionSlots, rootedSlot uint64) error {
	if !alpenglowMode {
		return nil
	}
	return server.EnableBlockHistory(dir, retentionSlots, rootedSlot)
}

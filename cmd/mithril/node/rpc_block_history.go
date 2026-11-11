package node

import "github.com/Overclock-Validator/mithril/pkg/rpcserver"

func enableRPCBlockHistory(server *rpcserver.RpcServer, dir string, retentionSlots, rootedSlot uint64) error {
	return server.EnableBlockHistory(dir, retentionSlots, rootedSlot)
}

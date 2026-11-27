package node

import (
	"errors"
	"fmt"
	"strings"
)

// rpcServeLevel is the primary RPC setting: what this node answers. Each
// level includes the ones before it, and brings the stores it depends on, so
// an operator names a purpose and not a set of interdependent switches.
type rpcServeLevel int

const (
	// rpcServeNone opens no RPC listener.
	rpcServeNone rpcServeLevel = iota
	// rpcServeAccounts answers account reads, simulation and sendTransaction.
	rpcServeAccounts
	// rpcServeBlocks adds rooted block summaries (getBlock, getBlocks).
	rpcServeBlocks
	// rpcServeWallet adds recent signature outcomes, for send and confirm.
	rpcServeWallet
	// rpcServeHistory adds full transaction history (getTransaction).
	rpcServeHistory
)

var rpcServeNames = [...]string{"none", "accounts", "blocks", "wallet", "history"}

func (level rpcServeLevel) String() string { return rpcServeNames[level] }

func parseRPCServeLevel(name string) (rpcServeLevel, error) {
	for level, known := range rpcServeNames {
		if name == known {
			return rpcServeLevel(level), nil
		}
	}
	return 0, fmt.Errorf("unknown rpc.serve %q (valid: %s)", name, strings.Join(rpcServeNames[:], ", "))
}

// rpcServePlan is the resolved setting: the level and what it turns on.
type rpcServePlan struct {
	Level              rpcServeLevel
	BlockHistory       bool
	SignatureStatuses  bool
	TransactionHistory bool
}

// resolveRPCServe combines rpc.serve with the individual switches. A switch
// can add a feature to a lower level; nothing removes what a level includes.
// An empty name means the level is unset: the listener's port decides between
// none and accounts, and the switches raise it from there.
func resolveRPCServe(name string, blockHistory, signatureStatuses, transactionHistory bool, mode string, port int) (rpcServePlan, error) {
	level := rpcServeAccounts
	if port == 0 {
		level = rpcServeNone
	}
	if name != "" {
		var err error
		if level, err = parseRPCServeLevel(name); err != nil {
			return rpcServePlan{}, err
		}
	}
	plan := rpcServePlan{
		Level:              level,
		BlockHistory:       blockHistory || level >= rpcServeBlocks,
		SignatureStatuses:  signatureStatuses || level >= rpcServeWallet,
		TransactionHistory: transactionHistory || level >= rpcServeHistory,
	}
	// Both of these select their block through block history.
	plan.BlockHistory = plan.BlockHistory || plan.SignatureStatuses || plan.TransactionHistory
	switch {
	case plan.TransactionHistory:
		plan.Level = max(plan.Level, rpcServeHistory)
	case plan.SignatureStatuses:
		plan.Level = max(plan.Level, rpcServeWallet)
	case plan.BlockHistory:
		plan.Level = max(plan.Level, rpcServeBlocks)
	}
	if name == rpcServeNone.String() && plan.Level != rpcServeNone {
		return rpcServePlan{}, errors.New("rpc.serve is none, but a history option is enabled")
	}
	if plan.Level == rpcServeNone {
		return plan, nil
	}
	if port < 1 || port > 65535 {
		return rpcServePlan{}, fmt.Errorf("rpc.serve %s requires an RPC port between 1 and 65535", plan.Level)
	}
	// A voting node keeps history capture and its stores off its replay path.
	if plan.Level > rpcServeAccounts && mode != "verifying" {
		return rpcServePlan{}, fmt.Errorf("rpc.serve %s requires consensus.mode verifying", plan.Level)
	}
	return plan, nil
}

// summary is the one startup line stating what the node serves.
func (plan rpcServePlan) summary(history transactionHistoryConfig) string {
	if plan.Level == rpcServeNone {
		return "RPC: not serving"
	}
	parts := []string{"RPC serves " + plan.Level.String()}
	if plan.BlockHistory {
		parts = append(parts, "block history 2 epochs")
	}
	if plan.SignatureStatuses {
		parts = append(parts, "recent signature outcomes")
	}
	if plan.TransactionHistory {
		parts = append(parts, fmt.Sprintf("transaction history %d slots, budget %d MiB, free reserve %d MiB",
			history.RetentionSlots, history.MaxMiB, history.ReserveMiB))
	}
	if plan.SignatureStatuses || plan.TransactionHistory {
		parts = append(parts, "HTTP only")
	}
	return strings.Join(parts, "; ")
}

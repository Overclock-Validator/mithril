package snapshotdl

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/solana-snapshot-finder-go/pkg/config"
	"github.com/Overclock-Validator/solana-snapshot-finder-go/pkg/rpc"
	"github.com/gagliardetto/solana-go"
	solrpc "github.com/gagliardetto/solana-go/rpc"
)

// ValidateKnownValidators rejects an unusable source restriction before bootstrap.
func (sc SnapshotConfig) ValidateKnownValidators() error {
	_, err := sc.knownValidatorSet()
	return err
}

func (sc SnapshotConfig) knownValidatorSet() (map[solana.PublicKey]struct{}, error) {
	if sc.OnlyKnownRPC && len(sc.KnownValidators) == 0 {
		return nil, fmt.Errorf("snapshot.only_known_rpc requires nonempty snapshot.known_validators")
	}
	known := make(map[solana.PublicKey]struct{}, len(sc.KnownValidators))
	for _, identity := range sc.KnownValidators {
		pubkey, err := solana.PublicKeyFromBase58(identity)
		if err != nil {
			return nil, fmt.Errorf("invalid snapshot.known_validators identity %q: %w", identity, err)
		}
		known[pubkey] = struct{}{}
	}
	return known, nil
}

// discoverSnapshotRPCNodes is shared by all full and incremental selection paths.
// Restricted discovery retains identities from getClusterNodes, which the upstream
// finder otherwise discards. Configured discovery RPCs are not implicitly trusted
// as snapshot sources. The discovery RPC's mapping is trusted; this does not verify
// signed gossip records or change the downloader's handling of HTTP redirects.
func discoverSnapshotRPCNodes(ctx context.Context, sc SnapshotConfig, cfg config.Config, preferredRPC string) ([]rpc.RPCNode, error) {
	known, err := sc.knownValidatorSet()
	if err != nil {
		return nil, err
	}
	if !sc.OnlyKnownRPC {
		nodes := rpc.FetchClusterNodes(cfg, preferredRPC)
		if len(nodes) == 0 {
			return nil, fmt.Errorf("no rpc nodes available from cluster")
		}
		nodes, added := addConfiguredSnapshotRPCNodes(nodes, preferredRPC, cfg.RPCAddresses)
		if added > 0 {
			mlog.Log.Infof("Added %d configured RPC endpoint(s) to snapshot probe candidates", added)
		}
		return nodes, nil
	}

	endpoints := make([]string, 0, len(cfg.RPCAddresses)+1)
	seen := make(map[string]bool)
	for _, endpoint := range append([]string{preferredRPC}, cfg.RPCAddresses...) {
		if endpoint != "" && !seen[endpoint] {
			endpoints = append(endpoints, endpoint)
			seen[endpoint] = true
		}
	}
	var lastErr error
	for _, endpoint := range endpoints {
		requestCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		client := solrpc.New(endpoint)
		clusterNodes, err := client.GetClusterNodes(requestCtx)
		cancel()
		_ = client.Close()
		if err != nil {
			lastErr = err
			continue
		}
		var nodes []rpc.RPCNode
		for _, node := range clusterNodes {
			if node == nil || node.RPC == nil || strings.TrimSpace(*node.RPC) == "" {
				continue
			}
			if _, allowed := known[node.Pubkey]; !allowed {
				continue
			}
			candidate := rpc.RPCNode{Address: *node.RPC}
			if node.Version != nil {
				candidate.Version = *node.Version
			}
			nodes = append(nodes, candidate)
		}
		if len(nodes) > 0 {
			mlog.Log.Infof("Restricted snapshot discovery to %d known-validator RPC node(s)", len(nodes))
			return nodes, nil
		}
	}
	if lastErr != nil {
		return nil, fmt.Errorf("no known-validator snapshot RPC nodes available: %w", lastErr)
	}
	return nil, fmt.Errorf("no known-validator snapshot RPC nodes available (snapshot.only_known_rpc is enabled)")
}

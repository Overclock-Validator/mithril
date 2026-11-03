package config

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/mr-tron/base58"
)

// Every cluster runs Alpenglow: certificate fork choice, rooted-durable
// storage, and turbine as the default block source. The cluster name only
// selects the expected genesis and the built-in network defaults; it never
// selects a replay path.
const DefaultCluster = "mainnet"

// Cluster describes the network a node joins. Name is the canonical name
// recorded in the state file and ledger binding.
type Cluster struct {
	Name             string
	GenesisHash      string
	RPC              string
	GossipEntrypoint string
	Custom           bool
}

var builtinClusters = map[string]Cluster{
	"mainnet": {
		Name:             "mainnet",
		GenesisHash:      "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d",
		RPC:              "https://api.mainnet-beta.solana.com",
		GossipEntrypoint: "entrypoint.mainnet-beta.solana.com:8001",
	},
	"testnet": {
		Name:             "testnet",
		GenesisHash:      "4uhcVJyU9pJkvQyS88uRDiswHXSCkY3zQawwpjk2NsNY",
		RPC:              "https://api.testnet.solana.com",
		GossipEntrypoint: "entrypoint.testnet.solana.com:8001",
	},
	"devnet": {
		Name:             "devnet",
		GenesisHash:      "EtWTRABZaYq6iMfeYKouRu166VU2xqa1wcaWoxPkrZBG",
		RPC:              "https://api.devnet.solana.com",
		GossipEntrypoint: "entrypoint.devnet.solana.com:8001",
	},
}

// clusterAliases keeps existing configs and state files working.
var clusterAliases = map[string]string{
	"mainnet-beta": "mainnet",
}

var customClusterName = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)

// BuiltinClusters returns the built-in clusters, mainnet first.
func BuiltinClusters() []Cluster {
	out := make([]Cluster, 0, len(builtinClusters))
	for _, c := range builtinClusters {
		out = append(out, c)
	}
	order := map[string]int{"mainnet": 0, "testnet": 1, "devnet": 2}
	sort.Slice(out, func(i, j int) bool { return order[out[i].Name] < order[out[j].Name] })
	return out
}

// CanonicalClusterName maps aliases to their canonical name and leaves every
// other name untouched.
func CanonicalClusterName(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	if canonical, ok := clusterAliases[name]; ok {
		return canonical
	}
	return name
}

// ResolveCluster turns network.cluster and network.genesis_hash into a
// Cluster. Built-in clusters carry their genesis; any other name is a custom
// (private) cluster, which must name its genesis explicitly and gets no
// network defaults, so it can never borrow another cluster's identity.
func ResolveCluster(name, genesisHash string) (Cluster, error) {
	name = CanonicalClusterName(name)
	genesisHash = strings.TrimSpace(genesisHash)
	if name == "" {
		name = DefaultCluster
	}
	if genesisHash != "" {
		if decoded, err := base58.Decode(genesisHash); err != nil || len(decoded) != 32 {
			return Cluster{}, fmt.Errorf("network.genesis_hash must be base58 encoding of exactly 32 bytes")
		}
	}
	if c, ok := builtinClusters[name]; ok {
		if genesisHash != "" && genesisHash != c.GenesisHash {
			return Cluster{}, fmt.Errorf("network.genesis_hash %s does not match %s genesis %s; give a private cluster its own network.cluster name",
				genesisHash, c.Name, c.GenesisHash)
		}
		return c, nil
	}
	if !customClusterName.MatchString(name) {
		return Cluster{}, fmt.Errorf("invalid network.cluster %q: use mainnet, testnet, devnet, or a custom cluster name of lowercase letters, digits, '.', '_' or '-'", name)
	}
	if genesisHash == "" {
		hint := ""
		if name == "alpenglow" {
			hint = " (\"alpenglow\" no longer names a cluster: mainnet and testnet both run Alpenglow)"
		}
		return Cluster{}, fmt.Errorf("network.cluster %q is not a built-in cluster (mainnet, testnet, devnet)%s; a custom cluster requires network.genesis_hash", name, hint)
	}
	return Cluster{Name: name, GenesisHash: genesisHash, Custom: true}, nil
}

// CheckGenesis refuses an RPC endpoint that serves a different chain than the
// configured cluster, before any state or ledger binding is touched.
func (c Cluster) CheckGenesis(rpcGenesis string) error {
	if rpcGenesis != c.GenesisHash {
		return fmt.Errorf("RPC genesis hash %s does not match network.cluster=%s (genesis %s); the RPC endpoint belongs to another cluster",
			rpcGenesis, c.Name, c.GenesisHash)
	}
	return nil
}

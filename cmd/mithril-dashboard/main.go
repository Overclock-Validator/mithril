// mithril-dashboard serves a read-only API from a local node status socket.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
)

func defaultNodeSocket(goos string) string {
	if goos == "linux" {
		return dashboard.DefaultNodeSocket
	}
	return ""
}

type readerConfig struct {
	socket, listen, publicOrigin string
	options                      dashboard.PublicOptions
	check                        bool
	fromNode                     bool
}

func parseReaderConfig(flags *flag.FlagSet, args []string, goos string) (readerConfig, error) {
	var config readerConfig
	var standardDetails bool
	o := &config.options
	flags.StringVar(&config.socket, "node-socket", defaultNodeSocket(goos), "Absolute path to the node's private dashboard socket")
	flags.StringVar(&config.listen, "listen", "127.0.0.1:8080", "Loopback IP:port for the read-only API")
	flags.StringVar(&config.publicOrigin, "public-origin", "", "Explicit HTTPS origin for a reverse proxy")
	flags.BoolVar(&config.fromNode, "from-node", false, "Read the HTTPS origin from the private node feed and enable standard details")
	flags.BoolVar(&config.check, "check", false, "Check reader addresses and a fresh private feed snapshot, then exit")
	flags.BoolVar(&standardDetails, "standard-details", false, "Enable standard dashboard details (requires --public-origin and an HTTPS reverse proxy)")
	flags.BoolVar(&o.Resources, "public-resources", false, "Include process, filesystem and enabled host-resource observations")
	flags.BoolVar(&o.Peers, "public-peers", false, "Include up to eight recent signed peer adverts without network endpoints")
	flags.BoolVar(&o.LocalPeers, "local-peers", false, "Include bounded locally observed signed contacts without network endpoints")
	flags.BoolVar(&o.Harmonic, "public-harmonic", false, "Include Harmonic health and requested strategy (requires --public-resources)")
	flags.BoolVar(&o.ReplayDetails, "replay-details", false, "Include bounded replay details and rooted-bank history (requires --public-resources)")
	flags.BoolVar(&o.FastExecuted, "fast-executed", false, "Include provisional local-execution updates (requires --replay-details)")
	flags.BoolVar(&o.NetworkDetails, "network-details", false, "Include TVU and repair socket-memory details (requires --public-resources)")
	flags.BoolVar(&o.ReceptionHistory, "reception-history", false, "Include bounded authenticated reception history (requires --public-resources, --replay-details and --network-details)")
	flags.BoolVar(&o.CacheDetails, "cache-details", false, "Include AccountsDB read-cache gauges (requires --public-resources)")
	flags.BoolVar(&o.ProductionDetails, "production-details", false, "Include local producer lifecycle and latest terminal details (requires --public-resources)")
	flags.BoolVar(&o.StorageDetails, "storage-details", false, "Include runtime database write-stall details (requires --public-resources)")
	flags.BoolVar(&o.MachineDetails, "machine-details", false, "Include bounded machine topology and sampled thread-mask details (requires --public-resources)")
	flags.StringVar(&o.HistoryDir, "history-dir", "", "Private dedicated directory for 30 days of public aggregate history")
	flags.StringVar(&o.ClusterFile, "cluster-file", "", "Read-only public snapshot written by mithril-dashboard-collect")
	if err := flags.Parse(args); err != nil {
		return config, err
	}
	if config.fromNode && config.publicOrigin != "" {
		return config, errors.New("--from-node and --public-origin are mutually exclusive")
	}
	if standardDetails || config.fromNode {
		if !config.fromNode && config.publicOrigin == "" {
			return config, errors.New("--standard-details requires --public-origin and an HTTPS reverse proxy")
		}
		explicit := make(map[string]bool)
		flags.Visit(func(f *flag.Flag) { explicit[f.Name] = true })
		infer := func(name string, value *bool, enabled bool) {
			if !explicit[name] {
				*value = enabled
			}
		}
		infer("public-resources", &o.Resources, true)
		infer("replay-details", &o.ReplayDetails, o.Resources)
		infer("network-details", &o.NetworkDetails, o.Resources)
		infer("local-peers", &o.LocalPeers, o.NetworkDetails)
		infer("cache-details", &o.CacheDetails, o.Resources)
		infer("storage-details", &o.StorageDetails, o.Resources)
		infer("fast-executed", &o.FastExecuted, o.ReplayDetails)
	}
	requiresResources := o.Harmonic || o.ReplayDetails || o.NetworkDetails || o.CacheDetails || o.ProductionDetails || o.StorageDetails || o.MachineDetails || o.ReceptionHistory
	requiresPublic := requiresResources || o.Resources || o.Peers || o.LocalPeers || o.HistoryDir != "" || o.ClusterFile != ""
	switch {
	case !filepath.IsAbs(config.socket):
		return config, errors.New("--node-socket must be an absolute path (required outside Linux)")
	case flags.NArg() != 0:
		return config, errors.New("unexpected positional arguments")
	case o.FastExecuted && !o.ReplayDetails:
		return config, errors.New("--fast-executed requires --replay-details")
	case requiresPublic && config.publicOrigin == "" && !config.fromNode:
		return config, errors.New("public options require --public-origin and an HTTPS reverse proxy")
	case requiresResources && !o.Resources:
		return config, errors.New("detail options require --public-resources")
	case o.ReceptionHistory && (!o.ReplayDetails || !o.NetworkDetails):
		return config, errors.New("--reception-history requires --replay-details and --network-details")
	case o.HistoryDir != "" && !filepath.IsAbs(o.HistoryDir), o.ClusterFile != "" && !filepath.IsAbs(o.ClusterFile):
		return config, errors.New("--history-dir and --cluster-file, if set, must be absolute paths")
	}
	return config, nil
}

func main() {
	config, err := parseReaderConfig(flag.CommandLine, os.Args[1:], runtime.GOOS)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		flag.Usage()
		os.Exit(2)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	switch {
	case config.fromNode && config.check:
		var origin string
		origin, err = dashboard.CheckReaderFromNode(ctx, config.socket, config.listen, config.options)
		if err == nil {
			fmt.Fprintf(os.Stdout, "Dashboard reader addresses and private feed OK. Configured URL: %s\n", origin)
		}
	case config.fromNode:
		err = dashboard.ServeReaderFromNode(ctx, config.socket, config.listen, config.options)
	case config.check:
		err = dashboard.CheckReader(ctx, config.socket, config.listen, config.publicOrigin, config.options)
		if err == nil {
			fmt.Fprintln(os.Stdout, "Dashboard reader addresses and private feed OK.")
		}
	case config.publicOrigin != "":
		err = dashboard.ServePublicWithOptions(ctx, config.socket, config.listen, config.publicOrigin, config.options)
	default:
		err = dashboard.Serve(ctx, config.socket, config.listen)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

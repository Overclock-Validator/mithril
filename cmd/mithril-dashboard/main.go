// mithril-dashboard serves a read-only API from a local node status socket.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
)

func main() {
	socket := flag.String("node-socket", "", "Absolute path to the node's private dashboard socket")
	listen := flag.String("listen", "127.0.0.1:8080", "Loopback IP:port for the read-only WebSocket API")
	publicOrigin := flag.String("public-origin", "", "Explicit HTTPS origin for the public-only profile (requires a reverse proxy)")
	publicResources := flag.Bool("public-resources", false, "Include process, filesystem and enabled host-resource observations in the public profile")
	publicPeers := flag.Bool("public-peers", false, "Include up to eight recent signed peer adverts without network endpoints")
	localPeers := flag.Bool("local-peers", false, "Include bounded locally observed signed contacts without network endpoints (requires a public profile)")
	networkDetails := flag.Bool("network-details", false, "Include TVU and repair socket-memory details (requires a public profile and --public-resources)")
	machineDetails := flag.Bool("machine-details", false, "Include bounded machine topology and process history (requires a public profile and --public-resources)")
	historyDir := flag.String("history-dir", "", "Private dedicated directory for 30 days of public aggregate history (requires a public profile)")
	clusterFile := flag.String("cluster-file", "", "Read a separately collected public cluster snapshot")
	flag.Parse()
	publicProfile := *publicOrigin != ""
	if !filepath.IsAbs(*socket) || flag.NArg() != 0 || (*publicResources || *publicPeers || *localPeers || *networkDetails || *machineDetails || *historyDir != "" || *clusterFile != "") && !publicProfile || (*networkDetails || *machineDetails) && !*publicResources || *historyDir != "" && !filepath.IsAbs(*historyDir) || *clusterFile != "" && !filepath.IsAbs(*clusterFile) {
		fmt.Fprintln(os.Stderr, "usage: mithril-dashboard --node-socket /private/directory/dashboard.sock [--listen 127.0.0.1:8080] [--public-origin https://example.com] [--public-resources] [--public-peers] [--local-peers] [--network-details] [--machine-details] [--history-dir /private/history] [--cluster-file /public/cluster.json]")
		os.Exit(2)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	var err error
	options := dashboard.PublicOptions{Resources: *publicResources, Peers: *publicPeers, LocalPeers: *localPeers, NetworkDetails: *networkDetails, MachineDetails: *machineDetails, HistoryDir: *historyDir, ClusterFile: *clusterFile}
	if *publicOrigin != "" {
		err = dashboard.ServePublicWithOptions(ctx, *socket, *listen, *publicOrigin, options)
	} else {
		err = dashboard.Serve(ctx, *socket, *listen)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

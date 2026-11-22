package main

import (
	"flag"
	"strings"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
)

func TestDefaultNodeSocket(t *testing.T) {
	if got := defaultNodeSocket("linux"); got != dashboard.DefaultNodeSocket {
		t.Fatalf("linux socket = %q, want %q", got, dashboard.DefaultNodeSocket)
	}
	if got := defaultNodeSocket("darwin"); got != "" {
		t.Fatalf("non-linux socket = %q, want empty", got)
	}
}

func TestStandardDetails(t *testing.T) {
	const origin = "--public-origin=https://stats.example"
	standard := dashboard.PublicOptions{Resources: true, ReplayDetails: true, FastExecuted: true, NetworkDetails: true, LocalPeers: true, CacheDetails: true, StorageDetails: true}
	withoutReplay := standard
	withoutReplay.ReplayDetails, withoutReplay.FastExecuted = false, false
	withoutNetwork := standard
	withoutNetwork.NetworkDetails, withoutNetwork.LocalPeers = false, false
	for _, tc := range []struct {
		name string
		args []string
		want dashboard.PublicOptions
	}{
		{"diagnostics", nil, dashboard.PublicOptions{}},
		{"HTTPS defaults", []string{origin}, dashboard.PublicOptions{}},
		{"HTTPS explicit only", []string{origin, "--public-resources", "--replay-details"}, dashboard.PublicOptions{Resources: true, ReplayDetails: true}},
		{"HTTPS preset off", []string{origin, "--standard-details=false"}, dashboard.PublicOptions{}},
		{"HTTPS preset", []string{origin, "--standard-details"}, standard},
		{"node-managed preset", []string{"--from-node"}, standard},
		{"node-managed override", []string{"--from-node", "--replay-details=false"}, withoutReplay},
		{"replay disabled", []string{origin, "--replay-details=false", "--standard-details"}, withoutReplay},
		{"network disabled", []string{origin, "--standard-details", "--network-details=false"}, withoutNetwork},
		{"resources disabled", []string{origin, "--standard-details", "--public-resources=false"}, dashboard.PublicOptions{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config, err := parseReaderConfig(flag.NewFlagSet(t.Name(), flag.ContinueOnError), tc.args, "linux")
			if err != nil || config.options != tc.want {
				t.Fatalf("options = %+v, error = %v, want %+v", config.options, err, tc.want)
			}
		})
	}
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{origin, "--from-node"}, "mutually exclusive"},
		{[]string{"--standard-details"}, "requires --public-origin and an HTTPS reverse proxy"},
		{[]string{origin, "--standard-details", "--replay-details=false", "--fast-executed"}, "requires --replay-details"},
		{[]string{origin, "--standard-details", "--public-resources=false", "--cache-details"}, "require --public-resources"},
		{[]string{origin, "--standard-details", "--network-details=false", "--reception-history"}, "requires --replay-details and --network-details"},
	} {
		_, err := parseReaderConfig(flag.NewFlagSet(t.Name(), flag.ContinueOnError), tc.args, "linux")
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("options %v: error = %v, want %q", tc.args, err, tc.want)
		}
	}
}

package main

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/dashboard/cluster"
)

func TestCollectionSucceeded(t *testing.T) {
	s := cluster.Snapshot{Providers: []cluster.ProviderStatus{{ID: "rpc", State: "fresh"}, {ID: "puffin", State: "unavailable", Reason: "not_collected"}}}
	if !collectionSucceeded(s) {
		t.Fatal("an unconfigured adapter should not fail the collection")
	}
	s.Providers[1].State, s.Providers[1].Reason = "retired", "provider_retired"
	if !collectionSucceeded(s) {
		t.Fatal("legacy metadata should remain supported")
	}
	s.Providers[0].State, s.Providers[0].Reason = "unavailable", "request_failed"
	if collectionSucceeded(s) {
		t.Fatal("a failed configured provider must fail the collection")
	}
}

package dashboard

import (
	"testing"
	"time"
)

func TestStoragePhysicalUsageContract(t *testing.T) {
	opened := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	x := StoragePhysicalUsage{Scope: "primary_db_tree_and_secondary_account_trees", StartedAt: opened.Add(time.Second), CompletedAt: opened.Add(2 * time.Second), DurationNanoseconds: uint64(time.Second), Directories: 1}
	if !validStoragePhysicalUsage(&x, opened, opened.Add(3*time.Second)) {
		t.Fatal("valid historical observation rejected")
	}
	if validStoragePhysicalUsage(&x, opened, opened.Add(77*time.Second)) {
		t.Fatal("expired observation accepted")
	}
	x.AllocatedBytes = 1
	if validStoragePhysicalUsage(&x, opened, opened.Add(3*time.Second)) {
		t.Fatal("non-block-aligned allocation accepted")
	}
	x.AllocatedBytes = 0
	x.RegularFiles = 1_000_000
	if validStoragePhysicalUsage(&x, opened, opened.Add(3*time.Second)) {
		t.Fatal("over-cap count accepted")
	}
	x.RegularFiles = 0
	x.Directories = 0
	if validStoragePhysicalUsage(&x, opened, opened.Add(3*time.Second)) {
		t.Fatal("zero directories accepted")
	}
	x.Directories = 1
	x.Scope = "whole_store_live_bytes"
	if validStoragePhysicalUsage(&x, opened, opened.Add(3*time.Second)) {
		t.Fatal("false scope accepted")
	}
}

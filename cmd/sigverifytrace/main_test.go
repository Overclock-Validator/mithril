package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/sigverifytelemetry"
)

func TestRunAnalyzesPassiveTraceWithoutInventingSIMDGroups(t *testing.T) {
	sigverifytelemetry.Enable(4)
	t.Cleanup(sigverifytelemetry.Disable)
	var pub [32]byte
	var sig [64]byte
	pub[0], sig[0] = 1, 2
	attempt, _, _, _ := sigverifytelemetry.BeginVerification(sigverifytelemetry.SourceReplay, pub, sig, []byte("message"))
	attempt.RecordResult(true)
	path := filepath.Join(t.TempDir(), "trace.jsonl")
	if err := sigverifytelemetry.WriteJSONLFile(path); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if err := run([]string{
		"-input", path,
		"-key-entries", "1",
		"-table-bytes", "1024",
		"-admit-after", "1",
		"-duplicate-entries", "2",
	}, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v, stderr=%s", err, stderr.String())
	}
	var result struct {
		InputSHA256   string `json:"input_sha256"`
		SIMDInference string `json:"simd_inference"`
		Policy        struct {
			MaxKeyEntries     int    `json:"max_key_entries"`
			TableBytesPerKey  uint64 `json:"table_bytes_per_key"`
			AdmitAfterValid   uint64 `json:"admit_after_valid"`
			DuplicateCapacity int    `json:"duplicate_capacity"`
			SIMDWidths        []int  `json:"simd_widths"`
		} `json:"policy"`
		KeyCache struct {
			Admissions         uint64 `json:"admissions"`
			ResidentTableBytes uint64 `json:"resident_table_bytes"`
		} `json:"key_cache"`
		ExactDuplicates struct {
			Insertions uint64 `json:"insertions"`
		} `json:"exact_duplicate_lru"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("decode output: %v\n%s", err, stdout.String())
	}
	if result.SIMDInference != "unavailable_for_passive_trace" {
		t.Fatalf("SIMD inference=%q", result.SIMDInference)
	}
	if len(result.InputSHA256) != 64 || result.Policy.MaxKeyEntries != 1 || result.Policy.TableBytesPerKey != 1024 || result.Policy.AdmitAfterValid != 1 || result.Policy.DuplicateCapacity != 2 || len(result.Policy.SIMDWidths) != 2 || result.Policy.SIMDWidths[0] != 4 || result.Policy.SIMDWidths[1] != 8 {
		t.Fatalf("evidence identity=%+v", result)
	}
	if result.KeyCache.Admissions != 1 || result.KeyCache.ResidentTableBytes != 1024 || result.ExactDuplicates.Insertions != 1 {
		t.Fatalf("result=%+v output=%s", result, stdout.String())
	}
}

func TestRunRejectsAmbiguousInputAndBadWidth(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if err := run([]string{"-input", "a", "b"}, &stdout, &stderr); err == nil {
		t.Fatal("ambiguous input accepted")
	}
	if _, err := parseWidths("4,5"); err == nil {
		t.Fatal("width 5 accepted")
	}
	if got, err := parseWidths("8,4,8"); err != nil || len(got) != 2 || got[0] != 8 || got[1] != 4 {
		t.Fatalf("widths=%v err=%v", got, err)
	}
}

func TestValidateOutputPathRejectsInputAndExistingEvidence(t *testing.T) {
	directory := t.TempDir()
	input := filepath.Join(directory, "trace.jsonl")
	if err := os.WriteFile(input, []byte("trace"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validateOutputPath(input, input); err == nil || !strings.Contains(err.Error(), "input trace") {
		t.Fatalf("same-path error=%v", err)
	}
	hardlink := filepath.Join(directory, "same-trace.jsonl")
	if err := os.Link(input, hardlink); err != nil {
		t.Fatal(err)
	}
	if err := validateOutputPath(input, hardlink); err == nil || !strings.Contains(err.Error(), "input trace") {
		t.Fatalf("hardlink error=%v", err)
	}
	existing := filepath.Join(directory, "evidence.json")
	if err := os.WriteFile(existing, []byte("prior"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validateOutputPath(input, existing); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("existing-output error=%v", err)
	}
	if err := validateOutputPath(input, filepath.Join(directory, "new-evidence.json")); err != nil {
		t.Fatalf("new output rejected: %v", err)
	}
}

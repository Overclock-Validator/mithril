package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const testOtherHash = "4vJ9JU1bJJE96FWSJKvHsmmF3drisEAs5XFWmZ7BvC7Y"

const sampleDivergence = `{
	"type": "bankhash_mismatch",
	"checked_slot": 285000042,
	"our_bankhash": "` + testHash + `",
	"winning_bankhash": "` + testOtherHash + `",
	"policy": "halt",
	"run_id": "run-abc",
	"created_at": "2026-06-29T12:00:00.123456789Z",
	"path_anchor_slot": 285000040,
	"source": {"kind": "leaf"},
	"observed_consensus_blocks": [285000042]
}`

func divergenceFixture(slot uint64) string {
	return fmt.Sprintf(`{"type":"bankhash_mismatch","checked_slot":%d,"our_bankhash":"%s","winning_bankhash":"%s","policy":"halt","run_id":"run-test","created_at":"2026-06-29T12:00:00Z"}`, slot, testHash, testOtherHash)
}

func writeDivergenceFixture(t *testing.T, dir string, slot uint64) {
	t.Helper()
	name := filepath.Join(dir, "bankhash_mismatch_slot_"+strconv.FormatUint(slot, 10)+".json")
	if err := os.WriteFile(name, []byte(divergenceFixture(slot)), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestParseDivergenceArtifact(t *testing.T) {
	var a DivergenceArtifact
	if err := json.Unmarshal([]byte(sampleDivergence), &a); err != nil {
		t.Fatal(err)
	}
	if a.ArtifactType == nil || *a.ArtifactType != "bankhash_mismatch" {
		t.Errorf("type = %v", a.ArtifactType)
	}
	if a.CheckedSlot == nil || *a.CheckedSlot != 285000042 {
		t.Errorf("checked_slot = %v", a.CheckedSlot)
	}
	if a.Policy == nil || *a.Policy != "halt" {
		t.Errorf("policy = %v", a.Policy)
	}
	for _, k := range []string{"path_anchor_slot", "source", "observed_consensus_blocks"} {
		if _, ok := a.Extra[k]; !ok {
			t.Errorf("extra should preserve %q", k)
		}
	}
	// Round-trip preserves extras.
	out, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), "path_anchor_slot") {
		t.Errorf("marshal should re-flatten extras: %s", out)
	}
}

func TestReadDivergenceMissingDir(t *testing.T) {
	got, _, err := readDivergenceArtifactsContext(context.Background(), t.TempDir())
	if err != nil || len(got) != 0 {
		t.Errorf("missing consensus dir should be empty, got %v/%v", got, err)
	}
}

func TestDivergenceOutputNamesArtifactScanCompleteness(t *testing.T) {
	wire, err := json.Marshal(divergenceOutput{ScanComplete: true})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(wire), `"scan_complete":true`) || strings.Contains(string(wire), "evidence_complete") {
		t.Fatalf("divergence output overstates scan completeness: %s", wire)
	}
}

func TestReadDivergenceFlat(t *testing.T) {
	dir := t.TempDir()
	cdir := filepath.Join(dir, "consensus")
	if err := os.Mkdir(cdir, 0o755); err != nil {
		t.Fatal(err)
	}
	writeDivergenceFixture(t, cdir, 285000042)
	if err := os.WriteFile(filepath.Join(cdir, "other.json"), []byte("{}"), 0o644); err != nil { // must be ignored
		t.Fatal(err)
	}
	got, _, err := readDivergenceArtifactsContext(context.Background(), dir)
	if err != nil || len(got) != 1 || got[0].CheckedSlot == nil || *got[0].CheckedSlot != 285000042 {
		t.Errorf("flat read = %v/%v", got, err)
	}
}

func TestReadDivergenceDoesNotFallBackWhenActiveRunHasNoConsensusDir(t *testing.T) {
	logDir := t.TempDir()
	flat := filepath.Join(logDir, "consensus")
	if err := os.Mkdir(flat, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(flat, "bankhash_mismatch_slot_1.json"), []byte(divergenceFixture(1)), 0o644); err != nil {
		t.Fatal(err)
	}
	run := filepath.Join(logDir, "run-current")
	if err := os.Mkdir(run, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("run-current", filepath.Join(logDir, "latest")); err != nil {
		t.Fatal(err)
	}
	artifacts, meta, err := readDivergenceArtifactsContext(context.Background(), logDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(artifacts) != 0 || meta.SourceLayout != "latest" {
		t.Fatalf("stale flat divergence leaked into current run: artifacts=%+v meta=%+v", artifacts, meta)
	}
}

func TestReadDivergenceSkipsUnparseable(t *testing.T) {
	dir := t.TempDir()
	cdir := filepath.Join(dir, "consensus")
	if err := os.Mkdir(cdir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cdir, "bankhash_mismatch_slot_1.json"), []byte("not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	writeDivergenceFixture(t, cdir, 2)
	got, meta, err := readDivergenceArtifactsContext(context.Background(), dir)
	if err != nil || len(got) != 1 || meta.Invalid != 1 {
		t.Errorf("unparseable evidence accounting: got=%d meta=%+v err=%v", len(got), meta, err)
	}
}

func TestReadDivergenceRejectsMalformedFilenameAsInvalidEvidence(t *testing.T) {
	dir := t.TempDir()
	cdir := filepath.Join(dir, "consensus")
	if err := os.Mkdir(cdir, 0o755); err != nil {
		t.Fatal(err)
	}
	malformed := strings.Replace(divergenceFixture(1), `"checked_slot":1`, `"checked_slot":0`, 1)
	if err := os.WriteFile(filepath.Join(cdir, "bankhash_mismatch_slot_.json"), []byte(malformed), 0o644); err != nil {
		t.Fatal(err)
	}
	artifacts, meta, err := readDivergenceArtifactsContext(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(artifacts) != 0 || meta.Candidates != 1 || meta.Invalid != 1 {
		t.Fatalf("malformed filename accepted: artifacts=%+v meta=%+v", artifacts, meta)
	}
}

func TestReadDivergenceSymlinkEscapeRejected(t *testing.T) {
	outside := t.TempDir()
	ocons := filepath.Join(outside, "consensus")
	if err := os.Mkdir(ocons, 0o755); err != nil {
		t.Fatal(err)
	}
	writeDivergenceFixture(t, ocons, 9)
	logDir := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(logDir, "latest")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readDivergenceArtifactsContext(context.Background(), logDir); err == nil {
		t.Error("escaping active latest symlink must be an explicit error")
	}
}

func TestReadDivergenceFlatDirectorySymlinkEscapeRejected(t *testing.T) {
	outside := t.TempDir()
	writeDivergenceFixture(t, outside, 9)
	logDir := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(logDir, "consensus")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readDivergenceArtifactsContext(context.Background(), logDir); err == nil {
		t.Fatal("escaping flat consensus symlink was accepted")
	}
}

func TestDivergenceArtifactsSortedBySlotNumerically(t *testing.T) {
	dir := t.TempDir()
	cons := filepath.Join(dir, "consensus")
	if err := os.Mkdir(cons, 0o755); err != nil {
		t.Fatal(err)
	}
	// Written out of order and with different digit counts to defeat lexicographic sort.
	for _, slot := range []uint64{100, 9, 2, 10} {
		writeDivergenceFixture(t, cons, slot)
	}
	arts, _, err := readDivergenceArtifactsContext(context.Background(), dir)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var got []uint64
	for _, a := range arts {
		got = append(got, *a.CheckedSlot)
	}
	want := []uint64{100, 10, 9, 2}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("artifact order = %v, want newest-first by slot %v", got, want)
	}
}

func TestDivergenceMetaReportsTruncation(t *testing.T) {
	dir := t.TempDir()
	cons := filepath.Join(dir, "consensus")
	if err := os.Mkdir(cons, 0o755); err != nil {
		t.Fatal(err)
	}
	for slot := 1; slot <= maxDivergenceArtifacts+3; slot++ {
		writeDivergenceFixture(t, cons, uint64(slot))
	}
	arts, meta, err := readDivergenceArtifactsContext(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(arts) != maxDivergenceArtifacts || !meta.Truncated || meta.Candidates != maxDivergenceArtifacts+3 || meta.Returned != maxDivergenceArtifacts {
		t.Fatalf("unexpected meta: artifacts=%d meta=%+v", len(arts), meta)
	}
	if *arts[0].CheckedSlot != maxDivergenceArtifacts+3 {
		t.Fatalf("newest evidence was not retained: %+v", arts[0])
	}
}

func TestReadDivergenceReturnsPartialResultsAtDirectoryScanLimit(t *testing.T) {
	dir := t.TempDir()
	cons := filepath.Join(dir, "consensus")
	if err := os.Mkdir(cons, 0o755); err != nil {
		t.Fatal(err)
	}
	for slot := 1; slot <= maxDivergenceEntries+1; slot++ {
		writeDivergenceFixture(t, cons, uint64(slot))
	}

	artifacts, meta, err := readDivergenceArtifactsContext(context.Background(), dir)
	if err != nil {
		t.Fatalf("bounded directory scan returned a hard error: %v", err)
	}
	if !meta.ScanTruncated || !meta.Truncated || meta.Scanned != maxDivergenceEntries {
		t.Fatalf("bounded scan was not reported honestly: %+v", meta)
	}
	if len(artifacts) == 0 || len(artifacts) > maxDivergenceArtifacts {
		t.Fatalf("partial scan returned %d artifacts", len(artifacts))
	}
	for i := 1; i < len(artifacts); i++ {
		if *artifacts[i-1].CheckedSlot < *artifacts[i].CheckedSlot {
			t.Fatalf("partial artifacts are not newest-first within the scanned set: %d before %d", *artifacts[i-1].CheckedSlot, *artifacts[i].CheckedSlot)
		}
	}
}

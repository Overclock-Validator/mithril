package sigverifytelemetry

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteJSONLIsExactChronologicalAndBounded(t *testing.T) {
	EnableSchedulingSimulation(2)
	t.Cleanup(Disable)

	for i, message := range [][]byte{{0x00, 0xff}, []byte("second"), []byte("third\nline")} {
		var pub [32]byte
		var sig [64]byte
		pub[0], sig[0] = byte(i+1), byte(0xa0+i)
		attempt, _, _, _ := BeginVerification(SourceReplay, pub, sig, message)
		attempt.RecordResult(i != 1)
	}
	RecordSignatureDispatch(SourceTPU, 4, 1, []uint16{2, 3})

	var output strings.Builder
	if err := WriteJSONL(&output); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 4 {
		t.Fatalf("JSONL lines = %d, want summary, two retained verifications, and one dispatch", len(lines))
	}
	var summary traceSummary
	if err := json.Unmarshal([]byte(lines[0]), &summary); err != nil {
		t.Fatalf("decode summary: %v", err)
	}
	if summary.Schema != traceSchema || !summary.Enabled || summary.Mode != CollectionSchedulingSimulation || summary.Capacity != 2 || summary.VerificationAttempts != 3 || summary.ObservedEvents != 8 || summary.RetainedVerifications != 2 || summary.RetainedDispatches != 1 {
		t.Fatalf("unexpected summary: %+v", summary)
	}
	for index, wantSequence := range []uint64{2, 3} {
		var entry traceVerification
		if err := json.Unmarshal([]byte(lines[index+1]), &entry); err != nil {
			t.Fatalf("decode entry %d: %v", index, err)
		}
		if entry.Sequence != wantSequence || entry.Source != SourceReplay {
			t.Fatalf("entry %d = %+v", index, entry)
		}
		wantBegin := []uint64{3, 5}[index]
		wantComplete := []uint64{4, 6}[index]
		if entry.BeginEventSequence != wantBegin || entry.CompletionEventSequence != wantComplete {
			t.Fatalf("entry %d event order = %+v", index, entry)
		}
		decodedMessage, err := base64.StdEncoding.DecodeString(entry.Message)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := string(decodedMessage), []string{"second", "third\nline"}[index]; got != want {
			t.Fatalf("entry %d message = %q, want %q", index, got, want)
		}
	}
	var dispatch traceDispatch
	if err := json.Unmarshal([]byte(lines[3]), &dispatch); err != nil {
		t.Fatalf("decode dispatch: %v", err)
	}
	if dispatch.Type != "dispatch" || dispatch.DispatchID != 1 || dispatch.Mode != CollectionSchedulingSimulation || dispatch.ClaimEventSequence != 7 || dispatch.ReadyEventSequence != 8 || dispatch.SignatureLanes != 5 || dispatch.QueuedItemsBefore != 4 || dispatch.QueuedItemsAfter != 1 || len(dispatch.JobSignatures) != 2 || dispatch.JobSignatures[0] != 2 || dispatch.JobSignatures[1] != 3 {
		t.Fatalf("dispatch = %+v", dispatch)
	}
}

func TestWriteJSONLFileAtomicallyReplacesAndProtectsOutput(t *testing.T) {
	Enable(1)
	t.Cleanup(Disable)
	var pub [32]byte
	var sig [64]byte
	RecordVerification(SourceTPU, pub, sig, []byte("payload"))

	path := filepath.Join(t.TempDir(), "trace.jsonl")
	if err := os.WriteFile(path, []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := WriteJSONLFile(path); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("trace permissions = %o, want 600", got)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	if !scanner.Scan() || !strings.Contains(scanner.Text(), `"schema":"`+traceSchema+`"`) {
		t.Fatalf("missing trace summary: %q", scanner.Text())
	}
	if !scanner.Scan() || !strings.Contains(scanner.Text(), `"message_base64":"cGF5bG9hZA=="`) {
		t.Fatalf("missing exact verification: %q", scanner.Text())
	}
	if scanner.Scan() || scanner.Err() != nil {
		t.Fatalf("unexpected trailing trace data or scan error: %q %v", scanner.Text(), scanner.Err())
	}
	matches, err := filepath.Glob(filepath.Join(filepath.Dir(path), ".trace.jsonl.tmp-*"))
	if err != nil || len(matches) != 0 {
		t.Fatalf("temporary traces = %v, err=%v", matches, err)
	}
}

func TestWriteJSONLRejectsNilWriter(t *testing.T) {
	if err := WriteJSONL(nil); err == nil {
		t.Fatal("nil writer succeeded")
	}
}

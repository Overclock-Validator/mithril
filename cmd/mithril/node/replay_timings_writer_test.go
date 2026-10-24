package node

import (
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
)

func TestReplayTimingsRotateAndFlush(t *testing.T) {
	dir := t.TempDir()
	name := filepath.Join(dir, "replay_timings.jsonl")
	w, closeWriter, err := newReplayTimingsWriter(name, mlog.LogConfig{MaxSizeMB: 1, MaxBackups: 10})
	if err != nil {
		t.Fatal(err)
	}
	line := append(bytes.Repeat([]byte("x"), 1023), '\n')
	const count = 1500
	for i := 0; i < count; i++ {
		if _, err := w.Write(line); err != nil {
			t.Fatal(err)
		}
	}
	closeWriter()
	deadline := time.Now().Add(5 * time.Second)
	var archives []string
	for time.Now().Before(deadline) {
		archives, err = filepath.Glob(filepath.Join(dir, "*.gz"))
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := filepath.Glob(filepath.Join(dir, "replay_timings-*.jsonl"))
		if len(archives) == 1 && len(raw) == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(archives) != 1 {
		t.Fatalf("expected a compressed rotated file, got %v", archives)
	}
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) > 1<<20 {
		t.Fatalf("active file exceeded rotation size: %d", len(data))
	}
	f, err := os.Open(archives[0])
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer gz.Close()
	older, err := io.ReadAll(gz)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(append(older, data...), bytes.Repeat(line, count)) {
		t.Fatal("rotation or cleanup lost/corrupted timing records")
	}
}

func TestReplayTimingsRejectInvalidDestination(t *testing.T) {
	_, _, err := newReplayTimingsWriter(t.TempDir(), mlog.DefaultConfig())
	if err == nil {
		t.Fatal("directory destination should fail")
	}
}

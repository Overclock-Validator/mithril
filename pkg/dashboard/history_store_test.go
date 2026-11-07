package dashboard

import (
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func historyTestDir(t *testing.T) string {
	t.Helper()
	parent, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Join(parent, "history")
}

func TestHistoryStoreOwnershipAndLock(t *testing.T) {
	path := historyTestDir(t)
	s, err := openHistoryStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	if _, err := openHistoryStore(path); err == nil {
		t.Fatal("second writer acquired lock")
	}
	for _, name := range []string{path, filepath.Join(path, historyOwnerFile)} {
		info, err := os.Lstat(name)
		if err != nil || !privateHistoryInfo(info, name == path) {
			t.Fatalf("permissions: %s %v", name, err)
		}
	}
	for _, kind := range []string{"foreign", "symlink", "mode", "hardlink", "owner"} {
		t.Run(kind, func(t *testing.T) {
			path := historyTestDir(t)
			if err := os.Mkdir(path, 0700); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "foreign":
				os.WriteFile(filepath.Join(path, "accounts.db"), []byte("preserve"), 0600)
			case "symlink":
				os.Remove(path)
				os.Symlink(filepath.Dir(path), path)
			case "mode":
				os.Chmod(path, 0755)
			case "hardlink":
				other := filepath.Join(filepath.Dir(path), "other")
				os.WriteFile(other, []byte(historyOwnerText), 0600)
				os.Link(other, filepath.Join(path, historyOwnerFile))
			case "owner":
				os.WriteFile(filepath.Join(path, historyOwnerFile), []byte("other-format\n"), 0600)
			}
			if s, err := openHistoryStore(path); err == nil {
				s.close()
				t.Fatal("unsafe store accepted")
			}
		})
	}
}

func TestHistoryStoreRestartAndTornTail(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	path := historyTestDir(t)
	s, err := openHistoryStore(path)
	if err != nil {
		t.Fatal(err)
	}
	r := testHistoryRecord(now, 123)
	if added, err := s.append(r, now); err != nil || !added {
		t.Fatal(err)
	}
	if added, err := s.append(r, now); err != nil || added {
		t.Fatalf("duplicate: %v %v", added, err)
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(path, now.Format("2006-01-02.jsonl"))
	original, _ := os.ReadFile(file)
	f, err := os.OpenFile(file, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString(`{"record":`)
	f.Close()
	s, err = openHistoryStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	if err := s.load(now); err != nil {
		t.Fatal(err)
	}
	if !s.recoveredTail || len(s.records) != 1 || *s.records[0].Values[0] != "123" {
		t.Fatalf("tail recovery: %+v", s)
	}
	after, _ := os.ReadFile(file)
	if !bytes.Equal(after, original) {
		t.Fatal("recovered more than torn tail")
	}
}

func TestHistoryStoreRecoversAfterProcessExit(t *testing.T) {
	const variable = "MITHRIL_HISTORY_CRASH_TEST_DIR"
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	if path := os.Getenv(variable); path != "" {
		s, err := openHistoryStore(path)
		if err != nil {
			os.Exit(24)
		}
		if added, err := s.append(testHistoryRecord(now, 123), now); err != nil || !added {
			os.Exit(24)
		}
		f, err := os.OpenFile(filepath.Join(path, now.Format("2006-01-02.jsonl")), os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			os.Exit(24)
		}
		if _, err := f.WriteString(`{"record":`); err != nil {
			os.Exit(24)
		}
		os.Exit(23) // Simulate abrupt process death without closing either handle.
	}
	path := historyTestDir(t)
	cmd := exec.Command(os.Args[0], "-test.run=^TestHistoryStoreRecoversAfterProcessExit$")
	cmd.Env = append(os.Environ(), variable+"="+path)
	var exit *exec.ExitError
	if err := cmd.Run(); !errors.As(err, &exit) || exit.ExitCode() != 23 {
		t.Fatalf("child did not exit at the intended point: %v", err)
	}
	s, err := openHistoryStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	if err := s.load(now); err != nil {
		t.Fatal(err)
	}
	if !s.recoveredTail || len(s.records) != 1 || s.records[0].Values[0] == nil || *s.records[0].Values[0] != "123" {
		t.Fatal("recovery did not preserve the committed record and discard the torn tail")
	}
}

func TestChainHistoryStoreReloadsMixedVersions(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	path := historyTestDir(t)
	s, err := openHistoryStore(path)
	if err != nil {
		t.Fatal(err)
	}
	old := testHistoryRecord(now, 123)
	newer := testHistoryRecord(now.Add(time.Minute), 124)
	newer.Version = 2
	newer.Chain = &historyChainRecord{At: newer.ObservedAt, Values: [historyChainMetrics]string{"2", "0.1", "0.1", "1.6", "0.2", "300"}}
	for _, r := range []historyRecord{old, newer} {
		if added, err := s.append(r, now.Add(time.Minute)); err != nil || !added {
			t.Fatalf("append: %v %v", added, err)
		}
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	s, err = openHistoryStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	if err := s.load(now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if len(s.records) != 2 || s.records[0].Chain != nil || s.records[1].Chain == nil || s.records[1].Chain.Values != newer.Chain.Values {
		t.Fatalf("mixed history: %+v", s.records)
	}
}

func TestHistoryStorePreservesCompleteCorruption(t *testing.T) {
	for _, damage := range []string{"checksum", "interior", "oversized", "schema", "old-tail"} {
		t.Run(damage, func(t *testing.T) {
			now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
			path := historyTestDir(t)
			s, err := openHistoryStore(path)
			if err != nil {
				t.Fatal(err)
			}
			s.close()
			line, _ := encodeHistoryRecord(testHistoryRecord(now, 123))
			var bad []byte
			switch damage {
			case "checksum":
				bad = bytes.Replace(line, []byte(`"123"`), []byte(`"124"`), 1)
			case "interior":
				bad = append(append([]byte{}, line...), []byte("garbage\n")...)
			case "oversized":
				bad = []byte(strings.Repeat("x", historyRecordBytes) + "\n")
			case "schema":
				bad = []byte("{\"record\":{\"v\":2},\"crc32c\":0}\n")
			case "old-tail":
				bad = append(append([]byte{}, line...), []byte("partial")...)
				os.WriteFile(filepath.Join(path, now.Add(24*time.Hour).Format("2006-01-02.jsonl")), nil, 0600)
			}
			file := filepath.Join(path, now.Format("2006-01-02.jsonl"))
			if err := os.WriteFile(file, bad, 0600); err != nil {
				t.Fatal(err)
			}
			s, err = openHistoryStore(path)
			if err != nil {
				t.Fatal(err)
			}
			defer s.close()
			if err := s.load(now); !errors.Is(err, errHistoryCorrupt) {
				t.Fatalf("wanted corrupt: %v", err)
			}
			after, _ := os.ReadFile(file)
			if !bytes.Equal(bad, after) {
				t.Fatal("corruption silently repaired")
			}
		})
	}
}

func TestHistoryStoreWriteFailuresDoNotPublish(t *testing.T) {
	for _, failure := range []string{"short", "disk-full", "sync"} {
		t.Run(failure, func(t *testing.T) {
			s, err := openHistoryStore(historyTestDir(t))
			if err != nil {
				t.Fatal(err)
			}
			defer s.close()
			switch failure {
			case "short":
				s.write = func(_ *os.File, b []byte) (int, error) { return len(b) - 1, nil }
			case "disk-full":
				s.write = func(*os.File, []byte) (int, error) { return 0, syscall.ENOSPC }
			case "sync":
				s.sync = func(*os.File) error { return syscall.EIO }
			}
			now := time.Now().UTC()
			if added, err := s.append(testHistoryRecord(now, 1), now); err == nil || added {
				t.Fatalf("published failed write: %v %v", added, err)
			}
			if len(s.records) != 0 || s.lastAt != 0 {
				t.Fatal("failed write entered durable ring")
			}
		})
	}
}

func TestHistoryStoreRetentionAndClock(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	path := historyTestDir(t)
	s, err := openHistoryStore(path)
	if err != nil {
		t.Fatal(err)
	}
	s.close()
	for day := 30; day >= 0; day-- {
		at := now.Add(-time.Duration(day) * 24 * time.Hour)
		line, _ := encodeHistoryRecord(testHistoryRecord(at, uint64(31-day)))
		if err := os.WriteFile(filepath.Join(path, at.Format("2006-01-02.jsonl")), line, 0600); err != nil {
			t.Fatal(err)
		}
	}
	s, err = openHistoryStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	if err := s.load(now); err != nil {
		t.Fatal(err)
	}
	if len(s.records) != 31 || len(s.files) != 31 {
		t.Fatal("retention edge removed")
	}
	if err := s.prune(now.Add(24 * time.Hour)); err != nil {
		t.Fatal(err)
	}
	if len(s.records) != 30 || len(s.files) != 30 {
		t.Fatalf("retention: %d %d", len(s.records), len(s.files))
	}
	if _, err := s.append(testHistoryRecord(now.Add(-time.Minute), 9), now); !errors.Is(err, errHistoryClock) {
		t.Fatalf("clock regression: %v", err)
	}
	if err := s.prune(now.Add(-time.Hour)); !errors.Is(err, errHistoryClock) {
		t.Fatalf("regressed pruning: %v", err)
	}
	if len(s.records) != 30 {
		t.Fatal("regressed clock pruned data")
	}
}

func TestHistoryStoreSegmentLimits(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	s, err := openHistoryStore(historyTestDir(t))
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	name := now.Format("2006-01-02.jsonl")
	s.files[name] = historySegmentBytes
	if _, err := s.append(testHistoryRecord(now, 1), now); !errors.Is(err, errHistoryLimit) {
		t.Fatalf("segment cap: %v", err)
	}
	if historyFailure(io.ErrShortWrite) != "history_io" {
		t.Fatal("public error exposes details")
	}
}

func TestHistoryRejectsSymlinkParentBeforeCreatingDirectory(t *testing.T) {
	parent, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	real := filepath.Join(parent, "real")
	if err := os.Mkdir(real, 0700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(parent, "alias")
	if err := os.Symlink(real, alias); err != nil {
		t.Fatal(err)
	}
	if s, err := openHistoryStore(filepath.Join(alias, "history")); err == nil {
		s.close()
		t.Fatal("accepted symlink parent")
	}
	if _, err := os.Lstat(filepath.Join(real, "history")); !os.IsNotExist(err) {
		t.Fatal("created a directory through a rejected parent")
	}
}

func TestHistoryRecoveryDoesNotCallUnsyncedRecordsDurable(t *testing.T) {
	now := time.Now().UTC()
	path := historyTestDir(t)
	s, err := openHistoryStore(path)
	if err != nil {
		t.Fatal(err)
	}
	line, _ := encodeHistoryRecord(testHistoryRecord(now, 7))
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, now.Format("2006-01-02.jsonl")), line, 0600); err != nil {
		t.Fatal(err)
	}
	s, err = openHistoryStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	s.sync = func(*os.File) error { return syscall.EIO }
	if err := s.load(now); err == nil || len(s.records) != 0 {
		t.Fatal("unsynced recovery record advertised as durable")
	}
}

func TestHistoryStoreRejectsFIFOWithoutBlocking(t *testing.T) {
	path := historyTestDir(t)
	s, err := openHistoryStore(path)
	if err != nil {
		t.Fatal(err)
	}
	s.close()
	name := filepath.Join(path, "2026-10-31.jsonl")
	if err := syscall.Mkfifo(name, 0600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		s, err := openHistoryStore(path)
		if s != nil {
			s.close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("FIFO accepted as history")
		}
	case <-time.After(time.Second):
		// Release a regressed blocking opener before failing the test.
		fd, err := syscall.Open(name, syscall.O_RDWR|syscall.O_NONBLOCK, 0600)
		if err == nil {
			defer syscall.Close(fd)
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("FIFO opener did not stop")
		}
		t.Fatal("history startup blocked on a FIFO")
	}
}

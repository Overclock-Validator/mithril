package dashboard

import (
	"bufio"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const historyOwnerFile = ".mithril-history"
const historyOwnerText = "mithril-dashboard-history-v1\n"

var (
	errHistoryCorrupt = errors.New("history_corrupt")
	errHistoryLimit   = errors.New("history_limit")
	errHistoryClock   = errors.New("clock_regressed")
)

// historyStore has one owner: the history worker. HTTP handlers never touch it.
type historyStore struct {
	root          *os.Root
	directory     *os.File
	lock          *os.File
	files         map[string]int64
	records       []historyRecord
	lastAt        int64
	recoveredTail bool
	write         func(*os.File, []byte) (int, error)
	sync          func(*os.File) error
}

func privateHistoryInfo(info os.FileInfo, directory bool) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return false
	}
	if directory {
		return info.IsDir() && info.Mode().Perm() == 0700
	}
	return info.Mode().IsRegular() && info.Mode().Perm() == 0600 && stat.Nlink == 1
}

func (s *historyStore) openFile(name string, flags int) (*os.File, error) {
	f, err := s.root.OpenFile(name, flags|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0600)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil || !privateHistoryInfo(info, false) {
		_ = f.Close()
		return nil, errors.New("history file must be private, regular and owned by the reader")
	}
	return f, nil
}

func openHistoryStore(path string) (_ *historyStore, result error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("history directory must be a clean absolute path")
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(path))
	if err != nil || parent != filepath.Dir(path) {
		return nil, errors.New("history directory parent must not traverse symlinks")
	}
	if err := os.Mkdir(path, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || resolved != path {
		return nil, errors.New("history directory must not traverse symlinks")
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	s := &historyStore{root: root, files: make(map[string]int64), write: (*os.File).Write, sync: (*os.File).Sync}
	defer func() {
		if result != nil {
			_ = s.close()
		}
	}()
	s.directory, err = root.Open(".")
	if err != nil {
		return nil, err
	}
	info, err := s.directory.Stat()
	if err != nil || !privateHistoryInfo(info, true) {
		return nil, errors.New("history directory must be mode 0700 and owned by the reader")
	}
	entries, err := s.directory.ReadDir(historySegments + 2)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if len(entries) > historySegments+1 {
		return nil, errHistoryLimit
	}
	hasOwner := false
	for _, entry := range entries {
		if entry.Name() == historyOwnerFile {
			hasOwner = true
		}
	}
	if !hasOwner && len(entries) != 0 {
		return nil, errors.New("history directory is not empty or reader-owned")
	}
	flags := os.O_RDWR
	if !hasOwner {
		flags |= os.O_CREATE | os.O_EXCL
	}
	s.lock, err = s.openFile(historyOwnerFile, flags)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(s.lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, errors.New("history directory is already locked")
	}
	if !hasOwner {
		if n, err := s.lock.WriteString(historyOwnerText); err != nil || n != len(historyOwnerText) {
			return nil, errors.Join(err, io.ErrShortWrite)
		}
		if err := s.lock.Sync(); err != nil {
			return nil, err
		}
		if err := s.directory.Sync(); err != nil {
			return nil, err
		}
	} else {
		b, err := io.ReadAll(io.LimitReader(s.lock, int64(len(historyOwnerText)+1)))
		if err != nil || string(b) != historyOwnerText {
			return nil, errors.New("unknown history ownership format")
		}
	}
	for _, entry := range entries {
		if entry.Name() == historyOwnerFile {
			continue
		}
		if _, err := historyDay(entry.Name()); err != nil {
			return nil, errors.New("unexpected history directory entry")
		}
		f, err := s.openFile(entry.Name(), os.O_RDONLY)
		if err != nil {
			return nil, err
		}
		info, statErr := f.Stat()
		closeErr := f.Close()
		if statErr != nil || closeErr != nil {
			return nil, errors.Join(statErr, closeErr)
		}
		if info.Size() > historySegmentBytes {
			return nil, errHistoryLimit
		}
		s.files[entry.Name()] = info.Size()
	}
	return s, nil
}

func historyDay(name string) (time.Time, error) {
	if len(name) != len("2006-01-02.jsonl") || !strings.HasSuffix(name, ".jsonl") {
		return time.Time{}, errHistoryCorrupt
	}
	day, err := time.Parse("2006-01-02", strings.TrimSuffix(name, ".jsonl"))
	if err != nil || day.Format("2006-01-02.jsonl") != name {
		return time.Time{}, errHistoryCorrupt
	}
	return day, nil
}

func (s *historyStore) load(now time.Time) error {
	// A previous process may have written a complete line before its sync
	// failed. Flush recovered directory entries and files before publishing
	// those validated records as durable history.
	if err := s.sync(s.directory); err != nil {
		return err
	}
	names := make([]string, 0, len(s.files))
	for name := range s.files {
		names = append(names, name)
	}
	sort.Strings(names)
	for index, name := range names {
		day, _ := historyDay(name)
		f, err := s.openFile(name, os.O_RDONLY)
		if err != nil {
			return err
		}
		if err := s.sync(f); err != nil {
			_ = f.Close()
			return err
		}
		reader := bufio.NewReaderSize(f, historyRecordBytes)
		var offset int64
		for {
			line, readErr := reader.ReadSlice('\n')
			if readErr == io.EOF && len(line) == 0 {
				break
			}
			if readErr == io.EOF && len(line) > 0 && index == len(names)-1 && len(line) < historyRecordBytes {
				// Only a torn final append is repairable. A complete bad record or
				// damage in an older segment is preserved for operator inspection.
				if err := f.Close(); err != nil {
					return err
				}
				f = nil
				tail, err := s.openFile(name, os.O_RDWR)
				if err != nil {
					return err
				}
				err = tail.Truncate(offset)
				if err == nil {
					err = s.sync(tail)
				}
				err = errors.Join(err, tail.Close())
				if err != nil {
					return err
				}
				s.files[name], s.recoveredTail = offset, true
				break
			}
			if readErr != nil {
				_ = f.Close()
				return errHistoryCorrupt
			}
			r, err := decodeHistoryRecord(line)
			if err != nil || r.At < day.Unix() || r.At >= day.Add(24*time.Hour).Unix() || r.At <= s.lastAt || len(s.records) >= historyRecordLimit {
				_ = f.Close()
				return errHistoryCorrupt
			}
			s.records = append(s.records, r)
			s.lastAt = r.At
			offset += int64(len(line))
		}
		if f != nil {
			if err := f.Close(); err != nil {
				return err
			}
		}
	}
	return s.prune(now)
}

func (s *historyStore) prune(now time.Time) error {
	if now.Unix() < s.lastAt {
		return errHistoryClock
	}
	cutoff := now.UTC().Truncate(time.Minute).Add(-historyRetention).Unix()
	changed := false
	for name := range s.files {
		day, err := historyDay(name)
		if err != nil {
			return err
		}
		if day.Add(24*time.Hour).Unix() <= cutoff {
			if err := s.root.Remove(name); err != nil {
				return err
			}
			delete(s.files, name)
			changed = true
		}
	}
	if changed {
		if err := s.directory.Sync(); err != nil {
			return err
		}
	}
	first := sort.Search(len(s.records), func(i int) bool { return s.records[i].At >= cutoff })
	if first > 0 {
		copy(s.records, s.records[first:])
		clear(s.records[len(s.records)-first:])
		s.records = s.records[:len(s.records)-first]
	}
	return nil
}

func (s *historyStore) append(r historyRecord, now time.Time) (bool, error) {
	line, err := encodeHistoryRecord(r)
	if err != nil {
		return false, err
	}
	if r.At < s.lastAt {
		return false, errHistoryClock
	}
	if r.At == s.lastAt {
		return false, nil
	}
	if r.At > now.Unix() {
		return false, errHistoryClock
	}
	if err := s.prune(now); err != nil {
		return false, err
	}
	name := time.Unix(r.At, 0).UTC().Format("2006-01-02.jsonl")
	size, exists := s.files[name]
	if size+int64(len(line)) > historySegmentBytes || !exists && len(s.files) >= historySegments || len(s.records) >= historyRecordLimit {
		return false, errHistoryLimit
	}
	flags := os.O_WRONLY | os.O_APPEND
	if !exists {
		flags |= os.O_CREATE | os.O_EXCL
	}
	f, err := s.openFile(name, flags)
	if err != nil {
		return false, err
	}
	info, err := f.Stat()
	if err == nil && info.Size() != size {
		err = errHistoryCorrupt
	}
	if err == nil {
		var n int
		n, err = s.write(f, line)
		if err == nil && n != len(line) {
			err = io.ErrShortWrite
		}
	}
	if err == nil {
		err = s.sync(f)
	}
	err = errors.Join(err, f.Close())
	if err == nil && !exists {
		err = s.directory.Sync()
	}
	if err != nil {
		return false, err
	}
	s.files[name], s.lastAt = size+int64(len(line)), r.At
	s.records = append(s.records, r)
	return true, nil
}

func (s *historyStore) close() error {
	if s == nil {
		return nil
	}
	var err error
	if s.lock != nil {
		err = errors.Join(err, s.lock.Close())
		s.lock = nil
	}
	if s.directory != nil {
		err = errors.Join(err, s.directory.Close())
		s.directory = nil
	}
	if s.root != nil {
		err = errors.Join(err, s.root.Close())
		s.root = nil
	}
	return err
}

func historyFailure(err error) string {
	switch {
	case errors.Is(err, errHistoryCorrupt):
		return "history_corrupt"
	case errors.Is(err, errHistoryLimit):
		return "history_limit"
	case errors.Is(err, errHistoryClock):
		return "clock_regressed"
	default:
		return "history_io"
	}
}

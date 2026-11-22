//go:build linux || darwin

package accountsdb

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"hash"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

type censusInode struct{ dev, ino uint64 }
type censusSignature struct {
	dev, ino, mode, links, size, blocks, mtime, ctime uint64
}
type physicalCensusPass struct {
	sum    PhysicalUsage
	digest [32]byte
}
type physicalCensusTestHooks struct {
	betweenPasses func()
	mountIdentity func(*os.File) (uint64, bool)
}

type physicalCensusWalk struct {
	ctx           context.Context
	started       time.Time
	limits        PhysicalUsageLimits
	entries       uint64
	seen          map[censusInode]struct{}
	hash          hash.Hash
	mountIdentity func(*os.File) (uint64, bool)
}

func (w *physicalCensusWalk) budget() string {
	if w.ctx.Err() != nil {
		return "cancelled"
	}
	if time.Since(w.started) > w.limits.MaxDuration {
		return "time_limit"
	}
	return ""
}
func (w *physicalCensusWalk) fingerprint(name string, s censusSignature) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(len(name)))
	w.hash.Write(b[:])
	w.hash.Write([]byte(name))
	for _, v := range []uint64{s.dev, s.ino, s.mode, s.links, s.size, s.blocks, s.mtime, s.ctime} {
		binary.LittleEndian.PutUint64(b[:], v)
		w.hash.Write(b[:])
	}
}
func addCensusValue(dst *uint64, n uint64) bool {
	if n > math.MaxUint64-*dst {
		return false
	}
	*dst += n
	return true
}

// walk opens one component relative to its already-open parent directory.
// O_NOFOLLOW applies at every level, including regular files, so an entry swap
// cannot redirect the traversal through a symlink. No file payload is read.
func (w *physicalCensusWalk) walk(parent int, name, relative string, dev, mount uint64, depth int, sum *PhysicalUsage) string {
	if reason := w.budget(); reason != "" {
		return reason
	}
	if depth > 32 || len(relative) > 4096 || name != "." && (name == "" || name == ".." || strings.ContainsAny(name, "/\x00")) {
		return "path_limit"
	}
	if w.entries >= w.limits.MaxEntries {
		return "entry_limit"
	}
	w.entries++
	fd, err := unix.Openat(parent, name, censusMetadataOpenFlags(), 0)
	if err != nil {
		return "open_or_race"
	}
	file := os.NewFile(uintptr(fd), name)
	if file == nil {
		unix.Close(fd)
		return "open_or_race"
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return "stat_or_race"
	}
	s, ok := physicalCensusSignature(info)
	actualMount, mountKnown := w.mountIdentity(file)
	if !mountKnown {
		return "unsupported_mount_identity"
	}
	if actualMount != mount {
		return "mount_changed"
	}
	if !ok || s.dev != dev {
		return "unsupported_or_device_changed"
	}
	if !info.Mode().IsRegular() && !info.IsDir() {
		return "unsupported_file_type"
	}
	if info.Mode().IsRegular() && s.links != 1 {
		return "hardlink"
	}
	identity := censusInode{s.dev, s.ino}
	if _, exists := w.seen[identity]; exists {
		return "duplicate_inode"
	}
	w.seen[identity] = struct{}{}
	if s.blocks > math.MaxUint64/512 || !addCensusValue(&sum.AllocatedBytes, s.blocks*512) {
		return "overflow"
	}
	if info.IsDir() {
		if !addCensusValue(&sum.Directories, 1) {
			return "overflow"
		}
	} else {
		if !addCensusValue(&sum.RegularFiles, 1) || !addCensusValue(&sum.LogicalFileBytes, s.size) {
			return "overflow"
		}
	}
	w.fingerprint(relative, s)
	if info.IsDir() {
		readFD, readErr := unix.Openat(parent, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if readErr != nil {
			return "directory_open_or_race"
		}
		directory := os.NewFile(uintptr(readFD), name)
		defer directory.Close()
		readInfo, readErr := directory.Stat()
		if readErr != nil {
			return "directory_stat_or_race"
		}
		readSignature, readKnown := physicalCensusSignature(readInfo)
		readMount, mountKnown := w.mountIdentity(directory)
		if !mountKnown || readMount != mount {
			return "mount_changed"
		}
		if !readKnown || readSignature != s {
			return "directory_changed"
		}
		for {
			if reason := w.budget(); reason != "" {
				return reason
			}
			entries, readErr := directory.ReadDir(256)
			for _, entry := range entries {
				if reason := w.walk(readFD, entry.Name(), relative+"/"+entry.Name(), dev, mount, depth+1, sum); reason != "" {
					return reason
				}
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				return "read_or_race"
			}
		}
	}
	after, err := file.Stat()
	if err != nil {
		return "stat_or_race"
	}
	end, ok := physicalCensusSignature(after)
	if !ok || end != s {
		return "changed_during_pass"
	}
	return w.budget()
}

// Open each absolute component relative to an already-open directory; even
// ancestors are never followed through symlinks. Reopening this way at each
// pass end verifies that the pinned tree still occupies its configured path.
func openCensusRoot(path string) (*os.File, error) {
	fd, err := unix.Open("/", censusRootOpenFlags(), 0)
	if err != nil {
		return nil, err
	}
	for _, name := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		next, openErr := unix.Openat(fd, name, censusRootOpenFlags(), 0)
		unix.Close(fd)
		if openErr != nil {
			return nil, openErr
		}
		fd = next
	}
	return os.NewFile(uintptr(fd), path), nil
}

func observePhysicalUsage(ctx context.Context, roots []string, limits PhysicalUsageLimits, hooks *physicalCensusTestHooks) (PhysicalUsage, string) {
	if ctx == nil || len(roots) == 0 || len(roots) > 32 || limits.MaxEntries == 0 || limits.MaxEntries > 2_000_000 || limits.MaxDuration <= 0 || limits.MaxDuration > 5*time.Second {
		return PhysicalUsage{}, "invalid_limits"
	}
	started := time.Now()
	w := physicalCensusWalk{ctx: ctx, started: started, limits: limits, seen: make(map[censusInode]struct{}), hash: sha256.New(), mountIdentity: physicalCensusMountIdentity}
	if hooks != nil && hooks.mountIdentity != nil {
		w.mountIdentity = hooks.mountIdentity
	}
	opened := make([]*os.File, 0, len(roots))
	signatures := make([]censusSignature, 0, len(roots))
	mounts := make([]uint64, 0, len(roots))
	defer func() {
		for _, f := range opened {
			f.Close()
		}
	}()
	for i, path := range roots {
		if reason := w.budget(); reason != "" {
			return PhysicalUsage{}, reason
		}
		if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == string(filepath.Separator) {
			return PhysicalUsage{}, "invalid_root"
		}
		for _, earlier := range roots[:i] {
			relative, err := filepath.Rel(earlier, path)
			if err != nil || relative == "." || relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
				return PhysicalUsage{}, "overlapping_roots"
			}
			relative, err = filepath.Rel(path, earlier)
			if err != nil || relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
				return PhysicalUsage{}, "overlapping_roots"
			}
		}
		f, err := openCensusRoot(path)
		if err != nil || f == nil {
			return PhysicalUsage{}, "root_open_or_race"
		}
		opened = append(opened, f)
		info, err := f.Stat()
		if err != nil {
			return PhysicalUsage{}, "root_stat_or_race"
		}
		s, ok := physicalCensusSignature(info)
		if !ok {
			return PhysicalUsage{}, "unsupported_stat"
		}
		signatures = append(signatures, s)
		mount, known := w.mountIdentity(f)
		if !known {
			return PhysicalUsage{}, "unsupported_mount_identity"
		}
		mounts = append(mounts, mount)
	}
	var passes [2]physicalCensusPass
	for pass := range passes {
		clear(w.seen)
		w.hash.Reset()
		for index, f := range opened {
			if reason := w.walk(int(f.Fd()), ".", roots[index], signatures[index].dev, mounts[index], 0, &passes[pass].sum); reason != "" {
				return PhysicalUsage{}, reason
			}
			attached, err := openCensusRoot(roots[index])
			if err != nil {
				return PhysicalUsage{}, "root_replaced"
			}
			info, statErr := attached.Stat()
			attachedMount, attachedKnown := w.mountIdentity(attached)
			attached.Close()
			if !attachedKnown || attachedMount != mounts[index] {
				return PhysicalUsage{}, "root_replaced"
			}
			if statErr != nil {
				return PhysicalUsage{}, "root_replaced"
			}
			s, ok := physicalCensusSignature(info)
			if !ok || s.dev != signatures[index].dev || s.ino != signatures[index].ino || !info.IsDir() {
				return PhysicalUsage{}, "root_replaced"
			}
		}
		copy(passes[pass].digest[:], w.hash.Sum(nil))
		if pass == 0 && hooks != nil && hooks.betweenPasses != nil {
			hooks.betweenPasses()
		}
	}
	if reason := w.budget(); reason != "" {
		return PhysicalUsage{}, reason
	}
	// Directory iteration order need not be stable on every filesystem; a
	// different order is a safe false negative rather than a partial total.
	if passes[0] != passes[1] {
		return PhysicalUsage{}, "changed_between_passes"
	}
	finished := time.Now()
	duration := finished.Sub(started)
	wall := finished.UnixNano() - started.UnixNano()
	difference := wall - int64(duration)
	if duration <= 0 || duration > limits.MaxDuration || difference > 250_000_000 || difference < -250_000_000 {
		return PhysicalUsage{}, "clock_or_time_limit"
	}
	result := passes[1].sum
	result.StartedAt = started
	result.CompletedAt = finished
	result.DurationNanoseconds = uint64(duration)
	return result, ""
}

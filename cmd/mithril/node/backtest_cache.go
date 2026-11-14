package node

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"time"
)

// Backtest cache modes: how much of the AccountsDB the OS page cache holds
// when timed replay begins. In-process caches always start empty, since
// every run is a fresh node process.
const (
	// cacheCold evicts the AccountsDB from the page cache just before replay
	// and measures from the first block: a node right after a restart.
	cacheCold = "cold"
	// cacheNormal evicts the same way, then leaves the warm-up blocks out of
	// the summary: a node that has been replaying the preceding blocks, with
	// nothing cached from earlier runs over these same blocks.
	cacheNormal = "normal"
	// cacheWarm evicts nothing: the page cache keeps what earlier runs over
	// the same blocks left in it. Least noisy, so best for A/B comparisons
	// of two builds, but warmer than a running node.
	cacheWarm = "warm"
)

// describeCacheMode is the summary's settings line for the cache mode.
func describeCacheMode(mode string, eviction *cacheEviction) string {
	switch {
	case mode == cacheWarm:
		return "warm (page cache as earlier runs left it)"
	case eviction == nil:
		return mode + " (eviction did not run)"
	default:
		return fmt.Sprintf("%s: %s, inside replay_init_s", mode, eviction.describe())
	}
}

func validCacheMode(mode string) bool {
	return mode == cacheCold || mode == cacheNormal || mode == cacheWarm
}

// cacheEviction reports what evictPageCache did.
type cacheEviction struct {
	Files    int
	Bytes    int64
	Duration time.Duration
	Err      error // nil on success; errPageCacheUnsupported off Linux
}

func (e cacheEviction) describe() string {
	if e.Err != nil {
		return fmt.Sprintf("not evicted (%v)", e.Err)
	}
	return fmt.Sprintf("%d files, %.1f GiB evicted from the page cache in %s", e.Files, float64(e.Bytes)/(1<<30), e.Duration.Round(time.Millisecond))
}

// evictPageCache asks the kernel to drop every file under dir from the page
// cache (posix_fadvise DONTNEED). Dirty pages are not dropped; the store is
// flushed by the time replay starts. Unreadable files are skipped.
func evictPageCache(dir string) cacheEviction {
	start := time.Now()
	var out cacheEviction
	walkErr := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || !d.Type().IsRegular() {
			return nil
		}
		size, ok, err := dropFileCache(path)
		if err != nil {
			return err
		}
		if ok {
			out.Files++
			out.Bytes += size
		}
		return nil
	})
	out.Duration = time.Since(start)
	out.Err = walkErr
	return out
}

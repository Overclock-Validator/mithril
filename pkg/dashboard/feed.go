package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/netutil"
	"golang.org/x/time/rate"
)

// The reader polls once per SampleInterval. Accepting more requests than this
// only lets a feed client spend node CPU; excess connections wait in the
// listen backlog instead of being parsed.
const (
	feedRequestsPerSecond = 4
	feedRequestBurst      = 4
)

// Feed samples once per second, independently of reader requests. Readers only
// receive the cached encoding; they never call into node state.
type Feed struct {
	server   *http.Server
	listener net.Listener
	cancel   context.CancelFunc
	done     chan struct{}
	served   chan struct{}
	closed   sync.Once
	latest   atomic.Pointer[feedStatus]
}

const statusRemainingHeader = "Mithril-Status-Remaining-Milliseconds"

// feedStatus keeps the cached body and its original monotonic lifetime together.
type feedStatus struct {
	body       []byte
	freshUntil time.Time
}

// ListenFeed requires an existing private directory owned by the node. It
// replaces only a stale socket owned by the node that refuses connections, as
// left by an exit that skipped Close (SIGKILL, OOM, os.Exit); any other existing
// path is left in place and the listen fails.
// The sampler must return a bounded immutable copy without I/O or long-held locks.
func ListenFeed(parent context.Context, path string, sample func() Snapshot) (*Feed, error) {
	return listenFeed(parent, path, "", false, sample)
}

// ListenFeedGroup permits a dedicated Linux group to connect to the feed.
// Its parent directory must be trusted, owned by the node, and mode 2710.
func ListenFeedGroup(parent context.Context, path, group string, sample func() Snapshot) (*Feed, error) {
	return listenFeed(parent, path, group, true, sample)
}

func listenFeed(parent context.Context, path, group string, groupMode bool, sample func() Snapshot) (*Feed, error) {
	if !filepath.IsAbs(path) || sample == nil {
		return nil, errors.New("dashboard feed requires an absolute socket path and a sampler")
	}
	var gid int
	if groupMode {
		var err error
		gid, err = inspectFeedGroup(filepath.Dir(path), group)
		if err != nil {
			return nil, err
		}
	} else {
		info, err := os.Lstat(filepath.Dir(path))
		if err != nil {
			return nil, fmt.Errorf("inspect dashboard socket directory: %w", err)
		}
		if !info.IsDir() || info.Mode().Perm()&0o077 != 0 || !ownedByEffectiveUser(info) {
			return nil, errors.New("dashboard socket directory must be private (mode 0700) and owned by the node")
		}
	}
	if err := removeStaleFeedSocket(path); err != nil {
		return nil, err
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if groupMode {
		err = configureFeedGroup(path, gid)
	} else {
		err = os.Chmod(path, 0o600)
	}
	if err != nil {
		listener.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	f := &Feed{listener: listener, cancel: cancel, done: make(chan struct{}), served: make(chan struct{})}
	rejected := false
	update := func() {
		s := sample()
		now := time.Now()
		s.SchemaVersion, s.SampledAt = SchemaVersion, now.UTC()
		var b []byte
		reason := ""
		if s.validate() != nil {
			reason = "sample_invalid"
		} else {
			var err error
			b, err = json.Marshal(s)
			if err != nil {
				reason = "marshal_failed"
			} else if len(b) > MaxPrivateSnapshotBytes {
				reason = "snapshot_oversized"
			}
		}
		if reason != "" {
			if !rejected {
				attrs := []any{"component", "dashboard", "operation", "feed_sample", "entrypoint", "private_feed", "reason", reason}
				if reason == "snapshot_oversized" {
					attrs = append(attrs, "encoded_bytes", len(b), "limit_bytes", MaxPrivateSnapshotBytes)
				}
				slog.Warn("dashboard feed sample rejected", attrs...)
				rejected = true
			}
			return
		}
		f.latest.Store(&feedStatus{body: b, freshUntil: now.Add(StaleAfter)})
		if rejected {
			slog.Info("dashboard feed sampling recovered", "component", "dashboard", "operation", "feed_sample", "entrypoint", "private_feed")
			rejected = false
		}
	}
	update()
	mux := http.NewServeMux()
	mux.HandleFunc("/snapshot", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.RawQuery != "" || r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
			http.Error(w, "unsupported request", http.StatusBadRequest)
			return
		}
		status := f.latest.Load()
		if status == nil {
			http.Error(w, "snapshot unavailable", http.StatusServiceUnavailable)
			return
		}
		remaining := status.freshUntil.Sub(time.Now()).Milliseconds()
		remaining = max(0, min(remaining, StaleAfter.Milliseconds()))
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set(statusRemainingHeader, strconv.FormatInt(remaining, 10))
		_, _ = w.Write(status.body)
	})
	var handler http.Handler = mux
	f.server = &http.Server{Handler: handler, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 2 * time.Second,
		WriteTimeout: 2 * time.Second, IdleTimeout: 2 * time.Second, MaxHeaderBytes: 4096}
	// One request per connection, so the accept rate bounds the request rate.
	f.server.SetKeepAlivesEnabled(false)
	limited := &rateLimitedListener{Listener: listener, ctx: ctx,
		limiter: rate.NewLimiter(feedRequestsPerSecond, feedRequestBurst)}
	go func() {
		defer close(f.served)
		if err := f.server.Serve(netutil.LimitListener(limited, 4)); err != nil && ctx.Err() == nil {
			slog.Warn("dashboard feed listener stopped unexpectedly", "component", "dashboard", "entrypoint", "private_feed")
		}
		cancel()
	}()
	go func() {
		defer close(f.done)
		ticker := time.NewTicker(SampleInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				_ = f.server.Close()
				return
			case <-ticker.C:
				update()
			}
		}
	}()
	return f, nil
}

// rateLimitedListener delays Accept until the limiter allows another
// connection. Cancelling ctx (Feed.Close) ends a pending wait.
type rateLimitedListener struct {
	net.Listener
	ctx     context.Context
	limiter *rate.Limiter
}

func (l *rateLimitedListener) Accept() (net.Conn, error) {
	if err := l.limiter.Wait(l.ctx); err != nil {
		return nil, net.ErrClosed
	}
	return l.Listener.Accept()
}

// removeStaleFeedSocket unlinks a socket that is owned by the node and refuses
// connections. Regular files, symlinks, other owners and live listeners are left
// for net.Listen to reject.
func removeStaleFeedSocket(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect existing dashboard socket: %w", err)
	}
	if info.Mode().Type() != os.ModeSocket || !ownedByEffectiveUser(info) {
		return nil
	}
	conn, err := net.DialTimeout("unix", path, time.Second)
	if err == nil {
		conn.Close()
		return nil
	}
	if !isConnectionRefused(err) {
		return nil
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove stale dashboard socket: %w", err)
	}
	slog.Info("dashboard feed replaced a stale socket", "component", "dashboard", "entrypoint", "private_feed")
	return nil
}

func (f *Feed) Close() {
	if f == nil {
		return
	}
	f.closed.Do(func() {
		f.cancel()
		_ = f.server.Close()
		_ = f.listener.Close()
		<-f.done
		<-f.served
	})
}

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
	urlpath "path"
	"path/filepath"
	"strconv"
	"strings"
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
	feedRequestsPerSecond       = 4
	feedRequestBurst            = 4
	detailFeedRequestsPerSecond = 64
	detailFeedRequestBurst      = 16
)

// Feed samples once per second, independently of reader requests. Readers only
// receive the cached encoding; they never call into node state.
type Feed struct {
	server     *http.Server
	listener   net.Listener
	cancel     context.CancelFunc
	done       chan struct{}
	served     chan struct{}
	closed     sync.Once
	latest     atomic.Pointer[feedStatus]
	details    *replayDetailSource
	network    *networkDetailSource
	cache      *cacheDetailSource
	production *productionDetailSource
	storage    *storageDetailSource
	rooted     *rootedHistorySource
	reception  *receptionHistorySource
	machine    *machineDetailSource
	localPeers *localPeerSource
}

const statusRemainingHeader = "Mithril-Status-Remaining-Milliseconds"

// feedStatus keeps the cached body and its original monotonic lifetime together.
type feedStatus struct {
	body       []byte
	freshUntil time.Time
}

// DetailSamplers enables only the named private detail routes. The callbacks
// return bounded in-memory copies; a nil callback leaves its route disabled.
type DetailSamplers struct {
	Replay         ReplayDetailSampler
	SelectedWindow SelectedWindowSampler
	Network        NetworkDetailSampler
	NodeNetwork    NodeNetworkSampler
	Cache          CacheDetailSampler
	Production     ProductionDetailSampler
	Storage        StorageDetailSampler
	RootedHistory  RootedHistorySampler
	HeldSchedules  HeldScheduleSampler
	Reception      *ReceptionHistorySampler
	Machine        MachineDetailSampler
	LocalPeers     LocalPeerSampler
}

// ListenFeed requires an existing private directory owned by the node. It
// replaces only a stale socket owned by the node that refuses connections, as
// left by an exit that skipped Close (SIGKILL, OOM, os.Exit); any other existing
// path is left in place and the listen fails.
// The sampler must return a bounded immutable copy without I/O or long-held locks.
func ListenFeed(parent context.Context, path string, sample func() Snapshot) (*Feed, error) {
	return listenFeed(parent, path, "", false, sample, DetailSamplers{})
}

// ListenFeedGroup permits a dedicated Linux group to connect to the feed.
// Its parent directory must be trusted, owned by the node, and mode 2710.
func ListenFeedGroup(parent context.Context, path, group string, sample func() Snapshot) (*Feed, error) {
	return listenFeed(parent, path, group, true, sample, DetailSamplers{})
}

// ListenFeedWithReplayDetails adds an optional, independently sampled private
// replay-detail route on the same listener. An empty group uses private mode;
// a nonempty group uses ListenFeedGroup's existing permission checks. A nil
// details sampler leaves the route and its worker disabled.
func ListenFeedWithReplayDetails(parent context.Context, path, group string, sample func() Snapshot, details ReplayDetailSampler) (*Feed, error) {
	return ListenFeedWithDetails(parent, path, group, sample, DetailSamplers{Replay: details})
}

// ListenFeedWithDetails uses the existing listener and permission checks. Replay
// and held schedules share one bounded worker. Reception has one optional
// 250ms preparation worker; local peers use one optional low-rate preparation
// worker. Other families copy existing captures on the ordinary status cycle.
func ListenFeedWithDetails(parent context.Context, path, group string, sample func() Snapshot, details DetailSamplers) (*Feed, error) {
	return listenFeed(parent, path, group, group != "", sample, details)
}

func listenFeed(parent context.Context, path, group string, groupMode bool, sample func() Snapshot, details DetailSamplers) (*Feed, error) {
	if !filepath.IsAbs(path) || sample == nil {
		return nil, errors.New("dashboard feed requires an absolute socket path and a sampler")
	}
	if details.Network != nil && details.NodeNetwork != nil {
		return nil, errors.New("dashboard network samplers are mutually exclusive")
	}
	if details.HeldSchedules != nil && details.Replay == nil {
		return nil, errors.New("held schedules require the replay detail worker")
	}
	if details.SelectedWindow != nil && details.Replay == nil {
		return nil, errors.New("selected window requires the replay detail worker")
	}
	if details.Reception != nil && (details.Reception.Metadata == nil || details.Reception.ChunkBody == nil) {
		return nil, errors.New("reception history requires both bounded callbacks")
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
	if details.Replay != nil {
		f.details = &replayDetailSource{sample: details.Replay, done: make(chan struct{})}
		if details.HeldSchedules != nil {
			f.details.held = &heldScheduleSource{sample: details.HeldSchedules, gate: &f.details.gate,
				gateMu: &f.details.gateMu, active: make(chan struct{}, 2)}
		}
		if details.SelectedWindow != nil {
			f.details.selected = &selectedWindowSource{sample: details.SelectedWindow, gate: &f.details.gate,
				gateMu: &f.details.gateMu, revision: 1}
		}
	}
	if details.Network != nil {
		f.network = &networkDetailSource{sample: details.Network}
	}
	if details.NodeNetwork != nil {
		f.network = &networkDetailSource{nodeSample: details.NodeNetwork}
	}
	if details.Cache != nil {
		f.cache = &cacheDetailSource{sample: details.Cache}
	}
	if details.Production != nil {
		f.production = &productionDetailSource{sample: details.Production}
	}
	if details.Storage != nil {
		f.storage = &storageDetailSource{sample: details.Storage}
	}
	if details.RootedHistory != nil {
		f.rooted = &rootedHistorySource{sample: details.RootedHistory}
	}
	if details.Reception != nil {
		f.reception = newReceptionHistorySource(*details.Reception)
	}
	if details.Machine != nil {
		f.machine = newMachineDetailSource(details.Machine)
	}
	if details.LocalPeers != nil {
		f.localPeers = &localPeerSource{sample: details.LocalPeers, active: make(chan struct{}, 2), done: make(chan struct{})}
	}
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
		// Fence peer data before a replacement ordinary status is visible.
		if f.localPeers != nil {
			f.localPeers.observeStatus(s, now)
		}
		f.latest.Store(&feedStatus{body: b, freshUntil: now.Add(StaleAfter)})
		if f.reception != nil {
			f.reception.observeStatus(s, now)
		}
		if f.details != nil {
			f.details.observeStatus(s, now)
		}
		if f.network != nil {
			f.network.update(s, now)
		}
		if f.cache != nil {
			f.cache.update(s, now)
		}
		if f.production != nil {
			f.production.update(s, now)
		}
		if f.storage != nil {
			f.storage.update(s, now)
		}
		if f.rooted != nil {
			f.rooted.update(s, now)
		}
		if f.machine != nil {
			f.machine.update(ctx, s, now)
		}
		if rejected {
			slog.Info("dashboard feed sampling recovered", "component", "dashboard", "operation", "feed_sample", "entrypoint", "private_feed")
			rejected = false
		}
	}
	update()
	mux := http.NewServeMux()
	if f.details != nil {
		mux.HandleFunc("/details/replay", f.details.serveHTTP)
		if f.details.selected != nil {
			mux.HandleFunc("/details/selected", f.details.selected.serveHTTP)
		}
		if f.details.held != nil {
			mux.HandleFunc("/details/schedule", f.details.held.serveHTTP)
			mux.HandleFunc("/details/schedule/", f.details.held.serveHTTP)
		}
	}
	if f.network != nil {
		mux.HandleFunc("/details/network", f.network.serveHTTP)
	}
	if f.cache != nil {
		mux.HandleFunc("/details/cache", f.cache.serveHTTP)
	}
	if f.production != nil {
		mux.HandleFunc("/details/production", f.production.serveHTTP)
	}
	if f.storage != nil {
		mux.HandleFunc("/details/storage", f.storage.serveHTTP)
	}
	if f.rooted != nil {
		mux.HandleFunc("/details/slots", f.rooted.serveHTTP)
	}
	if f.localPeers != nil {
		mux.HandleFunc(localPeerPath, f.localPeers.serveHTTP)
	}
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
	if f.details != nil && f.details.selected != nil {
		next := handler
		handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/details/selected" || strings.HasPrefix(r.URL.Path, "/details/selected/") || urlpath.Clean(r.URL.Path) == "/details/selected" {
				f.details.selected.serveHTTP(w, r)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
	if f.localPeers != nil {
		next := handler
		handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == localPeerPath || strings.HasPrefix(r.URL.Path, localPeerPath+"/") || urlpath.Clean(r.URL.Path) == localPeerPath {
				f.localPeers.serveHTTP(w, r)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
	if f.machine != nil {
		next := handler
		handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == machineDetailPath || strings.HasPrefix(r.URL.Path, machineDetailPath+"/") {
				f.machine.serveHTTP(w, r)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
	if f.reception != nil {
		// Parse this fixed family before ServeMux's path-cleaning redirects.
		// Noncanonical paths must be refused, not silently rewritten.
		next := handler
		handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == receptionHistoryPath || strings.HasPrefix(r.URL.Path, receptionHistoryPath+"/") {
				f.reception.serveHTTP(w, r)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
	f.server = &http.Server{Handler: handler, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 2 * time.Second,
		WriteTimeout: 2 * time.Second, IdleTimeout: 2 * time.Second, MaxHeaderBytes: 4096}
	if f.details != nil {
		go f.details.run(ctx)
	}
	if f.reception != nil {
		go f.reception.run(ctx)
	}
	if f.localPeers != nil {
		go f.localPeers.run(ctx)
	}
	// One request per connection, so the accept rate bounds the request rate.
	f.server.SetKeepAlivesEnabled(false)
	requestRate, requestBurst := feedRequestsPerSecond, feedRequestBurst
	if details.Replay != nil || details.Cache != nil || details.SelectedWindow != nil || details.RootedHistory != nil ||
		details.HeldSchedules != nil || details.Network != nil || details.NodeNetwork != nil || details.Machine != nil ||
		details.LocalPeers != nil || details.Production != nil || details.Storage != nil || details.Reception != nil {
		// Replay detail can contain 128 pages; its reader has a 12-second deadline.
		requestRate, requestBurst = detailFeedRequestsPerSecond, detailFeedRequestBurst
	}
	limited := &rateLimitedListener{Listener: listener, ctx: ctx,
		limiter: rate.NewLimiter(rate.Limit(requestRate), requestBurst)}
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
				if f.machine != nil {
					f.machine.close()
				}
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
		if f.machine != nil {
			f.machine.close()
		}
		f.cancel()
		_ = f.server.Close()
		_ = f.listener.Close()
		<-f.done
		<-f.served
		if f.details != nil {
			<-f.details.done
		}
		if f.reception != nil {
			<-f.reception.done
		}
		if f.localPeers != nil {
			<-f.localPeers.done
		}
	})
}

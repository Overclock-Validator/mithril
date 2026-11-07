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
	rooted     *rootedHistorySource
	cache      *cacheDetailSource
	network    *networkDetailSource
	machine    *machineDetailSource
	localPeers *localPeerSource
}

const statusRemainingHeader = "Mithril-Status-Remaining-Milliseconds"

// feedStatus keeps the cached body and its original monotonic lifetime together.
type feedStatus struct {
	body       []byte
	freshUntil time.Time
}

// DetailSamplers enables bounded private detail routes when configured.
type DetailSamplers struct {
	Replay         ReplayDetailSampler
	Cache          CacheDetailSampler
	SelectedWindow SelectedWindowSampler
	RootedHistory  RootedHistorySampler
	HeldSchedules  HeldScheduleSampler
	Network        NetworkDetailSampler
	NodeNetwork    NodeNetworkSampler
	Machine        MachineDetailSampler
	LocalPeers     LocalPeerSampler
}

// ListenFeedWithReplayDetails enables replay details on the private feed.
func ListenFeedWithReplayDetails(parent context.Context, path, group string, sample func() Snapshot, details ReplayDetailSampler) (*Feed, error) {
	return ListenFeedWithDetails(parent, path, group, sample, DetailSamplers{Replay: details})
}

// ListenFeed requires an existing private directory. It never removes an
// existing socket: a stale path must be inspected by the operator before reuse.
// The sampler must return a bounded immutable copy without I/O or long-held locks.
func ListenFeed(parent context.Context, path string, sample func() Snapshot) (*Feed, error) {
	return listenFeed(parent, path, "", false, sample, DetailSamplers{})
}

// ListenFeedGroup permits a dedicated Linux group to connect to the feed.
// Its parent directory must be trusted, owned by the node, and mode 2710.
func ListenFeedGroup(parent context.Context, path, group string, sample func() Snapshot) (*Feed, error) {
	return listenFeed(parent, path, group, true, sample, DetailSamplers{})
}

// ListenFeedWithDetails uses the existing socket permission checks.
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
		if !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
			return nil, errors.New("dashboard socket directory must be private (mode 0700)")
		}
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
	if details.RootedHistory != nil {
		f.rooted = &rootedHistorySource{sample: details.RootedHistory}
	}
	if details.Cache != nil {
		f.cache = &cacheDetailSource{sample: details.Cache}
	}
	if details.Network != nil {
		f.network = &networkDetailSource{sample: details.Network}
	}
	if details.NodeNetwork != nil {
		f.network = &networkDetailSource{nodeSample: details.NodeNetwork}
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
		if f.localPeers != nil {
			f.localPeers.observeStatus(s, now)
		}
		f.latest.Store(&feedStatus{body: b, freshUntil: now.Add(StaleAfter)})
		if f.details != nil {
			f.details.observeStatus(s, now)
		}
		if f.rooted != nil {
			f.rooted.update(s, now)
		}
		if f.cache != nil {
			f.cache.update(s, now)
		}
		if f.network != nil {
			f.network.update(s, now)
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
	if f.rooted != nil {
		mux.HandleFunc("/details/slots", f.rooted.serveHTTP)
	}
	if f.cache != nil {
		mux.HandleFunc("/details/cache", f.cache.serveHTTP)
	}
	if f.network != nil {
		mux.HandleFunc("/details/network", f.network.serveHTTP)
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
	f.server = &http.Server{Handler: handler, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 2 * time.Second,
		WriteTimeout: 2 * time.Second, IdleTimeout: 2 * time.Second, MaxHeaderBytes: 4096}
	if f.details != nil {
		go f.details.run(ctx)
	}
	if f.localPeers != nil {
		go f.localPeers.run(ctx)
	}
	go func() {
		defer close(f.served)
		if err := f.server.Serve(netutil.LimitListener(listener, 4)); err != nil && ctx.Err() == nil {
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
		if f.localPeers != nil {
			<-f.localPeers.done
		}
	})
}

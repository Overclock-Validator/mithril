package accountsdb

import (
	"context"
	"errors"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/history"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/gagliardetto/solana-go"
)

// Measurement switch, not a feature: MITHRIL_BENCH_HISTORY_QUERIES=<per second>
// (0 or "max" for back to back) runs past-slot account reads against account
// history on one goroutine while the node replays, the way RPC reads would.
// Keys are sampled from what folds recorded; slots are random within the
// recorded range. Accounts history reports unchanged are read from the store,
// as the RPC path does. Latency percentiles are logged every 10 s.
//
// MITHRIL_BENCH_KEYS_FILE=<path> writes the sampled keys (base58, one per
// line) to that file every 10 s, for an outside RPC load driver.

type queryKeySample struct {
	mu   sync.Mutex
	rng  *rand.Rand
	keys [][32]byte
	seen int
}

const querySampleSize = 50000

func (q *queryKeySample) add(k [32]byte) {
	q.mu.Lock()
	defer q.mu.Unlock()
	rng := q.rng
	q.seen++
	if len(q.keys) < querySampleSize {
		q.keys = append(q.keys, k)
		return
	}
	if j := rng.Intn(q.seen); j < querySampleSize {
		q.keys[j] = k
	}
}

func (q *queryKeySample) pick(rng *rand.Rand) ([32]byte, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.keys) == 0 {
		return [32]byte{}, false
	}
	return q.keys[rng.Intn(len(q.keys))], true
}

var benchQueryKeys atomic.Pointer[queryKeySample]

func benchHistorySample(keys []history.KeyHistory) {
	q := benchQueryKeys.Load()
	if q == nil {
		return
	}
	for i := range keys {
		q.add(keys[i].Key)
	}
}

// StartBenchHistoryQueries starts the query load and the keys file if their
// switches are set. Only the first call starts them; later ones (replay
// restarts) are no-ops.
func (db *AccountsDb) StartBenchHistoryQueries(ctx context.Context) {
	v, queries := os.LookupEnv("MITHRIL_BENCH_HISTORY_QUERIES")
	keysFile := os.Getenv("MITHRIL_BENCH_KEYS_FILE")
	if !queries && keysFile == "" {
		return
	}
	if !benchQueryKeys.CompareAndSwap(nil, &queryKeySample{rng: rand.New(rand.NewSource(2))}) {
		return
	}
	if keysFile != "" {
		go writeBenchKeys(ctx, keysFile)
	}
	if queries {
		perSec := 0
		if v != "max" {
			perSec, _ = strconv.Atoi(v)
		}
		go db.runBenchHistoryQueries(ctx, perSec)
	}
}

func writeBenchKeys(ctx context.Context, path string) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		q := benchQueryKeys.Load()
		q.mu.Lock()
		var b strings.Builder
		for _, k := range q.keys {
			b.WriteString(solana.PublicKey(k).String())
			b.WriteByte('\n')
		}
		q.mu.Unlock()
		if b.Len() == 0 {
			continue
		}
		tmp := path + ".tmp"
		if err := os.WriteFile(tmp, []byte(b.String()), 0o644); err == nil {
			_ = os.Rename(tmp, path)
		}
	}
}

func (db *AccountsDb) runBenchHistoryQueries(ctx context.Context, perSec int) {
	rng := rand.New(rand.NewSource(1))
	var interval time.Duration
	if perSec > 0 {
		interval = time.Second / time.Duration(perSec)
	}
	var lat []time.Duration
	var fromHistory, fromStore, outside, failed int
	report := time.NewTicker(10 * time.Second)
	defer report.Stop()
	next := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-report.C:
			if len(lat) > 0 {
				sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
				q := func(p float64) time.Duration { return lat[min(len(lat)-1, int(p*float64(len(lat))))] }
				mlog.Log.Infof("bench history queries: n=%d history=%d store=%d outside=%d failed=%d p50=%s p99=%s max=%s",
					len(lat), fromHistory, fromStore, outside, failed, q(0.5), q(0.99), lat[len(lat)-1])
			}
			lat = lat[:0]
			fromHistory, fromStore, outside, failed = 0, 0, 0, 0
		default:
		}
		if interval > 0 {
			if d := time.Until(next); d > 0 {
				time.Sleep(d)
			}
			next = next.Add(interval)
		}
		hs := db.History()
		if hs == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		first, through, ok := hs.Range()
		key, okKey := benchQueryKeys.Load().pick(rng)
		if !ok || !okKey {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		slot := first + uint64(rng.Int63n(int64(through-first+1)))
		start := time.Now()
		_, _, err := hs.AccountAt(key, slot)
		switch {
		case err == nil:
			fromHistory++
		case errors.Is(err, history.ErrUnchanged):
			if _, err := db.GetAccountsBatch(ctx, through, []solana.PublicKey{solana.PublicKey(key)}); err != nil {
				failed++
			} else {
				fromStore++
			}
		case errors.Is(err, history.ErrBeforeHistory), errors.Is(err, history.ErrAfterHistory):
			outside++
		default:
			failed++
		}
		lat = append(lat, time.Since(start))
	}
}

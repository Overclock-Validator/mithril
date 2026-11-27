// Command rpc-load sends a fixed-rate mix of JSON-RPC requests to a node and
// reports latency per method. It is a benchmark tool: run it beside a replay
// (for example a backtest with the RPC server on localhost) to measure what
// RPC queries cost replay and how fast they are answered.
//
// Requests are scheduled open loop at -rate per second and sent by
// -workers goroutines; a request that cannot start on time is counted as
// late, not queued without bound. Account keys come from -keys, a file of
// base58 keys one per line (re-read as it changes), so queries hit accounts
// the chain really uses. Past-slot reads pick a slot between the first
// finalized slot seen and the current finalized slot.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type kind struct {
	name   string
	weight int
	build  func(g *gen) (string, string) // method, params
}

type gen struct {
	rng   *rand.Rand
	keys  []string
	first uint64
	last  uint64
}

func (g *gen) key() string { return g.keys[g.rng.Intn(len(g.keys))] }

func (g *gen) keyList(n int) string {
	ks := make([]string, n)
	for i := range ks {
		ks[i] = `"` + g.key() + `"`
	}
	return "[" + strings.Join(ks, ",") + "]"
}

func (g *gen) pastSlot() uint64 {
	if g.last <= g.first {
		return g.last
	}
	return g.first + uint64(g.rng.Int63n(int64(g.last-g.first+1)))
}

var kinds = map[string]*kind{
	"account": {name: "getAccountInfo", build: func(g *gen) (string, string) {
		return "getAccountInfo", fmt.Sprintf(`["%s",{"encoding":"base64"}]`, g.key())
	}},
	"account-processed": {name: "getAccountInfo/processed", build: func(g *gen) (string, string) {
		return "getAccountInfo", fmt.Sprintf(`["%s",{"encoding":"base64","commitment":"processed"}]`, g.key())
	}},
	"multiple": {name: "getMultipleAccounts", build: func(g *gen) (string, string) {
		return "getMultipleAccounts", fmt.Sprintf(`[%s,{"encoding":"base64"}]`, g.keyList(10))
	}},
	"balance-past": {name: "getBalance/slot", build: func(g *gen) (string, string) {
		return "getBalance", fmt.Sprintf(`["%s",{"slot":%d}]`, g.key(), g.pastSlot())
	}},
	"account-past": {name: "getAccountInfo/slot", build: func(g *gen) (string, string) {
		return "getAccountInfo", fmt.Sprintf(`["%s",{"encoding":"base64","slot":%d}]`, g.key(), g.pastSlot())
	}},
	"multiple-past": {name: "getMultipleAccounts/slot", build: func(g *gen) (string, string) {
		return "getMultipleAccounts", fmt.Sprintf(`[%s,{"encoding":"base64","slot":%d}]`, g.keyList(10), g.pastSlot())
	}},
	"slot":      {name: "getSlot", build: func(*gen) (string, string) { return "getSlot", `[]` }},
	"epoch":     {name: "getEpochInfo", build: func(*gen) (string, string) { return "getEpochInfo", `[]` }},
	"blockhash": {name: "getLatestBlockhash", build: func(*gen) (string, string) { return "getLatestBlockhash", `[]` }},
}

type stats struct {
	mu     sync.Mutex
	lat    map[string][]time.Duration
	errs   map[string]int
	sample map[string]string
}

func (s *stats) add(name string, d time.Duration, errMsg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if errMsg != "" {
		s.errs[name]++
		if _, ok := s.sample[name]; !ok {
			s.sample[name] = errMsg
		}
		return
	}
	s.lat[name] = append(s.lat[name], d)
}

type methodSummary struct {
	Method      string  `json:"method"`
	OK          int     `json:"ok"`
	Errors      int     `json:"errors"`
	P50ms       float64 `json:"p50_ms"`
	P90ms       float64 `json:"p90_ms"`
	P99ms       float64 `json:"p99_ms"`
	Maxms       float64 `json:"max_ms"`
	Meanms      float64 `json:"mean_ms"`
	ErrorSample string  `json:"error_sample,omitempty"`
}

func (s *stats) summary() []methodSummary {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := map[string]bool{}
	for n := range s.lat {
		names[n] = true
	}
	for n := range s.errs {
		names[n] = true
	}
	var out []methodSummary
	for n := range names {
		l := append([]time.Duration(nil), s.lat[n]...)
		sort.Slice(l, func(i, j int) bool { return l[i] < l[j] })
		m := methodSummary{Method: n, OK: len(l), Errors: s.errs[n], ErrorSample: s.sample[n]}
		if len(l) > 0 {
			q := func(p float64) float64 { return float64(l[min(len(l)-1, int(p*float64(len(l))))]) / 1e6 }
			var sum time.Duration
			for _, d := range l {
				sum += d
			}
			m.P50ms, m.P90ms, m.P99ms, m.Maxms = q(.5), q(.9), q(.99), float64(l[len(l)-1])/1e6
			m.Meanms = float64(sum) / float64(len(l)) / 1e6
		}
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Method < out[j].Method })
	return out
}

func main() {
	url := flag.String("url", "http://127.0.0.1:8899", "JSON-RPC endpoint")
	rate := flag.Float64("rate", 100, "requests per second")
	workers := flag.Int("workers", 4, "concurrent requests at most")
	mixFlag := flag.String("mix", "account:40,multiple:15,balance-past:20,multiple-past:10,account-processed:5,slot:4,epoch:3,blockhash:3", "kind:weight list")
	keysFile := flag.String("keys", "", "file of base58 account keys, one per line")
	out := flag.String("out", "", "write the summary JSON here on exit")
	waitFor := flag.Duration("wait", 10*time.Minute, "how long to wait for the endpoint and keys before giving up")
	flag.Parse()

	var mix []*kind
	total := 0
	for _, part := range strings.Split(*mixFlag, ",") {
		name, w, ok := strings.Cut(part, ":")
		k := kinds[name]
		var weight int
		if _, err := fmt.Sscan(w, &weight); !ok || err != nil || k == nil || weight <= 0 {
			fmt.Fprintf(os.Stderr, "bad mix entry %q\n", part)
			os.Exit(2)
		}
		k.weight = weight
		mix = append(mix, k)
		total += weight
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	client := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: *workers, MaxConnsPerHost: *workers}}
	call := func(method, params string) (json.RawMessage, string) {
		body := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"%s","params":%s}`, method, params)
		resp, err := client.Post(*url, "application/json", strings.NewReader(body))
		if err != nil {
			return nil, err.Error()
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err.Error()
		}
		var r struct {
			Result json.RawMessage `json:"result"`
			Error  *struct {
				Code    int    `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(b, &r); err != nil {
			return nil, "bad response: " + string(bytes.TrimSpace(b[:min(len(b), 200)]))
		}
		if r.Error != nil {
			return nil, fmt.Sprintf("%d %s", r.Error.Code, r.Error.Message)
		}
		return r.Result, ""
	}
	finalized := func() (uint64, bool) {
		res, e := call("getSlot", `[{"commitment":"finalized"}]`)
		var s uint64
		return s, e == "" && json.Unmarshal(res, &s) == nil
	}

	// Wait for the node to serve and for keys to exist.
	deadline := time.Now().Add(*waitFor)
	var keys []string
	var keysMod time.Time
	loadKeys := func() {
		st, err := os.Stat(*keysFile)
		if err != nil || !st.ModTime().After(keysMod) {
			return
		}
		f, err := os.Open(*keysFile)
		if err != nil {
			return
		}
		defer f.Close()
		var ks []string
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			if k := strings.TrimSpace(sc.Text()); k != "" {
				ks = append(ks, k)
			}
		}
		if len(ks) > 0 {
			keys, keysMod = ks, st.ModTime()
		}
	}
	var first uint64
	for {
		if ctx.Err() != nil || time.Now().After(deadline) {
			fmt.Fprintln(os.Stderr, "rpc-load: endpoint or keys never became ready")
			os.Exit(1)
		}
		loadKeys()
		if s, ok := finalized(); ok && len(keys) > 0 {
			first = s
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	fmt.Fprintf(os.Stderr, "rpc-load: start at finalized slot %d with %d keys, %.0f/s\n", first, len(keys), *rate)

	var cur atomic.Pointer[gen]
	cur.Store(&gen{keys: keys, first: first, last: first})
	go func() { // keep the slot range and keys current
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			loadKeys()
			if s, ok := finalized(); ok {
				cur.Store(&gen{keys: keys, first: first, last: s})
			}
		}
	}()

	st := &stats{lat: map[string][]time.Duration{}, errs: map[string]int{}, sample: map[string]string{}}
	var sent, late atomic.Int64
	slots := make(chan struct{}, *workers)
	var wg sync.WaitGroup
	interval := time.Duration(float64(time.Second) / *rate)
	next := time.Now()
	rngSeed := int64(1)
	started := time.Now()
loop:
	for {
		if d := time.Until(next); d > 0 {
			select {
			case <-ctx.Done():
				break loop
			case <-time.After(d):
			}
		}
		next = next.Add(interval)
		select {
		case slots <- struct{}{}:
		default:
			late.Add(1) // every worker busy: the open-loop slot is missed
			continue
		}
		rngSeed++
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			defer func() { <-slots }()
			g := *cur.Load()
			g.rng = rand.New(rand.NewSource(seed))
			r := g.rng.Intn(total)
			var k *kind
			for _, k = range mix {
				if r < k.weight {
					break
				}
				r -= k.weight
			}
			method, params := k.build(&g)
			t0 := time.Now()
			_, e := call(method, params)
			st.add(k.name, time.Since(t0), e)
			sent.Add(1)
		}(rngSeed)
	}
	wg.Wait()
	elapsed := time.Since(started)
	sum := struct {
		Rate     float64         `json:"rate"`
		Workers  int             `json:"workers"`
		Mix      string          `json:"mix"`
		Seconds  float64         `json:"seconds"`
		Sent     int64           `json:"sent"`
		Late     int64           `json:"late"`
		Achieved float64         `json:"achieved_per_second"`
		Methods  []methodSummary `json:"methods"`
	}{*rate, *workers, *mixFlag, elapsed.Seconds(), sent.Load(), late.Load(), float64(sent.Load()) / elapsed.Seconds(), st.summary()}
	b, _ := json.MarshalIndent(sum, "", "  ")
	if *out != "" {
		_ = os.WriteFile(*out, b, 0o644)
	}
	os.Stdout.Write(append(b, '\n'))
}

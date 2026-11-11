package node

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/config"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/stretchr/testify/require"
)

func TestOpenBacktestWorkdirRefusesForeignDirectories(t *testing.T) {
	_, err := openBacktestWorkdir("")
	require.ErrorContains(t, err, "--workdir is required")

	fresh := filepath.Join(t.TempDir(), "new")
	m, err := openBacktestWorkdir(fresh)
	require.NoError(t, err)
	require.Zero(t, m)
	require.DirExists(t, fresh)

	foreign := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(foreign, "mithril_state.json"), []byte("{}"), 0o644))
	_, err = openBacktestWorkdir(foreign)
	require.ErrorContains(t, err, "was not created by mithril backtest")

	ours := t.TempDir()
	want := backtestMarker{Capture: captureFingerprint{Name: "c.pcapng.zst", Size: 9, ModUnix: 1}, FromSlot: 5, SnapshotSlot: 4}
	require.NoError(t, writeBacktestMarker(ours, want))
	got, err := openBacktestWorkdir(ours)
	require.NoError(t, err)
	want.Version = 1
	require.Equal(t, want, got)
}

func TestBacktestSnapshotSlot(t *testing.T) {
	full := "/data/snapshot-467883265-J8Z7DwmGsfUqiNdek71axkgBZQkNRDEE8QtAbBPFwxPU.tar.zst"
	incr := "incremental-snapshot-467883265-467975829-DTf6eiMkKtYEDRqSG6KjEP2VwuRXQ3S3LMNueqjujsoL.tar.zst"
	slot, err := backtestSnapshotSlot(full, "")
	require.NoError(t, err)
	require.Equal(t, uint64(467883265), slot)
	slot, err = backtestSnapshotSlot(full, incr)
	require.NoError(t, err)
	require.Equal(t, uint64(467975829), slot)
	_, err = backtestSnapshotSlot(full, strings.Replace(incr, "467883265", "467883264", 1))
	require.ErrorContains(t, err, "builds on slot 467883264")
	_, err = backtestSnapshotSlot("accounts.tar", "")
	require.ErrorContains(t, err, "is not named snapshot-")
}

func TestPlanBacktestStart(t *testing.T) {
	const cp = 1000
	for _, tc := range []struct {
		name         string
		haveSnapshot bool
		builtAt      uint64
		view         storeView
		want         startPlan
		wantErr      string
	}{
		{"empty store builds from the snapshot", true, 0, storeView{}, startPlan{Bootstrap: "snapshot", FirstSlot: cp + 1}, ""},
		{"empty store without a snapshot", false, 0, storeView{}, startPlan{}, "has no state file"},
		{"store at the checkpoint", false, 0, storeView{HasState: true, StateSlot: cp}, startPlan{Bootstrap: "accountsdb", FirstSlot: cp + 1}, ""},
		{"store behind the capture", false, 0, storeView{HasState: true, StateSlot: cp - 1}, startPlan{}, "snapshot too old for capture"},
		{"store past a retained checkpoint rewinds", false, 0, storeView{HasState: true, StateSlot: 2000, Boundaries: []uint64{900, cp, 1128}},
			startPlan{Bootstrap: "accountsdb", RewindTo: cp, FirstSlot: cp + 1}, ""},
		{"snapshot-built store rewinds to the first boundary after the snapshot", true, cp, storeView{HasState: true, StateSlot: 2000, Boundaries: []uint64{1128, 1256}},
			startPlan{Bootstrap: "accountsdb", RewindTo: 1128, FirstSlot: 1129}, ""},
		{"foreign store past an unretained checkpoint", false, 0, storeView{HasState: true, StateSlot: 2000, Boundaries: []uint64{1128}}, startPlan{}, "not a retained fold boundary (retained: 1128)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := planBacktestStart(cp, tc.haveSnapshot, tc.builtAt, tc.view)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			got.Note = ""
			require.Equal(t, tc.want, got)
		})
	}
}

func TestRestoreBacktestSpoolKeepsTheCorpus(t *testing.T) {
	corpus := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(corpus, "shreds"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(corpus, "shreds", turbine.SpoolFileName(7)), []byte("slot7"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(corpus, turbine.SpoolJournalName), []byte("idx"), 0o644))
	spool := filepath.Join(t.TempDir(), "catchup-spool")
	require.NoError(t, os.MkdirAll(spool, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(spool, turbine.SpoolFileName(3)), []byte("stale"), 0o644))

	n, err := restoreBacktestSpool(corpus, spool)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.NoFileExists(t, filepath.Join(spool, turbine.SpoolFileName(3)), "an earlier run's spool is cleared")
	got, err := os.ReadFile(filepath.Join(spool, turbine.SpoolFileName(7)))
	require.NoError(t, err)
	require.Equal(t, "slot7", string(got))

	// The node appends to its completion index; the corpus copy must not change.
	f, err := os.OpenFile(filepath.Join(spool, turbine.SpoolJournalName), os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.WriteString("more")
	require.NoError(t, err)
	require.NoError(t, f.Close())
	idx, err := os.ReadFile(filepath.Join(corpus, turbine.SpoolJournalName))
	require.NoError(t, err)
	require.Equal(t, "idx", string(idx))
}

func TestGenesisStubAnswersOnlyGenesisHash(t *testing.T) {
	stub, err := startGenesisStub("5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d")
	require.NoError(t, err)
	defer stub.Close()
	require.True(t, strings.HasPrefix(stub.URL, "http://127.0.0.1:"))

	post := func(body string) []byte {
		resp, err := http.Post(stub.URL, "application/json", bytes.NewBufferString(body))
		require.NoError(t, err)
		defer resp.Body.Close()
		out, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		return out
	}
	var single map[string]any
	require.NoError(t, json.Unmarshal(post(`{"jsonrpc":"2.0","id":1,"method":"getGenesisHash"}`), &single))
	require.Equal(t, "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d", single["result"])

	var batch []map[string]any
	require.NoError(t, json.Unmarshal(post(`[{"jsonrpc":"2.0","id":1,"method":"getSlot"},{"jsonrpc":"2.0","id":2,"method":"getGenesisHash"}]`), &batch))
	require.Len(t, batch, 2)
	require.NotNil(t, batch[0]["error"])
	require.Equal(t, "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d", batch[1]["result"])
	require.Equal(t, int64(1), stub.Rejected.Load())
}

func TestSummarizeBacktest(t *testing.T) {
	ms := int64(1_000_000)
	rows := []timingRow{
		{Slot: 10, CU: 1e6, Transactions: 10, SlotReplay: 100 * ms, TxLoop: 90 * ms},
		{Slot: 11, CU: 2e6, Transactions: 20, SlotReplay: 10 * ms, TxLoop: 8 * ms, SinceLastRow: 12 * ms},
		{Slot: 13, CU: 4e6, Transactions: 30, SlotReplay: 20 * ms, TxLoop: 16 * ms, SinceLastRow: 22 * ms},
		{Slot: 14, CU: 6e6, Transactions: 40, SlotReplay: 30 * ms, TxLoop: 24 * ms, SinceLastRow: 33 * ms},
	}
	s := summarizeBacktest(rows, 1, 0)
	require.Equal(t, 4, s.Replayed)
	require.Equal(t, 1, s.Warmup)
	require.Equal(t, 3, s.Measured)
	require.Equal(t, uint64(11), s.FirstSlot)
	require.Equal(t, uint64(14), s.LastSlot)
	require.InDelta(t, 4e6, s.AvgCU, 1)
	require.InDelta(t, 20.0, s.SlotReplay.MeanMs, 1e-9)
	require.InDelta(t, 20.0, s.SlotReplay.P50Ms, 1e-9)
	require.InDelta(t, 30.0, s.SlotReplay.P99Ms, 1e-9)
	// Total CU over total ns, not the mean of per-block ratios.
	require.InDelta(t, 12e6/60e6, s.SlotReplay.CUPerNs, 1e-12)
	require.InDelta(t, 12e6/48e6, s.TxLoop.CUPerNs, 1e-12)
	// Firedancer-comparable figures cover every replayed block.
	require.InDelta(t, 0.167, s.SlotExecSeconds, 1e-9)
	require.InDelta(t, 0.167/4, s.SecPerSlot, 1e-9)
	require.InDelta(t, 100/0.167, s.TPS, 1e-6)

	s = summarizeBacktest(rows, 0, 13)
	require.Equal(t, 2, s.Measured)
	require.Equal(t, uint64(13), s.FirstSlot)

	var out bytes.Buffer
	s.write(&out, [][2]string{{"txpar", "8"}}, 1.5)
	require.Contains(t, out.String(), "FD_COMPARABLE slots=4 slot_exec_s=0.167")
	require.Contains(t, out.String(), "SlotReplay")
}

func TestReadTimingRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replay_timings.jsonl")
	line := `{"Slot":5,"ComputeUnitsConsumed":7,"Transactions":3,"SlotReplay":{"SumNanoseconds":11},"TxLoop":{"SumNanoseconds":9},"SinceLastRow":{"SumNanoseconds":13},"Other":1}`
	require.NoError(t, os.WriteFile(path, []byte(line+"\n\n"+line+"\n"), 0o644))
	rows, err := readTimingRows(path)
	require.NoError(t, err)
	require.Equal(t, []timingRow{{Slot: 5, CU: 7, Transactions: 3, SlotReplay: 11, TxLoop: 9, SinceLastRow: 13}, {Slot: 5, CU: 7, Transactions: 3, SlotReplay: 11, TxLoop: 9, SinceLastRow: 13}}, rows)
}

func TestBacktestConfigStaysInsideTheWorkdir(t *testing.T) {
	mainnet, err := config.ResolveCluster("mainnet", "")
	require.NoError(t, err)
	cfg := backtestConfig(mainnet, "http://127.0.0.1:4000", "/w/accounts", "/w", 50093, 8, 64)
	for _, want := range []string{
		`accounts = "/w/accounts"`, `logs = "/w/logs"`, `shredstore = "/w/shred"`, `snapshots = "/w/snapshots"`,
		`rpc = ["http://127.0.0.1:4000"]`, "offline = true", "shred_version = 50093", "[storage.compact]\nenabled = false",
		"[verifier]\nenabled = false", "[rpc]\nport = 0", "txpar = 8", "rewind_horizon_batches = 64",
	} {
		require.Contains(t, cfg, want)
	}
	require.NotContains(t, cfg, "/mnt/")
	require.NotContains(t, cfg, "genesis_hash", "a built-in cluster carries its own genesis hash")

	custom, err := config.ResolveCluster("private", "11111111111111111111111111111111")
	require.NoError(t, err)
	require.Contains(t, backtestConfig(custom, "u", "/s", "/w", 1, 8, 64), `genesis_hash = "11111111111111111111111111111111"`)
}

package node

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/config"
	"github.com/Overclock-Validator/mithril/pkg/state"
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

func TestPrepareBacktestShredStoreBindsBeforeRestoring(t *testing.T) {
	corpus := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(corpus, "shreds"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(corpus, "shreds", turbine.SpoolFileName(7)), []byte("slot7"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(corpus, turbine.SpoolJournalName), []byte("idx"), 0o644))
	mainnet, err := config.ResolveCluster("mainnet", "")
	require.NoError(t, err)
	shredDir := filepath.Join(t.TempDir(), "shred")

	for run := 0; run < 2; run++ { // a second run finds its own binding and spool
		n, err := prepareBacktestShredStore(corpus, shredDir, mainnet)
		require.NoError(t, err)
		require.Equal(t, 1, n)
		binding, err := state.LoadLedgerChainBinding(shredDir)
		require.NoError(t, err)
		require.NotNil(t, binding)
		require.Equal(t, mainnet.GenesisHash, binding.GenesisHash)
		require.FileExists(t, filepath.Join(shredDir, "catchup-spool", turbine.SpoolFileName(7)))
	}

	// The workdir's shred store holds nothing but the restored capture, so a
	// capture from another cluster rebinds it; the AccountsDB's own genesis
	// binding is what refuses state from the wrong chain.
	devnet, err := config.ResolveCluster("devnet", "")
	require.NoError(t, err)
	_, err = prepareBacktestShredStore(corpus, shredDir, devnet)
	require.NoError(t, err)
	binding, err := state.LoadLedgerChainBinding(shredDir)
	require.NoError(t, err)
	require.Equal(t, devnet.GenesisHash, binding.GenesisHash)
}

func TestSummarizeBacktest(t *testing.T) {
	ms := int64(1_000_000)
	boot := time.Unix(1000, 0)
	clock := backtestClock{
		Boot:        boot,
		ImportDone:  boot.Add(2 * time.Second),
		StateReady:  boot.Add(5 * time.Second),
		ReplayReady: boot.Add(6 * time.Second),
	}
	// Four blocks after slot 9; the first starts 50 ms after replay is ready.
	at := func(d time.Duration) int64 { return clock.ReplayReady.Add(d).UnixNano() }
	rows := []timingRow{
		{Slot: 10, CU: 1e6, Transactions: 10, SlotReplay: 100 * ms, TxLoop: 90 * ms, Completed: at(150 * time.Millisecond)},
		{Slot: 11, CU: 2e6, Transactions: 20, SlotReplay: 10 * ms, TxLoop: 8 * ms, Completed: at(170 * time.Millisecond)},
		{Slot: 13, CU: 4e6, Transactions: 30, SlotReplay: 20 * ms, TxLoop: 16 * ms, Completed: at(200 * time.Millisecond)},
		{Slot: 14, CU: 6e6, Transactions: 40, SlotReplay: 30 * ms, TxLoop: 24 * ms, Completed: at(250 * time.Millisecond)},
	}
	s := summarizeBacktest(rows, 1, 0, 9, clock)
	require.Equal(t, 4, s.Replayed)
	require.Equal(t, 1, s.Warmup)
	require.Equal(t, 3, s.Measured)
	require.Equal(t, uint64(11), s.FirstSlot)
	require.Equal(t, uint64(14), s.LastSlot)
	require.InDelta(t, 4e6, s.AvgCU, 1)
	require.InDelta(t, 20.0, s.SlotReplay.MeanMs, 1e-9)
	require.InDelta(t, 30.0, s.SlotReplay.P99Ms, 1e-9)
	// Total CU over total ns, not the mean of per-block ratios.
	require.InDelta(t, 12e6/60e6, s.SlotReplay.CUPerNs, 1e-12)
	require.InDelta(t, 12e6/48e6, s.TxLoop.CUPerNs, 1e-12)

	fd := s.FD
	require.True(t, fd.Complete)
	require.Equal(t, uint64(9), fd.SnapshotSlot)
	require.Equal(t, uint64(14), fd.EndSlot)
	require.Equal(t, 4, fd.Slots, "every replayed block, no warm-up excluded, never the start slot")
	require.Equal(t, uint64(100), fd.Txns)
	require.InDelta(t, 2.0, fd.ImportS, 1e-9)
	require.InDelta(t, 3.0, fd.SnapBytesS, 1e-9)
	require.InDelta(t, 1.0, fd.ReplayInitS, 1e-9)
	require.InDelta(t, 4.0, fd.LoadTotalS, 1e-9)
	require.InDelta(t, 0.250, fd.ReplayS, 1e-9, "replay ready to the last block's completion")
	require.InDelta(t, 0.200, fd.SlotExecS, 1e-9, "first block's start to the last block's completion")
	require.InDelta(t, 6.250, fd.TotalS, 1e-9)

	// Internal consistency, as Firedancer computes the derived fields.
	require.InDelta(t, fd.ReplayS, fd.SecPerSlot*float64(fd.Slots), 1e-9)
	require.InDelta(t, float64(fd.Txns), fd.TPS*fd.ReplayS, 1e-6)
	require.InDelta(t, fd.LoadTotalS, fd.SnapBytesS+fd.ReplayInitS, 1e-9)
	require.InDelta(t, fd.TotalS, fd.ImportS+fd.LoadTotalS+fd.ReplayS, 1e-9)
	require.LessOrEqual(t, fd.SlotExecS, fd.ReplayS)

	s = summarizeBacktest(rows, 0, 13, 9, clock)
	require.Equal(t, 2, s.Measured)
	require.Equal(t, uint64(13), s.FirstSlot)
	require.Equal(t, 4, s.FD.Slots, "--measure-from does not touch the Firedancer line")

	var out bytes.Buffer
	s.write(&out, [][2]string{{"txpar", "8"}}, 7.0)
	require.Contains(t, out.String(), "BACKTEST_SUMMARY snapshot_slot=9 end_slot=14 slots=4 txns=100 snap_bytes_s=3.000 replay_init_s=1.000 load_total_s=4.000 replay_s=0.250 slot_exec_s=0.200 sec_per_slot=0.062500 tps=400 total_s=6.250")
	require.Contains(t, out.String(), "MITHRIL_EXTRA import_s=2.000 shutdown_s=0.750")
}

func TestSummarizeBacktestWithoutTimestamps(t *testing.T) {
	rows := []timingRow{{Slot: 10, CU: 1, Transactions: 1, SlotReplay: 5, TxLoop: 4}}
	s := summarizeBacktest(rows, 0, 0, 9, backtestClock{})
	require.False(t, s.FD.Complete)
	var out bytes.Buffer
	s.write(&out, nil, 1)
	require.Contains(t, out.String(), "BACKTEST_SUMMARY unavailable")
	require.NotContains(t, out.String(), "replay_s=")
}

func TestReadTimingRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replay_timings.jsonl")
	line := `{"Slot":5,"ComputeUnitsConsumed":7,"Transactions":3,"SlotReplay":{"SumNanoseconds":11},"TxLoop":{"SumNanoseconds":9},"SinceLastRow":{"SumNanoseconds":13},"CompletedUnixNano":17,"Other":1}`
	require.NoError(t, os.WriteFile(path, []byte(line+"\n\n"+line+"\n"), 0o644))
	rows, err := readTimingRows(path)
	require.NoError(t, err)
	want := timingRow{Slot: 5, CU: 7, Transactions: 3, SlotReplay: 11, TxLoop: 9, SinceLastRow: 13, Completed: 17}
	require.Equal(t, []timingRow{want, want}, rows)
}

func TestBacktestConfigStaysInsideTheWorkdir(t *testing.T) {
	mainnet, err := config.ResolveCluster("mainnet", "")
	require.NoError(t, err)
	cfg := backtestConfig(mainnet, "/w/accounts", "/w", 50093, 8, 64)
	for _, want := range []string{
		`accounts = "/w/accounts"`, `logs = "/w/logs"`, `shredstore = "/w/shred"`, `snapshots = "/w/snapshots"`,
		"offline = true", "shred_version = 50093", "[storage.compact]\nenabled = false",
		"[verifier]\nenabled = false", "[rpc]\nport = 0", "txpar = 8", "rewind_horizon_batches = 64",
	} {
		require.Contains(t, cfg, want)
	}
	require.NotContains(t, cfg, "/mnt/")
	require.NotContains(t, cfg, "rpc =", "an offline node needs no RPC endpoint")
	require.NotContains(t, cfg, "genesis_hash", "a built-in cluster carries its own genesis hash")

	custom, err := config.ResolveCluster("private", "11111111111111111111111111111111")
	require.NoError(t, err)
	require.Contains(t, backtestConfig(custom, "/s", "/w", 1, 8, 64), `genesis_hash = "11111111111111111111111111111111"`)
}

func TestBacktestHead(t *testing.T) {
	slots := []uint64{101, 102, 104, 105} // 103 was skipped
	head, err := backtestHead(slots, 101, 0)
	require.NoError(t, err)
	require.Equal(t, uint64(105), head, "default: the capture's last block")
	head, err = backtestHead(slots, 101, 104)
	require.NoError(t, err)
	require.Equal(t, uint64(104), head)
	head, err = backtestHead(slots, 101, 103)
	require.NoError(t, err)
	require.Equal(t, uint64(102), head, "a skipped slot stops at the block before it")
	_, err = backtestHead(slots, 101, 106)
	require.ErrorContains(t, err, "past the capture's last block")
	_, err = backtestHead(slots, 104, 102)
	require.ErrorContains(t, err, "before the first block this run replays")
	_, err = backtestHead(nil, 1, 0)
	require.Error(t, err)
}

func TestSummaryPrintsFiredancerLineWithoutMeasuredBlocks(t *testing.T) {
	boot := time.Unix(100, 0)
	clock := backtestClock{Boot: boot, ImportDone: boot.Add(time.Second), StateReady: boot.Add(2 * time.Second), ReplayReady: boot.Add(3 * time.Second)}
	rows := []timingRow{{Slot: 10, CU: 5, Transactions: 2, SlotReplay: 1e6, TxLoop: 1e6, Completed: boot.Add(4 * time.Second).UnixNano()}}
	s := summarizeBacktest(rows, 30, 0, 9, clock)
	require.Zero(t, s.Measured)
	var out bytes.Buffer
	s.write(&out, nil, 5)
	require.Contains(t, out.String(), "no measured blocks")
	require.Contains(t, out.String(), "BACKTEST_SUMMARY snapshot_slot=9 end_slot=10 slots=1 txns=2")
}

func TestSummaryCountsEachSlotOnce(t *testing.T) {
	// A fork switch replays slots 11 and 12 again; only the standing rows count.
	rows := []timingRow{
		{Slot: 10, Transactions: 1, SlotReplay: 1, TxLoop: 1, Completed: 1},
		{Slot: 11, Transactions: 100, SlotReplay: 1, TxLoop: 1, Completed: 2},
		{Slot: 12, Transactions: 100, SlotReplay: 1, TxLoop: 1, Completed: 3},
		{Slot: 11, Transactions: 1, SlotReplay: 1, TxLoop: 1, Completed: 4},
		{Slot: 12, Transactions: 1, SlotReplay: 1, TxLoop: 1, Completed: 5},
	}
	got := lastRowPerSlot(rows)
	require.Equal(t, []uint64{10, 11, 12}, []uint64{got[0].Slot, got[1].Slot, got[2].Slot})
	require.Equal(t, int64(4), got[1].Completed)
	s := summarizeBacktest(rows, 0, 0, 9, backtestClock{})
	require.Equal(t, 3, s.Replayed)
	require.Equal(t, uint64(3), s.FD.Txns)
}

func TestFailedFirstImportLeavesAnAcceptedWorkdir(t *testing.T) {
	resetBacktestOptions(t)
	workdir := filepath.Join(t.TempDir(), "w")
	marker, err := openBacktestWorkdir(workdir)
	require.NoError(t, err)
	require.NoError(t, writeBacktestMarker(workdir, marker)) // what runBacktest does first
	bt.capture = filepath.Join(t.TempDir(), "broken.pcapng")
	require.NoError(t, os.WriteFile(bt.capture, []byte("not a capture"), 0o644))
	fp, err := fingerprintCapture(bt.capture)
	require.NoError(t, err)

	marker.Version = 1
	_, err = backtestCorpus(&Backtest, workdir, &marker, fp, 0)
	require.Error(t, err)

	again, err := openBacktestWorkdir(workdir)
	require.NoError(t, err, "a failed import must not lock the workdir out")
	require.Equal(t, fp, again.Capture)
}

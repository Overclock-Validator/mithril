package node

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/config"
	"github.com/Overclock-Validator/mithril/pkg/shredcap"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
	"github.com/spf13/cobra"
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

// backtestStore writes a store directory holding a state file at stateSlot and
// one fold manifest per boundary, with the given batch sequence numbers.
func backtestStore(t *testing.T, stateSlot uint64, folds map[uint64]uint64) string {
	t.Helper()
	store := t.TempDir()
	st := state.NewReadyState(stateSlot, 0, "", "", 0, 0)
	st.LastRootedSlot = stateSlot
	require.NoError(t, st.Save(store))
	accts := filepath.Join(store, "accounts")
	require.NoError(t, os.MkdirAll(accts, 0o755))
	for seq, through := range folds {
		require.NoError(t, accountsdb.WriteSegmentManifest(accts, &accountsdb.SegmentManifest{
			Kind: accountsdb.ManifestKindFold, BatchSeq: seq, ThroughSlot: through, FileId: 100 + seq,
		}))
	}
	return store
}

// A killed run leaves the state file (written on graceful shutdown) behind the
// fold manifests. Startup recovery advances the store to the manifests'
// frontier, so the plan must start from there: a state file that still names
// the checkpoint does not mean the store is at the checkpoint.
func TestBacktestStoreViewFollowsFoldManifestsPastAStaleStateFile(t *testing.T) {
	const cp = 1000

	// The checkpoint is a retained boundary: rewind to it.
	view, err := backtestStoreView(backtestStore(t, cp, map[uint64]uint64{7: cp, 8: 1128, 9: 1256}))
	require.NoError(t, err)
	require.True(t, view.HasState)
	require.Equal(t, uint64(1256), view.StateSlot, "the store is durable through its last fold, not the state file's slot")
	require.Equal(t, []uint64{cp, 1128, 1256}, view.Boundaries)
	plan, err := planBacktestStart(cp, false, 0, view)
	require.NoError(t, err)
	require.Equal(t, uint64(cp), plan.RewindTo, "replay must not start at slot 1257")
	require.Equal(t, uint64(cp+1), plan.FirstSlot)

	// A store built from the snapshot at the checkpoint has no boundary there:
	// the workdir's own store rewinds to the first boundary after it, and a
	// store given with --accounts is refused.
	view, err = backtestStoreView(backtestStore(t, cp, map[uint64]uint64{1: 1128, 2: 1256}))
	require.NoError(t, err)
	require.Equal(t, uint64(1256), view.StateSlot)
	plan, err = planBacktestStart(cp, true, cp, view)
	require.NoError(t, err)
	require.Equal(t, uint64(1128), plan.RewindTo)
	require.Equal(t, uint64(1129), plan.FirstSlot)
	_, err = planBacktestStart(cp, false, 0, view)
	require.ErrorContains(t, err, "is at slot 1256 and cannot return to the capture's checkpoint 1000")

	// A state file at or past the manifests is the position, as before.
	view, err = backtestStoreView(backtestStore(t, 1256, map[uint64]uint64{7: cp, 8: 1128, 9: 1256}))
	require.NoError(t, err)
	require.Equal(t, uint64(1256), view.StateSlot)
	view, err = backtestStoreView(backtestStore(t, cp, nil))
	require.NoError(t, err)
	require.Equal(t, uint64(cp), view.StateSlot)
	plan, err = planBacktestStart(cp, false, 0, view)
	require.NoError(t, err)
	require.Equal(t, startPlan{Bootstrap: "accountsdb", FirstSlot: cp + 1}, plan)

	// Recovery stops at the first gap in the batch sequence and deletes what
	// lies above it, so a manifest above a gap is not a position or a boundary.
	view, err = backtestStoreView(backtestStore(t, cp, map[uint64]uint64{7: cp, 8: 1128, 10: 1384}))
	require.NoError(t, err)
	require.Equal(t, uint64(1128), view.StateSlot)
	require.Equal(t, []uint64{cp, 1128}, view.Boundaries)
}

func TestCheckBacktestCoverage(t *testing.T) {
	chain := []uint64{1001, 1002, 1004, 1005, 1006} // 1003 was skipped
	rowsFor := func(slots ...uint64) []timingRow {
		rows := make([]timingRow, len(slots))
		for i, s := range slots {
			rows[i] = timingRow{Slot: s, Completed: int64(i + 1)}
		}
		return rows
	}
	require.NoError(t, checkBacktestCoverage(chain, 1001, 1006, rowsFor(1001, 1002, 1004, 1005, 1006)))
	require.NoError(t, checkBacktestCoverage(chain, 1004, 1005, rowsFor(1004, 1005)), "a later planned start and --end-slot")
	require.NoError(t, checkBacktestCoverage(chain, 1001, 1006, rowsFor(1001, 1002, 1004, 1002, 1004, 1005, 1006)),
		"a slot replayed again after a fork switch counts once")

	// The store was ahead of the plan: the run ends on the last block but
	// replayed fewer blocks than planned.
	err := checkBacktestCoverage(chain, 1001, 1006, rowsFor(1005, 1006))
	require.ErrorContains(t, err, "first replayed block is slot 1005")
	require.ErrorContains(t, err, "planned 5 blocks, slots 1001..1006")
	require.ErrorContains(t, err, "replayed 2 blocks, slots 1005..1006")

	err = checkBacktestCoverage(chain, 1001, 1006, rowsFor(1001, 1002, 1005, 1006))
	require.ErrorContains(t, err, "slot 1004 was not replayed")
	err = checkBacktestCoverage(chain, 1001, 1006, rowsFor(1001, 1002, 1004, 1005))
	require.ErrorContains(t, err, "slot 1006 was not replayed")
	err = checkBacktestCoverage(chain, 1001, 1006, nil)
	require.ErrorContains(t, err, "replayed no blocks")
	err = checkBacktestCoverage(chain, 1002, 1006, rowsFor(1001, 1002, 1004, 1005, 1006))
	require.ErrorContains(t, err, "slot 1001 was replayed but is not one of the planned blocks")
	err = checkBacktestCoverage(chain, 1001, 1006, rowsFor(1001, 1002, 1003, 1004, 1005, 1006))
	require.ErrorContains(t, err, "slot 1003 was replayed but is not one of the planned blocks")
	err = checkBacktestCoverage(chain, 1001, 1005, rowsFor(1001, 1002, 1004, 1005, 1006))
	require.ErrorContains(t, err, "slot 1006 was replayed but is not one of the planned blocks")
	err = checkBacktestCoverage(chain, 1001, 1006, rowsFor(1001, 1004, 1002, 1005, 1006))
	require.ErrorContains(t, err, "slot 1002 was replayed out of chain order")
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

// oneSlotCapture is a valid capture of slot 100 whose bank hash record is hash.
func oneSlotCapture(t *testing.T, hash [32]byte) []byte {
	t.Helper()
	leader, err := solana.NewRandomPrivateKey()
	require.NoError(t, err)
	gen := turbine.ShredGenerator{Slot: 100, ParentSlot: 99, Version: 7}
	packets, _, _, _, err := gen.MakeShredsFromData(leader, nil, true, solana.Hash{}, 0, 0)
	require.NoError(t, err)
	var b bytes.Buffer
	w, err := shredcap.NewWriter(&b, "test")
	require.NoError(t, err)
	for _, p := range packets {
		s, err := turbine.ParseShred(p)
		require.NoError(t, err)
		if s.Type == turbine.ShredTypeData {
			require.NoError(t, w.WriteShred(p))
		}
	}
	require.NoError(t, w.WriteBankHash(100, hash, uint64(len(packets))))
	require.NoError(t, w.WriteRootSlot(100))
	require.NoError(t, w.Flush())
	return b.Bytes()
}

// Different captures that share name, size and mtime must not share an
// import.
func TestBacktestCorpusFollowsCaptureContents(t *testing.T) {
	resetBacktestOptions(t)
	a, b := [32]byte{1}, [32]byte{2}
	stamp := time.Unix(1700000000, 0)
	write := func(path string, data []byte) {
		require.NoError(t, os.WriteFile(path, data, 0o600))
		require.NoError(t, os.Chtimes(path, stamp, stamp))
	}
	importFrom := func(workdir string, marker *backtestMarker, path string) string {
		bt.capture = path
		fp, err := fingerprintCapture(path)
		require.NoError(t, err)
		man, err := backtestCorpus(&Backtest, workdir, marker, fp, 0)
		require.NoError(t, err)
		return man.BankHashes["100"]
	}

	// Same basename in two directories, equal size and mtime.
	pathA := filepath.Join(t.TempDir(), "shredcap.pcapng")
	pathB := filepath.Join(t.TempDir(), "shredcap.pcapng")
	dataA, dataB := oneSlotCapture(t, a), oneSlotCapture(t, b)
	require.Equal(t, len(dataA), len(dataB))
	write(pathA, dataA)
	write(pathB, dataB)
	workdir := t.TempDir()
	marker := backtestMarker{Version: 1}
	require.Equal(t, solana.Hash(a).String(), importFrom(workdir, &marker, pathA))
	require.Equal(t, solana.Hash(b).String(), importFrom(workdir, &marker, pathB), "the selected capture is the one imported")

	// The same path replaced in place by an equal-size capture within the same second.
	write(pathA, dataB)
	marker = backtestMarker{Version: 1}
	workdir = t.TempDir()
	write(pathA, dataA)
	require.Equal(t, solana.Hash(a).String(), importFrom(workdir, &marker, pathA))
	write(pathA, dataB)
	require.Equal(t, solana.Hash(b).String(), importFrom(workdir, &marker, pathA))

	// Unchanged contents reuse the import even after the file is touched.
	stamp = stamp.Add(time.Hour)
	write(pathA, dataB)
	require.Equal(t, solana.Hash(b).String(), importFrom(workdir, &marker, pathA))
}

// captureSlot describes one slot of a test capture: complete, rooted, with a
// bank hash record unless noBankHash is set.
type captureSlot struct {
	slot, parent uint64
	noBankHash   bool
}

func captureOf(t *testing.T, slots ...captureSlot) []byte {
	t.Helper()
	leader, err := solana.NewRandomPrivateKey()
	require.NoError(t, err)
	var b bytes.Buffer
	w, err := shredcap.NewWriter(&b, "test")
	require.NoError(t, err)
	for _, cs := range slots {
		gen := turbine.ShredGenerator{Slot: cs.slot, ParentSlot: cs.parent, Version: 7}
		packets, _, _, _, err := gen.MakeShredsFromData(leader, nil, true, solana.Hash{}, 0, 0)
		require.NoError(t, err)
		data := uint64(0)
		for _, p := range packets {
			s, err := turbine.ParseShred(p)
			require.NoError(t, err)
			if s.Type == turbine.ShredTypeData {
				require.NoError(t, w.WriteShred(p))
				data++
			}
		}
		if !cs.noBankHash {
			require.NoError(t, w.WriteBankHash(cs.slot, [32]byte{byte(cs.slot)}, data))
		}
		require.NoError(t, w.WriteRootSlot(cs.slot))
	}
	require.NoError(t, w.Flush())
	return b.Bytes()
}

// backtestImport runs backtestCorpus the way one `mithril backtest` invocation
// does: the marker comes from the work directory, not from an earlier call.
func backtestImport(t *testing.T, workdir, capture string) (*shredcap.Manifest, string, error) {
	t.Helper()
	marker, err := openBacktestWorkdir(workdir)
	require.NoError(t, err)
	if marker.Version == 0 {
		require.NoError(t, writeBacktestMarker(workdir, marker))
		marker.Version = 1
	}
	bt.capture = capture
	fp, err := fingerprintCapture(capture)
	require.NoError(t, err)
	var out bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetOut(&out)
	man, err := backtestCorpus(cmd, workdir, &marker, fp, 0)
	return man, out.String(), err
}

// A capture that fails the import checks must fail them on every invocation:
// the rejected import must not be picked up as a reusable one.
func TestRejectedCaptureIsRejectedAgain(t *testing.T) {
	for _, tc := range []struct {
		name    string
		capture []captureSlot
		wantErr string
	}{
		{"missing bank hash", []captureSlot{{slot: 100, parent: 99}, {slot: 101, parent: 100, noBankHash: true}},
			"no bank hash available for slot 101"},
		{"broken parent chain", []captureSlot{{slot: 100, parent: 99}, {slot: 102, parent: 101}},
			"the capture's chain breaks at slot 102"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resetBacktestOptions(t)
			workdir := filepath.Join(t.TempDir(), "w")
			capture := filepath.Join(t.TempDir(), "shredcap.pcapng")
			require.NoError(t, os.WriteFile(capture, captureOf(t, tc.capture...), 0o600))
			for attempt := 1; attempt <= 3; attempt++ {
				man, out, err := backtestImport(t, workdir, capture)
				require.ErrorContains(t, err, tc.wantErr, "invocation %d", attempt)
				require.Nil(t, man)
				require.Contains(t, out, "importing", "invocation %d imports and checks the capture again", attempt)
				require.NoDirExists(t, filepath.Join(workdir, "corpus"), "a rejected capture leaves no import behind")
			}
		})
	}
}

// An import is reused only when the marker records that it was checked, and
// only while its manifest still is the checked import with every bank hash.
func TestBacktestCorpusReusesOnlyACheckedImport(t *testing.T) {
	resetBacktestOptions(t)
	workdir := filepath.Join(t.TempDir(), "w")
	capture := filepath.Join(t.TempDir(), "shredcap.pcapng")
	require.NoError(t, os.WriteFile(capture, captureOf(t, captureSlot{slot: 100, parent: 99}, captureSlot{slot: 101, parent: 100}), 0o600))
	manifestPath := filepath.Join(workdir, "corpus", "manifest.json")
	importAgain := func(why string) {
		t.Helper()
		man, out, err := backtestImport(t, workdir, capture)
		require.NoError(t, err)
		require.Equal(t, []uint64{100, 101}, man.Slots)
		require.Contains(t, out, "importing", why)
	}
	reuse := func(why string) {
		t.Helper()
		man, out, err := backtestImport(t, workdir, capture)
		require.NoError(t, err)
		require.Equal(t, []uint64{100, 101}, man.Slots)
		require.Empty(t, out, why)
	}

	importAgain("the first run imports")
	marker, err := openBacktestWorkdir(workdir)
	require.NoError(t, err)
	require.Equal(t, validatedImport{Validated: true, Checkpoint: 99, Head: 101, Blocks: 2}, marker.Import)
	reuse("a checked import is reused")

	// A marker from before the flag existed: same capture, manifest readable.
	old := marker
	old.Import = validatedImport{}
	require.NoError(t, writeBacktestMarker(workdir, old))
	importAgain("an import that was never recorded as checked is not trusted")
	reuse("and is reused once it has been checked")

	// The cached manifest no longer has a bank hash for every chain slot.
	man, err := shredcap.ReadManifest(filepath.Join(workdir, "corpus"))
	require.NoError(t, err)
	delete(man.BankHashes, "101")
	data, err := json.Marshal(man)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(manifestPath, data, 0o644))
	importAgain("a manifest missing a bank hash is not reused")

	// The cached manifest is not the import that was checked.
	man.BankHashes["101"] = man.BankHashes["100"]
	man.Slots, man.Blocks, man.Head = []uint64{100}, 1, 100
	data, err = json.Marshal(man)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(manifestPath, data, 0o644))
	importAgain("a different manifest is not reused")
	reuse("the fresh import is")
}

func TestCheckReusedManifest(t *testing.T) {
	man := &shredcap.Manifest{Checkpoint: 99, Head: 101, Blocks: 2, Slots: []uint64{100, 101},
		BankHashes: map[string]string{"100": "a", "101": "b"}}
	checked := validatedImportOf(man)
	require.NoError(t, checkReusedManifest(man, checked))
	require.ErrorContains(t, checkReusedManifest(man, validatedImport{}), "not recorded as checked")
	other := checked
	other.Checkpoint = 98
	require.ErrorContains(t, checkReusedManifest(man, other), "not the import that was checked")
	delete(man.BankHashes, "100")
	require.ErrorContains(t, checkReusedManifest(man, checked), "no bank hash for slot 100")
	require.ErrorContains(t, checkReusedManifest(&shredcap.Manifest{}, validatedImport{Validated: true}), "lists no slots")
}

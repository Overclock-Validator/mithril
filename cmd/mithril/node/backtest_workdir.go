package node

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/Overclock-Validator/mithril/pkg/turbine"
)

// A backtest work directory holds everything one capture's runs need:
//
//	BACKTEST.json   marker: proves the directory is a backtest's own
//	backtest.toml   the generated node configuration
//	corpus/         the imported capture (shreds, complete.idx, manifest.json)
//	accounts/       the AccountsDB built from --snapshot (absent with --accounts)
//	shred/          the node's shred store; catchup-spool/ is restored per run
//	logs/, scratch/ node logs and temporary files
const backtestMarkerName = "BACKTEST.json"

type backtestMarker struct {
	Version int `json:"version"`
	// Capture identifies the imported capture by name, size and mtime.
	Capture captureFingerprint `json:"capture"`
	// FromSlot is the first slot the import kept (the slot after the
	// snapshot for --snapshot runs, else 0).
	FromSlot uint64 `json:"from_slot"`
	// SnapshotSlot is set once accounts/ holds a store built from a snapshot
	// at that slot.
	SnapshotSlot uint64 `json:"snapshot_slot,omitempty"`
}

type captureFingerprint struct {
	Name    string `json:"name"`
	Size    int64  `json:"size"`
	ModUnix int64  `json:"mod_unix"`
}

func fingerprintCapture(path string) (captureFingerprint, error) {
	info, err := os.Stat(path)
	if err != nil {
		return captureFingerprint{}, err
	}
	if info.IsDir() {
		return captureFingerprint{}, fmt.Errorf("%s is a directory, not a capture file", path)
	}
	return captureFingerprint{Name: filepath.Base(path), Size: info.Size(), ModUnix: info.ModTime().Unix()}, nil
}

// openBacktestWorkdir creates dir, or accepts it when it is empty or already a
// backtest work directory, and returns its marker (zero when new). Any other
// non-empty directory is refused, so a backtest never writes into a store or
// directory it was not given for this purpose.
func openBacktestWorkdir(dir string) (backtestMarker, error) {
	if dir == "" {
		return backtestMarker{}, errors.New("--workdir is required")
	}
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return backtestMarker{}, os.MkdirAll(dir, 0o755)
	}
	if err != nil {
		return backtestMarker{}, err
	}
	if len(entries) == 0 {
		return backtestMarker{}, nil
	}
	data, err := os.ReadFile(filepath.Join(dir, backtestMarkerName))
	if err != nil {
		return backtestMarker{}, fmt.Errorf("--workdir %s is not empty and was not created by mithril backtest (no %s); use a new directory", dir, backtestMarkerName)
	}
	var m backtestMarker
	if err := json.Unmarshal(data, &m); err != nil || m.Version != 1 {
		return backtestMarker{}, fmt.Errorf("--workdir %s has an unreadable %s; use a new directory", dir, backtestMarkerName)
	}
	return m, nil
}

func writeBacktestMarker(dir string, m backtestMarker) error {
	m.Version = 1
	data, err := json.MarshalIndent(m, "", " ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, backtestMarkerName+".tmp")
	if err := os.WriteFile(tmp, append(data, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(dir, backtestMarkerName))
}

var (
	fullSnapshotName        = regexp.MustCompile(`^snapshot-(\d+)-[1-9A-HJ-NP-Za-km-z]+\.tar\.(zst|bz2|gz|lz4)$`)
	incrementalSnapshotName = regexp.MustCompile(`^incremental-snapshot-(\d+)-(\d+)-[1-9A-HJ-NP-Za-km-z]+\.tar\.(zst|bz2|gz|lz4)$`)
)

// backtestSnapshotSlot returns the slot replay starts from for a full snapshot
// and an optional incremental one, checking that their names agree.
func backtestSnapshotSlot(full, incremental string) (uint64, error) {
	m := fullSnapshotName.FindStringSubmatch(filepath.Base(full))
	if m == nil {
		return 0, fmt.Errorf("--snapshot %s is not named snapshot-<slot>-<hash>.tar.zst", filepath.Base(full))
	}
	fullSlot, _ := strconv.ParseUint(m[1], 10, 64)
	if incremental == "" {
		return fullSlot, nil
	}
	m = incrementalSnapshotName.FindStringSubmatch(filepath.Base(incremental))
	if m == nil {
		return 0, fmt.Errorf("--incremental %s is not named incremental-snapshot-<base>-<slot>-<hash>.tar.zst", filepath.Base(incremental))
	}
	base, _ := strconv.ParseUint(m[1], 10, 64)
	slot, _ := strconv.ParseUint(m[2], 10, 64)
	if base != fullSlot {
		return 0, fmt.Errorf("--incremental builds on slot %d but --snapshot is at slot %d", base, fullSlot)
	}
	return slot, nil
}

// storeView is what a backtest needs to know about an AccountsDB before
// starting the node: whether it holds state, the slot it is durable through,
// and the fold boundaries it can rewind to.
type storeView struct {
	HasState   bool
	StateSlot  uint64
	Boundaries []uint64 // retained fold boundaries (ThroughSlot), ascending
}

// startPlan says how the node starts: build from the snapshot, or open the
// store, rewinding it first when RewindTo is set. FirstSlot is the first
// block this run replays.
type startPlan struct {
	Bootstrap string // "snapshot" or "accountsdb"
	RewindTo  uint64
	FirstSlot uint64
	Note      string
}

// planBacktestStart decides how to reach the capture's checkpoint. builtAt
// is the snapshot slot a store in the work directory was built from (0 when
// the store came from --accounts).
func planBacktestStart(checkpoint uint64, haveSnapshot bool, builtAt uint64, v storeView) (startPlan, error) {
	if !v.HasState {
		if !haveSnapshot {
			return startPlan{}, fmt.Errorf("the AccountsDB has no state file; pass --snapshot (and --incremental) at slot %d, or --accounts with a store that holds slot %d", checkpoint, checkpoint)
		}
		return startPlan{Bootstrap: "snapshot", FirstSlot: checkpoint + 1}, nil
	}
	switch {
	case v.StateSlot == checkpoint:
		return startPlan{Bootstrap: "accountsdb", FirstSlot: checkpoint + 1}, nil
	case v.StateSlot < checkpoint:
		return startPlan{}, fmt.Errorf("snapshot too old for capture: the AccountsDB is at slot %d but the capture's first block builds on slot %d, and the blocks between are not in the capture", v.StateSlot, checkpoint)
	}
	if slices.Contains(v.Boundaries, checkpoint) {
		return startPlan{Bootstrap: "accountsdb", RewindTo: checkpoint, FirstSlot: checkpoint + 1,
			Note: fmt.Sprintf("rewinding the AccountsDB from slot %d to the capture's checkpoint %d", v.StateSlot, checkpoint)}, nil
	}
	if builtAt == checkpoint {
		// A store built from the snapshot has no fold boundary at the snapshot
		// slot itself; the earliest one after it is the closest start.
		for _, b := range v.Boundaries {
			if b > checkpoint && b <= v.StateSlot {
				return startPlan{Bootstrap: "accountsdb", RewindTo: b, FirstSlot: b + 1,
					Note: fmt.Sprintf("rewinding to slot %d, the first fold boundary after the snapshot (slot %d); this run replays from slot %d, the first run replayed from slot %d", b, checkpoint, b+1, checkpoint+1)}, nil
			}
		}
	}
	return startPlan{}, fmt.Errorf("the AccountsDB is at slot %d and cannot return to the capture's checkpoint %d: it is not a retained fold boundary (retained: %s); rebuild from the snapshot or use a store at slot %d",
		v.StateSlot, checkpoint, formatBoundaries(v.Boundaries), checkpoint)
}

func formatBoundaries(b []uint64) string {
	if len(b) == 0 {
		return "none"
	}
	if len(b) > 6 {
		return fmt.Sprintf("%d..%d, %d boundaries", b[0], b[len(b)-1], len(b))
	}
	parts := make([]string, len(b))
	for i, s := range b {
		parts[i] = strconv.FormatUint(s, 10)
	}
	return strings.Join(parts, " ")
}

// restoreBacktestSpool replaces spoolDir with the corpus's shreds and
// completion index. Files are hard-linked when possible (replay only reads and
// deletes spool files) and copied otherwise.
func restoreBacktestSpool(corpusDir, spoolDir string) (int, error) {
	if err := os.RemoveAll(spoolDir); err != nil {
		return 0, err
	}
	if err := os.MkdirAll(spoolDir, 0o755); err != nil {
		return 0, err
	}
	src := filepath.Join(corpusDir, "shreds")
	entries, err := os.ReadDir(src)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".shreds") {
			continue
		}
		if err := linkOrCopy(filepath.Join(src, e.Name()), filepath.Join(spoolDir, e.Name())); err != nil {
			return n, err
		}
		n++
	}
	// The completion index is appended to by the node, so it is always a copy.
	return n, copyFile(filepath.Join(corpusDir, turbine.SpoolJournalName), filepath.Join(spoolDir, turbine.SpoolJournalName))
}

func linkOrCopy(src, dst string) error {
	if err := os.Link(src, dst); err == nil {
		return nil
	}
	return copyFile(src, dst)
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

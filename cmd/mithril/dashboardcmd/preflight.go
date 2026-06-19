// Pre-flight checks run before spawning mithril: systemd conflicts, network
// filesystems where flock is unreliable, and root/non-root ownership mistakes.

package dashboardcmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/Overclock-Validator/mithril/pkg/procctl"
	"github.com/Overclock-Validator/mithril/pkg/state"
)

// preflightCheck returns "" if Start is allowed, else an operator-friendly
// refusal shown verbatim in the Process view.
func preflightCheck(cfg *configData, accountsDir string) string {
	var reasons []string
	if reason := checkSystemd(); reason != "" {
		reasons = append(reasons, reason)
	}
	if reason := checkStateDirFilesystem(); reason != "" {
		reasons = append(reasons, reason)
	}
	if reason := checkUIDMatch(accountsDir); reason != "" {
		reasons = append(reasons, reason)
	}
	if cfg != nil {
		if reason := checkLogPathWritable(cfg.logsPath); reason != "" {
			reasons = append(reasons, reason)
		}
		if reason := checkAccountsStateCompatible(cfg, accountsDir); reason != "" {
			reasons = append(reasons, reason)
		}
	}
	return strings.Join(reasons, "\n\n")
}

// checkSystemd refuses only when a running mithril is supervised by systemd
// (per its cgroup); a fresh start is always allowed.
func checkSystemd() string {
	info, err := procctl.ReadPidFile(procctl.DefaultPidFile())
	if err != nil {
		return "" // no PID file → no supervisor
	}
	cgroupPath := fmt.Sprintf("/proc/%d/cgroup", info.Pid)
	data, err := os.ReadFile(cgroupPath)
	if err != nil {
		return "" // process gone or no /proc (macOS) — nothing to block
	}
	if info.SpawnedBy == "dashboard" {
		return ""
	}
	if _, ok := mithrilSystemdUnit(string(data)); ok {
		return strings.Join([]string{
			"Mithril is managed by systemd. The dashboard would fight",
			"systemd's restart loop. To control it, use:",
			"",
			"    sudo systemctl stop mithril",
			"    sudo systemctl restart mithril",
			"",
			"Stop the systemd unit first, then return to this dashboard.",
		}, "\n")
	}
	return ""
}

func mithrilSystemdUnit(cgroup string) (string, bool) {
	for _, line := range strings.Split(cgroup, "\n") {
		if line == "" {
			continue
		}
		path := line
		if idx := strings.LastIndex(line, ":"); idx >= 0 {
			path = line[idx+1:]
		}
		unit := filepath.Base(path)
		lower := strings.ToLower(unit)
		if !strings.Contains(lower, "mithril") {
			continue
		}
		if strings.HasSuffix(lower, ".service") || strings.HasSuffix(lower, ".scope") {
			return unit, true
		}
	}
	return "", false
}

// checkStateDirFilesystem refuses if the PID-file directory is on a network
// filesystem where flock is unreliable. macOS passes (dev-only, lower stakes).
func checkStateDirFilesystem() string {
	pidDir := stateDirForPidFile()
	if pidDir == "" {
		return ""
	}
	statDir := nearestExistingDir(pidDir)
	if statDir == "" {
		return ""
	}
	fsType, ok := statFSType(statDir)
	if !ok {
		return "" // can't tell, don't refuse
	}
	switch fsType {
	case 0x6969: // NFS_SUPER_MAGIC
		return refusedByNetworkFS("NFS", pidDir)
	case 0x517B: // SMB_SUPER_MAGIC
		return refusedByNetworkFS("SMB", pidDir)
	case 0xFF534D42, 0xFE534D42: // CIFS_MAGIC_NUMBER / SMB2_MAGIC_NUMBER
		return refusedByNetworkFS("CIFS", pidDir)
	}
	return ""
}

func refusedByNetworkFS(name, dir string) string {
	return strings.Join([]string{
		fmt.Sprintf("PID-file directory is on %s — flock is unreliable.", name),
		"Two dashboards on different hosts could both think they hold the lock,",
		"which would lead to a corrupted AccountsDB.",
		"",
		"Move the PID file to a local filesystem by setting MITHRIL_PID_FILE,",
		fmt.Sprintf("or relocate %s to a non-networked path.", dir),
	}, "\n")
}

// stateDirForPidFile returns the directory of DefaultPidFile().
func stateDirForPidFile() string {
	pidPath := procctl.DefaultPidFile()
	// Root-level PID ("/mithril.pid") checks "/", not cwd.
	if i := strings.LastIndex(pidPath, "/"); i > 0 {
		return pidPath[:i]
	} else if i == 0 {
		return "/"
	}
	return "."
}

func nearestExistingDir(dir string) string {
	for dir != "" {
		if info, err := os.Stat(dir); err == nil && info.IsDir() {
			return dir
		}
		next := strings.TrimRight(dir, "/")
		i := strings.LastIndex(next, "/")
		if i <= 0 {
			if !strings.HasPrefix(next, "/") {
				return "."
			}
			if info, err := os.Stat("/"); err == nil && info.IsDir() {
				return "/"
			}
			return ""
		}
		dir = next[:i]
	}
	return ""
}

// checkUIDMatch refuses if the dashboard runs as root but the AccountsDB dir is
// owned by a non-root user — root-written files the real user couldn't read.
func checkUIDMatch(accountsDir string) string {
	if os.Geteuid() != 0 {
		return "" // not root, no mismatch possible
	}
	if accountsDir == "" {
		return "" // no AccountsDB configured yet — fresh setup
	}
	info, err := os.Stat(accountsDir)
	if err != nil {
		return "" // not created yet — bootstrap makes it as root, consistent
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return "" // non-Unix, no UID concept here
	}
	if stat.Uid == 0 {
		return "" // AccountsDB is already root-owned; consistent
	}
	return strings.Join([]string{
		"Dashboard is running as root but AccountsDB at",
		"    " + accountsDir,
		fmt.Sprintf("is owned by uid %d.", stat.Uid),
		"",
		"Starting Mithril as root would create files the original user cannot read.",
		"Either:",
		"  - Run the dashboard as the correct user:  sudo -u <user> mithril dashboard",
		"  - Or fix ownership before starting Mithril from this dashboard.",
	}, "\n")
}

func checkLogPathWritable(logsPath string) string {
	if strings.TrimSpace(logsPath) == "" {
		return ""
	}
	clean := filepath.Clean(logsPath)
	info, err := os.Stat(clean)
	if err == nil {
		if !info.IsDir() {
			return strings.Join([]string{
				"Log path is not a directory:",
				"    " + clean,
				"",
				"Change storage.logs to a directory, or remove that file first.",
			}, "\n")
		}
		if !dirWritableByCurrentUser(info) {
			return unwritableLogPathReason(clean, clean, "write log files")
		}
		return ""
	}
	if !os.IsNotExist(err) {
		return strings.Join([]string{
			"Cannot inspect log directory:",
			"    " + clean,
			"",
			"Error: " + err.Error(),
		}, "\n")
	}

	parent := nearestExistingDir(filepath.Dir(clean))
	if parent == "" {
		return strings.Join([]string{
			"Cannot create log directory:",
			"    " + clean,
			"",
			"No existing parent directory was found.",
		}, "\n")
	}
	parentInfo, err := os.Stat(parent)
	if err != nil {
		return ""
	}
	if !dirWritableByCurrentUser(parentInfo) {
		return unwritableLogPathReason(clean, parent, "create the log directory")
	}
	return ""
}

func unwritableLogPathReason(logsPath, checkedPath, action string) string {
	return strings.Join([]string{
		"Mithril cannot write logs in the configured folder.",
		"    logs: " + logsPath,
		"    checked: " + checkedPath,
		"",
		"Recommended fix: choose Fix with safe folders.",
		"That creates user-owned folders and keeps existing data untouched.",
		"",
		"Advanced fix: ask an administrator to create/chown the log folder.",
		"Needed permission: " + action + ".",
	}, "\n")
}

func dirWritableByCurrentUser(info os.FileInfo) bool {
	if os.Geteuid() == 0 {
		return true
	}
	if !info.IsDir() {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return false
	}
	mode := int(info.Mode().Perm())
	perm := mode & 0007
	if uint32(os.Geteuid()) == stat.Uid {
		perm = (mode >> 6) & 0007
	} else if currentUserInGroup(stat.Gid) {
		perm = (mode >> 3) & 0007
	}
	return perm&0003 == 0003 // write + execute/search
}

func currentUserInGroup(gid uint32) bool {
	if uint32(os.Getegid()) == gid {
		return true
	}
	groups, err := os.Getgroups()
	if err != nil {
		return false
	}
	for _, group := range groups {
		if uint32(group) == gid {
			return true
		}
	}
	return false
}

func checkAccountsStateCompatible(cfg *configData, accountsDir string) string {
	if strings.TrimSpace(accountsDir) == "" {
		return ""
	}
	st, err := state.LoadState(accountsDir)
	if err != nil {
		return strings.Join([]string{
			"Existing AccountsDB state cannot be read.",
			"    " + filepath.Join(accountsDir, state.StateFileName),
			"",
			"Error: " + err.Error(),
			"",
			"Use a fresh AccountsDB path or rebuild from snapshot before starting.",
		}, "\n")
	}
	if st == nil {
		return ""
	}
	// "unknown" is the unset-cluster sentinel; treat it as no opinion, not a
	// mismatch (matches data.go). The state file only stores real names.
	if cfg != nil && cfg.cluster != "" && cfg.cluster != "unknown" && st.Cluster != "" && cfg.cluster != st.Cluster {
		return strings.Join([]string{
			"Existing AccountsDB belongs to a different cluster.",
			"    config: " + cfg.cluster,
			"    state:  " + st.Cluster,
			"",
			"Use a cluster-matched AccountsDB path or rebuild from snapshot.",
		}, "\n")
	}
	if st.Stage == "ready" && len(st.ManifestEpochStakes) > 0 && len(st.ManifestEpochAuthorizedVoters) == 0 {
		return strings.Join([]string{
			"Stored node data cannot be resumed by this Mithril version.",
			"    " + filepath.Join(accountsDir, state.StateFileName),
			"",
			"Recommended fix: choose Fix with safe folders.",
			"That builds fresh local data and leaves the old folder untouched.",
			"",
			"Advanced detail: state file is missing manifest_epoch_authorized_voters.",
		}, "\n")
	}
	return ""
}

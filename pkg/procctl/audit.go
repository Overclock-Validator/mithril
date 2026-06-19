package procctl

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// AuditEntry is one row of the control audit log. Extra values are truncated at
// write time so a line stays under PIPE_BUF (4 KiB) for atomic-append.
type AuditEntry struct {
	Action string            // "START" | "STOP" | "FORCE_STOP" | "RESTART"
	Result string            // e.g. "signal_sent" | "signal_failed" | "refused" | "killed" | "ok" | "failed" | "timeout"
	Pid    int               // affected mithril PID (0 if not applicable)
	Extra  map[string]string // arbitrary key=value context
}

// maxExtraValueLen caps each Extra value so a long embedded error can't
// push the line past the 4 KiB atomic-append window.
const maxExtraValueLen = 256

// AppendAudit writes one line, creating dir (0700) and file (0600) as needed.
// Each call is a single atomic O_APPEND write of <4 KiB; errors are non-fatal.
func AppendAudit(path string, entry AuditEntry) error {
	if err := EnsurePidDir(filepath.Dir(path)); err != nil {
		return err
	}
	line := formatAuditLine(time.Now().UTC(), entry, os.Getpid())

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("open audit log %s: %w", path, err)
	}
	defer f.Close()
	if _, err := f.WriteString(line + "\n"); err != nil {
		return fmt.Errorf("append audit log %s: %w", path, err)
	}
	return nil
}

// sanitizeAuditField strips control chars so an untrusted value (run_id, OS
// error string) can't smuggle a newline and forge or split audit lines.
func sanitizeAuditField(s string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, s)
}

// auditValue prepares an Extra value for the key=value format. Quoted only if it
// contains space/'='/'"'; plain values stay bare so they're greppable.
func auditValue(s string) string {
	s = sanitizeAuditField(s)
	if s == "" || strings.ContainsAny(s, " =\"") {
		return strconv.Quote(s)
	}
	return s
}

// formatAuditLine renders the line without I/O. Pulled out for testability.
func formatAuditLine(ts time.Time, entry AuditEntry, dashboardPid int) string {
	var b strings.Builder
	b.WriteString(ts.Format(time.RFC3339Nano))
	b.WriteString(" action=")
	b.WriteString(sanitizeAuditField(entry.Action))
	b.WriteString(" result=")
	b.WriteString(sanitizeAuditField(entry.Result))
	fmt.Fprintf(&b, " dashboard_pid=%d", dashboardPid)
	if entry.Pid != 0 {
		fmt.Fprintf(&b, " pid=%d", entry.Pid)
	}
	// Sorted for deterministic output.
	if len(entry.Extra) > 0 {
		keys := make([]string, 0, len(entry.Extra))
		for k := range entry.Extra {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			v := sanitizeAuditField(entry.Extra[k])
			if len(v) > maxExtraValueLen {
				// ToValidUTF8 drops the partial rune left by the byte cut.
				v = strings.ToValidUTF8(v[:maxExtraValueLen], "") + "...(truncated)"
			}
			fmt.Fprintf(&b, " %s=%s", sanitizeAuditField(k), auditValue(v))
		}
	}
	return b.String()
}

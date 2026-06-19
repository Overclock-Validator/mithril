package procctl

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Audit log — append-only, greppable key=value forensic record (one line per action).

// Line format: RFC3339Nano timestamp + core fields + sorted key=value extras.
func TestAppendAudit_BasicLine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "control.audit")

	entry := AuditEntry{
		Action: "START",
		Result: "ok",
		Pid:    12345,
		Extra:  map[string]string{"run_id": "abc123"},
	}
	require.NoError(t, AppendAudit(path, entry))

	lines := readAuditLines(t, path)
	require.Len(t, lines, 1)
	line := lines[0]
	assert.Contains(t, line, "action=START")
	assert.Contains(t, line, "result=ok")
	assert.Contains(t, line, "pid=12345")
	assert.Contains(t, line, "run_id=abc123")
	// Leading token must be an RFC3339Nano timestamp.
	firstField := strings.SplitN(line, " ", 2)[0]
	_, tErr := time.Parse(time.RFC3339Nano, firstField)
	require.NoError(t, tErr, "audit line must begin with an RFC3339Nano timestamp, got %q", firstField)
}

// Multiple calls append rather than overwrite.
func TestAppendAudit_AppendsAcrossCalls(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "control.audit")

	require.NoError(t, AppendAudit(path, AuditEntry{Action: "START", Result: "ok"}))
	require.NoError(t, AppendAudit(path, AuditEntry{Action: "STOP", Result: "ok"}))
	require.NoError(t, AppendAudit(path, AuditEntry{Action: "FORCE_STOP", Result: "ok"}))

	lines := readAuditLines(t, path)
	assert.Len(t, lines, 3)
	assert.Contains(t, lines[0], "action=START")
	assert.Contains(t, lines[1], "action=STOP")
	assert.Contains(t, lines[2], "action=FORCE_STOP")
}

// File is 0600 — audit holds usernames and TTY identifiers.
func TestAppendAudit_Permissions(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "control.audit")
	require.NoError(t, AppendAudit(path, AuditEntry{Action: "START", Result: "ok"}))

	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0600), info.Mode().Perm())
}

// Auditing to a path with no parent dir auto-creates it (0700).
func TestAppendAudit_CreatesParentDir(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "control.audit")
	require.NoError(t, AppendAudit(path, AuditEntry{Action: "START", Result: "ok"}))
	_, err := os.Stat(path)
	assert.NoError(t, err)
}

// O_APPEND keeps concurrent short-line writes from interleaving.
func TestAppendAudit_ConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "control.audit")

	const writers = 8
	const perWriter = 50

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				_ = AppendAudit(path, AuditEntry{
					Action: "START",
					Result: "ok",
					Extra:  map[string]string{"writer": itoaTest(id), "seq": itoaTest(i)},
				})
			}
		}(w)
	}
	wg.Wait()

	lines := readAuditLines(t, path)
	assert.Len(t, lines, writers*perWriter, "every Append must produce one line")
	for _, line := range lines {
		assert.Contains(t, line, "writer=")
		assert.Contains(t, line, "seq=")
	}
}

// Extra keys are emitted in sorted order (deterministic output).
func TestAppendAudit_ExtraKeysAreSorted(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "control.audit")
	require.NoError(t, AppendAudit(path, AuditEntry{
		Action: "START",
		Result: "ok",
		Extra: map[string]string{
			"zebra": "1",
			"alpha": "2",
			"mango": "3",
		},
	}))

	lines := readAuditLines(t, path)
	require.Len(t, lines, 1)
	// alpha < mango < zebra.
	a := strings.Index(lines[0], "alpha=")
	m := strings.Index(lines[0], "mango=")
	z := strings.Index(lines[0], "zebra=")
	assert.True(t, a > 0 && a < m && m < z,
		"extras should be sorted alphabetically: %s", lines[0])
}

func readAuditLines(t *testing.T, path string) []string {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	var out []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		out = append(out, scanner.Text())
	}
	require.NoError(t, scanner.Err())
	return out
}

// itoaTest: minimal int-to-string for test extras.
func itoaTest(v int) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	buf := make([]byte, 0, 12)
	for v > 0 {
		buf = append([]byte{byte('0' + v%10)}, buf...)
		v /= 10
	}
	if neg {
		buf = append([]byte{'-'}, buf...)
	}
	return string(buf)
}

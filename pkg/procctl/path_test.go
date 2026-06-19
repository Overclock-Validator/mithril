package procctl

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// PID-file path resolution precedence (first set wins):
// MITHRIL_PID_FILE > $XDG_STATE_HOME > $HOME/.local/state > $XDG_RUNTIME_DIR.

// withEnv sets env vars for the test, restoring prior values (incl. unset) on cleanup.
func withEnv(t *testing.T, kvs map[string]string) {
	t.Helper()
	for k, v := range kvs {
		prior, had := os.LookupEnv(k)
		require.NoError(t, os.Setenv(k, v))
		t.Cleanup(func() {
			if had {
				_ = os.Setenv(k, prior)
			} else {
				_ = os.Unsetenv(k)
			}
		})
	}
}

// withUnsetEnv unsets env vars for the test, restoring on cleanup (for fallback chains).
func withUnsetEnv(t *testing.T, keys ...string) {
	t.Helper()
	for _, k := range keys {
		prior, had := os.LookupEnv(k)
		require.NoError(t, os.Unsetenv(k))
		t.Cleanup(func() {
			if had {
				_ = os.Setenv(k, prior)
			}
		})
	}
}

// MITHRIL_PID_FILE overrides every XDG layer.
func TestDefaultPidFile_HonorsExplicitEnvOverride(t *testing.T) {
	dir := t.TempDir()
	override := filepath.Join(dir, "custom", "my.pid")
	withEnv(t, map[string]string{"MITHRIL_PID_FILE": override})

	got := DefaultPidFile()
	assert.Equal(t, override, got, "explicit env override must be returned verbatim")
}

// XDG_STATE_HOME (persistent) wins over XDG_RUNTIME_DIR (logout-wiped).
func TestDefaultPidFile_PrefersXdgStateHomeOverRuntimeDir(t *testing.T) {
	runtimeDir := t.TempDir()
	stateDir := t.TempDir()
	withUnsetEnv(t, "MITHRIL_PID_FILE")
	withEnv(t, map[string]string{
		"XDG_RUNTIME_DIR": runtimeDir,
		"XDG_STATE_HOME":  stateDir,
	})

	got := DefaultPidFile()
	assert.Equal(t, filepath.Join(stateDir, "mithril", "mithril.pid"), got,
		"persistent XDG_STATE_HOME must win over logout-wiped XDG_RUNTIME_DIR")
}

func TestDefaultPidFile_UsesXdgStateHome(t *testing.T) {
	stateDir := t.TempDir()
	withUnsetEnv(t, "MITHRIL_PID_FILE", "XDG_RUNTIME_DIR")
	withEnv(t, map[string]string{"XDG_STATE_HOME": stateDir})

	got := DefaultPidFile()
	assert.Equal(t, filepath.Join(stateDir, "mithril", "mithril.pid"), got)
}

// $HOME/.local/state (persistent) wins over XDG_RUNTIME_DIR when XDG_STATE_HOME is unset.
func TestDefaultPidFile_PrefersHomeOverRuntimeDir(t *testing.T) {
	home := t.TempDir()
	runtimeDir := t.TempDir()
	withUnsetEnv(t, "MITHRIL_PID_FILE", "XDG_STATE_HOME")
	withEnv(t, map[string]string{"HOME": home, "XDG_RUNTIME_DIR": runtimeDir})

	got := DefaultPidFile()
	assert.Equal(t, filepath.Join(home, ".local", "state", "mithril", "mithril.pid"), got,
		"persistent $HOME/.local/state must win over logout-wiped XDG_RUNTIME_DIR")
}

// With no HOME and no XDG_STATE_HOME, falls back to XDG_RUNTIME_DIR.
func TestDefaultPidFile_FallsBackToRuntimeDirWhenNoHome(t *testing.T) {
	runtimeDir := t.TempDir()
	withUnsetEnv(t, "MITHRIL_PID_FILE", "XDG_STATE_HOME", "HOME")
	withEnv(t, map[string]string{"XDG_RUNTIME_DIR": runtimeDir})

	got := DefaultPidFile()
	assert.Equal(t, filepath.Join(runtimeDir, "mithril", "mithril.pid"), got)
}

// Lock file shares the PID file's directory.
func TestDefaultLockFile_LivesAlongsidePidFile(t *testing.T) {
	withUnsetEnv(t, "MITHRIL_PID_FILE")
	pidPath := DefaultPidFile()
	lockPath := DefaultLockFile()
	assert.Equal(t, filepath.Dir(pidPath), filepath.Dir(lockPath),
		"lock file and PID file must be in the same directory")
	assert.Equal(t, "mithril.lock", filepath.Base(lockPath))
}

// Audit log shares the PID file's directory — must not sit under rotating storage.logs.
func TestDefaultAuditLog_LivesAlongsidePidFile(t *testing.T) {
	withUnsetEnv(t, "MITHRIL_PID_FILE")
	pidPath := DefaultPidFile()
	auditPath := DefaultAuditLog()
	assert.Equal(t, filepath.Dir(pidPath), filepath.Dir(auditPath),
		"audit log and PID file must be in the same directory")
	assert.Equal(t, "control.audit", filepath.Base(auditPath))
}

// EnsurePidDir creates the dir 0700 (per-user state).
func TestEnsurePidDir_CreatesDirWith0700(t *testing.T) {
	base := t.TempDir()
	target := filepath.Join(base, "nested", "dir")
	require.NoError(t, EnsurePidDir(target))

	info, err := os.Stat(target)
	require.NoError(t, err)
	assert.True(t, info.IsDir())
	assert.Equal(t, os.FileMode(0700), info.Mode().Perm())
}

func TestEnsurePidDir_IdempotentOnExisting(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, EnsurePidDir(dir))
	require.NoError(t, EnsurePidDir(dir))
}

// Existing dirs aren't chmod'd — protects shared dirs like /tmp.
func TestEnsurePidDir_DoesNotChmodExistingDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "shared")
	require.NoError(t, os.Mkdir(dir, 0755))
	// Chmod explicitly: os.Mkdir applies mode &^ umask, so a hardened umask
	// would otherwise strip bits and make the assertion flaky.
	require.NoError(t, os.Chmod(dir, 0755))
	require.NoError(t, EnsurePidDir(dir))

	info, err := os.Stat(dir)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0755), info.Mode().Perm())
}

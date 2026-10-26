package snapshot

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/stretchr/testify/require"
)

// Repeated, aliased or nested accounts directories are refused before the
// cleanup touches anything in them.
func TestDuplicateAccountsDirectoriesDoNotTruncate(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "db")
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "accounts"), 0755))
	file := filepath.Join(dir, "accounts", "1.1")
	require.NoError(t, os.WriteFile(file, []byte("keep me"), 0644))
	alias := filepath.Join(root, "alias")
	require.NoError(t, os.Symlink(dir, alias))
	for _, paths := range [][]string{{dir, dir}, {dir, alias}, {dir, dir + "/nested"}} {
		require.Error(t, accountsdb.ValidateAccountsPaths(paths), "%v", paths)
		CleanAccountsDbDirs(paths)
		data, err := os.ReadFile(file)
		require.NoError(t, err)
		require.Equal(t, "keep me", string(data))
	}
}

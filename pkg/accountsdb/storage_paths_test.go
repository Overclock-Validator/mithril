package accountsdb

import (
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
)

func TestValidateStorageDirectories(t *testing.T) {
	root := t.TempDir()
	a, b := filepath.Join(root, "a"), filepath.Join(root, "b")
	require.NoError(t, os.Mkdir(a, 0755))
	require.NoError(t, os.Mkdir(b, 0755))
	alias := filepath.Join(root, "alias")
	require.NoError(t, os.Symlink(a, alias))
	file := filepath.Join(root, "file")
	require.NoError(t, os.WriteFile(file, []byte("sentinel"), 0644))
	cases := []struct {
		name  string
		paths []string
		valid bool
	}{
		{"empty", nil, false}, {"empty-path", []string{""}, false},
		{"siblings", []string{a, b}, true}, {"new-siblings", []string{a + "/new", b + "/new"}, true},
		{"same", []string{a, a}, false}, {"clean", []string{a, a + "/../a"}, false},
		{"symlink", []string{a, alias}, false}, {"new-symlink-suffix", []string{a + "/new", alias + "/new"}, false},
		{"nested", []string{a, a + "/new"}, false}, {"root", []string{a, "/"}, false},
		{"file", []string{file}, false}, {"file-parent", []string{file + "/new"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := ValidateStorageDirectories(c.paths)
			if c.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
	require.NoError(t, os.Symlink(a, filepath.Join(b, "accounts")))
	require.NoError(t, os.Mkdir(filepath.Join(a, "accounts"), 0755))
	require.Error(t, ValidateAccountsPaths([]string{a, b}), "aliased accounts subdirectories")
	_, err := OpenDbPaths([]string{a, alias})
	require.Error(t, err)
	require.NoDirExists(t, filepath.Join(a, "mithril_db"), "validation must precede database creation")
}

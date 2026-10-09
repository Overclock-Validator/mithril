//go:build linux || darwin

package util

import (
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
)

func TestRenameNoReplacePreservesDestination(t *testing.T) {
	root := t.TempDir()
	from, to := filepath.Join(root, "from"), filepath.Join(root, "to")
	require.NoError(t, os.WriteFile(from, []byte("new"), 0600))
	require.NoError(t, os.WriteFile(to, []byte("existing"), 0600))
	require.ErrorIs(t, RenameNoReplace(from, to), os.ErrExist)
	got, err := os.ReadFile(to)
	require.NoError(t, err)
	require.Equal(t, "existing", string(got))
	require.NoError(t, os.Remove(to))
	require.NoError(t, RenameNoReplace(from, to))
	got, err = os.ReadFile(to)
	require.NoError(t, err)
	require.Equal(t, "new", string(got))
	_, err = os.Stat(from)
	require.True(t, os.IsNotExist(err))
}

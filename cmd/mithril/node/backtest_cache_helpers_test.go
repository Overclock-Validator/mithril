package node

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func writeFiles(t *testing.T, dir string, n int) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "sub"), 0o755))
	for i := range n {
		require.NoError(t, os.WriteFile(filepath.Join(dir, "sub", fmt.Sprintf("f%d", i)), make([]byte, 4096), 0o644))
	}
}

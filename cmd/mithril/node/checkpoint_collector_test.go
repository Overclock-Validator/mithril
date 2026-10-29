package node

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/stretchr/testify/require"
)

// A damaged retained or parked manifest stops checkpoint cleanup before any
// checkpoint file is deleted.
func TestCheckpointCollectorCorruptionFailsBeforeDeletion(t *testing.T) {
	for _, parked := range []bool{false, true} {
		t.Run(fmt.Sprint(parked), func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, "accounts")
			require.NoError(t, os.MkdirAll(dir, 0700))
			ref := writeCheckpointManifest(t, root, dir, 1, 10, 1, "")
			orphan, err := replay.PrepareTransactionStatusCheckpoint(root, 2, []byte("must-survive"))
			require.NoError(t, err)
			path := filepath.Join(dir, "10.1.manifest")
			if parked {
				require.NoError(t, os.Rename(path, path+".rewound"))
				path += ".rewound"
			}
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			if parked {
				data[len(data)-1] ^= 1
			} else {
				data = data[:len(data)-1]
			}
			require.NoError(t, os.WriteFile(path, data, 0600))
			_, err = cleanupRetainedTransactionStatusCheckpoints(root, &accountsdb.AccountsDb{AcctsDir: dir}, 2, nil)
			require.Error(t, err)
			require.FileExists(t, checkpointFilePath(root, ref))
			require.FileExists(t, checkpointFilePath(root, orphan))
		})
	}
}

//go:build !linux

package accountsdb

import (
	"errors"
	"os"
)

// Non-Linux targets deliberately have no weak approximation for inode ctime.
// Callers treat this error as "seal unavailable" and perform the complete
// semantic/CRC/SHA-256 validation pass instead.
func readShardedBaseScanFileIdentity(*os.File) (shardedBaseScanFileIdentity, error) {
	return shardedBaseScanFileIdentity{}, errors.New(
		"accountsdb: strong exact-sidecar identity seal is unavailable on this platform",
	)
}

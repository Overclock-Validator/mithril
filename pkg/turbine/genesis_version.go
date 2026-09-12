package turbine

import "github.com/gagliardetto/solana-go"

// ShredVersionFromGenesisHash matches Agave's compute_shred_version with no hard
// forks. It is for a new cluster; an existing cluster must also account for its
// hard-fork history instead of deriving its current version from genesis alone.
func ShredVersionFromGenesisHash(hash solana.Hash) uint16 {
	var version uint16
	for i := 0; i < len(hash); i += 2 {
		version ^= uint16(hash[i])<<8 | uint16(hash[i+1])
	}
	if version < 65535 {
		version++
	}
	return version
}

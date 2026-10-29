package node

import "fmt"

// resolveProtocolMode keeps network identity separate from replay semantics.
// Empty preserves existing cluster defaults. Alpenglow selections still pass
// ValidateAlpenglowStartupState against the imported bank before replay starts.
func resolveProtocolMode(cluster, protocol string) (bool, error) {
	defaultMode, err := alpenglowModeForCluster(cluster)
	if err != nil {
		return false, err
	}
	switch protocol {
	case "":
		return defaultMode, nil
	case "alpenglow":
		return true, nil
	case "classic":
		return false, nil
	default:
		return false, fmt.Errorf("invalid consensus.protocol %q: expected alpenglow or classic", protocol)
	}
}

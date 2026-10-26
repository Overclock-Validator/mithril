package node

import (
	"fmt"
	"net"
	"strings"
)

// parseShredReceivers resolves host:port addresses that get copies of shreds,
// such as a block builder's shred listener.
func parseShredReceivers(key string, addrs []string) ([]*net.UDPAddr, error) {
	var out []*net.UDPAddr
	for _, raw := range addrs {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		addr, err := net.ResolveUDPAddr("udp4", raw)
		if err != nil {
			return nil, fmt.Errorf("%s: %q: %w", key, raw, err)
		}
		if addr.Port == 0 || addr.IP == nil || addr.IP.IsUnspecified() {
			return nil, fmt.Errorf("%s: %q needs a host and a port", key, raw)
		}
		out = append(out, addr)
	}
	return out, nil
}

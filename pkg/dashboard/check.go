package dashboard

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"time"
)

// CheckReader validates reader addresses and checks a fresh snapshot using the
// reader's normal admission rules. It neither listens nor returns private data.
func CheckReader(ctx context.Context, socket, address, origin string, options PublicOptions) error {
	return checkReader(ctx, socket, address, origin, options, false)
}

func checkReader(ctx context.Context, socket, address, origin string, options PublicOptions, fromNode bool) error {
	if err := validateReaderAddress(socket, address); err != nil {
		return err
	}
	if _, err := net.ResolveTCPAddr("tcp", address); err != nil {
		return err
	}
	if origin != "" {
		if err := ValidateManagedOrigin(origin); err != nil {
			return err
		}
	}
	info, err := os.Stat(socket)
	if errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("dashboard feed socket %q is missing; check the node's dashboard profile and startup", socket)
	}
	if err != nil {
		return fmt.Errorf("inspect dashboard feed socket: %w", err)
	}
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("dashboard feed path %q is not a socket", socket)
	}

	c := newCollector(socket)
	defer c.client.CloseIdleConnections()
	c.public = origin != ""
	profile := 0
	if c.public {
		// Check the viewer's status projection without starting detail workers.
		profile = snapshotProfile(advancedProfileQuery)
		c.publicOptions = PublicOptions{Resources: options.Resources, Peers: options.Peers, Harmonic: options.Harmonic}
	}
	if fromNode {
		c.expectedOrigin = origin
	}
	reason := c.poll(ctx, time.Now().UTC())
	if frame := c.latest.Load(); frame != nil {
		if remaining, ok := frame.remainingStatus(profile, time.Now()); ok && remaining >= time.Millisecond {
			return nil
		}
	}
	switch reason {
	case readerOriginChanged:
		return errors.New("dashboard feed origin changed during the check; retry")
	case "source_unavailable":
		return errors.New("dashboard feed is unavailable; check node readiness and reader feed-group permissions")
	case "source_invalid":
		return errors.New("dashboard feed snapshot was rejected; check node and reader versions and the host clock")
	default:
		return errors.New("dashboard feed did not provide fresh status; check node progress, reader version and the host clock")
	}
}

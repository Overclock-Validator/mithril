//go:build !linux

package dashboard

import (
	"context"
	"strings"
	"testing"
)

func TestGroupFeedRefusesUnsupportedPlatform(t *testing.T) {
	_, err := ListenFeedGroup(context.Background(), socketPath(t), "test", func() Snapshot { return Snapshot{} })
	if err == nil || !strings.Contains(err.Error(), "requires Linux") {
		t.Fatalf("group feed error: %v", err)
	}
}

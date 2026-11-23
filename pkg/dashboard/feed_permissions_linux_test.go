//go:build linux

package dashboard

import (
	"context"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"
)

func TestGroupFeedPermissionsAndExistingPath(t *testing.T) {
	group, err := user.LookupGroupId(strconv.Itoa(os.Getegid()))
	if err != nil {
		t.Fatal(err)
	}
	path := socketPath(t)
	dir := filepath.Dir(path)
	sample := func() Snapshot { return testSnapshot(time.Now().UTC()) }
	for _, mode := range []os.FileMode{0o700, 0o710, os.ModeSetgid | 0o750, os.ModeSetgid | 0o770} {
		if err := os.Chmod(dir, mode); err != nil {
			t.Fatal(err)
		}
		if _, err := ListenFeedGroup(context.Background(), path, group.Name, sample); err == nil {
			t.Fatalf("accepted directory mode %v", mode)
		}
	}
	if err := os.Chmod(dir, os.ModeSetgid|0o710); err != nil {
		t.Fatal(err)
	}
	if _, err := ListenFeedGroup(context.Background(), path, "", sample); err == nil {
		t.Fatal("empty group accepted")
	}
	if _, err := ListenFeedGroup(context.Background(), path, "mithril-dashboard-group-does-not-exist", sample); err == nil {
		t.Fatal("unknown group accepted")
	}
	alias := filepath.Join(dir, "alias")
	if err := os.Symlink(dir, alias); err != nil {
		t.Fatal(err)
	}
	if _, err := ListenFeedGroup(context.Background(), filepath.Join(alias, "node.sock"), group.Name, sample); err == nil {
		t.Fatal("symlink parent accepted")
	}
	if err := os.WriteFile(path, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ListenFeedGroup(context.Background(), path, group.Name, sample); err == nil {
		t.Fatal("existing path accepted")
	}
	if b, err := os.ReadFile(path); err != nil || string(b) != "keep" {
		t.Fatalf("existing file changed: %q %v", b, err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	feed, err := ListenFeedGroup(context.Background(), path, group.Name, sample)
	if err != nil {
		t.Fatal(err)
	}
	defer feed.Close()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	stat := info.Sys().(*syscall.Stat_t)
	if info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != 0o620 || stat.Gid != uint32(os.Getegid()) || stat.Uid != uint32(os.Geteuid()) {
		t.Fatalf("wrong socket owner, group or mode: %v uid=%d gid=%d", info.Mode(), stat.Uid, stat.Gid)
	}
	c := newCollector(path)
	defer c.client.CloseIdleConnections()
	response, err := c.client.Get("http://node/snapshot")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatalf("feed status %d", response.StatusCode)
	}
	if _, err := ListenFeedGroup(context.Background(), path, group.Name, sample); err == nil {
		t.Fatal("live socket replaced")
	}
	feed.Close()
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("socket remains after close: %v", err)
	}
}

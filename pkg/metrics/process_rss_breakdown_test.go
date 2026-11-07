package metrics

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
)

func TestProcessRSSBreakdownParsing(t *testing.T) {
	valid := "Name:\tmithril\nRssAnon:\t4 kB\nRssFile:\t0 kB\nRssShmem:\t2 kB\n"
	got, err := parseProcessRSSBreakdown([]byte(valid))
	if err != nil || got != (ProcessRSSBreakdown{AnonymousBytes: 4096, SharedBytes: 2048}) {
		t.Fatalf("parse = %+v, %v", got, err)
	}
	for _, bad := range []string{
		"", "RssAnon: 1 kB\nRssFile: 1 kB\n",
		"RssAnon: 1 kB\nRssFile: 1 kB\nRssShmem: 1 kB",
		valid + "RssAnon: 1 kB\n",
		"RssAnon: -1 kB\nRssFile: 1 kB\nRssShmem: 1 kB\n",
		"RssAnon: 18014398509481984 kB\nRssFile: 1 kB\nRssShmem: 1 kB\n",
		"RssAnon: 1 bytes\nRssFile: 1 kB\nRssShmem: 1 kB\n",
	} {
		if _, err := parseProcessRSSBreakdown([]byte(bad)); err == nil {
			t.Fatalf("accepted malformed status %q", bad)
		}
	}
	if runtime.GOOS != "linux" {
		if _, ok := ReadProcessRSSBreakdown(); ok {
			t.Fatal("unsupported platform reported a Linux measurement")
		}
	}
}

func TestProcessRSSBreakdownLiveLinux(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("requires Linux procfs")
	}
	got, ok := ReadProcessRSSBreakdown()
	if !ok {
		t.Fatal("current process RSS split unavailable")
	}
	body, err := os.ReadFile("/proc/self/status")
	if err != nil {
		t.Fatal(err)
	}
	fields := map[string]uint64{"RssAnon": got.AnonymousBytes, "RssFile": got.FileBytes, "RssShmem": got.SharedBytes}
	for _, line := range strings.Split(string(body), "\n") {
		name, value, found := strings.Cut(line, ":")
		observed, wanted := fields[name]
		if !found || !wanted {
			continue
		}
		var kib uint64
		var unit string
		if n, err := fmt.Sscanf(value, "%d %s", &kib, &unit); err != nil || n != 2 || unit != "kB" {
			t.Fatalf("invalid procfs %s value", name)
		}
		actual := kib * 1024
		if observed > actual+4<<20 || actual > observed+4<<20 {
			t.Fatalf("%s: dashboard %d, procfs %d", name, observed, actual)
		}
		delete(fields, name)
	}
	if len(fields) != 0 {
		t.Fatalf("missing procfs fields: %v", fields)
	}
}

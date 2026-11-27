package sealevel

import (
	"slices"
	"strings"
	"testing"
	"unsafe"
)

func TestLogRecorderDefaultUnbounded(t *testing.T) {
	large := strings.Repeat("x", maxRecordedLogBytes+1)
	r := LogRecorder{Logs: []string{"existing"}}
	r.Log(large)
	for range maxRecordedLogEntries + 1 {
		r.Log("")
	}
	if len(r.Logs) != maxRecordedLogEntries+3 || r.Logs[1] != large {
		t.Fatalf("default recorder changed: %d entries", len(r.Logs))
	}
	if unsafe.StringData(r.Logs[1]) != unsafe.StringData(large) {
		t.Fatal("default recorder copied the message")
	}
}

func TestBoundedLogRecorderByteLimit(t *testing.T) {
	nearLimit := strings.Repeat("x", maxRecordedLogBytes-2)
	unicodeNearLimit := strings.Repeat("é", (maxRecordedLogBytes-2)/2)
	for _, test := range []struct {
		name string
		logs []string
		want []string
	}{
		{name: "no messages"},
		{name: "empty message", logs: []string{""}, want: []string{""}},
		{name: "below limit", logs: []string{nearLimit + "x"}, want: []string{nearLimit + "x"}},
		{name: "exact limit", logs: []string{nearLimit + "xx"}, want: []string{"Log truncated"}},
		{name: "above limit then small", logs: []string{nearLimit + "xxx", "small"}, want: []string{"Log truncated", "small"}},
		{name: "exact remainder then smaller", logs: []string{nearLimit, "xx", "x", "x"}, want: []string{nearLimit, "Log truncated", "x"}},
		{name: "UTF-8 byte length", logs: []string{unicodeNearLimit, "é", "x"}, want: []string{unicodeNearLimit, "Log truncated", "x"}},
		{name: "marker text is content", logs: []string{"Log truncated", nearLimit, nearLimit}, want: []string{"Log truncated", "Log truncated"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := NewBoundedLogRecorder()
			for _, log := range test.logs {
				r.Log(log)
			}
			if !slices.Equal(r.Logs, test.want) {
				t.Fatalf("unexpected retained messages: got %d entries, want %d", len(r.Logs), len(test.want))
			}
			contentBytes := 0
			for _, log := range r.Logs {
				contentBytes += len(log)
			}
			if r.truncated {
				contentBytes -= len("Log truncated")
			}
			if contentBytes >= maxRecordedLogBytes || contentBytes != r.bytesWritten {
				t.Fatalf("retained %d content bytes, accounted %d", contentBytes, r.bytesWritten)
			}
		})
	}
}

func TestBoundedLogRecorderEmptyMessages(t *testing.T) {
	for _, rejectFirst := range []bool{false, true} {
		r := NewBoundedLogRecorder()
		if rejectFirst {
			r.Log(strings.Repeat("x", maxRecordedLogBytes))
		}
		for range maxRecordedLogEntries * 2 {
			r.Log("")
		}
		wantEntries := maxRecordedLogEntries + 1
		if rejectFirst {
			wantEntries-- // The earlier marker already consumed an entry.
		}
		if len(r.Logs) != wantEntries {
			t.Fatalf("rejectFirst=%t: got %d entries, want %d", rejectFirst, len(r.Logs), wantEntries)
		}
		markers, bytes := 0, 0
		for _, log := range r.Logs {
			if log == "Log truncated" {
				markers++
			} else {
				bytes += len(log)
			}
		}
		if markers != 1 || bytes != 0 || r.bytesWritten != 0 {
			t.Fatalf("rejectFirst=%t: markers=%d, content bytes=%d, accounted bytes=%d", rejectFirst, markers, bytes, r.bytesWritten)
		}
	}
}

func TestBoundedLogRecorderOwnsRetainedStrings(t *testing.T) {
	backing := strings.Repeat("x", 1<<20)
	message := backing[100:105]
	r := NewBoundedLogRecorder()
	r.Log(message)
	if len(r.Logs) != 1 || r.Logs[0] != message {
		t.Fatal("message was not retained")
	}
	if unsafe.StringData(r.Logs[0]) == unsafe.StringData(message) {
		t.Fatal("retained message still references the large source string")
	}
}

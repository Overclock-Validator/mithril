//go:build linux

package procctl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// /proc/<pid>/stat parser. The comm field can hold spaces and embedded parens
// (kernel writes the filename verbatim), so a naive whitespace split breaks.

func TestParseStatStartTime_TypicalLine(t *testing.T) {
	line := buildStatLine("bash", 1234567)
	v, err := parseStatStartTime([]byte(line))
	require.NoError(t, err)
	assert.Equal(t, uint64(1234567), v)
}

// comm with a space must not shift the state field.
func TestParseStatStartTime_CommWithSpaces(t *testing.T) {
	line := buildStatLine("my program", 9999999)
	v, err := parseStatStartTime([]byte(line))
	require.NoError(t, err)
	assert.Equal(t, uint64(9999999), v)
}

// Embedded parens in comm: parser must skip to the LAST ')'.
func TestParseStatStartTime_CommWithEmbeddedParens(t *testing.T) {
	line := buildStatLine("(weird) name)", 42)
	v, err := parseStatStartTime([]byte(line))
	require.NoError(t, err)
	assert.Equal(t, uint64(42), v)
}

// Kernel emits a trailing '\n'; parser tolerates it.
func TestParseStatStartTime_TrailingNewline(t *testing.T) {
	line := buildStatLine("bash", 100) + "\n"
	v, err := parseStatStartTime([]byte(line))
	require.NoError(t, err)
	assert.Equal(t, uint64(100), v)
}

func TestParseStatStartTime_MalformedNoParen(t *testing.T) {
	_, err := parseStatStartTime([]byte("totally not a stat line"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no closing paren")
}

// Truncated post-comm tail errors cleanly (no panic).
func TestParseStatStartTime_TooFewFields(t *testing.T) {
	_, err := parseStatStartTime([]byte("1 (bash) R 0 0"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fields")
}

// buildStatLine builds a synthetic /proc/<pid>/stat line. Non-starttime fields
// are zero so they can't accidentally match the value under test.
func buildStatLine(comm string, startTime uint64) string {
	// Whole-line field 22 (starttime) is post-paren index 19 (zero-based).
	post := []string{
		"R", // 3 state
		"0", // 4 ppid
		"0", // 5 pgrp
		"0", // 6 session
		"0", // 7 tty_nr
		"0", // 8 tpgid
		"0", // 9 flags
		"0", // 10 minflt
		"0", // 11 cminflt
		"0", // 12 majflt
		"0", // 13 cmajflt
		"0", // 14 utime
		"0", // 15 stime
		"0", // 16 cutime
		"0", // 17 cstime
		"0", // 18 priority
		"0", // 19 nice
		"0", // 20 num_threads
		"0", // 21 itrealvalue
		"",  // 22 starttime — filled below
	}
	post[19] = itoa(startTime)
	// Pad so a future bump in expected field count doesn't break these tests.
	for i := 0; i < 30; i++ {
		post = append(post, "0")
	}
	tail := joinSpaces(post)
	return "12345 (" + comm + ") " + tail
}

func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	buf := make([]byte, 0, 20)
	for v > 0 {
		buf = append([]byte{byte('0' + v%10)}, buf...)
		v /= 10
	}
	return string(buf)
}

func joinSpaces(parts []string) string {
	if len(parts) == 0 {
		return ""
	}
	out := parts[0]
	for _, p := range parts[1:] {
		out += " " + p
	}
	return out
}

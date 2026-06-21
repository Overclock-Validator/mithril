package config

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// maxUserInputLen caps a config value to bound TUI paste size.
const maxUserInputLen = 4096

// SanitizeUserInput cleans a TUI config value for single-line storage and terminal
// rendering: drops newlines (TOML injection) and ESC, keeps printable Unicode.
func SanitizeUserInput(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	n := 0
	for _, r := range s {
		// Drop control chars, Cf format chars (bidi overrides, zero-width,
		// BOM — invisible, enable display-spoofing), and invalid UTF-8.
		if r == utf8.RuneError || unicode.IsControl(r) || unicode.In(r, unicode.Cf) {
			continue
		}
		b.WriteRune(r)
		n++
		if n >= maxUserInputLen {
			break
		}
	}
	return b.String()
}

package config

import (
	"strings"
	"testing"
)

func TestSanitizeUserInput(t *testing.T) {
	rtl := string(rune(0x202e))  // RIGHT-TO-LEFT OVERRIDE
	zwsp := string(rune(0x200b)) // ZERO WIDTH SPACE
	bom := string(rune(0xFEFF))  // BYTE ORDER MARK / ZWNBSP
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"plain endpoint", "https://mainnet.helius-rpc.com/?api-key=abc123", "https://mainnet.helius-rpc.com/?api-key=abc123"},
		{"host:port", "203.0.113.10:8000", "203.0.113.10:8000"},
		{"ipv6", "[2001:db8::1]:8001", "[2001:db8::1]:8001"},
		{"keeps spaces", "foo bar", "foo bar"},
		{"strips trailing newline (paste)", "1.2.3.4:8000\n", "1.2.3.4:8000"},
		{"strips CR/LF", "1.2.3.4\r\n", "1.2.3.4"},
		{"strips tab", "a\tb", "ab"},
		// pasted newline must collapse to one line (TOML injection)
		{"blocks toml injection", "1.2.3.4:8000\nadmin_key = \"evil\"", "1.2.3.4:8000admin_key = \"evil\""},
		{"strips ESC/ANSI", "ip\x1b[31mRED\x1b[0m", "ip[31mRED[0m"},
		{"strips DEL and C0", "a\x7fb\x00c", "abc"},
		// bidi/invisible Unicode (Cf) — display-spoofing vectors
		{"strips RTL override", "1.2.3.4" + rtl + "X", "1.2.3.4X"},
		{"strips zero-width space", "rpc" + zwsp + ".evil.com", "rpc.evil.com"},
		{"strips BOM", bom + "host:8000", "host:8000"},
		{"empty", "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := SanitizeUserInput(c.in); got != c.want {
				t.Errorf("SanitizeUserInput(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

func TestSanitizeUserInput_NoControlCharsSurvive(t *testing.T) {
	// U+009B is a real C1 control (0xC2 0x9B), exercising the 0x80–0x9f branch.
	out := SanitizeUserInput("a\x1bb\nc\rd\te\x00f\x7fg" + string(rune(0x9b)) + "h")
	for _, r := range out {
		if r < 0x20 || r == 0x7f || (r >= 0x80 && r <= 0x9f) {
			t.Fatalf("control char %U survived sanitization in %q", r, out)
		}
	}
	if out != "abcdefgh" {
		t.Errorf("got %q, want %q", out, "abcdefgh")
	}
}

func TestSanitizeUserInput_CapsLength(t *testing.T) {
	in := strings.Repeat("x", maxUserInputLen+500)
	got := SanitizeUserInput(in)
	if len([]rune(got)) != maxUserInputLen {
		t.Errorf("length = %d, want capped at %d", len([]rune(got)), maxUserInputLen)
	}
}

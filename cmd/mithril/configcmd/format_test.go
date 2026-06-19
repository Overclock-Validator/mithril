package configcmd

import "testing"

// Canonicalizes `mithril config set` values: IPs/versions/host:port/inf/nan stay
// quoted strings; genuine ints/floats/bools/arrays pass through.
func TestFormatTOMLValue(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"int", "42", "42"},
		{"negative int", "-7", "-7"},
		{"float", "3.14", "3.14"},
		// Non-canonical floats are quoted, not rewritten (1.50 -> 1.5).
		{"non-canonical float quoted", "1.50", `"1.50"`},
		{"bool true", "true", "true"},
		{"bool false", "false", "false"},
		{"array passthrough", "[1, 2, 3]", "[1, 2, 3]"},

		// Numeric-looking values that must stay strings.
		{"ipv4 not float", "192.168.1.1", `"192.168.1.1"`},
		{"ipv4 leading octet", "203.0.113.10", `"203.0.113.10"`},
		{"version not float", "1.0.0", `"1.0.0"`},
		{"host:port not float", "0.0.0.0:8001", `"0.0.0.0:8001"`},
		{"float with trailing garbage", "1.5abc", `"1.5abc"`},

		// Non-finite floats must not become +Inf/NaN (invalid TOML).
		{"inf stays string", "inf", `"inf"`},
		{"Inf stays string", "Inf", `"Inf"`},
		{"+inf stays string", "+inf", `"+inf"`},
		{"nan stays string", "nan", `"nan"`},
		{"NaN stays string", "NaN", `"NaN"`},

		// Plain strings and control chars.
		{"plain string", "hello", `"hello"`},
		{"string with space", "hello world", `"hello world"`},
		{"newline escaped", "a\nb", `"a\nb"`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := formatTOMLValue(c.in); got != c.want {
				t.Errorf("formatTOMLValue(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

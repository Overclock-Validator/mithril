package mcp

import (
	"bytes"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/Overclock-Validator/mithril/internal/safedisplay"
)

const maxRedactedJSONKeyBytes = 512

// redactUntrustedText removes common credentials and strips URLs to their origin.
func redactUntrustedText(value string) string {
	return safedisplay.Text(value, sanitizeURLForDisplay)
}

func redactUntrustedMultilineWithTruncation(value string) (string, bool) {
	return safedisplay.MultilineWithTruncation(value, sanitizeURLForDisplay)
}

// truncateUTF8Bytes returns bounded valid UTF-8 and reports any content change.
func truncateUTF8Bytes(value string, maxBytes int) (string, bool) {
	if maxBytes < 0 {
		return value, false
	}
	normalized := strings.ToValidUTF8(value, "\uFFFD")
	changed := normalized != value
	if len(normalized) > maxBytes {
		normalized = normalized[:maxBytes]
		for !utf8.ValidString(normalized) {
			normalized = normalized[:len(normalized)-1]
		}
		changed = true
	}
	bounded := strings.TrimRight(normalized, "\x00")
	return bounded, changed || bounded != normalized
}

// redactRawJSON recursively sanitizes string values and object keys while
// preserving JSON numbers exactly. Invalid JSON is returned unchanged so the
// caller's normal validation path can report it.
func redactRawJSON(raw json.RawMessage) json.RawMessage {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return raw
	}
	redacted, err := json.Marshal(redactJSONValue(value))
	if err != nil {
		return raw
	}
	return redacted
}

func redactJSONValue(value any) any {
	switch value := value.(type) {
	case string:
		return redactUntrustedText(value)
	case []any:
		for i := range value {
			value[i] = redactJSONValue(value[i])
		}
		return value
	case map[string]any:
		keys := make([]string, 0, len(value))
		for key := range value {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		redacted := make(map[string]any, len(value))
		nextSuffix := make(map[string]int)
		for _, key := range keys {
			sensitive := len(key) > maxRedactedJSONKeyBytes || isSensitiveFieldName(key)
			safeKey := "[REDACTED]"
			if !sensitive {
				safeKey = redactUntrustedText(key)
			}
			base := safeKey
			if _, exists := redacted[safeKey]; exists {
				suffix := max(nextSuffix[base], 2)
				for {
					safeKey = base + "#" + strconv.Itoa(suffix)
					suffix++
					if _, exists := redacted[safeKey]; !exists {
						nextSuffix[base] = suffix
						break
					}
				}
			}
			if sensitive {
				redacted[safeKey] = "[REDACTED]"
			} else {
				redacted[safeKey] = redactJSONValue(value[key])
			}
		}
		return redacted
	default:
		return value
	}
}

func isSensitiveFieldName(name string) bool {
	return safedisplay.SensitiveName(name)
}

func isPlainSensitiveFieldName(name string) bool {
	return safedisplay.PlainSensitiveName(name)
}

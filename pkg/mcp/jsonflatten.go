package mcp

import (
	"encoding/json"
	"maps"
	"sort"
)

// splitExtra returns JSON fields not present in known.
func splitExtra(data []byte, known map[string]bool) (map[string]json.RawMessage, error) {
	var all map[string]json.RawMessage
	if err := json.Unmarshal(data, &all); err != nil {
		return nil, err
	}
	extra := map[string]json.RawMessage{}
	for k, v := range all {
		if !known[k] {
			extra[k] = v
		}
	}
	return extra, nil
}

// mergeExtra adds unknown fields back to a marshaled object.
func mergeExtra(named []byte, extra map[string]json.RawMessage) ([]byte, error) {
	var merged map[string]json.RawMessage
	if err := json.Unmarshal(named, &merged); err != nil {
		return nil, err
	}
	maps.Copy(merged, extra)
	return json.Marshal(merged)
}

// boundedExtraMetadata returns a stable, redacted inventory of omitted JSON
// fields without exposing their values. It is shared by bounded state and
// divergence summaries so both apply the same accounting rules.
func boundedExtraMetadata(extra map[string]json.RawMessage, maxNames, maxNameBytes int) (names []string, totalBytes int64, truncated bool) {
	names = make([]string, 0, len(extra))
	for name, raw := range extra {
		names = append(names, name)
		totalBytes += int64(len(name) + len(raw))
	}
	sort.Strings(names)
	truncated = len(names) > maxNames
	if truncated {
		names = names[:maxNames]
	}
	for i, name := range names {
		if len(name) > maxNameBytes {
			name = "[REDACTED]"
			truncated = true
		} else if isSensitiveFieldName(name) {
			name = "[REDACTED]"
		} else {
			name = redactUntrustedText(name)
		}
		names[i], _ = truncateUTF8Bytes(name, maxNameBytes)
	}
	return names, totalBytes, truncated
}

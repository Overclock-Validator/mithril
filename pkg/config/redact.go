package config

import (
	"net/url"
	"regexp"
	"strings"
)

var sensitiveEndpointAssignmentRE = regexp.MustCompile(`(?i)(^|[?&\s])([a-z0-9_.-]*(?:api[-_]?key|apikey|access[-_]?token|x[-_]?api[-_]?key|authorization|auth|token|secret|password|passwd|bearer|jwt)[a-z0-9_.-]*|key)=([^\s&"'<>;,)]+)`)
var sensitiveURLUserInfoRE = regexp.MustCompile(`(?i)\b((?:https?|wss?)://)[^\s/@]+@`)

// pathSecretRE matches a path segment that looks like an embedded credential
// (20+ token-ish chars) — some RPC providers put the key in the path, not a query.
var pathSecretRE = regexp.MustCompile(`^[A-Za-z0-9_-]{20,}$`)

// looksLikePathSecret reports whether a path segment is likely a secret token.
// 20+ token-charset path segment; over-redaction is harmless (display-only).
func looksLikePathSecret(seg string) bool {
	return pathSecretRE.MatchString(seg)
}

// urlInTextRE matches a URL token embedded in free-form log text.
var urlInTextRE = regexp.MustCompile(`(?i)(?:https?|wss?)://[^\s"'<>]+`)

// RedactEndpointForDisplay hides credentials in RPC URLs for display only;
// callers must keep using the original endpoint for network calls.
func RedactEndpointForDisplay(endpoint string) string {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return endpoint
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		return redactEndpointQueryFallback(endpoint)
	}

	if u.User != nil {
		u.User = url.User("REDACTED")
	}

	query, qErr := url.ParseQuery(u.RawQuery)
	if qErr != nil {
		// Malformed query: ParseQuery drops params silently, so regex-scrub the raw string.
		return RedactSecretsInText(endpoint)
	}
	changed := false
	for key := range query {
		if isSensitiveEndpointParam(key) {
			query[key] = []string{"REDACTED"}
			changed = true
		}
	}
	if changed {
		u.RawQuery = query.Encode()
	}

	// Redact path-embedded tokens (e.g. Alchemy /v2/<key>, rpcpool /<token>/).
	if u.Path != "" {
		segs := strings.Split(u.Path, "/")
		for i, seg := range segs {
			if looksLikePathSecret(seg) {
				segs[i] = "REDACTED"
			}
		}
		u.Path = strings.Join(segs, "/")
	}

	return u.String()
}

// RedactSecretsInText hides endpoint-style credentials in log lines without
// URL-encoding the surrounding text.
func RedactSecretsInText(text string) string {
	text = sensitiveEndpointAssignmentRE.ReplaceAllString(text, "${1}${2}=REDACTED")
	text = sensitiveURLUserInfoRE.ReplaceAllString(text, "${1}REDACTED@")
	// Also scrub path-embedded tokens inside any URL in the text (Alchemy /v2/<key>,
	// rpcpool /<token>/), which the query/userinfo regexes above don't catch.
	return urlInTextRE.ReplaceAllStringFunc(text, redactURLPathSecrets)
}

// redactURLPathSecrets redacts secret-looking path segments in a single URL,
// preserving any trailing punctuation that isn't part of the URL.
func redactURLPathSecrets(rawURL string) string {
	trailing := ""
	for len(rawURL) > 0 && strings.IndexByte(".,);]}", rawURL[len(rawURL)-1]) >= 0 {
		trailing = string(rawURL[len(rawURL)-1]) + trailing
		rawURL = rawURL[:len(rawURL)-1]
	}
	u, err := url.Parse(rawURL)
	if err != nil || u.Path == "" {
		return rawURL + trailing
	}
	segs := strings.Split(u.Path, "/")
	changed := false
	for i, seg := range segs {
		if looksLikePathSecret(seg) {
			segs[i] = "REDACTED"
			changed = true
		}
	}
	if !changed {
		return rawURL + trailing
	}
	u.Path = strings.Join(segs, "/")
	return u.String() + trailing
}

func isSensitiveEndpointParam(key string) bool {
	normalized := strings.ToLower(strings.ReplaceAll(key, "_", "-"))
	switch normalized {
	case "api-key", "apikey", "key", "token", "access-token", "auth", "authorization", "x-api-key", "passwd", "bearer", "jwt":
		return true
	default:
		return strings.Contains(normalized, "secret") ||
			strings.Contains(normalized, "password")
	}
}

func redactEndpointQueryFallback(endpoint string) string {
	// url.Parse failed, so regex-scrub user:pass@host and sensitive query keys
	// from the raw string instead.
	endpoint = sensitiveURLUserInfoRE.ReplaceAllString(endpoint, "${1}REDACTED@")
	queryStart := strings.Index(endpoint, "?")
	if queryStart == -1 {
		return endpoint
	}
	prefix := endpoint[:queryStart+1]
	query := endpoint[queryStart+1:]
	parts := strings.Split(query, "&")
	for i, part := range parts {
		key, _, found := strings.Cut(part, "=")
		if found && isSensitiveEndpointParam(key) {
			parts[i] = key + "=REDACTED"
		}
	}
	return prefix + strings.Join(parts, "&")
}

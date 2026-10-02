// Package safedisplay removes credentials and unsafe controls from diagnostic text.
package safedisplay

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	maxClassifiedAssignments       = 1024
	authorizationRedactedMarker    = "\x01"
	assignmentValuePattern         = `(?:"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|".*$|'.*$|\[REDACTED\][^\s,"';}:=\]]*|[^\s,"';}:=\]]+)`
	escapedValuePattern            = `(?:\\"(?:\\.|[^"\\])*\\"|\\".*$|` + assignmentValuePattern + `)`
	flexibleTokenPattern           = `t[-_\s]*o[-_\s]*k[-_\s]*e[-_\s]*n`
	flexibleSecretPattern          = `s[-_\s]*e[-_\s]*c[-_\s]*r[-_\s]*e[-_\s]*t`
	flexiblePasswordPattern        = `p[-_\s]*a[-_\s]*s[-_\s]*s[-_\s]*w[-_\s]*o[-_\s]*r[-_\s]*d`
	flexibleAuthPattern            = `a[-_\s]*u[-_\s]*t[-_\s]*h[-_\s]*o[-_\s]*r[-_\s]*i[-_\s]*z[-_\s]*a[-_\s]*t[-_\s]*i[-_\s]*o[-_\s]*n`
	flexibleCredentialPattern      = `c[-_\s]*r[-_\s]*e[-_\s]*d[-_\s]*e[-_\s]*n[-_\s]*t[-_\s]*i[-_\s]*a[-_\s]*l`
	flexibleAPIKeyPattern          = `a[-_\s]*p[-_\s]*i[-_\s]*k[-_\s]*e[-_\s]*y`
	flexibleAccessPattern          = `a[-_\s]*c[-_\s]*c[-_\s]*e[-_\s]*s[-_\s]*s`
	flexibleRefreshPattern         = `r[-_\s]*e[-_\s]*f[-_\s]*r[-_\s]*e[-_\s]*s[-_\s]*h`
	flexiblePrivatePattern         = `p[-_\s]*r[-_\s]*i[-_\s]*v[-_\s]*a[-_\s]*t[-_\s]*e`
	flexiblePrivPattern            = `p[-_\s]*r[-_\s]*i[-_\s]*v`
	flexibleSigningPattern         = `s[-_\s]*i[-_\s]*g[-_\s]*n[-_\s]*i[-_\s]*n[-_\s]*g`
	flexibleSignerPattern          = `s[-_\s]*i[-_\s]*g[-_\s]*n[-_\s]*e[-_\s]*r`
	flexibleWalletPattern          = `w[-_\s]*a[-_\s]*l[-_\s]*l[-_\s]*e[-_\s]*t`
	flexibleKeyPattern             = `k[-_\s]*e[-_\s]*y`
	flexiblePairPattern            = `p[-_\s]*a[-_\s]*i[-_\s]*r`
	flexibleSeedPattern            = `s[-_\s]*e[-_\s]*e[-_\s]*d`
	flexiblePhrasePattern          = `p[-_\s]*h[-_\s]*r[-_\s]*a[-_\s]*s[-_\s]*e`
	flexibleRecoveryPattern        = `r[-_\s]*e[-_\s]*c[-_\s]*o[-_\s]*v[-_\s]*e[-_\s]*r[-_\s]*y`
	flexibleMnemonicPattern        = `m[-_\s]*n[-_\s]*e[-_\s]*m[-_\s]*o[-_\s]*n[-_\s]*i[-_\s]*c`
	flexiblePassPattern            = `p[-_\s]*a[-_\s]*s[-_\s]*s`
	flexibleSigningMaterialPattern = `(?:(?:` + flexiblePrivatePattern + `|` + flexiblePrivPattern + `|` +
		flexibleSigningPattern + `|` + flexibleSignerPattern + `|` + flexibleWalletPattern + `)[-_\s]*(?:` +
		flexibleKeyPattern + `(?:[-_\s]*` + flexiblePairPattern + `)?|` + flexibleSeedPattern + `)|` +
		flexibleKeyPattern + `[-_\s]*` + flexiblePairPattern + `|` +
		flexibleMnemonicPattern + `(?:[-_\s]*` + flexiblePhrasePattern + `)?|` +
		flexibleSeedPattern + `[-_\s]*` + flexiblePhrasePattern + `|` +
		flexibleRecoveryPattern + `[-_\s]*` + flexiblePhrasePattern + `|` +
		flexiblePassPattern + `[-_\s]*` + flexiblePhrasePattern + `)`
	flexibleBareMarkerPattern      = `(?:` + flexibleTokenPattern + `|` + flexibleSecretPattern + `|` + flexiblePasswordPattern + `|` + flexibleAuthPattern + `|` + flexibleCredentialPattern + `|` + flexibleAPIKeyPattern + `|` + flexibleAccessPattern + `[-_\s]*` + flexibleTokenPattern + `|` + flexibleRefreshPattern + `[-_\s]*` + flexibleTokenPattern + `)`
	sensitiveAssignmentNamePattern = `(?:` + flexibleAPIKeyPattern + `|` +
		flexibleAccessPattern + `[-_\s]*` + flexibleTokenPattern + `|` +
		flexibleRefreshPattern + `[-_\s]*` + flexibleTokenPattern + `|` +
		flexibleTokenPattern + `[-.$\s]+balances?|[a-z0-9]*` + flexibleTokenPattern + `[-_.$\s]*(?:value|hash|id|key|credential|secret)|` +
		flexibleTokenPattern + `|` + flexibleSecretPattern + `|` + flexiblePasswordPattern + `|` +
		flexibleAuthPattern + `|` + flexibleCredentialPattern + `|` + flexibleSigningMaterialPattern + `)`
)

var (
	// Apostrophes are valid URL path characters, so they must remain inside the
	// redacted token. Stopping at one can expose a credential suffix.
	uriPattern                       = regexp.MustCompile(`(?i)[a-z][a-z0-9+.-]*://[^\s"<>]+`)
	uriPlaceholderPattern            = regexp.MustCompile(`\x00[0-9]+\x00`)
	flexibleTokenRegexp              = regexp.MustCompile(`(?i)` + flexibleTokenPattern)
	splitCredentialIdentifierPattern = regexp.MustCompile(`(?i)[a-z0-9_.-]*` + flexibleBareMarkerPattern + `(?:\s*[-_.$]+[a-z0-9_.-]+)+`)
	bearerPattern                    = regexp.MustCompile(`(?i)(authorization\s*[:=]\s*bearer\s+|bearer\s+)[^\s,;]+`)
	basicPattern                     = regexp.MustCompile(`(?i)(authorization\s*[:=]\s*basic\s+|basic\s+)[^\s,;]+`)
	// Authorization schemes may use RFC token punctuation, and their credentials
	// can contain spaces and commas. Once an assignment begins, consume the
	// remainder rather than leaking fragments after the first token.
	opaqueAuthorizationPattern     = regexp.MustCompile("(?i)(authorization\\s*[:=]\\s*)([!#$%&'*+.^_`|~0-9a-z-]+)(\\s+)(.*)$")
	escapedQuotedAssignmentPattern = regexp.MustCompile(`(\\"((?:\\.|[^"\\])*)\\"\s*(?:[:=]\s*)+)` + escapedValuePattern)
	doubleQuotedAssignmentPattern  = regexp.MustCompile(`("((?:\\.|[^"\\])*)"\s*(?:[:=]\s*)+)` + assignmentValuePattern)
	singleQuotedAssignmentPattern  = regexp.MustCompile(`('((?:\\.|[^'\\])*)'\s*(?:[:=]\s*)+)` + assignmentValuePattern)
	plainAssignmentPattern         = regexp.MustCompile(`(([^[:space:]"'{}\[\],;:=]+)\s*(?:[:=]\s*)+)` + assignmentValuePattern)
	secretPattern                  = regexp.MustCompile(`(?i)((` + sensitiveAssignmentNamePattern + `)\s*(?:[:=]\s*)+)` + assignmentValuePattern)
)

type assignmentClassifier struct {
	pattern             *regexp.Regexp
	decodeJSONKeyPasses int
	replacement         string
	requireKeyBoundary  bool
}

var assignmentClassifiers = [...]assignmentClassifier{
	{pattern: escapedQuotedAssignmentPattern, decodeJSONKeyPasses: 2, replacement: `\"[REDACTED]\"`},
	{pattern: doubleQuotedAssignmentPattern, decodeJSONKeyPasses: 1, replacement: `"[REDACTED]"`},
	{pattern: singleQuotedAssignmentPattern, decodeJSONKeyPasses: 1, replacement: `"[REDACTED]"`},
	{pattern: plainAssignmentPattern, decodeJSONKeyPasses: 1, replacement: "[REDACTED]"},
	{pattern: secretPattern, replacement: "[REDACTED]", requireKeyBoundary: true},
}

var sensitiveNameMarkers = []string{
	"authorization", "credential", "password", "secret", "apikey",
	"accesstoken", "refreshtoken", "bearertoken",
}

var signingMaterialMarkers = []string{
	"privkey", "signingkey", "signerkey", "walletkey",
	"mnemonic", "seedphrase", "recoveryphrase", "passphrase",
	"privateseed", "signingseed", "walletseed",
}

func signingMaterialMarker(normalized string) bool {
	for _, marker := range signingMaterialMarkers {
		if strings.Contains(normalized, marker) {
			return true
		}
	}
	return false
}

func signingMaterialName(normalized string) bool {
	return normalized == "privatekey" || normalized == "keypair" || signingMaterialMarker(normalized)
}

func signingMaterialFieldName(normalized string) bool {
	return strings.Contains(normalized, "privatekey") ||
		strings.Contains(normalized, "keypair") ||
		signingMaterialMarker(normalized)
}

func sensitiveAssignment(name, rawValue string, valueComplete bool) bool {
	if !SensitiveName(name) {
		return false
	}
	if !signingMaterialFieldName(normalizedName(name)) {
		return true
	}
	// Generic text has no trustworthy type information. A path, JSON suffix, or
	// base58-shaped value can be attacker-controlled signing material.
	value, ok := assignmentLiteral(rawValue)
	return !ok || value != "" || !valueComplete
}

func assignmentLiteral(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	switch {
	case len(raw) >= 4 && strings.HasPrefix(raw, `\"`) && strings.HasSuffix(raw, `\"`):
		value, err := strconv.Unquote(`"` + raw[2:len(raw)-2] + `"`)
		return value, err == nil
	case len(raw) >= 2 && raw[0] == '"':
		value, err := strconv.Unquote(raw)
		return value, err == nil
	case len(raw) >= 2 && raw[0] == '\'' && raw[len(raw)-1] == '\'':
		return raw[1 : len(raw)-1], true
	case strings.ContainsAny(raw, `"'`):
		return "", false
	default:
		return raw, true
	}
}

var escapedNameSeparators = strings.NewReplacer(`\n`, " ", `\r`, " ", `\t`, " ")

// Text sanitizes one logical line. sanitizeURL controls how callers render a
// URL origin; a nil function replaces the complete URL.
func Text(value string, sanitizeURL func(string) string) string {
	value, _ = textWithTruncation(value, sanitizeURL)
	return value
}

// textWithTruncation also reports when adversarial assignment density made
// the sanitizer discard a suffix rather than perform unbounded work.
func textWithTruncation(value string, sanitizeURL func(string) string) (string, bool) {
	value = strings.Map(func(r rune) rune {
		if unicode.In(r, unicode.Cf) {
			return -1
		}
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return ' '
		}
		return r
	}, value)
	if !uriPattern.MatchString(value) {
		return textWithoutURIs(value)
	}
	urls := make([]string, 0, 1)
	value = uriPattern.ReplaceAllStringFunc(value, func(raw string) string {
		schemeEnd := strings.IndexByte(raw, ':')
		if sanitizeURL != nil && schemeEnd > 0 &&
			(strings.EqualFold(raw[:schemeEnd], "http") || strings.EqualFold(raw[:schemeEnd], "https")) {
			urls = append(urls, sanitizeURL(raw))
		} else {
			urls = append(urls, "[REDACTED URL]")
		}
		return "\x00" + strconv.Itoa(len(urls)-1) + "\x00"
	})
	value, truncated := textWithoutURIs(value)
	value = uriPlaceholderPattern.ReplaceAllStringFunc(value, func(marker string) string {
		index, err := strconv.Atoi(marker[1 : len(marker)-1])
		if err != nil || index < 0 || index >= len(urls) {
			return "[REDACTED URL]"
		}
		return urls[index]
	})
	return strings.ReplaceAll(value, "\x00", " "), truncated
}

func textWithoutURIs(value string) (string, bool) {
	// Quote the intermediate marker so the generic assignment pass consumes it
	// as one value instead of leaving a second closing bracket behind.
	value = opaqueAuthorizationPattern.ReplaceAllString(value, `${1}"[REDACTED]"`)
	value = bearerPattern.ReplaceAllString(value, `${1}[REDACTED]`)
	value = basicPattern.ReplaceAllString(value, `${1}[REDACTED]`)
	truncated := false
	for _, classifier := range assignmentClassifiers {
		var limited bool
		value, limited = redactClassifiedAssignments(value, classifier)
		truncated = truncated || limited
	}
	value = SensitiveIdentifiers(value)
	return strings.ReplaceAll(value, authorizationRedactedMarker, "[REDACTED]"), truncated
}

// MultilineWithTruncation preserves LF boundaries and reports any line whose
// assignment scan was bounded.
func MultilineWithTruncation(value string, sanitizeURL func(string) string) (string, bool) {
	lines := strings.Split(value, "\n")
	truncated := false
	for i := range lines {
		var limited bool
		lines[i], limited = textWithTruncation(lines[i], sanitizeURL)
		truncated = truncated || limited
	}
	return strings.Join(lines, "\n"), truncated
}

// SensitiveName reports whether a field name describes credential material.
func SensitiveName(name string) bool {
	if knownTokenDataName(name) || knownTokenDomainIdentifier(name) {
		return false
	}
	name = escapedNameSeparators.Replace(name)
	normalized := normalizedName(name)
	if normalized == "jwt" {
		return true
	}
	if normalizedTokenDataName(normalized) || signingMaterialFieldName(normalized) {
		return true
	}
	for _, marker := range sensitiveNameMarkers {
		if strings.Contains(normalized, marker) {
			return true
		}
	}
	return strings.HasSuffix(normalized, "token") || containsDelimitedToken(name) || containsTokenCredentialQualifier(normalized)
}

// PlainSensitiveName reports whether the complete name is a credential field.
func PlainSensitiveName(name string) bool {
	switch normalizedName(name) {
	case "authorization", "credential", "password", "secret", "apikey",
		"accesstoken", "refreshtoken", "bearertoken", "token", "jwt":
		return true
	// Keep bare signing terms readable in prose while redacting assigned values.
	case "privatekey", "privkey", "keypair", "signingkey", "signerkey",
		"walletkey", "mnemonic", "seedphrase", "recoveryphrase", "passphrase",
		"privateseed", "signingseed", "walletseed":
		return true
	default:
		return false
	}
}

// SensitiveIdentifiers redacts standalone credential-bearing identifiers.
func SensitiveIdentifiers(value string) string {
	value = splitCredentialIdentifierPattern.ReplaceAllStringFunc(value, func(identifier string) string {
		canonical := flexibleTokenRegexp.ReplaceAllString(identifier, "token")
		if sensitiveBareName(canonical) && !PlainSensitiveName(canonical) {
			return "[REDACTED]"
		}
		return identifier
	})
	var output strings.Builder
	start := -1
	last := 0
	for index, r := range value {
		if displayIdentifierRune(r) {
			if start < 0 {
				start = index
			}
			continue
		}
		if start >= 0 {
			output.WriteString(value[last:start])
			identifier := value[start:index]
			if sensitiveBareName(identifier) && !PlainSensitiveName(identifier) {
				output.WriteString("[REDACTED]")
			} else {
				output.WriteString(identifier)
			}
			last = index
			start = -1
		}
	}
	if start >= 0 {
		output.WriteString(value[last:start])
		identifier := value[start:]
		if sensitiveBareName(identifier) && !PlainSensitiveName(identifier) {
			output.WriteString("[REDACTED]")
		} else {
			output.WriteString(identifier)
		}
		last = len(value)
	}
	if last == 0 {
		return value
	}
	output.WriteString(value[last:])
	return output.String()
}

func sensitiveBareName(name string) bool {
	if knownTokenDataName(name) || knownTokenDomainIdentifier(name) || knownTokenDataAssignment(name) {
		return false
	}
	name = escapedNameSeparators.Replace(name)
	normalized := normalizedName(name)
	if normalizedTokenDataName(normalized) || signingMaterialName(normalized) {
		return true
	}
	for _, marker := range sensitiveNameMarkers {
		if strings.Contains(normalized, marker) {
			return true
		}
	}
	return strings.HasSuffix(normalized, "token") || containsTokenCredentialQualifier(normalized) || containsOpaqueTokenSuffix(name)
}

func knownTokenDomainIdentifier(name string) bool {
	name = strings.Trim(name, ".,:;!?()")
	switch strings.ToLower(name) {
	case "p-token", "spl-token", "token-account", "replacespltokenwithptoken":
		return true
	default:
		return false
	}
}

func normalizedName(name string) string {
	name = escapedNameSeparators.Replace(name)
	return strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return unicode.ToLower(r)
		}
		return -1
	}, name)
}

// knownTokenDataName recognizes public SPL balance fields after trimming only
// edge punctuation.
func knownTokenDataName(name string) bool {
	name = strings.Trim(name, ".,:;!?()")
	switch strings.ToLower(name) {
	case "token_balance", "tokenbalance", "token_balances", "tokenbalances",
		"spl_token_balance", "spltokenbalance", "spl_token_balances", "spltokenbalances",
		"pretokenbalances", "posttokenbalances", "uitokenamount":
		return true
	default:
		return false
	}
}

// knownTokenDataAssignment recognizes an exact public field followed by ':'.
// Using the last separator keeps multi-colon secret-shaped names rejected.
func knownTokenDataAssignment(name string) bool {
	i := strings.LastIndex(name, ":")
	return i >= 0 && knownTokenDataName(name[:i]) && !sensitiveBareName(name[i+1:])
}

func normalizedTokenDataName(normalized string) bool {
	switch normalized {
	case "tokenbalance", "tokenbalances", "spltokenbalance", "spltokenbalances",
		"pretokenbalances", "posttokenbalances", "uitokenamount":
		return true
	default:
		return false
	}
}

func containsDelimitedToken(name string) bool {
	for i := 0; i+len("token") <= len(name); i++ {
		if !asciiTokenAt(name, i) {
			continue
		}
		left := true
		if i > 0 {
			r, _ := utf8.DecodeLastRuneInString(name[:i])
			left = !unicode.IsLetter(r) && !unicode.IsDigit(r)
		}
		end := i + len("token")
		right := true
		if end < len(name) {
			r, _ := utf8.DecodeRuneInString(name[end:])
			right = !unicode.IsLetter(r) && !unicode.IsDigit(r)
		}
		if left && right {
			return true
		}
	}
	return false
}

func containsTokenCredentialQualifier(normalized string) bool {
	for _, qualifier := range []string{"value", "hash", "id", "key", "credential", "secret"} {
		if strings.Contains(normalized, "token"+qualifier) {
			return true
		}
	}
	return false
}

func containsOpaqueTokenSuffix(name string) bool {
	for i := 0; i+len("token") <= len(name); i++ {
		if !asciiTokenAt(name, i) {
			continue
		}
		end := i + len("token")
		if i > 0 {
			r, _ := utf8.DecodeLastRuneInString(name[:i])
			if unicode.IsLetter(r) || unicode.IsDigit(r) {
				continue
			}
		}
		// A credential suffix must be separated from the word "token". This
		// excludes public identifiers such as Solana's Tokenkeg program ID.
		if end >= len(name) || asciiNameByte(name[end]) {
			continue
		}
		for end < len(name) && !asciiNameByte(name[end]) {
			end++
		}
		hasDigit := false
		hasUpper := false
		length := 0
		for end < len(name) {
			if asciiNameByte(name[end]) {
				hasDigit = hasDigit || name[end] >= '0' && name[end] <= '9'
				hasUpper = hasUpper || name[end] >= 'A' && name[end] <= 'Z'
				length++
			}
			end++
		}
		if length >= 8 && (hasDigit || hasUpper || length >= 16) {
			return true
		}
	}
	return false
}

func redactClassifiedAssignments(value string, classifier assignmentClassifier) (string, bool) {
	var output strings.Builder
	last := 0
	searchAt := 0
	matchesSeen := 0
	changed := false
	for searchAt < len(value) {
		match := classifier.pattern.FindStringSubmatchIndex(value[searchAt:])
		if match == nil {
			break
		}
		for i := range match {
			if match[i] >= 0 {
				match[i] += searchAt
			}
		}
		matchesSeen++
		if matchesSeen > maxClassifiedAssignments {
			output.WriteString(value[last:match[0]])
			output.WriteString("[REDACTED]")
			return output.String(), true
		}
		rawKey := value[match[4]:match[5]]
		if classifier.requireKeyBoundary && !keyStartsAtBoundary(value, match[4], rawKey) {
			searchAt = match[5]
			continue
		}
		key := rawKey
		for range classifier.decodeJSONKeyPasses {
			var decoded string
			if json.Unmarshal([]byte(`"`+key+`"`), &decoded) != nil || decoded == key {
				break
			}
			key = decoded
		}
		rawValue := value[match[3]:match[1]]
		valueComplete := match[1] == len(value) || displayValueDelimiter(value[match[1]])
		if !sensitiveAssignment(key, rawValue, valueComplete) {
			searchAt = match[3]
			if searchAt <= match[0] {
				searchAt = match[1]
			}
			continue
		}
		normalizedKey := normalizedName(key)
		if normalizedKey == "authorization" && rawValue == authorizationRedactedMarker {
			searchAt = match[1]
			continue
		}
		quotedValue := quotedAssignmentValue(rawValue)
		matchEnd := extendSensitiveValue(value, match[1], quotedValue)
		if (normalizedKey == "authorization" && !quotedValue) ||
			signingMaterialFieldName(normalizedKey) ||
			structuredSensitiveValue(rawValue) ||
			structuredSensitiveContinuation(value, match[1]) {
			matchEnd = len(value)
		}
		output.WriteString(value[last:match[4]])
		if PlainSensitiveName(key) && PlainSensitiveName(rawKey) {
			output.WriteString(value[match[4]:match[5]])
		} else {
			output.WriteString("[REDACTED]")
		}
		output.WriteString(value[match[5]:match[3]])
		replacement := classifier.replacement
		if normalizedKey == "authorization" && replacement == "[REDACTED]" {
			replacement = authorizationRedactedMarker
		}
		output.WriteString(replacement)
		last = matchEnd
		searchAt = matchEnd
		changed = true
	}
	if !changed {
		return value, false
	}
	output.WriteString(value[last:])
	return output.String(), false
}

func keyStartsAtBoundary(value string, start int, key string) bool {
	if start == 0 {
		return true
	}
	r, _ := utf8.DecodeLastRuneInString(value[:start])
	hasSpace := strings.IndexFunc(key, unicode.IsSpace) >= 0
	if unicode.IsLetter(r) || unicode.IsDigit(r) {
		return hasSpace
	}
	return r != '_' && r != '-' || hasSpace
}

func structuredSensitiveValue(raw string) bool {
	raw = strings.TrimSpace(raw)
	if strings.HasPrefix(raw, "{") {
		return true
	}
	if !strings.HasPrefix(raw, "[") {
		return false
	}
	if !strings.HasPrefix(raw, "[REDACTED]") {
		return true
	}
	suffix := strings.TrimPrefix(raw, "[REDACTED]")
	return strings.HasPrefix(suffix, "[") || strings.HasPrefix(suffix, "{")
}

func structuredSensitiveContinuation(value string, end int) bool {
	return end < len(value) && (value[end] == '[' || value[end] == '{')
}

func quotedAssignmentValue(value string) bool {
	return strings.HasPrefix(value, `\"`) || len(value) > 0 && (value[0] == '"' || value[0] == '\'')
}

func extendSensitiveValue(value string, end int, initiallyQuoted bool) int {
	if end >= len(value) {
		return end
	}
	if displayValueDelimiter(value[end]) {
		return end
	}
	if (value[end] == '"' || value[end] == '\'') && !initiallyQuoted {
		if end+1 >= len(value) || displayValueDelimiter(value[end+1]) {
			return end
		}
	}
	for end < len(value) && !displayValueDelimiter(value[end]) {
		end++
	}
	return end
}

func displayValueDelimiter(value byte) bool {
	switch value {
	case ' ', '\t', '\r', '\n', ',', ';', '}', ']':
		return true
	default:
		return false
	}
}

func displayIdentifierRune(r rune) bool {
	return !unicode.IsSpace(r) && !unicode.IsControl(r) && !strings.ContainsRune(`"'{}[]=,`, r)
}

func asciiTokenAt(value string, offset int) bool {
	if offset+len("token") > len(value) {
		return false
	}
	for index, want := range []byte("token") {
		got := value[offset+index]
		if got >= 'A' && got <= 'Z' {
			got += 'a' - 'A'
		}
		if got != want {
			return false
		}
	}
	return true
}

func asciiNameByte(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9'
}

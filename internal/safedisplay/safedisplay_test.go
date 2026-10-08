package safedisplay

import (
	"strings"
	"testing"
)

// publicTokenFieldNames are SPL/Solana field names that carry no credential and
// must survive display intact. Redacting them corrupts legitimate output.
var publicTokenFieldNames = []string{
	"token_balance", "tokenBalance", "token_balances", "tokenBalances",
	"spl_token_balance", "splTokenBalance", "spl_token_balances", "splTokenBalances",
	"preTokenBalances", "postTokenBalances", "uiTokenAmount",
}

// credentialNames must always be treated as sensitive. These pin the safety
// half so a precision fix cannot quietly weaken redaction.
//
// Scope note: these are the HTTP/API credential names the package started with.
// Signing material — private_key, keypair, mnemonic, seed_phrase and friends —
// was added later and is covered separately by signingMaterial below, because
// those values span the delimiters that bound an ordinary value. The two lists
// stay apart so the public-key precision cases in publicKeyFieldNames keep
// guarding against widening this to anything containing "key".
var credentialNames = []string{
	"authorization", "api_key", "apiKey", "secret", "password",
	"access_token", "refresh_token", "bearer_token", "jwt", "JWT",
}

// assignmentForms renders one key/value pair in the shapes credentials actually
// appear in: shell-style, log-style, JSON, and spaced variants.
func assignmentForms(key, value string) []string {
	return []string{
		key + "=" + value,
		key + ": " + value,
		key + ":" + value,
		key + " = " + value,
		`"` + key + `": "` + value + `"`,
		`"` + key + `":"` + value + `"`,
		"prefix text " + key + "=" + value + " suffix text",
	}
}

func TestTextRedactsCredentialAssignments(t *testing.T) {
	const secret = "SUPERSECRETVALUE1234"

	for _, key := range credentialNames {
		for _, line := range assignmentForms(key, secret) {
			got := Text(line, nil)
			if strings.Contains(got, secret) {
				t.Errorf("Text leaked a credential\n key: %s\n  in: %q\n out: %q", key, line, got)
			}
		}
	}
}

func TestTextPreservesPublicValues(t *testing.T) {
	cases := map[string]string{
		"slot":          "435409407",
		"epoch":         "1007",
		"block_height":  "420000000",
		"token_balance": "1500000",
		"status":        "ok",
	}
	for key, value := range cases {
		for _, line := range assignmentForms(key, value) {
			got := Text(line, nil)
			if !strings.Contains(got, value) {
				t.Errorf("Text dropped a public value\n key: %s\n  in: %q\n out: %q", key, line, got)
			}
		}
	}
}

// Exact public token-balance fields remain visible across assignment forms.
func TestTextPreservesUndelimitedPublicAssignments(t *testing.T) {
	for _, input := range []string{
		"token_balance:1500000",
		"token_balance: 1500000",
		"token_balance=1500000",
		"tokenBalance:1500000",
		"uiTokenAmount:1500000",
		"preTokenBalances:1500000",
		"token_balance: aBc123XYZdef",
		"token_balance:aBc123XYZdef",
	} {
		if got := Text(input, nil); got != input {
			t.Errorf("public token field altered\n in: %q\nout: %q", input, got)
		}
	}

	// Surrounding evidence must survive too, not just the pair in isolation.
	const line = "token_balance:1500000, slot:435409407"
	if got := Text(line, nil); got != line {
		t.Errorf("public log line altered\n in: %q\nout: %q", line, got)
	}
}

func TestSensitiveBareNameExemptionIsExact(t *testing.T) {
	exempt := []string{
		"token_balance:1500000",
		"token_balance:aBc123XYZdef",
		"tokenbalances:1500000",
		"uitokenamount:1500000",
	}
	for _, name := range exempt {
		if sensitiveBareName(name) {
			t.Errorf("sensitiveBareName(%q) = true, want false: public field:value pair redacted", name)
		}
	}

	// Only an exact public field name before the first ':' may exempt. These
	// were sensitive before this exemption existed and must stay sensitive.
	notExempt := []string{
		"token_balancer:aBc123XYZdef",    // near-miss key
		"token_balance_x:aBc123XYZdef",   // near-miss key
		"api_key:token_balance:aBc123XY", // public name after the first ':'
	}
	for _, name := range notExempt {
		if !sensitiveBareName(name) {
			t.Errorf("sensitiveBareName(%q) = false, want true: near-miss of a public field was exempted", name)
		}
	}
	if knownTokenDataAssignment("token_balance") {
		t.Error(`knownTokenDataAssignment("token_balance") = true: exemption must require a ':'`)
	}
}

func TestTextStillRedactsNearMissesOfPublicFields(t *testing.T) {
	const secret = "SUPERSECRETVALUE1234"
	for _, input := range []string{
		"token_balance:apiKey" + secret,
		"token_balancer:" + secret,   // near-miss key
		"token_balances_x:" + secret, // near-miss key
		"api_key:token_balance:" + secret,
		"xtoken_balance:" + secret,
		"token_" + secret,
	} {
		if got := Text(input, nil); strings.Contains(got, secret) {
			t.Errorf("near-miss of a public field leaked\n in: %q\nout: %q", input, got)
		}
	}
}

// Credential assignments must not survive display sanitization.
func FuzzTextNeverLeaksCredentialValues(f *testing.F) {
	const secret = "SUPERSECRETVALUE1234"

	for _, seed := range []string{"", " ", "log line: ", "level=info msg=", "\t", "{", `{"a":1,`} {
		f.Add(seed, "")
	}

	f.Fuzz(func(t *testing.T, prefix, suffix string) {
		// The secret appearing in the surrounding text is the caller's own
		// doing, not a redaction failure.
		if strings.Contains(prefix, secret) || strings.Contains(suffix, secret) {
			t.Skip()
		}

		for _, key := range credentialNames {
			for _, form := range assignmentForms(key, secret) {
				line := prefix + form + suffix
				if got := Text(line, nil); strings.Contains(got, secret) {
					t.Fatalf("Text leaked a credential\n key: %s\n  in: %q\n out: %q", key, line, got)
				}
			}
		}
	})
}

func TestSensitiveNameRedactsCredentials(t *testing.T) {
	for _, name := range credentialNames {
		if !SensitiveName(name) {
			t.Errorf("SensitiveName(%q) = false, want true: a credential field was not redacted", name)
		}
	}
}

func TestSensitiveNameExemptsPublicTokenFields(t *testing.T) {
	for _, name := range publicTokenFieldNames {
		if SensitiveName(name) {
			t.Errorf("SensitiveName(%q) = true, want false: a public SPL field was redacted", name)
		}
	}
}

func TestSensitiveNameIgnoresTrailingPunctuationOnPublicFields(t *testing.T) {
	for _, name := range publicTokenFieldNames {
		for _, punct := range []string{":", ".", ",", ";", "!", "?", ")", "():"} {
			decorated := name + punct
			if SensitiveName(decorated) {
				t.Errorf("SensitiveName(%q) = true, want false: punctuation turned a public field into a redaction", decorated)
			}
		}
	}
}

func FuzzSensitiveNamePunctuationIsIrrelevantForPublicFields(f *testing.F) {
	for _, seed := range []string{":", ".", ",", ";", "!", "?", "(", ")", "():", "...", "", " ", "\t"} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, punct string) {
		// Only punctuation is in scope. Letters or digits would genuinely change
		// the name, and whitespace is handled by a different code path.
		for _, r := range punct {
			if !strings.ContainsRune(".,:;!?()", r) {
				t.Skip()
			}
		}

		for _, name := range publicTokenFieldNames {
			for _, decorated := range []string{name + punct, punct + name, punct + name + punct} {
				if SensitiveName(decorated) {
					t.Fatalf("punctuation changed the decision: SensitiveName(%q) = true, but SensitiveName(%q) = %v",
						decorated, name, SensitiveName(name))
				}
			}
		}
	})
}

func FuzzSensitiveNameNeverExemptsCredentials(f *testing.F) {
	for _, seed := range []string{"", ":", "_", "-", ".", "x", "1", "  "} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, suffix string) {
		for _, name := range []string{"api_key", "secret", "password", "authorization"} {
			decorated := name + suffix
			// A suffix can legitimately create a different word ("secretary"),
			// so only assert where the credential root is still delimited.
			if suffix != "" && (isAlphanumeric(rune(suffix[0]))) {
				continue
			}
			if !SensitiveName(decorated) {
				t.Fatalf("decoration defeated redaction: SensitiveName(%q) = false", decorated)
			}
		}
	})
}

func isAlphanumeric(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
}

// Signing material can contain spaces or comma-separated bytes.
var signingMaterial = map[string]string{
	"mnemonic":        "fake-one fake-two fake-three fake-four fake-five fake-six",
	"seed_phrase":     "fake-one fake-two fake-three fake-four fake-five fake-six",
	"seedPhrase":      "fake-one fake-two fake-three fake-four fake-five fake-six",
	"recovery_phrase": "fake-one fake-two fake-three fake-four fake-five fake-six",
	"passphrase":      "fake-one fake-two fake-three fake-four",
	"private_key":     "[12,34,56,78,90,11,22,33,44,55,66,77,88,99,10,20]",
	"privateKey":      "[12,34,56,78,90,11,22,33,44,55,66,77,88,99,10,20]",
	"priv_key":        "[12,34,56,78,90,11,22,33,44,55,66,77,88,99,10,20]",
	"keypair":         "[12,34,56,78,90,11,22,33,44,55,66,77,88,99,10,20]",
	"key_pair":        "[12,34,56,78,90,11,22,33,44,55,66,77,88,99,10,20]",
	"signing_key":     "[12,34,56,78,90,11,22,33,44,55,66,77,88,99,10,20]",
	"signer_key":      "[12,34,56,78,90,11,22,33,44,55,66,77,88,99,10,20]",
	"wallet_key":      "[12,34,56,78,90,11,22,33,44,55,66,77,88,99,10,20]",
	"private_seed":    "PRIVATE-SEED-VALUE",
	"privateSeed":     "PRIVATE-SEED-VALUE",
	"signing_seed":    "SIGNING-SEED-VALUE",
	"wallet_seed":     "WALLET-SEED-VALUE",
}

// publicKeyFieldNames must stay readable. On Solana most *_key fields hold a
// public key, and redacting them would destroy the evidence an operator reads.
var publicKeyFieldNames = []string{
	"pubkey", "public_key", "publicKey", "vote_pubkey", "vote_account",
	"identity", "node_pubkey", "authorized_voter", "staker", "withdrawer",
}

func TestTextRedactsSigningMaterialEntirely(t *testing.T) {
	for key, secret := range signingMaterial {
		for _, line := range []string{
			key + "=" + secret,
			key + ": " + secret,
			key + "=" + secret + " slot=435409407",
			"loading wallet " + key + "=" + secret,
		} {
			got := Text(line, nil)
			// Check pieces so partial redaction cannot pass.
			for _, piece := range strings.FieldsFunc(secret, func(r rune) bool {
				return r == ' ' || r == ',' || r == '[' || r == ']'
			}) {
				if len(piece) < 2 {
					continue // single digits recur in unrelated numbers
				}
				if strings.Contains(got, piece) {
					t.Errorf("signing material survived redaction\n key: %s\npiece: %q\n  in: %q\n out: %q",
						key, piece, line, got)
					break
				}
			}
		}
	}
}

func TestTextRedactsQualifiedSigningMaterialFields(t *testing.T) {
	const secret = "SUPERSECRETVALUE1234"
	for _, key := range []string{
		"senderPrivateKey",
		"wallet_private_key",
		"validator_private_key",
		"privateKeyBytes",
		"walletKeypair",
	} {
		if !SensitiveName(key) {
			t.Errorf("SensitiveName(%q) = false, want true", key)
		}
		for _, line := range assignmentForms(key, secret) {
			if got := Text(line, nil); strings.Contains(got, secret) {
				t.Errorf("qualified signing field leaked\n key: %s\n  in: %q\n out: %q", key, line, got)
			}
		}
	}
}

func TestTextPreservesPublicKeyFields(t *testing.T) {
	const pubkey = "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin"
	for _, key := range publicKeyFieldNames {
		line := key + "=" + pubkey
		if got := Text(line, nil); !strings.Contains(got, pubkey) {
			t.Errorf("public key field was redacted\n key: %s\n out: %q", key, got)
		}
	}
	// A public field holding an array must not be consumed by the bracket rule.
	const arrayLine = "observed_slots=[1,2,3] slot=42"
	if got := Text(arrayLine, nil); got != arrayLine {
		t.Errorf("public array field altered\n in: %q\nout: %q", arrayLine, got)
	}
}

func TestTextResistsMarkerPrefixEvasionOnBracketedValues(t *testing.T) {
	for _, key := range []string{"token", "api_key", "keypair", "private_key", "password"} {
		for _, line := range []string{
			key + "=[REDACTED]Q7x8R9",
			key + "=[]Q7x8R9",
			key + "=[REDACTED]Q7x8R9 slot=42",
		} {
			if got := Text(line, nil); strings.Contains(got, "Q7x8R9") {
				t.Errorf("marker-prefix evasion leaked the value\n in: %q\nout: %q", line, got)
			}
		}
	}
	if got := Text("keypair=[12,34,56", nil); strings.Contains(got, "34") {
		t.Errorf("unterminated keypair array leaked bytes: %q", got)
	}
}

func TestTextRedactsMalformedSigningValues(t *testing.T) {
	tests := []struct {
		input     string
		forbidden []string
	}{
		{input: "keypair=[[12,34],[56,78]]", forbidden: []string{"12", "34", "56", "78"}},
		{input: `keypair=""[12,34]`, forbidden: []string{"12", "34"}},
		{input: `keypair={"kind":"raw","data":"Q7x8R9"}`, forbidden: []string{"Q7x8R9"}},
		{input: "private key=PRIVATE-KEY-VALUE", forbidden: []string{"PRIVATE-KEY-VALUE"}},
		{input: "signing key=SIGNING-KEY-VALUE", forbidden: []string{"SIGNING-KEY-VALUE"}},
		{input: "seed phrase=fake-one fake-two", forbidden: []string{"fake-one", "fake-two"}},
		{input: "private seed=PRIVATE-SEED-VALUE", forbidden: []string{"PRIVATE-SEED-VALUE"}},
		{input: "wallet seed=WALLET-SEED-VALUE", forbidden: []string{"WALLET-SEED-VALUE"}},
	}
	for _, test := range tests {
		got := Text(test.input, nil)
		for _, forbidden := range test.forbidden {
			if strings.Contains(got, forbidden) {
				t.Errorf("signing material survived redaction\n in: %q\nout: %q", test.input, got)
			}
		}
	}
}

func TestTextRedactsStructuredCredentialValues(t *testing.T) {
	tests := []struct {
		input     string
		forbidden []string
	}{
		{input: "token=[[12,34],[56,78]]", forbidden: []string{"12", "34", "56", "78"}},
		{input: `credential=""[90,11]`, forbidden: []string{"90", "11"}},
		{input: `"token":{"kind":"raw","data":"DOUBLE-KEY-VALUE"}`, forbidden: []string{"DOUBLE-KEY-VALUE"}},
		{input: `'token':{"kind":"raw","data":"SINGLE-KEY-VALUE"}`, forbidden: []string{"SINGLE-KEY-VALUE"}},
		{input: `{\"token\":{\"kind\":\"raw\",\"data\":\"ESCAPED-KEY-VALUE\"}}`, forbidden: []string{"ESCAPED-KEY-VALUE"}},
		{input: `api key={"kind":"raw","data":"MULTIWORD-KEY-VALUE"}`, forbidden: []string{"MULTIWORD-KEY-VALUE"}},
	}
	for _, test := range tests {
		got := Text(test.input, nil)
		for _, forbidden := range test.forbidden {
			if strings.Contains(got, forbidden) {
				t.Errorf("credential survived redaction\n in: %q\nout: %q", test.input, got)
			}
		}
	}

	keys := append([]string{"token", "credential"}, credentialNames...)
	for _, key := range keys {
		input := key + `={"kind":"raw","data":"OBJECT-VALUE"}`
		if got := Text(input, nil); strings.Contains(got, "OBJECT-VALUE") {
			t.Errorf("structured %s value survived redaction: %q", key, got)
		}
	}

	const scalar = "token=SCALAR-VALUE slot=42 status=ok"
	got := Text(scalar, nil)
	if strings.Contains(got, "SCALAR-VALUE") || !strings.Contains(got, "slot=42 status=ok") {
		t.Errorf("scalar boundary was not preserved: %q", got)
	}
}

func TestTextPreservesBareSeedField(t *testing.T) {
	const input = "seed=public-seed-label"
	if SensitiveName("seed") {
		t.Fatal("bare seed field was classified as signing material")
	}
	if got := Text(input, nil); got != input {
		t.Fatalf("bare seed field changed: %q", got)
	}
}

// These repository messages use signing words as prose, not values.
var mithrilVocabulary = []string{
	`authorized_voter_keypair = ""`,
	`authorized_withdrawer_keypair = ""`,
	"Set [validator] identity_keypair, vote_account_keypair, and advertised_ip",
	"keep the authorized withdrawer keypair OFFLINE; it is not needed at runtime",
	"panic: solana.PrivateKey invalid length",
	"ed25519.PrivateKeySize mismatch",
	"unknown mnemonic for opcode 0x85",
	"Generate a validator config (consensus.mode=validator with required keypair/socket fields)",
}

func TestTextPreservesMithrilVocabulary(t *testing.T) {
	for _, line := range mithrilVocabulary {
		if got := Text(line, nil); got != line {
			t.Errorf("real Mithril output was altered\n in: %q\nout: %q", line, got)
		}
	}
}

func TestSensitiveNameTreatsSigningPathFieldsAsStructuredSecrets(t *testing.T) {
	for _, name := range []string{
		"identity_keypair",
		"vote_account_keypair",
		"authorized_voter_keypair",
		"authorized_withdrawer_keypair",
	} {
		if !SensitiveName(name) {
			t.Errorf("SensitiveName(%q) = false, want true", name)
		}
	}
}

func TestTextSigningFieldsRedactEveryNonemptyValue(t *testing.T) {
	fields := []string{
		"identity_keypair",
		"vote_account_keypair",
		"authorized_voter_keypair",
		"authorized_withdrawer_keypair",
	}
	values := []struct {
		name      string
		value     string
		forbidden []string
	}{
		{name: "byte_array", value: "[12,34,56,78]", forbidden: []string{"12", "34", "56", "78"}},
		{name: "encoded_secret", value: strings.Repeat("A", 88), forbidden: []string{strings.Repeat("A", 88)}},
		{name: "slash_secret", value: strings.Repeat("A", 40) + "/" + strings.Repeat("B", 40), forbidden: []string{strings.Repeat("B", 40)}},
		{name: "absolute_path", value: `"/run/mithril/ABSOLUTE-SECRET.json"`, forbidden: []string{"ABSOLUTE-SECRET"}},
		{name: "relative_path", value: `"./RELATIVE-SECRET.json"`, forbidden: []string{"RELATIVE-SECRET"}},
		{name: "parent_path", value: `"../PARENT-SECRET.json"`, forbidden: []string{"PARENT-SECRET"}},
		{name: "home_path", value: `"~/HOME-SECRET.json"`, forbidden: []string{"HOME-SECRET"}},
		{name: "windows_path", value: `"C:\mithril\WINDOWS-SECRET.json"`, forbidden: []string{"WINDOWS-SECRET"}},
		{name: "json_suffix", value: "validator-JSON-SECRET.json", forbidden: []string{"JSON-SECRET"}},
		{name: "base58", value: "11111111111111111111111111111111", forbidden: []string{"11111111111111111111111111111111"}},
		{name: "empty_quote_prefix", value: `""EMPTY-PREFIX-SECRET`, forbidden: []string{"EMPTY-PREFIX-SECRET"}},
		{name: "empty_apostrophe_prefix", value: `''APOSTROPHE-PREFIX-SECRET`, forbidden: []string{"APOSTROPHE-PREFIX-SECRET"}},
	}
	for _, field := range fields {
		for _, test := range values {
			t.Run(field+"_"+test.name, func(t *testing.T) {
				line := field + "=" + test.value
				got := Text(line, nil)
				for _, forbidden := range test.forbidden {
					if strings.Contains(got, forbidden) {
						t.Fatalf("signing material survived redaction\n in: %q\nout: %q", line, got)
					}
				}
			})
		}
	}

	for _, line := range []string{
		`identity_keypair=""`,
		`vote_account_keypair=''`,
		`authorized_voter_keypair = ""`,
		`authorized_withdrawer_keypair=""`,
	} {
		if got := Text(line, nil); got != line {
			t.Errorf("empty signing field was altered\n in: %q\nout: %q", line, got)
		}
	}
}

package mcp

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"
)

func assertRedaction(t *testing.T, got string, forbidden, required []string) {
	t.Helper()
	for _, value := range forbidden {
		if strings.Contains(got, value) {
			t.Errorf("forbidden value %q leaked in %q", value, got)
		}
	}
	for _, value := range required {
		if !strings.Contains(got, value) {
			t.Errorf("required value %q missing from %q", value, got)
		}
	}
}

func TestRedactUntrustedText(t *testing.T) {
	tests := []struct {
		name, input, wantPrefix string
		forbidden, required     []string
	}{
		{
			name:      "mixed secrets",
			input:     "rpc HTTPS://user:pass@example.com/PATH_SECRET?api-key=QUERY_SECRET#fragment Authorization: Bearer BEARER_SECRET token=TOKEN_SECRET Authorization: Basic BASIC_SECRET {\"token\":\"JSON_SECRET\"}",
			forbidden: []string{"user", "pass", "PATH_SECRET", "QUERY_SECRET", "fragment", "BEARER_SECRET", "TOKEN_SECRET", "BASIC_SECRET", "JSON_SECRET"},
			required:  []string{"https://example.com"},
		},
		{
			name:      "escaped quotes in JSON-like text",
			input:     `backend said {"token":"PREFIX\\\"SECRET_SUFFIX","safe":"visible"}`,
			forbidden: []string{"PREFIX", "SECRET_SUFFIX"}, required: []string{`"safe":"visible"`},
		},
		{
			name:      "opaque authorization and credential assignments",
			input:     `custom{credential="CREDENTIAL SECRET",authorization="OPAQUE_AUTH_SECRET",node="safe"} credential=UNQUOTED_CREDENTIAL authorization=OPAQUE_AUTH`,
			forbidden: []string{"CREDENTIAL SECRET", "OPAQUE_AUTH_SECRET", "UNQUOTED_CREDENTIAL", "OPAQUE_AUTH"}, required: []string{`node="safe"`},
		},
		{name: "API key authorization", input: "request Authorization: ApiKey TOP_SECRET", wantPrefix: "request Authorization:", forbidden: []string{"TOP_SECRET"}},
		{name: "punctuated authorization", input: "request Authorization: Foo+Bar PUNCTUATED_SECRET", wantPrefix: "request Authorization:", forbidden: []string{"PUNCTUATED_SECRET"}},
		{name: "dotted authorization", input: "request Authorization: Foo.Bar DOTTED_SECRET", wantPrefix: "request Authorization:", forbidden: []string{"DOTTED_SECRET"}},
		{name: "Digest authorization", input: "request authorization=Digest username=alice, response=DIGEST_SECRET", wantPrefix: "request authorization=", forbidden: []string{"alice", "DIGEST_SECRET"}},
		{name: "AWS authorization", input: "request Authorization: AWS4-HMAC-SHA256 Credential=AWS_SECRET, SignedHeaders=host, Signature=SIGNATURE_SECRET", wantPrefix: "request Authorization:", forbidden: []string{"AWS_SECRET", "SIGNATURE_SECRET"}},
		{
			name:      "control-separated field names",
			input:     "api\nkey=CONTROL_SPLIT_SECRET access\ttoken=ACCESS_SPLIT_SECRET refresh\r\ntoken=REFRESH_SPLIT_SECRET",
			forbidden: []string{"CONTROL_SPLIT_SECRET", "ACCESS_SPLIT_SECRET", "REFRESH_SPLIT_SECRET"},
		},
		{
			name:      "format controls",
			input:     "before\u202eafter middle\u2028end token=Q7x8R9",
			forbidden: []string{"\u202e", "\u2028", "Q7x8R9"},
			required:  []string{"beforeafter middle end", "token=[REDACTED]"},
		},
		{
			name:      "separators inside credential words",
			input:     "to\u200bken=Q7x8R9 pass\tword=A1b2C3 se\rcret=D4e5F6 clientSe\u200bcret=M4n5P6 creden\u2028tial=K1m2N3 status=session_to\u200bken_Z7y8X9w0 label=clientSe\u200bcret_V6w7X8y9 edge=session_token\u200b_W1v2U3t4 tabedge=session_token\t_T5s6R7q8 author\u2060ization=Custom G7h8J9",
			forbidden: []string{"Q7x8R9", "A1b2C3", "D4e5F6", "M4n5P6", "K1m2N3", "session_to", "Z7y8X9w0", "V6w7X8y9", "W1v2U3t4", "T5s6R7q8", "G7h8J9"},
		},
		{
			name:      "non-HTTP credential URIs",
			input:     "postgres://user:Q7x8R9@example/db redis://:A1b2C3@host custom://foo_token_ABC123XYZ",
			forbidden: []string{"user", "Q7x8R9", "A1b2C3", "foo_token_ABC123XYZ"},
			required:  []string{"[REDACTED URL]"},
		},
		{
			name:      "escaped control-separated field names",
			input:     `api\nkey=ESCAPED_N_SECRET access\ttoken=ESCAPED_T_SECRET`,
			forbidden: []string{"ESCAPED_N_SECRET", "ESCAPED_T_SECRET"},
		},
		{
			name:      "marker-prefixed secret values",
			input:     `token=[REDACTED]Q7x8R9 password=[REDACTED]A1b2C3 Authorization: [REDACTED] M4n5P6 "clientSecret":[REDACTED]D7e8F9 'clientSecret':[REDACTED]G1h2J3`,
			forbidden: []string{"Q7x8R9", "A1b2C3", "M4n5P6", "D7e8F9", "G1h2J3"},
			required:  []string{"token=[REDACTED]", "password=[REDACTED]", "Authorization: [REDACTED]"},
		},
		{
			name:      "structural bracket after secret",
			input:     "token=Q7x8R9]",
			forbidden: []string{"Q7x8R9"},
			required:  []string{"token=[REDACTED]]"},
		},
		{
			name:      "nested assignment in safe value",
			input:     `safe="foo_token_ABC123=Q7x8R9y0"`,
			forbidden: []string{"foo_token_ABC123", "Q7x8R9y0"},
			required:  []string{`safe="[REDACTED]=[REDACTED]"`},
		},
		{
			name:      "escaped nested assignment",
			input:     `note="{\"client\u0053ecret\":\"Q7x8R9y0\"}"`,
			forbidden: []string{`client\u0053ecret`, "Q7x8R9y0"},
			required:  []string{`{\"[REDACTED]\":\"[REDACTED]\"}`},
		},
		{
			name:      "escaped key with direct quoted values",
			input:     `note={\"clientSecret\":'Q7x8R9'} other={\"clientSecret\":"A1b2C3"}`,
			forbidden: []string{"Q7x8R9", "A1b2C3"},
		},
		{
			name:      "single-quoted escaped key",
			input:     `{'client\u0053ecret':'Q7x8R9y0'}`,
			forbidden: []string{`client\u0053ecret`, "Q7x8R9y0"},
		},
		{
			name:      "plain escaped key",
			input:     `client\u0053ecret=Q7x8R9y0 api\u0020key=A1b2C3d4`,
			forbidden: []string{`client\u0053ecret`, "Q7x8R9y0", `api\u0020key`, "A1b2C3d4"},
		},
		{
			name:      "unquoted separator and quote suffixes",
			input:     `token=Q7x8=R9y0 password=A1b2:C3d4 credential=E5f6'G7h8 secret=J9k0"L1m2 api key=N3p4=Q5r6`,
			forbidden: []string{"Q7x8", "R9y0", "A1b2", "C3d4", "E5f6", "G7h8", "J9k0", "L1m2", "N3p4", "Q5r6"},
			required:  []string{"token=[REDACTED]", "password=[REDACTED]", "credential=[REDACTED]", "secret=[REDACTED]", "api key=[REDACTED]"},
		},
		{
			name:      "URI assignment continuations",
			input:     `token=https://example.com/"Q7x8R9 password=https://example.com/<A1b2C3 url="https://example.com/private?api-key=D4e5F6"`,
			forbidden: []string{"Q7x8R9", "A1b2C3", "private", "D4e5F6"},
			required:  []string{"token=[REDACTED]", "password=[REDACTED]", `url="https://example.com:443/"`},
		},
		{
			name:      "closed quote continuations",
			input:     `token="Q7x8"R9y0 password='A1b2'=C3d4 note="{\"clientSecret\":\"E5f6\"G7h8}"`,
			forbidden: []string{"Q7x8", "R9y0", "A1b2", "C3d4", "E5f6", "G7h8"},
		},
		{
			name:      "noncanonical token balance fields",
			input:     "token-balance=Q7x8R9 t_o_k_e_n_b_a_l_a_n_c_e=A1b2C3 token\nbalance=D4e5F6",
			forbidden: []string{"Q7x8R9", "A1b2C3", "D4e5F6"},
		},
		{
			name:      "repeated separators",
			input:     "token==Q7x8R9 password=:A1b2C3 credential:=D4e5F6 token= : G7h8J9 api key: = K1m2N3",
			forbidden: []string{"Q7x8R9", "A1b2C3", "D4e5F6", "G7h8J9", "K1m2N3"},
		},
		{
			name:      "separator-prefixed compound keys",
			input:     "x-api key=Q7x8R9 prefix_api key=A1b2C3",
			forbidden: []string{"Q7x8R9", "A1b2C3"},
			required:  []string{"x-api key=[REDACTED]", "prefix_api key=[REDACTED]"},
		},
		{
			name:      "unterminated double-quoted value",
			input:     `token="Q7x'R8y\`,
			forbidden: []string{"Q7x", "R8y"},
		},
		{
			name:      "unterminated single-quoted value",
			input:     `password='A7b"C8d\`,
			forbidden: []string{"A7b", "C8d"},
		},
		{
			name:      "quoted compound field names",
			input:     `{"clientSecret":"CLIENTVALUE987","rpc_api_key":"RPCVALUE987","sessionTokenValue":"SESSIONVALUE987","token$value":"DOLLARVALUE987","client\qSecret":"MALFORMEDVALUE987","escapedSecret":'PREFIX\'SECRET_SUFFIX',"preTokenBalances":"visible","uiTokenAmount":"visible"} {'clientSecret':'SINGLEVALUE987'}`,
			forbidden: []string{"CLIENTVALUE987", "RPCVALUE987", "SESSIONVALUE987", "DOLLARVALUE987", "MALFORMEDVALUE987", "PREFIX", "SECRET_SUFFIX", "SINGLEVALUE987"},
			required:  []string{`"preTokenBalances":"visible"`, `"uiTokenAmount":"visible"`},
		},
		{
			name:      "signing material references",
			input:     `identity_keypair="/run/mithril/ABSOLUTE-SECRET.json" vote_account_keypair=11111111111111111111111111111111 authorized_voter_keypair=validator-JSON-SECRET.json private_key="C:\mithril\WINDOWS-SECRET.json"`,
			forbidden: []string{"ABSOLUTE-SECRET", "11111111111111111111111111111111", "JSON-SECRET", "WINDOWS-SECRET"},
		},
		{
			name:      "nested signing array",
			input:     `keypair=[[12,34],[56,78]]`,
			forbidden: []string{"12", "34", "56", "78"},
		},
		{
			name:      "quoted prefix before signing array",
			input:     `keypair=""[90,11]`,
			forbidden: []string{"90", "11"},
		},
		{
			name:      "object signing value",
			input:     `keypair={"kind":"raw","data":"OBJECT-VALUE"}`,
			forbidden: []string{"OBJECT-VALUE"},
		},
		{
			name:      "multiword private key",
			input:     `private key=PRIVATE-KEY-VALUE`,
			forbidden: []string{"PRIVATE-KEY-VALUE"},
		},
		{
			name:      "multiword signing key",
			input:     `signing key=SIGNING-KEY-VALUE`,
			forbidden: []string{"SIGNING-KEY-VALUE"},
		},
		{
			name:      "multiword seed phrase",
			input:     `seed phrase=fake-one fake-two`,
			forbidden: []string{"fake-one", "fake-two"},
		},
		{
			name:      "nested credential array",
			input:     `token=[[12,34],[56,78]]`,
			forbidden: []string{"12", "34", "56", "78"},
		},
		{
			name:      "object credential value",
			input:     `password={"kind":"raw","data":"OBJECT-VALUE"}`,
			forbidden: []string{"OBJECT-VALUE"},
		},
		{
			name:      "quoted prefix before credential array",
			input:     `credential=""[90,11]`,
			forbidden: []string{"90", "11"},
		},
		{
			name:      "credential scalar keeps safe suffix",
			input:     `token=SCALAR-VALUE slot=42 status=ok`,
			forbidden: []string{"SCALAR-VALUE"},
			required:  []string{"slot=42 status=ok"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := redactUntrustedText(test.input)
			assertRedaction(t, got, test.forbidden, test.required)
			if test.wantPrefix != "" && !strings.HasPrefix(got, test.wantPrefix) {
				t.Fatalf("authorization field context was lost: %q", got)
			}
		})
	}
}

func TestRedactSensitiveIdentifiers(t *testing.T) {
	got := redactUntrustedText("foo_token_CREDENTIAL123 token_ABC123XYZ session_token_ABC123XYZ αtokenβ token_balance spl_token_balance")
	assertRedaction(t, got, []string{"foo_token_CREDENTIAL123", "token_ABC123XYZ", "session_token_ABC123XYZ"}, []string{"[REDACTED]", "αtokenβ", "token_balance", "spl_token_balance"})
}

func TestRedactUntrustedTextPreservesTokenDomainProse(t *testing.T) {
	input := "ReplaceSplTokenWithPToken. p-token. spl-token: token-account " +
		"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA " +
		"TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
	if got := redactUntrustedText(input); got != input {
		t.Fatalf("token-domain prose changed: %q", got)
	}
	const rawURL = "https://token-observabilityservice.example.com/private?api-key=Q7x8R9"
	got := redactUntrustedText("session_token_A1b2C3d4(" + rawURL + ")")
	assertRedaction(t, got,
		[]string{"session_token_A1b2C3d4", "private", "Q7x8R9"},
		[]string{"[REDACTED]", "https://token-observabilityservice.example.com:443/"},
	)
}

func TestRedactUntrustedTextBoundsDenseAssignments(t *testing.T) {
	input := strings.Repeat("safe=0 ", 2048) + "clientSecret=DENSE_SECRET"
	got := redactUntrustedText(input)
	assertRedaction(t, got, []string{"DENSE_SECRET"}, []string{"[REDACTED]"})
	if len(got) >= len(input) {
		t.Fatalf("dense assignment suffix was not bounded: got %d bytes, input %d", len(got), len(input))
	}
}

func TestRedactUntrustedTextHandlesDenseURLs(t *testing.T) {
	const count = 512
	input := strings.Repeat("https://user:Q7x8R9@example.com/private?api-key=A1b2C3 ", count)
	got := redactUntrustedText(input)
	assertRedaction(t, got, []string{"user", "Q7x8R9", "private", "A1b2C3"}, nil)
	if origins := strings.Count(got, "https://example.com:443/"); origins != count {
		t.Fatalf("sanitized origins = %d, want %d", origins, count)
	}
}

func TestRedactionIsIdempotent(t *testing.T) {
	inputs := []string{
		"token=PLAIN_SECRET",
		"token=PLAIN_SECRET]",
		"token=[REDACTED]TOKEN_SUFFIX",
		`safe="foo_token_ABC123=NESTED_VALUE_SECRET"`,
		`note="{\"client\u0053ecret\":\"ESCAPED_NESTED_SECRET\"}"`,
		`token=EQUAL_PREFIX=EQUAL_SUFFIX`,
		`password=COLON_PREFIX:COLON_SUFFIX`,
		`credential=APOSTROPHE_PREFIX'APOSTROPHE_SUFFIX`,
		`secret=QUOTE_PREFIX"QUOTE_SUFFIX`,
		`{"clientSecret":"QUOTED_SECRET"}`,
		`{"clientSecret":'SINGLE_SECRET'}`,
		"Authorization: Bearer BEARER_SECRET",
		"Authorization: Custom OPAQUE_SECRET",
		"Authorization: [REDACTED] Q7x8R9",
		"rpc=https://user:pass@example.com/path?api-key=URL_SECRET",
	}
	for _, input := range inputs {
		once := redactUntrustedText(input)
		if twice := redactUntrustedText(once); twice != once {
			t.Errorf("redaction is not idempotent for %q: once %q, twice %q", input, once, twice)
		}
	}

	multiline := "# HELP first token=FIRST_SECRET\n# HELP second Authorization: Bearer SECOND_SECRET\n"
	once, truncated := redactUntrustedMultilineWithTruncation(multiline)
	if truncated {
		t.Fatal("short multiline input was marked truncated")
	}
	if twice, twiceTruncated := redactUntrustedMultilineWithTruncation(once); twice != once || twiceTruncated {
		t.Errorf("multiline redaction is not idempotent: once %q, twice %q", once, twice)
	}
}

func TestRedactRawJSON(t *testing.T) {
	tests := []struct {
		name, raw           string
		forbidden, required []string
	}{
		{
			name:      "sensitive keys",
			raw:       `{"token":"TOPSECRET","nested":{"api_key":"KEYSECRET","password":12345},"safe":"visible"}`,
			forbidden: []string{"TOPSECRET", "KEYSECRET", "12345"}, required: []string{`"safe":"visible"`},
		},
		{
			name:      "preserves numbers",
			raw:       `{"lamports":9007199254740993,"nested":{"url":"https://rpc.example/PATH?token=SECRET"}}`,
			forbidden: []string{"PATH", "SECRET"}, required: []string{"9007199254740993"},
		},
		{
			name:      "preserves Solana token-balance fields",
			raw:       `{"preTokenBalances":[{"uiTokenAmount":{"amount":"42"}}],"postTokenBalances":[],"token_balance":"visible","foo_token_ABC123XYZ":"OPAQUEVALUE987","token":"TOPSECRET","token_value":"CREDVALUE987654","sessionTokenHash":"HASHVALUE987654","token$value":"DOLLARVALUE987654"}`,
			forbidden: []string{"OPAQUEVALUE987", "TOPSECRET", "CREDVALUE987654", "HASHVALUE987654", "DOLLARVALUE987654"},
			required:  []string{`"preTokenBalances"`, `"postTokenBalances"`, `"uiTokenAmount"`, `"amount":"42"`, `"token_balance":"visible"`},
		},
		{
			name:      "sanitizes object keys",
			raw:       `{"[REDACTED]":"visible","api\nkey_SUPERHIDDEN123":0,"api_key_KEY_NAME_SECRET":1,"https://user:pass@rpc.example.com/KEY_PATH?token=KEY_QUERY":2,"nested":{"refresh_token_NESTED_SECRET":3},"line\nbreak":4}`,
			forbidden: []string{"SUPERHIDDEN123", "KEY_NAME_SECRET", "user", "pass", "KEY_PATH", "KEY_QUERY", "NESTED_SECRET", `line\nbreak`},
			required:  []string{`"[REDACTED]":"visible"`, `"[REDACTED]#2":"[REDACTED]"`, `"line break":4`},
		},
		{
			name: "redacts structured signing material",
			raw:  `{"identity_keypair":"/run/mithril/identity.json","vote_account_keypair":"11111111111111111111111111111111","authorized_voter_keypair":"validator-keypair.json","authorized_withdrawer_keypair":{"bytes":[56,78]},"private_key":"C:\\mithril\\identity.json","private_seed":"PRIVATE-SEED-VALUE","signingSeed":"SIGNING-SEED-VALUE","wallet_seed":"WALLET-SEED-VALUE","seed":"public-seed-label"}`,
			forbidden: []string{"/run/mithril/identity.json", "11111111111111111111111111111111",
				"validator-keypair.json", "56", "78", `C:\\mithril\\identity.json`,
				"PRIVATE-SEED-VALUE", "SIGNING-SEED-VALUE", "WALLET-SEED-VALUE"},
			required: []string{`"[REDACTED]":"[REDACTED]"`, `"[REDACTED]#4":"[REDACTED]"`, `"seed":"public-seed-label"`},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertRedaction(t, string(redactRawJSON(json.RawMessage(test.raw))), test.forbidden, test.required)
		})
	}
}

func TestRedactRawJSONKeepsCollidingKeys(t *testing.T) {
	const count = 2048
	input := make(map[string]any, count)
	for i := range count {
		input[fmt.Sprintf("api_key_%04d", i)] = i
	}
	raw, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	var output map[string]any
	if err := json.Unmarshal(redactRawJSON(raw), &output); err != nil {
		t.Fatal(err)
	}
	if len(output) != count {
		t.Fatalf("redacted keys = %d, want %d", len(output), count)
	}
	if _, ok := output[fmt.Sprintf("[REDACTED]#%d", count)]; !ok {
		t.Fatal("colliding keys did not receive stable suffixes")
	}
}

func TestRedactRawJSONBoundsObjectKeys(t *testing.T) {
	key := strings.Repeat("x", maxRedactedJSONKeyBytes+1)
	raw, err := json.Marshal(map[string]any{key: "visible"})
	if err != nil {
		t.Fatal(err)
	}
	got := string(redactRawJSON(raw))
	if strings.Contains(got, key) || got != `{"[REDACTED]":"[REDACTED]"}` {
		t.Fatalf("oversized JSON key was not bounded: %s", got)
	}
}

func TestBoundedExtraMetadataRedactsSensitiveFieldNames(t *testing.T) {
	names, _, _ := boundedExtraMetadata(map[string]json.RawMessage{
		"api_key_TOPSECRET": json.RawMessage(`1`),
		"password_suffix":   json.RawMessage(`2`),
		"safe_field":        json.RawMessage(`3`),
	}, 10, 128)
	joined := strings.Join(names, ",")
	if strings.Contains(joined, "TOPSECRET") || strings.Contains(joined, "password") {
		t.Fatalf("sensitive omitted field name leaked: %q", names)
	}
	if !strings.Contains(joined, "[REDACTED]") || !strings.Contains(joined, "safe_field") {
		t.Fatalf("omitted field inventory lost safe metadata: %q", names)
	}
}

func TestTruncateUTF8Bytes(t *testing.T) {
	got, truncated := truncateUTF8Bytes("ab😀cd", 5)
	if !truncated || !utf8.ValidString(got) || got != "ab" {
		t.Fatalf("got %q truncated=%v", got, truncated)
	}
	if got, truncated := truncateUTF8Bytes("short", 5); truncated || got != "short" {
		t.Fatalf("exact limit changed: %q %v", got, truncated)
	}
	invalid := "ab" + string([]byte{0xff}) + "cdefgh"
	got, truncated = truncateUTF8Bytes(invalid, 8)
	if !truncated || !utf8.ValidString(got) || !strings.HasSuffix(got, "cde") {
		t.Fatalf("invalid interior byte discarded suffix: %q truncated=%v", got, truncated)
	}
}

package cluster

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type mainnetTestTransport func(*http.Request) (*http.Response, error)

func (f mainnetTestTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestMainnetRPCSourceAllowlist(t *testing.T) {
	now := time.Now().UTC()
	c := collector{http: &http.Client{Transport: mainnetTestTransport(func(*http.Request) (*http.Response, error) {
		r := httptest.NewRecorder()
		r.WriteHeader(http.StatusForbidden)
		return r.Result(), nil
	})}, rpcURL: "https://example.invalid/", now: func() time.Time { return now }, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	s, err := c.collectMainnet(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		source string
		valid  bool
	}{
		{"https://api.mainnet-beta.solana.com", true},
		{"https://solana-rpc.publicnode.com", true},
		{"https://solana-rpc.publicnode.com/?api-key=secret", false},
		{"https://localhost/", false},
	} {
		provider(&s, "rpc").SourceURL = tc.source
		if valid := Validate(s) == nil; valid != tc.valid {
			t.Errorf("source %q accepted=%v, want %v", tc.source, valid, tc.valid)
		}
	}
}

func TestMainnetRejectedProviderIsUnavailable(t *testing.T) {
	now := time.Now().UTC()
	calls := 0
	c := collector{http: &http.Client{Transport: mainnetTestTransport(func(*http.Request) (*http.Response, error) {
		calls++
		r := httptest.NewRecorder()
		r.WriteHeader(http.StatusForbidden)
		return r.Result(), nil
	})}, rpcURL: "https://example.invalid/", now: func() time.Time { return now }, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	s, err := c.collectMainnet(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || len(s.Validators) != 0 || provider(&s, "rpc").State != "unavailable" || provider(&s, "rpc").Reason != "rpc_unavailable" {
		t.Fatalf("wrong rejection handling: calls=%d snapshot=%+v", calls, s)
	}
}

func TestMainnetRowsRejectInvalidInputs(t *testing.T) {
	valid := mainnetVote{Identity: testIdentities[0], Vote: testVotes[0]}
	for _, tc := range []struct {
		name string
		rows []mainnetVote
	}{
		{"duplicate vote account", []mainnetVote{valid, valid}},
		{"malformed epoch credits", []mainnetVote{{Identity: valid.Identity, Vote: valid.Vote, Credits: [][]uint64{{1, 2}}}}},
		{"decreasing epoch credits", []mainnetVote{{Identity: valid.Identity, Vote: valid.Vote, Credits: [][]uint64{{1, 2, 3}}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := mainnetRows(tc.rows, []mainnetVote{}, nil, 1); err == nil {
				t.Fatal("invalid input was accepted")
			}
		})
	}
}

func TestMainnetCollectionAndNetworkBinding(t *testing.T) {
	if MainnetGenesisHash != "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d" {
		t.Fatal("incorrect mainnet genesis hash")
	}
	for _, tc := range []struct {
		name          string
		firstGenesis  string
		secondGenesis string
		rollover      bool
		wantReason    string
	}{
		{"good", MainnetGenesisHash, MainnetGenesisHash, false, ""},
		{"wrong network", GenesisHash, GenesisHash, false, "rpc_genesis_mismatch"},
		{"network changes", MainnetGenesisHash, GenesisHash, false, "rpc_genesis_recheck_failed"},
		{"epoch changes", MainnetGenesisHash, MainnetGenesisHash, true, "rpc_epoch_changed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			genesisCalls, epochCalls := 0, 0
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req struct {
					Method string `json:"method"`
				}
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Fatal(err)
				}
				var value any
				switch req.Method {
				case "getGenesisHash":
					genesisCalls++
					value = tc.firstGenesis
					if genesisCalls > 1 {
						value = tc.secondGenesis
					}
				case "getEpochInfo":
					epochCalls++
					e := 1
					if epochCalls > 1 && tc.rollover {
						e++
					}
					value = map[string]any{"epoch": e, "absoluteSlot": 115, "slotIndex": 15, "slotsInEpoch": 100}
				case "getVoteAccounts":
					value = map[string]any{"current": []any{map[string]any{"votePubkey": testVotes[0], "nodePubkey": testIdentities[0], "activatedStake": uint64(9007199254740993), "commission": 3, "lastVote": 110, "rootSlot": 109, "epochCredits": [][3]uint64{{1, 1100, 1000}}}}, "delinquent": []any{map[string]any{"votePubkey": testVotes[1], "nodePubkey": testIdentities[1], "activatedStake": 0, "commission": 100}}}
				case "getClusterNodes":
					value = []any{map[string]any{"pubkey": testIdentities[0], "version": "3.1.0", "gossip": "8.8.8.8:8001"}, map[string]any{"pubkey": testIdentities[1], "gossip": "10.1.2.3:8001"}}
				default:
					t.Errorf("unexpected method %s", req.Method)
					w.WriteHeader(400)
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": value})
			})
			client := &http.Client{Transport: mainnetTestTransport(func(r *http.Request) (*http.Response, error) {
				recorder := httptest.NewRecorder()
				handler.ServeHTTP(recorder, r)
				return recorder.Result(), nil
			})}
			now := time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)
			c := collector{http: client, rpcURL: "https://example.invalid/", now: func() time.Time { return now }, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
			s, err := c.collectMainnet(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if p := provider(&s, "rpc"); p.Reason != tc.wantReason {
				t.Fatalf("reason %q, want %q", p.Reason, tc.wantReason)
			}
			if tc.wantReason != "" {
				if len(s.Validators) != 0 {
					t.Fatal("unbound rows published")
				}
				return
			}
			if len(s.Validators) != 2 || *s.Validators[0].StakeSOL != "9007199.254740993" || *s.Validators[0].CurrentEpochCredits != "100" || s.Validators[0].Version != "3.1.0" || *s.Validators[0].Delinquent || !*s.Validators[1].Delinquent {
				t.Fatalf("wrong rows: %+v", s.Validators)
			}
			if s.Validators[0].SkipRatePercent != nil || len(s.Validators[0].Inclusion) != 0 {
				t.Fatal("unsupported metrics published")
			}
			if s.Validators[0].CountryCode != "US" || s.Validators[0].CountrySource != "dbip" || provider(&s, "geoip").State != "fresh" {
				t.Fatal("local country estimate not published")
			}
			if s.Validators[1].CountryCode != "" {
				t.Fatal("private gossip address received a country")
			}
			b, err := json.Marshal(s)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(b), "8.8.8.8") || strings.Contains(string(b), "10.1.2.3") {
				t.Fatal("gossip address published")
			}
		})
	}
}

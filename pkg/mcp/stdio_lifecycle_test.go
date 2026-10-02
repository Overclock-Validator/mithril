package mcp

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// connectStdioSession starts the MCP server as a real subprocess over stdio and
// returns a connected session. It mirrors TestMCPStdioSubprocessE2E's harness,
// so these lifecycle tests exercise the actual transport.
func connectStdioSession(t *testing.T) (*mcpsdk.ClientSession, *syncTelemetryBuffer) {
	t.Helper()

	fixture := newStdioE2EFixture(t)
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("resolve test executable: %v", err)
	}
	cmd := exec.Command(executable, "-test.run=^TestMCPStdioHelperProcess$")
	cmd.Env = append([]string(nil), fixture.env...)
	// os/exec copies the subprocess's stderr from its own goroutine, so a bare
	// bytes.Buffer read from the test goroutine is a data race.
	telemetry := &syncTelemetryBuffer{}
	cmd.Stderr = telemetry

	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	t.Cleanup(cancel)

	client := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "mithril-lifecycle", Version: "1"}, nil)
	session, err := client.Connect(ctx, &mcpsdk.CommandTransport{Command: cmd, TerminateDuration: 5 * time.Second}, nil)
	if err != nil {
		t.Fatalf("connect to stdio subprocess: %v; stderr=%s", err, telemetry.String())
	}
	t.Cleanup(func() { _ = session.Close() })
	return session, telemetry
}

// assertSessionStillHealthy proves the server survived whatever just happened.
// This is the real assertion in both tests below: rejecting bad input is only
// correct if the connection remains usable afterwards. A server that rejects a
// call by dying takes the user's whole session with it.
func assertSessionStillHealthy(t *testing.T, session *mcpsdk.ClientSession, after string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	listed, err := session.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("session unusable after %s: %v", after, err)
	}
	if len(listed.Tools) == 0 {
		t.Fatalf("tool catalog empty after %s", after)
	}
	result, err := session.CallTool(ctx, &mcpsdk.CallToolParams{
		Name:      "mithril_mcp_info",
		Arguments: map[string]any{},
	})
	if err != nil {
		t.Fatalf("tool call failed after %s: %v", after, err)
	}
	if result.IsError {
		t.Fatalf("tool call returned IsError after %s: %s", after, toolResultText(result))
	}
}

// Well-formed frames with invalid calls must not terminate the stdio session.
// Oversized-frame termination is covered by the subprocess transport test.
func TestMCPStdioRejectsInvalidInputAndStaysUsable(t *testing.T) {
	cases := []struct {
		name      string
		tool      string
		arguments map[string]any
	}{
		{"unknown tool", "mithril_does_not_exist", map[string]any{}},
		{"removed operator tool", "mithril_service_status", map[string]any{}},
		{"empty tool name", "", map[string]any{}},
		{"wrong argument type", "mithril_read_rewards", map[string]any{"slot": "not-a-number"}},
		{"negative slot", "mithril_read_rewards", map[string]any{"slot": -1}},
		{"unknown argument", "mithril_mcp_info", map[string]any{"nonexistent_field": "x"}},
		{"wrong type for bool", "mithril_diagnose", map[string]any{"include_logs": "yes"}},
		// Large but within the frame bound, so the server must reject the
		// pattern on its own merits rather than by dropping the connection.
		{"large in-bound argument", "mithril_grep_log", map[string]any{"pattern": strings.Repeat("a", 8<<10)}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// A fresh session per case, so a case that does terminate the
			// connection cannot cascade into spurious failures for the rest.
			session, telemetry := connectStdioSession(t)

			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()

			result, err := session.CallTool(ctx, &mcpsdk.CallToolParams{Name: tc.tool, Arguments: tc.arguments})
			// Either a protocol-level error or a tool result flagged IsError is
			// acceptable; silently succeeding is not.
			switch {
			case err != nil:
				// Rejected at the protocol layer, as intended.
			case result == nil:
				t.Fatalf("nil result and nil error for invalid input")
			case !result.IsError:
				t.Fatalf("invalid input was accepted as a successful call: %s", toolResultText(result))
			}
			assertSessionStillHealthy(t, session, "invalid input: "+tc.name)

			// Rejection paths must not echo internals back to an untrusted caller.
			for _, leak := range []string{"/Users/", "goroutine ", "panic:"} {
				if strings.Contains(telemetry.String(), leak) {
					t.Errorf("server telemetry leaked %q while rejecting invalid input: %s", leak, telemetry.String())
				}
			}
		})
	}
}

func TestMCPStdioAcceptsOmittedOptionalArguments(t *testing.T) {
	session, _ := connectStdioSession(t)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	for _, args := range []map[string]any{nil, {}} {
		result, err := session.CallTool(ctx, &mcpsdk.CallToolParams{Name: "mithril_diagnose", Arguments: args})
		if err != nil {
			t.Fatalf("diagnose with arguments %v failed: %v", args, err)
		}
		if result.IsError {
			t.Fatalf("diagnose with arguments %v returned IsError: %s", args, toolResultText(result))
		}
		if !strings.Contains(toolResultText(result), `"evidence_complete"`) {
			t.Errorf("diagnose with arguments %v produced no evidence field", args)
		}
	}
}

func TestMCPStdioCancellationLeavesSessionUsable(t *testing.T) {
	session, _ := connectStdioSession(t)

	t.Run("cancel before the call is issued", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := session.CallTool(ctx, &mcpsdk.CallToolParams{
			Name:      "mithril_diagnose",
			Arguments: map[string]any{},
		}); err == nil {
			t.Error("call on an already-cancelled context reported success")
		}
		assertSessionStillHealthy(t, session, "pre-cancelled call")
	})

	t.Run("cancel while the call is in flight", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		go func() {
			time.Sleep(15 * time.Millisecond)
			cancel()
		}()
		// Either outcome is legitimate: the call may complete before the cancel
		// lands. What must hold is that the session survives either way.
		_, _ = session.CallTool(ctx, &mcpsdk.CallToolParams{
			Name:      "mithril_diagnose",
			Arguments: map[string]any{},
		})
		assertSessionStillHealthy(t, session, "in-flight cancellation")
	})

	t.Run("deadline expiry", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
		defer cancel()
		_, _ = session.CallTool(ctx, &mcpsdk.CallToolParams{
			Name:      "mithril_diagnose",
			Arguments: map[string]any{},
		})
		assertSessionStillHealthy(t, session, "deadline expiry")
	})

	t.Run("repeated cancellations do not exhaust the server", func(t *testing.T) {
		const attempts = MaxConcurrentLimit + 8
		for i := 0; i < attempts; i++ {
			ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
			_, _ = session.CallTool(ctx, &mcpsdk.CallToolParams{
				Name:      "mithril_diagnose",
				Arguments: map[string]any{},
			})
			cancel()
		}
		// If cancellation leaked an admission slot or a goroutine per call, the
		// server would now refuse or hang instead of answering.
		assertSessionStillHealthy(t, session, "more cancelled calls than the maximum concurrency")
	})
}

func TestMCPStdioCleanDisconnectIsReportedWithoutError(t *testing.T) {
	fixture := newStdioE2EFixture(t)
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("resolve test executable: %v", err)
	}
	cmd := exec.Command(executable, "-test.run=^TestMCPStdioHelperProcess$")
	cmd.Env = append([]string(nil), fixture.env...)
	telemetry := &syncTelemetryBuffer{}
	cmd.Stderr = telemetry

	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	client := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "mithril-disconnect", Version: "1"}, nil)
	session, err := client.Connect(ctx, &mcpsdk.CommandTransport{Command: cmd, TerminateDuration: 5 * time.Second}, nil)
	if err != nil {
		t.Fatalf("connect: %v; stderr=%s", err, telemetry.String())
	}
	if _, err := session.ListTools(ctx, nil); err != nil {
		t.Fatalf("list tools before disconnect: %v", err)
	}
	if err := session.Close(); err != nil {
		t.Fatalf("clean disconnect reported an error: %v", err)
	}
	// A second Close is what a deferred cleanup does after an explicit one.
	if err := session.Close(); err != nil {
		t.Fatalf("second Close reported an error: %v", err)
	}
	for _, noise := range []string{"panic:", "goroutine "} {
		if strings.Contains(telemetry.String(), noise) {
			t.Errorf("clean disconnect produced %q in telemetry: %s", noise, telemetry.String())
		}
	}
}

package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestValidateToolPolicy(t *testing.T) {
	tests := []struct {
		name    string
		profile Profile
		tool    string
		args    string
		wantErr bool
	}{
		{"monitor configured target", ProfileMonitor, "mithril_metric", `{"metric":"x"}`, false},
		{"monitor endpoint override", ProfileMonitor, "mithril_metric", `{"metric":"x","endpoint":"http://example.com"}`, true},
		{"monitor path override", ProfileMonitor, "mithril_tail_log", `{"log_dir":"/tmp"}`, true},
		{"monitor empty override", ProfileMonitor, "mithril_tail_log", `{"log_dir":""}`, false},
		{"monitor raw", ProfileMonitor, "mithril_scrape_metrics", `{"include_raw":true}`, true},
		{"diagnostic raw", ProfileDiagnostic, "mithril_scrape_metrics", `{"include_raw":true}`, false},
		{"diagnostic pprof", ProfileDiagnostic, "mithril_pprof_profile", `{}`, false},
		{"diagnostic override", ProfileDiagnostic, "mithril_pprof_heap", `{"endpoint":"http://example.com"}`, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateToolPolicy(test.profile, test.tool, json.RawMessage(test.args))
			if (err != nil) != test.wantErr {
				t.Fatalf("validateToolPolicy() error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}

func TestTokenBucket(t *testing.T) {
	start := time.Unix(100, 0)
	bucket := newTokenBucket(2, 2, start)
	if !bucket.allow(start) {
		t.Fatal("first burst token was not available")
	}
	if !bucket.allow(start) {
		t.Fatal("second burst token was not available")
	}
	if bucket.allow(start) {
		t.Fatal("third immediate token unexpectedly allowed")
	}
	if bucket.allow(start.Add(250 * time.Millisecond)) {
		t.Fatal("half token unexpectedly allowed")
	}
	if !bucket.allow(start.Add(500 * time.Millisecond)) {
		t.Fatal("refilled token was not allowed")
	}
}

type middlewareTestInput struct {
	Secret string `json:"secret,omitempty"`
}

type middlewareTestOutput struct {
	Value string `json:"value"`
}

const middlewareTestToolName = "mithril_metric"

func defaultMiddlewareTestConfig() Config {
	return Config{
		Profile:           ProfileMonitor,
		MaxConcurrent:     1,
		RatePerSecond:     100,
		RateBurst:         10,
		OutputBudgetBytes: 1024,
	}
}

func middlewareValueHandler(value string) mcpsdk.ToolHandlerFor[middlewareTestInput, middlewareTestOutput] {
	return func(_ context.Context, _ *mcpsdk.CallToolRequest, _ middlewareTestInput) (*mcpsdk.CallToolResult, middlewareTestOutput, error) {
		return nil, middlewareTestOutput{Value: value}, nil
	}
}

// startMiddlewareSessionWriter is startMiddlewareSession with an arbitrary
// telemetry sink, so a test needing race-free reads can supply a synchronized
// writer instead of a bare buffer.
func startMiddlewareSessionWriter(
	t *testing.T,
	cfg Config,
	telemetry io.Writer,
	handler mcpsdk.ToolHandlerFor[middlewareTestInput, middlewareTestOutput],
) *mcpsdk.ClientSession {
	t.Helper()
	server := mcpsdk.NewServer(&mcpsdk.Implementation{Name: "middleware-test", Version: "0"}, nil)
	mcpsdk.AddTool(server, &mcpsdk.Tool{Name: middlewareTestToolName}, handler)
	server.AddReceivingMiddleware(newToolCallMiddlewareWithTelemetry(cfg, newTelemetryWriter(telemetry)))
	return connectServerForTest(t, server, "middleware-client")
}

func startMiddlewareSession(
	t *testing.T,
	cfg Config,
	telemetry *bytes.Buffer,
	handler mcpsdk.ToolHandlerFor[middlewareTestInput, middlewareTestOutput],
) *mcpsdk.ClientSession {
	t.Helper()
	server := mcpsdk.NewServer(&mcpsdk.Implementation{Name: "middleware-test", Version: "0"}, nil)
	mcpsdk.AddTool(server, &mcpsdk.Tool{Name: middlewareTestToolName}, handler)
	server.AddReceivingMiddleware(newToolCallMiddlewareWithTelemetry(cfg, newTelemetryWriter(telemetry)))
	return connectServerForTest(t, server, "middleware-client")
}

func TestMiddlewarePreservesFallbackRateLimitsAndSanitizesTelemetry(t *testing.T) {
	var telemetry bytes.Buffer
	cfg := defaultMiddlewareTestConfig()
	cfg.RatePerSecond = 0.01
	cfg.RateBurst = 1
	session := startMiddlewareSession(t, cfg, &telemetry, middlewareValueHandler("ok"))

	secret := "DO_NOT_LOG_THIS_ARGUMENT"
	first, err := session.CallTool(context.Background(), &mcpsdk.CallToolParams{
		Name: middlewareTestToolName, Arguments: map[string]any{"secret": secret},
	})
	if err != nil || first.IsError {
		t.Fatalf("first call = %+v, %v", first, err)
	}
	second, err := session.CallTool(context.Background(), &mcpsdk.CallToolParams{Name: middlewareTestToolName})
	if err != nil {
		t.Fatalf("rate-limited call returned protocol error: %v", err)
	}
	if !second.IsError || !toolResultContains(second, "rate limit exceeded") {
		t.Fatalf("second call was not rate-limited: %+v", second)
	}

	logged := telemetry.String()
	if strings.Contains(logged, secret) {
		t.Fatalf("telemetry leaked caller-controlled data: %s", logged)
	}
	if !strings.Contains(logged, `"tool":"mithril_metric"`) || !strings.Contains(logged, `"status":"ok"`) || !strings.Contains(logged, `"status":"rate_limited"`) {
		t.Fatalf("telemetry is missing sanitized call outcomes: %s", logged)
	}
}

func TestUnknownToolTelemetryUsesFixedLabel(t *testing.T) {
	var telemetry bytes.Buffer
	session := startMiddlewareSession(t, defaultMiddlewareTestConfig(), &telemetry, middlewareValueHandler("must not run"))

	const unknownName = "unknown_DO_NOT_LOG_THIS_NAME"
	if _, err := session.CallTool(context.Background(), &mcpsdk.CallToolParams{Name: unknownName}); err == nil {
		t.Fatal("unknown tool unexpectedly succeeded")
	}
	logged := telemetry.String()
	if strings.Contains(logged, unknownName) || strings.Contains(logged, "DO_NOT_LOG_THIS_NAME") {
		t.Fatalf("telemetry leaked the caller-controlled tool name: %s", logged)
	}
	if !strings.Contains(logged, `"tool":"unregistered"`) || !strings.Contains(logged, `"status":"unknown_tool"`) {
		t.Fatalf("unknown-tool telemetry is missing fixed labels: %s", logged)
	}
}

func TestUnknownAndHiddenToolsStayProtocolErrorsWhenRateLimited(t *testing.T) {
	var telemetry bytes.Buffer
	cfg := defaultMiddlewareTestConfig()
	cfg.RatePerSecond = 0.01
	cfg.RateBurst = 1
	session := startMiddlewareSession(t, cfg, &telemetry, middlewareValueHandler("ok"))

	first, err := session.CallTool(context.Background(), &mcpsdk.CallToolParams{Name: middlewareTestToolName})
	if err != nil || first.IsError {
		t.Fatalf("initial call = %+v, %v", first, err)
	}
	for _, name := range []string{"unknown_tool", "mithril_pprof_profile"} {
		if result, err := session.CallTool(context.Background(), &mcpsdk.CallToolParams{Name: name}); err == nil {
			t.Fatalf("rate-exhausted unknown/hidden tool %q returned in-band result: %+v", name, result)
		}
	}
	limited, err := session.CallTool(context.Background(), &mcpsdk.CallToolParams{Name: middlewareTestToolName})
	if err != nil || !limited.IsError || !toolResultContains(limited, "rate limit exceeded") {
		t.Fatalf("known exposed tool did not retain in-band rate limit: %+v, %v", limited, err)
	}
}

func TestMiddlewareRejectsOversizedFinalResult(t *testing.T) {
	var telemetry bytes.Buffer
	cfg := defaultMiddlewareTestConfig()
	cfg.OutputBudgetBytes = 1
	session := startMiddlewareSession(t, cfg, &telemetry, middlewareValueHandler(strings.Repeat("x", 512)))

	result, err := session.CallTool(context.Background(), &mcpsdk.CallToolParams{Name: middlewareTestToolName})
	if err != nil {
		t.Fatalf("oversized call returned protocol error: %v", err)
	}
	if !result.IsError || !toolResultContains(result, "exceeds the configured response budget") {
		t.Fatalf("oversized output was not rejected: %+v", result)
	}
	size, err := encodedResultSize(result)
	if err != nil {
		t.Fatal(err)
	}
	if size > MinOutputBudgetBytes {
		t.Fatalf("replacement result is %d bytes, exceeds minimum budget %d", size, MinOutputBudgetBytes)
	}
	if !strings.Contains(telemetry.String(), `"status":"output_rejected"`) {
		t.Fatalf("missing output rejection telemetry: %s", telemetry.String())
	}
}

func TestMiddlewareContainsHandlerPanic(t *testing.T) {
	var telemetry bytes.Buffer
	const secret = "DO_NOT_EXPOSE_PANIC_VALUE"
	session := startMiddlewareSession(t, defaultMiddlewareTestConfig(), &telemetry, func(_ context.Context, _ *mcpsdk.CallToolRequest, _ middlewareTestInput) (*mcpsdk.CallToolResult, middlewareTestOutput, error) {
		panic(secret)
	})

	result, err := session.CallTool(context.Background(), &mcpsdk.CallToolParams{Name: middlewareTestToolName})
	if err != nil {
		t.Fatalf("panicking handler returned protocol error: %v", err)
	}
	if !result.IsError || !toolResultContains(result, toolHandlerPanicMessage) {
		t.Fatalf("panicking handler result = %+v, want fixed in-band error", result)
	}
	combined := toolResultText(result) + telemetry.String()
	if strings.Contains(combined, secret) {
		t.Fatalf("panic value leaked through MCP output or telemetry: %s", combined)
	}
	if !strings.Contains(telemetry.String(), `"status":"handler_panic"`) {
		t.Fatalf("panic telemetry is missing fixed status: %s", telemetry.String())
	}
}

func TestPolicyRejectedCallsConsumeAdmission(t *testing.T) {
	var telemetry bytes.Buffer
	cfg := defaultMiddlewareTestConfig()
	cfg.RatePerSecond = 0.01
	cfg.RateBurst = 1
	session := startMiddlewareSession(t, cfg, &telemetry, middlewareValueHandler("must not run"))

	first, err := session.CallTool(context.Background(), &mcpsdk.CallToolParams{Name: middlewareTestToolName, Arguments: map[string]any{"log_dir": "/etc"}})
	if err != nil || !first.IsError || !toolResultContains(first, "local path overrides are disabled") {
		t.Fatalf("first policy rejection = %+v, %v", first, err)
	}
	second, err := session.CallTool(context.Background(), &mcpsdk.CallToolParams{Name: middlewareTestToolName, Arguments: map[string]any{"log_dir": "/etc"}})
	if err != nil || !second.IsError || !toolResultContains(second, "rate limit exceeded") {
		t.Fatalf("second rejected call bypassed admission: %+v, %v", second, err)
	}
}

func TestMiddlewareConcurrencyLimitFailsFast(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseCall := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseCall()

	var telemetry bytes.Buffer
	session := startMiddlewareSession(t, defaultMiddlewareTestConfig(), &telemetry, func(_ context.Context, _ *mcpsdk.CallToolRequest, _ middlewareTestInput) (*mcpsdk.CallToolResult, middlewareTestOutput, error) {
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
		return nil, middlewareTestOutput{Value: "ok"}, nil
	})

	firstDone := make(chan error, 1)
	go func() {
		_, err := session.CallTool(context.Background(), &mcpsdk.CallToolParams{Name: middlewareTestToolName})
		firstDone <- err
	}()
	<-entered

	started := time.Now()
	second, err := session.CallTool(context.Background(), &mcpsdk.CallToolParams{Name: middlewareTestToolName})
	if err != nil || !second.IsError || !toolResultContains(second, "concurrency limit reached") {
		t.Fatalf("saturated call was not load-shed: %+v, %v", second, err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("saturated call queued for %v", elapsed)
	}
	releaseCall()
	if err := <-firstDone; err != nil {
		t.Fatalf("first call failed: %v", err)
	}
}

func TestMiddlewareCallTimeoutIsReportedAsTimedOut(t *testing.T) {
	var telemetry bytes.Buffer
	cfg := defaultMiddlewareTestConfig()
	cfg.ToolCallTimeout = 50 * time.Millisecond

	session := startMiddlewareSession(t, cfg, &telemetry, func(ctx context.Context, _ *mcpsdk.CallToolRequest, _ middlewareTestInput) (*mcpsdk.CallToolResult, middlewareTestOutput, error) {
		// Outlive the call budget, then return normally so the middleware
		// classifies the outcome rather than the handler reporting an error.
		<-ctx.Done()
		return nil, middlewareTestOutput{Value: "late"}, nil
	})

	started := time.Now()
	if _, err := session.CallTool(context.Background(), &mcpsdk.CallToolParams{Name: middlewareTestToolName}); err != nil {
		t.Fatalf("timed-out call returned a protocol error: %v", err)
	}
	if elapsed := time.Since(started); elapsed < cfg.ToolCallTimeout {
		t.Fatalf("call returned in %v, before the %v budget elapsed", elapsed, cfg.ToolCallTimeout)
	}
	if logged := telemetry.String(); !strings.Contains(logged, `"status":"timed_out"`) {
		t.Fatalf("call exceeding the budget was not recorded as timed_out: %s", logged)
	}
}

// syncTelemetryBuffer makes telemetry readable from the test goroutine while it
// is written asynchronously — by the middleware in-process, or by os/exec's
// stderr-copying goroutine for the stdio subprocess tests. Reading a bare
// bytes.Buffer in either case races, and worse, usually reads empty and makes an
// assertion pass vacuously.
type syncTelemetryBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncTelemetryBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncTelemetryBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// waitForRecord polls until a telemetry record lands or the deadline passes,
// so the assertion cannot pass simply because nothing had been written yet.
func (b *syncTelemetryBuffer) waitForRecord(t *testing.T) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if logged := b.String(); strings.TrimSpace(logged) != "" {
			return logged
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("no telemetry record was written within the deadline")
	return ""
}

func TestClientCancellationIsNotReportedAsTimeout(t *testing.T) {
	telemetry := &syncTelemetryBuffer{}
	cfg := defaultMiddlewareTestConfig()
	cfg.ToolCallTimeout = time.Minute // long enough that it cannot be the cause

	entered := make(chan struct{})
	var enteredOnce sync.Once
	session := startMiddlewareSessionWriter(t, cfg, telemetry, func(ctx context.Context, _ *mcpsdk.CallToolRequest, _ middlewareTestInput) (*mcpsdk.CallToolResult, middlewareTestOutput, error) {
		enteredOnce.Do(func() { close(entered) })
		<-ctx.Done()
		return nil, middlewareTestOutput{Value: "cancelled"}, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = session.CallTool(ctx, &mcpsdk.CallToolParams{Name: middlewareTestToolName})
	}()
	<-entered
	cancel()
	<-done

	logged := telemetry.waitForRecord(t)
	if strings.Contains(logged, `"status":"timed_out"`) {
		t.Fatalf("client cancellation was misreported as a call timeout: %s", logged)
	}
	if !strings.Contains(logged, `"status":"cancelled"`) {
		t.Fatalf("client cancellation was not reported as cancelled: %s", logged)
	}
}

type blockingTelemetryWriter struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *blockingTelemetryWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	<-w.release
	return len(p), nil
}

func TestAsyncTelemetryDropsInsteadOfBlockingAndBoundsShutdown(t *testing.T) {
	writer := &blockingTelemetryWriter{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	telemetry := newAsyncTelemetryWriter(writer, 1)
	event := toolCallTelemetry{Event: "mcp_tool_call", Profile: ProfileMonitor, Tool: "mithril_metric", Status: "ok"}

	telemetry.write(event) // consumed by the worker, which then blocks in Write
	select {
	case <-writer.started:
	case <-time.After(time.Second):
		t.Fatal("telemetry worker did not reach the blocking writer")
	}
	telemetry.write(event) // fills the sole pending queue slot

	writeDone := make(chan struct{})
	go func() {
		telemetry.write(event) // queue is full: this event must be dropped
		close(writeDone)
	}()
	select {
	case <-writeDone:
	case <-time.After(time.Second):
		t.Fatal("telemetry write blocked behind a stalled stderr writer")
	}
	if got := telemetry.dropped.Load(); got != 1 {
		t.Fatalf("dropped telemetry count = %d, want 1", got)
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	err := telemetry.close(closeCtx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close with blocked writer error = %v, want deadline exceeded", err)
	}

	close(writer.release)
	drainCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := telemetry.close(drainCtx); err != nil {
		t.Fatalf("close after releasing writer: %v", err)
	}

	// Closing is idempotent and post-close telemetry is safely discarded.
	telemetry.write(event)
	if got := telemetry.dropped.Load(); got != 2 {
		t.Fatalf("dropped telemetry count after close = %d, want 2", got)
	}
}

func toolResultContains(result *mcpsdk.CallToolResult, substring string) bool {
	return strings.Contains(toolResultText(result), substring)
}

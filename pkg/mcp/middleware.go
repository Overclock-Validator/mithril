package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

const methodToolsCall = "tools/call"

const toolHandlerPanicMessage = "tool handler failed unexpectedly"

var (
	endpointOverrideArguments = map[string]struct{}{
		"endpoint":          {},
		"grpc_addr":         {},
		"influxdb_url":      {},
		"reference_rpc_url": {},
	}
	localPathArguments = map[string]struct{}{
		"log_dir":    {},
		"path":       {},
		"state_path": {},
	}
	fixedConfigurationArguments = map[string]struct{}{
		"database": {},
	}
)

// toolCallMiddleware applies profile policy and bounded admission before a
// tool handler runs, then enforces the budget on the final serialized wire
// result (including the SDK's compatibility TextContent fallback).
// Its telemetry contains only fixed labels and bounded numeric metadata; tool
// arguments, endpoints, paths, and error strings are never recorded.
type toolCallMiddleware struct {
	profile      Profile
	gate         chan struct{}
	rate         *tokenBucket
	outputBudget int
	callTimeout  time.Duration
	telemetry    telemetrySink
}

func newToolCallMiddlewareWithTelemetry(cfg Config, telemetry telemetrySink) mcpsdk.Middleware {
	cfg = cfg.normalized()
	if telemetry == nil {
		telemetry = newTelemetryWriter(nil)
	}
	m := &toolCallMiddleware{
		profile:      cfg.Profile,
		gate:         make(chan struct{}, cfg.MaxConcurrent),
		rate:         newTokenBucket(cfg.RatePerSecond, cfg.RateBurst, time.Now()),
		outputBudget: cfg.OutputBudgetBytes,
		callTimeout:  cfg.ToolCallTimeout,
		telemetry:    telemetry,
	}
	return m.wrap
}

func (m *toolCallMiddleware) wrap(next mcpsdk.MethodHandler) mcpsdk.MethodHandler {
	return func(ctx context.Context, method string, req mcpsdk.Request) (mcpsdk.Result, error) {
		if method != methodToolsCall {
			return next(ctx, method, req)
		}

		started := time.Now()
		name, args := callToolDetails(req)
		telemetryName := telemetryToolName(name)
		finish := func(status string, responseBytes int) {
			m.telemetry.write(toolCallTelemetry{
				Event:         "mcp_tool_call",
				Profile:       m.profile,
				Tool:          telemetryName,
				Status:        status,
				DurationMS:    time.Since(started).Milliseconds(),
				ResponseBytes: responseBytes,
			})
		}

		if err := ctx.Err(); err != nil {
			finish("cancelled", 0)
			return nil, err
		}
		// Every attempted tool call, including policy-invalid calls, consumes
		// rate admission. Otherwise an attacker could bypass it by flooding
		// cheap rejected overrides and force unbounded telemetry/error work.
		rateAllowed := m.rate.allow(time.Now())
		if _, known := toolPolicies[name]; !known {
			// Preserve the SDK/spec protocol-level unknown-tool semantics. Argument
			// policy must not turn a nonexistent name into an executable-looking
			// in-band tool error.
			finish("unknown_tool", 0)
			return next(ctx, method, req)
		}
		if !profileAllowsTool(m.profile, name) {
			finish("unknown_tool", 0)
			return nil, &jsonrpc.Error{Code: jsonrpc.CodeInvalidParams, Message: fmt.Sprintf("unknown tool %q", name)}
		}
		// Unknown and profile-hidden names must retain MCP's protocol-level
		// unknown-tool semantics even when the bucket is empty. The attempt above
		// still consumed admission; only known, exposed tools get an in-band
		// operational rate-limit result.
		if !rateAllowed {
			result := toolErrorResult(errors.New("tool call rate limit exceeded; retry later"))
			finish("rate_limited", resultSize(result))
			return result, nil
		}
		if err := validateToolPolicy(m.profile, name, args); err != nil {
			result := toolErrorResult(err)
			finish("policy_rejected", resultSize(result))
			return result, nil
		}
		// Only admitted calls that will reach a handler consume the concurrency
		// gate. This load-sheds excess requests before they can queue behind long
		// diagnostics with unbounded client deadlines.
		select {
		case m.gate <- struct{}{}:
			defer func() { <-m.gate }()
		case <-ctx.Done():
			finish("cancelled", 0)
			return nil, ctx.Err()
		default:
			result := toolErrorResult(errors.New("tool concurrency limit reached; retry later"))
			finish("concurrency_limited", resultSize(result))
			return result, nil
		}

		// Bound each call so a slow endpoint or a large scan cannot hold a
		// concurrency slot indefinitely. Scan loops poll ctx per line.
		callCtx, cancelCall := context.WithTimeout(ctx, m.callTimeout)
		defer cancelCall()

		result, err, panicked := callToolHandler(callCtx, next, method, req)
		if panicked {
			finish("handler_panic", resultSize(result.(*mcpsdk.CallToolResult)))
			return result, nil
		}
		if err != nil {
			finish("protocol_error", 0)
			return nil, err
		}

		callResult, ok := result.(*mcpsdk.CallToolResult)
		if !ok || callResult == nil {
			finish("protocol_error", 0)
			return result, nil
		}
		size, sizeErr := encodedResultSize(callResult)
		if sizeErr != nil || size > m.outputBudget {
			callResult = toolErrorResult(errors.New("tool output exceeds the configured response budget"))
			finish("output_rejected", resultSize(callResult))
			return callResult, nil
		}

		status := "ok"
		if callResult.IsError {
			status = "tool_error"
		}
		if ctx.Err() != nil {
			status = "cancelled"
		} else if errors.Is(callCtx.Err(), context.DeadlineExceeded) {
			status = "timed_out"
		}
		finish(status, size)
		return callResult, nil
	}
}

// callToolHandler converts a handler panic into a fixed in-band error. Panic
// values are caller- or endpoint-controlled, so neither the result nor
// telemetry includes them.
func callToolHandler(ctx context.Context, next mcpsdk.MethodHandler, method string, req mcpsdk.Request) (result mcpsdk.Result, err error, panicked bool) {
	defer func() {
		if recover() != nil {
			result = toolErrorResult(errors.New(toolHandlerPanicMessage))
			err = nil
			panicked = true
		}
	}()
	result, err = next(ctx, method, req)
	return result, err, false
}

func callToolDetails(req mcpsdk.Request) (string, json.RawMessage) {
	if req == nil {
		return "", nil
	}
	params, ok := req.GetParams().(*mcpsdk.CallToolParamsRaw)
	if !ok || params == nil {
		return "", nil
	}
	return params.Name, params.Arguments
}

func validateToolPolicy(profile Profile, name string, args json.RawMessage) error {
	var values map[string]json.RawMessage
	if len(args) == 0 || json.Unmarshal(args, &values) != nil {
		return nil // Typed tool validation reports malformed arguments.
	}
	if profile != ProfileDiagnostic {
		var includeRaw bool
		if raw, ok := values["include_raw"]; ok && json.Unmarshal(raw, &includeRaw) == nil && includeRaw {
			return errors.New("raw output is disabled by the active MCP profile")
		}
	}
	// Local paths are configuration authority, never call authority. Otherwise
	// an untrusted MCP caller could repurpose a read-only parser as an arbitrary
	// local-file oracle. Operators can still select paths through environment
	// variables before the process starts.
	if restrictedArgumentIsSet(values, localPathArguments) {
		return errors.New("call-time local path overrides are disabled; configure paths in the MCP process environment")
	}
	if restrictedArgumentIsSet(values, fixedConfigurationArguments) {
		return errors.New("call-time credential-scope overrides are disabled; configure them in the MCP process environment")
	}
	if restrictedArgumentIsSet(values, endpointOverrideArguments) {
		return errors.New("call-time endpoint overrides are disabled; configure endpoints in the MCP process environment")
	}
	return nil
}

func restrictedArgumentIsSet(values map[string]json.RawMessage, restricted map[string]struct{}) bool {
	for key := range restricted {
		if raw, ok := values[key]; ok && argumentIsSet(raw) {
			return true
		}
	}
	return false
}

func argumentIsSet(raw json.RawMessage) bool {
	if len(raw) == 0 || string(raw) == "null" {
		return false
	}
	var value string
	if json.Unmarshal(raw, &value) == nil {
		return value != ""
	}
	return true
}

func toolErrorResult(err error) *mcpsdk.CallToolResult {
	result := &mcpsdk.CallToolResult{}
	result.SetError(err)
	return result
}

func resultSize(result *mcpsdk.CallToolResult) int {
	size, _ := encodedResultSize(result)
	return size
}

func encodedResultSize(result *mcpsdk.CallToolResult) (int, error) {
	encoded, err := json.Marshal(result)
	if err != nil {
		return 0, err
	}
	return len(encoded), nil
}

func telemetryToolName(name string) string {
	if _, ok := toolPolicies[name]; ok {
		return name
	}
	return "unregistered"
}

type tokenBucket struct {
	mu       sync.Mutex
	rate     float64
	burst    float64
	tokens   float64
	lastTime time.Time
}

func newTokenBucket(rate float64, burst int, now time.Time) *tokenBucket {
	return &tokenBucket{rate: rate, burst: float64(burst), tokens: float64(burst), lastTime: now}
}

func (b *tokenBucket) takeOrDelay(now time.Time) (time.Duration, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if elapsed := now.Sub(b.lastTime).Seconds(); elapsed > 0 {
		b.tokens += elapsed * b.rate
		if b.tokens > b.burst {
			b.tokens = b.burst
		}
		b.lastTime = now
	}
	if b.tokens < 1 {
		if b.rate <= 0 {
			return 0, false
		}
		seconds := (1 - b.tokens) / b.rate
		return time.Duration(math.Ceil(seconds * float64(time.Second))), true
	}
	b.tokens--
	return 0, true
}

func (b *tokenBucket) allow(now time.Time) bool {
	delay, possible := b.takeOrDelay(now)
	return possible && delay == 0
}

func (b *tokenBucket) wait() bool {
	for {
		delay, possible := b.takeOrDelay(time.Now())
		if !possible {
			return false
		}
		if delay <= 0 {
			return true
		}
		time.Sleep(delay)
	}
}

type toolCallTelemetry struct {
	Event         string  `json:"event"`
	Profile       Profile `json:"profile"`
	Tool          string  `json:"tool"`
	Status        string  `json:"status"`
	DurationMS    int64   `json:"duration_ms"`
	ResponseBytes int     `json:"response_bytes"`
}

type telemetrySink interface {
	write(toolCallTelemetry)
}

type telemetryWriter struct {
	mu      sync.Mutex
	encoder *json.Encoder
}

func newTelemetryWriter(writer io.Writer) *telemetryWriter {
	if writer == nil {
		writer = io.Discard
	}
	return &telemetryWriter{encoder: json.NewEncoder(writer)}
}

func (w *telemetryWriter) write(event toolCallTelemetry) {
	w.mu.Lock()
	defer w.mu.Unlock()
	_ = w.encoder.Encode(event)
}

// asyncTelemetryWriter keeps stderr I/O off tool-call response paths. Its fixed
// queue drops new events when the writer is slow.
type asyncTelemetryWriter struct {
	writer io.Writer
	queue  chan []byte
	done   chan struct{}

	mu        sync.RWMutex
	closed    bool
	closeOnce sync.Once
	dropped   atomic.Uint64
}

func newAsyncTelemetryWriter(writer io.Writer, capacity int) *asyncTelemetryWriter {
	if writer == nil {
		writer = io.Discard
	}
	if capacity < 1 {
		capacity = 1
	}
	w := &asyncTelemetryWriter{
		writer: writer,
		queue:  make(chan []byte, capacity),
		done:   make(chan struct{}),
	}
	go w.run()
	return w
}

func (w *asyncTelemetryWriter) run() {
	defer close(w.done)
	for line := range w.queue {
		_, _ = w.writer.Write(line)
	}
}

func (w *asyncTelemetryWriter) write(event toolCallTelemetry) {
	line, err := json.Marshal(event)
	if err != nil {
		return
	}
	line = append(line, '\n')

	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.closed {
		w.dropped.Add(1)
		return
	}
	select {
	case w.queue <- line:
	default:
		w.dropped.Add(1)
	}
}

// close stops admission and drains queued telemetry until either the writer
// catches up or ctx expires. A permanently blocked stderr therefore cannot
// stall MCP shutdown; a later close call can still wait for eventual drain.
func (w *asyncTelemetryWriter) close(ctx context.Context) error {
	w.closeOnce.Do(func() {
		w.mu.Lock()
		w.closed = true
		close(w.queue)
		w.mu.Unlock()
	})
	select {
	case <-w.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

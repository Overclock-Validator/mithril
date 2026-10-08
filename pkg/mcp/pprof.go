package mcp

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	// 320 KiB remains below the default 1 MiB final-result budget after base64
	// expansion and MCP's required structured-content compatibility fallback.
	maxProfileBytes   = 320 * 1024
	maxProfileDurSec  = 30
	defaultProfileSec = 10
)

var pprofGate = make(chan struct{}, 1)

var errPprofBusy = errors.New("another pprof collection is already in progress")

func acquirePprof(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case pprofGate <- struct{}{}:
		return nil
	default:
		// Fail fast: waiting here would hold a general MCP concurrency slot for
		// up to 40 seconds and a small burst of profiles could starve monitoring.
		return errPprofBusy
	}
}

func releasePprof() { <-pprofGate }

// PprofResult is a base64-encoded pprof profile.
type PprofResult struct {
	ProfileBytesB64 string `json:"profile_bytes_b64"`
	ContentType     string `json:"content_type"`
	SizeBytes       uint64 `json:"size_bytes"`
}

// buildPprofURL builds a pprof endpoint URL safely, replacing the path/query so
// a trailing slash or stray path on the endpoint doesn't produce a malformed URL.
func buildPprofURL(endpoint, path, query string) (string, error) {
	u, err := validateURL(endpoint)
	if err != nil {
		return "", err
	}
	u.Path = path
	u.RawQuery = query
	return u.String(), nil
}

// fetchPprof fetches a pprof profile with an SSRF guard and a size cap.
func fetchPprof(ctx context.Context, endpoint, path, query string, timeout time.Duration) (PprofResult, error) {
	rawURL, err := buildPprofURL(endpoint, path, query)
	if err != nil {
		return PprofResult{}, err
	}
	u, err := validateURL(rawURL)
	if err != nil {
		return PprofResult{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return PprofResult{}, sanitizeHTTPError(err)
	}
	resp, err := doPinnedRequest(ctx, req, timeout)
	if err != nil {
		return PprofResult{}, sanitizeHTTPError(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return PprofResult{}, fmt.Errorf("unexpected HTTP status %d", resp.StatusCode)
	}
	if err := requireMithrilEndpoint(resp, mithrilPprofEndpoint); err != nil {
		return PprofResult{}, err
	}
	body, err := readCappedBody(resp, maxProfileBytes)
	if err != nil {
		return PprofResult{}, err
	}
	return PprofResult{
		ProfileBytesB64: base64.StdEncoding.EncodeToString(body),
		ContentType:     "application/octet-stream",
		SizeBytes:       uint64(len(body)),
	}, nil
}

type pprofHeapInput struct{}

type pprofProfileInput struct {
	Seconds int `json:"seconds,omitempty" jsonschema:"CPU profile duration in seconds (default 10, max 30)"`
}

func registerPprofTools(server *mcpsdk.Server, cfg Config) {
	addTool(server, cfg, &mcpsdk.Tool{
		Name:        "mithril_pprof_heap",
		Annotations: annReadOnlyNetwork,
		Description: "Diagnostic profile only. Fetch a single-flight heap profile capped at 320 KiB before base64 encoding. Analyze it with `go tool pprof`.",
	}, func(ctx context.Context, _ *mcpsdk.CallToolRequest, _ pprofHeapInput) (*mcpsdk.CallToolResult, PprofResult, error) {
		if err := acquirePprof(ctx); err != nil {
			return nil, PprofResult{}, err
		}
		defer releasePprof()
		endpoint, err := requireConfiguredPath(cfg.PprofURL, "MITHRIL_PPROF_URL is not configured")
		if err != nil {
			return nil, PprofResult{}, err
		}
		res, err := fetchPprof(ctx, endpoint, "/debug/pprof/heap", "", 30*time.Second)
		if err != nil {
			return nil, PprofResult{}, err
		}
		return nil, res, nil
	})

	addTool(server, cfg, &mcpsdk.Tool{
		Name:        "mithril_pprof_profile",
		Annotations: annRuntimeDiagnostic,
		Description: "Diagnostic profile only. Collect a 1–30 second single-flight CPU profile capped at 320 KiB. Collection changes runtime profiling state.",
	}, func(ctx context.Context, _ *mcpsdk.CallToolRequest, in pprofProfileInput) (*mcpsdk.CallToolResult, PprofResult, error) {
		if err := acquirePprof(ctx); err != nil {
			return nil, PprofResult{}, err
		}
		defer releasePprof()
		endpoint, err := requireConfiguredPath(cfg.PprofURL, "MITHRIL_PPROF_URL is not configured")
		if err != nil {
			return nil, PprofResult{}, err
		}
		seconds := in.Seconds
		if seconds <= 0 {
			seconds = defaultProfileSec
		}
		if seconds > maxProfileDurSec {
			seconds = maxProfileDurSec
		}
		res, err := fetchPprof(ctx, endpoint, "/debug/pprof/profile", fmt.Sprintf("seconds=%d", seconds), time.Duration(seconds+10)*time.Second)
		if err != nil {
			return nil, PprofResult{}, err
		}
		return nil, res, nil
	})
}

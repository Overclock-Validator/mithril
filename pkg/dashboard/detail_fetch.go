package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func validDetailDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, c := range value {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func parseDetailQuery(raw string, force bool, maxPages int) (generation string, page int, pageRequest, ok bool) {
	if raw == "" {
		return "", 0, false, !force
	}
	first, second, found := strings.Cut(raw, "&")
	if len(raw) > 128 || !found || strings.Contains(second, "&") {
		return "", 0, false, false
	}
	var pageText string
	for _, part := range [...]string{first, second} {
		switch {
		case strings.HasPrefix(part, "generation=") && generation == "":
			generation = strings.TrimPrefix(part, "generation=")
		case strings.HasPrefix(part, "page=") && pageText == "":
			pageText = strings.TrimPrefix(part, "page=")
		default:
			return "", 0, false, false
		}
	}
	if !validDetailDigest(generation) || pageText == "" || len(pageText) > 3 || len(pageText) > 1 && pageText[0] == '0' {
		return "", 0, false, false
	}
	for _, c := range pageText {
		if c < '0' || c > '9' {
			return "", 0, false, false
		}
	}
	page, err := strconv.Atoi(pageText)
	return generation, page, true, err == nil && page < maxPages
}

func decodeDetailFailure(body []byte, family string, validReason func(string) bool) string {
	var value struct {
		SchemaVersion int    `json:"schema_version"`
		Family        string `json:"family"`
		State         string `json:"state"`
		Reason        string `json:"reason"`
	}
	if len(body) > 512 || json.Unmarshal(body, &value) != nil || value.SchemaVersion != 1 || value.Family != family || value.State != "unavailable" {
		return "source_invalid"
	}
	canonical, err := json.Marshal(value)
	if err != nil || !bytes.Equal(body, canonical) || !validReason(value.Reason) {
		return "source_invalid"
	}
	return value.Reason
}

func parseDetailRemaining(value string, freshFor time.Duration) (time.Duration, error) {
	milliseconds, err := strconv.ParseInt(value, 10, 64)
	if err != nil || milliseconds < 1 || milliseconds > int64(freshFor/time.Millisecond) || strconv.FormatInt(milliseconds, 10) != value {
		return 0, errors.New("source_invalid")
	}
	return time.Duration(milliseconds) * time.Millisecond, nil
}

// detailHTTPClient isolates each reader's connection pool and request limits.
func detailHTTPClient(transport *http.Transport, connections int, timeout time.Duration) *http.Client {
	transport = transport.Clone()
	transport.Proxy = nil
	transport.MaxResponseHeaderBytes = 4096
	transport.MaxConnsPerHost, transport.MaxIdleConnsPerHost, transport.MaxIdleConns = connections, connections, connections
	transport.DisableKeepAlives = true
	return &http.Client{Transport: transport, Timeout: timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

func runDetailReader(ctx context.Context, done chan struct{}, client *http.Client, interval time.Duration, collect func(context.Context)) {
	defer close(done)
	defer client.CloseIdleConnections()
	for ctx.Err() == nil {
		collect(ctx)
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func validDetailGet(request *http.Request, path string) bool {
	return request.Method == http.MethodGet && request.URL.Path == path && request.URL.EscapedPath() == path &&
		request.URL.RawQuery == "" && !request.URL.ForceQuery && request.ContentLength == 0 && len(request.TransferEncoding) == 0
}

func writeDetailResponse(w http.ResponseWriter, status int, body string, statusRemaining time.Duration, remainingMilliseconds int64) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if status == http.StatusOK {
		w.Header().Set(statusRemainingHeader, strconv.FormatInt(statusRemaining.Milliseconds(), 10))
		if remainingMilliseconds > 0 {
			w.Header().Set(replayDetailRemainingHeader, strconv.FormatInt(remainingMilliseconds, 10))
		}
	}
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}

// fetchDetail reads a bounded detail response without extending its source lease.
func fetchDetail(parent context.Context, client *http.Client, path string, maxBytes int, headers http.Header,
	parseRemaining func(string) (time.Duration, error), decodeFailure func([]byte) string,
) ([]byte, time.Duration, time.Time, string) {
	ctx, cancel := context.WithTimeout(parent, time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://node/details/"+path, nil)
	if err != nil {
		return nil, 0, time.Time{}, "source_invalid"
	}
	for name, values := range headers {
		for _, value := range values {
			request.Header.Add(name, value)
		}
	}
	started := time.Now()
	response, err := client.Do(request)
	if err != nil {
		return nil, 0, started, "source_unavailable"
	}
	defer response.Body.Close()
	limit := int64(maxBytes + 1)
	if response.StatusCode != http.StatusOK {
		limit = 513
	}
	if response.ContentLength > limit-1 {
		return nil, 0, started, "response_limit"
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, limit))
	if err != nil {
		return nil, 0, started, "source_unavailable"
	}
	if len(body) >= int(limit) {
		return nil, 0, started, "response_limit"
	}
	if response.StatusCode == http.StatusNotFound {
		return nil, 0, started, "unsupported_source"
	}
	if response.StatusCode != http.StatusOK {
		if response.StatusCode != http.StatusServiceUnavailable || len(response.Header.Values(replayDetailRemainingHeader)) != 0 {
			return nil, 0, started, "source_invalid"
		}
		return nil, 0, started, decodeFailure(body)
	}
	values := response.Header.Values(replayDetailRemainingHeader)
	if len(values) != 1 {
		return nil, 0, started, "source_invalid"
	}
	remaining, err := parseRemaining(values[0])
	if err != nil {
		return nil, 0, started, "source_invalid"
	}
	return body, remaining, started, ""
}

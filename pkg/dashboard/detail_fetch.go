package dashboard

import (
	"context"
	"io"
	"net/http"
	"time"
)

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

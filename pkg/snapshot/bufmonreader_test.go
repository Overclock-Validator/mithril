package snapshot

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBufMonReaderHTTPUsesContextBoundGETAndDurablyFinalizes(t *testing.T) {
	payload := []byte("compressed-snapshot-payload")
	var headRequests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodHead {
			headRequests.Add(1)
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		writer.Header().Set("Content-Length", "27")
		_, _ = writer.Write(payload)
	}))
	defer server.Close()

	destination := filepath.Join(t.TempDir(), "snapshot.tar.zst")
	reader, err := NewBufMonReaderHTTPWithSave(context.Background(), server.URL, destination)
	require.NoError(t, err)
	contents, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, payload, contents)
	require.Equal(t, int64(len(payload)), reader.BytesRead())
	require.NoError(t, reader.Close())
	require.NoError(t, FinalizePartialDownload(destination))
	require.Equal(t, int64(0), headRequests.Load())
	persisted, err := os.ReadFile(destination)
	require.NoError(t, err)
	require.Equal(t, payload, persisted)
}

func TestBufMonReaderHTTPRefusesPreexistingPartialSymlink(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte("payload"))
	}))
	defer server.Close()

	directory := t.TempDir()
	target := filepath.Join(directory, "target")
	require.NoError(t, os.WriteFile(target, []byte("do-not-overwrite"), 0o644))
	destination := filepath.Join(directory, "snapshot.tar.zst")
	require.NoError(t, os.Symlink(target, destination+PartialSuffix))

	_, err := NewBufMonReaderHTTPWithSave(context.Background(), server.URL, destination)
	require.Error(t, err)
	contents, readErr := os.ReadFile(target)
	require.NoError(t, readErr)
	require.Equal(t, []byte("do-not-overwrite"), contents)
}

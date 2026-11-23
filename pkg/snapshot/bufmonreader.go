package snapshot

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
)

// ProgressCallback is called with (bytesRead, totalBytes) to report download progress
type ProgressCallback func(bytesRead, totalBytes int64)

type bufmonreader struct {
	name       string
	b          io.Reader
	c          io.Closer
	bytesRead  int64
	totalSize  int64
	onProgress ProgressCallback
}

func NewBufMonReader(name string, r io.ReadCloser, totalSize int64) *bufmonreader {
	return &bufmonreader{
		name:      name,
		b:         r,
		totalSize: totalSize,
	}
}

func NewBufMonReaderFromFile(file *os.File) (*bufmonreader, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	return &bufmonreader{
		name:      file.Name(),
		b:         bufio.NewReader(file),
		c:         file,
		totalSize: info.Size(),
	}, nil
}

func NewBufMonReaderHTTP(ctx context.Context, url string) (*bufmonreader, error) {
	return NewBufMonReaderHTTPWithSave(ctx, url, "")
}

// PartialSuffix is appended to snapshot files during download to mark them as incomplete.
// Once download completes successfully, the file is atomically renamed to remove this suffix.
const PartialSuffix = ".partial"

// NewBufMonReaderHTTPWithSave streams from HTTP URL and optionally saves to disk.
// If savePath is non-empty, the data will be written to a .partial file while streaming.
// Use FinalizePartialDownload after successful processing to rename to the final path.
// Returns: (*bufmonreader, error)
func NewBufMonReaderHTTPWithSave(ctx context.Context, url string, savePath string) (*bufmonreader, error) {
	resp, err := http.Head(url)
	if err != nil {
		return nil, fmt.Errorf("HEAD %s: %v", url, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HEAD %s: had not-ok status: %s", url, resp.Status)
	}
	totalSize := resp.ContentLength
	resp.Body.Close()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating GET %s request: %w", url, err)
	}
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %v", url, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: had not-ok status: %s", url, resp.Status)
	}

	var reader io.Reader = resp.Body
	var closer io.Closer = resp.Body

	// If savePath is provided, use TeeReader to write to disk while streaming.
	// Write to .partial file first for crash safety.
	if savePath != "" {
		partialPath := savePath + PartialSuffix
		// Note: Don't log here - caller logs before progress bar starts to avoid breaking cursor positioning
		outFile, err := os.Create(partialPath)
		if err != nil {
			resp.Body.Close()
			return nil, fmt.Errorf("creating save file %s: %v", partialPath, err)
		}

		// TeeReader splits the stream: data goes to both the tar reader AND the file
		reader = io.TeeReader(resp.Body, outFile)

		// Create a multi-closer that closes both the HTTP body and the file
		closer = &multiCloser{closers: []io.Closer{resp.Body, outFile}}
	}

	return &bufmonreader{
		name:      url,
		b:         reader,
		c:         closer,
		totalSize: totalSize,
	}, nil
}

// FinalizePartialDownload atomically renames a completed .partial file to its final name.
// Call after successfully processing a snapshot saved with NewBufMonReaderHTTPWithSave.
// No-op if savePath is empty or the partial file doesn't exist.
func FinalizePartialDownload(savePath string) error {
	if savePath == "" {
		return nil
	}
	partialPath := savePath + PartialSuffix
	if _, err := os.Stat(partialPath); os.IsNotExist(err) {
		return nil
	}
	if err := os.Rename(partialPath, savePath); err != nil {
		return fmt.Errorf("failed to finalize snapshot %s: %w", savePath, err)
	}
	mlog.Log.FileOnlyf("Finalized snapshot download: %s", savePath)
	return nil
}

// CleanupPartialDownload removes a .partial file if it exists.
// Call on error/cancellation to clean up incomplete downloads.
func CleanupPartialDownload(savePath string) {
	if savePath == "" {
		return
	}
	partialPath := savePath + PartialSuffix
	if _, err := os.Stat(partialPath); err == nil {
		mlog.Log.Infof("Cleaning up partial download: %s", partialPath)
		os.Remove(partialPath)
	}
}

// multiCloser closes multiple io.Closers
type multiCloser struct {
	closers []io.Closer
}

func (mc *multiCloser) Close() error {
	var firstErr error
	for _, c := range mc.closers {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// SetProgressCallback sets an optional callback to receive progress updates.
// The callback is invoked on each Read() with (bytesRead, totalBytes).
// Configure it before the first Read; changing it during reads is not safe.
func (x *bufmonreader) SetProgressCallback(cb ProgressCallback) {
	x.onProgress = cb
}

// TotalSize returns the total size of the data being read
func (x *bufmonreader) TotalSize() int64 {
	return x.totalSize
}

func (x *bufmonreader) Read(p []byte) (int, error) {
	n, err := x.b.Read(p)
	x.bytesRead += int64(n)

	// Call progress callback if set
	if x.onProgress != nil {
		x.onProgress(x.bytesRead, x.totalSize)
	}
	return n, err
}

func (x *bufmonreader) Close() error {
	return x.c.Close()
}

package snapshot

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
)

// ProgressCallback is called with (bytesRead, totalBytes) to report download progress
type ProgressCallback func(bytesRead, totalBytes int64)

type bufmonreader struct {
	name       string
	b          io.Reader
	c          io.Closer
	bytesRead  atomic.Int64
	totalSize  int64
	progressMu sync.RWMutex
	onProgress ProgressCallback
}

func NewBufMonReader(name string, r io.ReadCloser, totalSize int64) *bufmonreader {
	return &bufmonreader{
		name:      name,
		b:         r,
		c:         r,
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
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating GET %s request: %w", url, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %v", url, err)
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("GET %s: had not-ok status: %s", url, resp.Status)
	}
	totalSize := resp.ContentLength

	var reader io.Reader = resp.Body
	var closer io.Closer = resp.Body

	// If savePath is provided, use TeeReader to write to disk while streaming.
	// Write to .partial file first for crash safety.
	if savePath != "" {
		partialPath := savePath + PartialSuffix
		// Note: Don't log here - caller logs before progress bar starts to avoid breaking cursor positioning
		outFile, err := os.OpenFile(partialPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if err != nil {
			resp.Body.Close()
			return nil, fmt.Errorf("creating save file %s: %v", partialPath, err)
		}

		// TeeReader splits the stream: data goes to both the tar reader AND the file
		reader = io.TeeReader(resp.Body, outFile)

		// Create a multi-closer that closes both the HTTP body and the file
		closer = &multiCloser{closers: []io.Closer{resp.Body, &syncingFileCloser{file: outFile}}}
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
	partialInfo, err := os.Lstat(partialPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect partial snapshot %s: %w", partialPath, err)
	}
	if !partialInfo.Mode().IsRegular() || partialInfo.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("partial snapshot %s is not a regular file", partialPath)
	}
	if err := os.Rename(partialPath, savePath); err != nil {
		return fmt.Errorf("failed to finalize snapshot %s: %w", savePath, err)
	}
	if err := syncDirectory(filepath.Dir(savePath)); err != nil {
		return fmt.Errorf("persist finalized snapshot %s: %w", savePath, err)
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

type syncingFileCloser struct {
	file *os.File
}

func (closer *syncingFileCloser) Close() error {
	if closer == nil || closer.file == nil {
		return nil
	}
	file := closer.file
	closer.file = nil
	return errors.Join(file.Sync(), file.Close())
}

func (mc *multiCloser) Close() error {
	var closeErr error
	for _, c := range mc.closers {
		closeErr = errors.Join(closeErr, c.Close())
	}
	return closeErr
}

// SetProgressCallback sets an optional callback to receive progress updates.
// The callback is invoked on each Read() with (bytesRead, totalBytes).
func (x *bufmonreader) SetProgressCallback(cb ProgressCallback) {
	x.progressMu.Lock()
	x.onProgress = cb
	x.progressMu.Unlock()
}

// TotalSize returns the total size of the data being read
func (x *bufmonreader) TotalSize() int64 {
	return x.totalSize
}

// BytesRead returns the number of compressed source bytes consumed.
func (x *bufmonreader) BytesRead() int64 {
	if x == nil {
		return 0
	}
	return x.bytesRead.Load()
}

func (x *bufmonreader) Read(p []byte) (int, error) {
	n, err := x.b.Read(p)
	bytesRead := x.bytesRead.Add(int64(n))

	// Call progress callback if set
	x.progressMu.RLock()
	callback := x.onProgress
	x.progressMu.RUnlock()
	if callback != nil {
		callback(bytesRead, x.totalSize)
	}
	return n, err
}

func (x *bufmonreader) Close() error {
	if x == nil || x.c == nil {
		return nil
	}
	return x.c.Close()
}

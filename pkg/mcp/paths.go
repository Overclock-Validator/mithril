package mcp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func requireConfiguredPath(value, errMsg string) (string, error) {
	if value != "" {
		return value, nil
	}
	return "", errors.New(errMsg)
}

func (r *contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.reader.Read(p)
	if ctxErr := r.ctx.Err(); ctxErr != nil {
		return n, ctxErr
	}
	return n, err
}

// Shared filesystem helpers used across log, replay, divergence, and reward
// tools. These surfaces support flat or per-run layouts where `latest` points
// at the active run directory.

// resolveConfined canonicalizes candidate (which must exist) and verifies it
// resolves inside base to prevent symlink escapes such as latest -> /etc.
// It returns the fully symlink-resolved path, which callers open instead of the
// original alias. Descriptor-rooted os.Root reads are still preferred where an
// artifact directory itself may be attacker-writable.
func resolveConfined(candidate, base string) (string, error) {
	cc, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return "", err
	}
	cb, err := filepath.EvalSymlinks(base)
	if err != nil {
		return "", err
	}
	if cc != cb && !strings.HasPrefix(cc, cb+string(filepath.Separator)) {
		return "", fmt.Errorf("resolved path escapes the configured directory: %s is outside %s", cc, cb)
	}
	return cc, nil
}

func confinedRelativePath(candidate, base string) (resolvedBase, relative string, err error) {
	resolved, err := resolveConfined(candidate, base)
	if err != nil {
		return "", "", err
	}
	resolvedBase, err = filepath.EvalSymlinks(base)
	if err != nil {
		return "", "", err
	}
	relative, err = filepath.Rel(resolvedBase, resolved)
	if err != nil {
		return "", "", err
	}
	return resolvedBase, relative, nil
}

// openConfinedRoot opens candidate through a descriptor rooted at base. This
// keeps the confinement boundary intact if candidate changes after validation.
func openConfinedRoot(candidate, base string) (*os.Root, error) {
	resolvedBase, relative, err := confinedRelativePath(candidate, base)
	if err != nil {
		return nil, err
	}
	baseRoot, err := os.OpenRoot(resolvedBase)
	if err != nil {
		return nil, err
	}
	defer baseRoot.Close()
	return baseRoot.OpenRoot(relative)
}

// openConfinedRegularFile opens candidate through a descriptor rooted at base
// and verifies that the checked directory entry is the file that was opened.
func openConfinedRegularFile(candidate, base string) (*os.File, error) {
	resolvedBase, relative, err := confinedRelativePath(candidate, base)
	if err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(resolvedBase)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	f, _, err := openRootRegularFile(root, relative)
	return f, err
}

// openRootRegularFile verifies that the checked directory entry is the file
// opened through root. The caller owns the returned file.
func openRootRegularFile(root *os.Root, name string) (*os.File, os.FileInfo, error) {
	linfo, err := root.Lstat(name)
	if err != nil {
		return nil, nil, err
	}
	if linfo.Mode()&os.ModeSymlink != 0 {
		return nil, nil, errors.New("symbolic-link files are not permitted")
	}
	if !linfo.Mode().IsRegular() {
		return nil, nil, errors.New("path is not a regular file")
	}
	f, err := root.OpenFile(name, os.O_RDONLY|nonBlockingOpenFlag, 0)
	if err != nil {
		return nil, nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	if !info.Mode().IsRegular() {
		f.Close()
		return nil, nil, errors.New("path is not a regular file")
	}
	if !os.SameFile(linfo, info) {
		f.Close()
		return nil, nil, errors.New("file changed while opening")
	}
	return f, info, nil
}

// resolveLatestRunDir reports whether an authoritative latest marker exists
// and, when present, returns its confined resolved directory. A broken or
// unsafe marker is an error: callers must never fall back to stale flat data
// merely because the active run has not produced a particular artifact yet.
func resolveLatestRunDir(base string) (resolved string, present bool, err error) {
	marker := filepath.Join(base, "latest")
	if _, err := os.Lstat(marker); err != nil {
		if os.IsNotExist(err) {
			return "", false, nil
		}
		return "", true, fmt.Errorf("inspect latest run marker: %w", err)
	}
	info, err := os.Stat(marker)
	if err != nil {
		return "", true, fmt.Errorf("resolve latest run marker: %w", err)
	}
	if !info.IsDir() {
		return "", true, errors.New("latest run marker is not a directory")
	}
	resolved, err = resolveConfined(marker, base)
	if err != nil {
		return "", true, fmt.Errorf("latest run marker is unsafe: %w", err)
	}
	return resolved, true, nil
}

// resolveActiveOrFlatDir finds a named artifact directory in either the
// authoritative active-run layout (<base>/latest/<name>) or the legacy flat
// layout (<base>/<name>). A present latest marker always wins, even when that
// run has not produced the requested artifact directory yet.
func resolveActiveOrFlatDir(base, name string) (dir, layout string, found bool, err error) {
	activeDir, active, err := resolveLatestRunDir(base)
	if err != nil {
		return "", "", false, err
	}
	if active {
		nested := filepath.Join(activeDir, name)
		info, statErr := os.Stat(nested)
		if os.IsNotExist(statErr) {
			return "", "latest", false, nil
		}
		if statErr != nil {
			return "", "", false, fmt.Errorf("inspect active %s directory: %w", name, statErr)
		}
		if !info.IsDir() {
			return "", "", false, fmt.Errorf("active %s path is not a directory: %s", name, nested)
		}
		resolved, err := resolveConfined(nested, base)
		if err != nil {
			return "", "", false, fmt.Errorf("active %s path is unsafe: %w", name, err)
		}
		return resolved, "latest", true, nil
	}

	flat := filepath.Join(base, name)
	if info, statErr := os.Stat(flat); statErr == nil {
		if !info.IsDir() {
			return "", "", false, fmt.Errorf("legacy %s path is not a directory: %s", name, flat)
		}
		resolved, err := resolveConfined(flat, base)
		if err != nil {
			return "", "", false, fmt.Errorf("legacy %s path is unsafe: %w", name, err)
		}
		return resolved, "flat", true, nil
	} else if !os.IsNotExist(statErr) {
		return "", "", false, fmt.Errorf("inspect legacy %s directory: %w", name, statErr)
	}
	return "", "", false, nil
}

// readRootFile performs one confined open and then checks the opened file. It
// rejects final-component symlinks and non-regular files and reads through a
// hard max+1 cap. Callers should retain the Root across related reads so a
// directory rename cannot silently change their authority boundary.
func readRootFile(ctx context.Context, root *os.Root, name string, maxBytes int64) ([]byte, error) {
	if filepath.Base(name) != name {
		return nil, errors.New("artifact name must be a basename")
	}
	f, info, err := openRootRegularFile(root, name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(&contextReader{ctx: ctx, reader: f}, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("artifact exceeds %d-byte limit", maxBytes)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	after, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if fileMetadataChanged(info, after) {
		return nil, errors.New("artifact changed while reading")
	}
	return data, nil
}

func fileMetadataChanged(before, after os.FileInfo) bool {
	return before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime())
}

// statRootRegularFile validates a confined file without reading its contents.
// This is used for large artifacts whose presence/metadata is the evidence and
// whose rows are never returned over MCP.
func statRootRegularFile(root *os.Root, name string) (int64, error) {
	if filepath.Base(name) != name {
		return 0, errors.New("artifact name must be a basename")
	}
	f, info, err := openRootRegularFile(root, name)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	if info.Size() == 0 {
		return 0, errors.New("artifact is empty")
	}
	return info.Size(), nil
}

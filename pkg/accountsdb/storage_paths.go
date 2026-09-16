package accountsdb

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ValidateStorageDirectories rejects aliases and overlapping roots before a
// caller removes artifacts or opens truncating writers. Nonexistent suffixes
// are resolved through their nearest existing parent, including symlinks.
func ValidateStorageDirectories(paths []string) error {
	if len(paths) == 0 {
		return fmt.Errorf("no storage directories configured")
	}
	resolved := make([]string, len(paths))
	infos := make([]os.FileInfo, len(paths))
	for i, p := range paths {
		if strings.TrimSpace(p) == "" {
			return fmt.Errorf("storage directory %d is empty", i)
		}
		abs, err := filepath.Abs(p)
		if err != nil {
			return err
		}
		ancestor := filepath.Clean(abs)
		var suffix []string
		for {
			_, err = os.Lstat(ancestor)
			if err == nil {
				break
			}
			if !os.IsNotExist(err) {
				return fmt.Errorf("checking storage directory %q: %w", p, err)
			}
			suffix = append(suffix, filepath.Base(ancestor))
			ancestor = filepath.Dir(ancestor)
		}
		real, err := filepath.EvalSymlinks(ancestor)
		if err != nil {
			return fmt.Errorf("resolving storage directory %q: %w", p, err)
		}
		info, err := os.Stat(real)
		if err != nil {
			return err
		}
		if !info.IsDir() {
			return fmt.Errorf("storage path %q is not a directory", ancestor)
		}
		if len(suffix) == 0 {
			infos[i] = info
		}
		for j := len(suffix) - 1; j >= 0; j-- {
			real = filepath.Join(real, suffix[j])
		}
		resolved[i] = real
		for j := 0; j < i; j++ {
			sameFile := infos[i] != nil && infos[j] != nil && os.SameFile(infos[i], infos[j])
			overlap := pathContains(real, resolved[j]) || pathContains(resolved[j], real)
			if sameFile || overlap {
				return fmt.Errorf("storage directories %q and %q alias or overlap", paths[j], p)
			}
		}
	}
	return nil
}

// ValidateAccountsPaths also checks existing accounts subdirectories: two
// otherwise distinct roots may contain symlinks to the same data directory.
func ValidateAccountsPaths(paths []string) error {
	if err := ValidateStorageDirectories(paths); err != nil {
		return err
	}
	data := make([]string, len(paths))
	for i, p := range paths {
		data[i] = filepath.Join(p, "accounts")
	}
	return ValidateStorageDirectories(data)
}

func pathContains(parent, child string) bool {
	rel, err := filepath.Rel(parent, child)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))
}

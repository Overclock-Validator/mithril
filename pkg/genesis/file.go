package genesis

import (
	"archive/tar"
	"bufio"
	"bytes"
	stdbzip2 "compress/bzip2"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/dsnet/compress/bzip2"
)

// ReadGenesisFromFile accepts raw genesis bytes or a standard compressed tar.
// Archive members are read directly; no untrusted paths are extracted.
func ReadGenesisFromFile(path string) (*Genesis, *[32]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()
	// Genesis has no magic number: its leading timestamp bytes can also spell
	// BZh or a gzip prefix. Prefer a valid canonical raw genesis over magic.
	raw, readErr := io.ReadAll(io.LimitReader(f, MaxGenesisSize+1))
	if readErr != nil {
		return nil, nil, readErr
	}
	g, hash, decodeErr := Decode(raw)
	if decodeErr == nil {
		return g, hash, nil
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, nil, err
	}
	r := bufio.NewReader(f)
	header, _ := r.Peek(512)
	if bytes.HasPrefix(header, []byte("BZh")) || bytes.HasPrefix(header, []byte{0x1f, 0x8b}) || len(header) >= 262 && string(header[257:262]) == "ustar" {
		return ReadGenesisFromArchive(r)
	}
	return nil, nil, decodeErr
}
func ReadGenesisFromArchive(r io.Reader) (*Genesis, *[32]byte, error) {
	buffered := bufio.NewReader(r)
	magic, err := buffered.Peek(3)
	if err != nil {
		return nil, nil, err
	}
	var unpacked io.Reader = buffered
	if bytes.Equal(magic, []byte("BZh")) {
		unpacked = stdbzip2.NewReader(buffered)
	} else if bytes.Equal(magic, []byte{0x1f, 0x8b, 0x08}) {
		z, err := gzip.NewReader(buffered)
		if err != nil {
			return nil, nil, err
		}
		defer z.Close()
		unpacked = z
	}
	// Match the pinned Agave default archive limit. Reading through EOF checks
	// compression checksums, including trailers after genesis.bin's contents.
	const maxArchiveSize = 10 * 1024 * 1024
	data, err := io.ReadAll(io.LimitReader(unpacked, maxArchiveSize+1))
	if err != nil {
		return nil, nil, err
	}
	if len(data) > maxArchiveSize {
		return nil, nil, fmt.Errorf("unpacked genesis archive exceeds 10 MiB")
	}
	files := tar.NewReader(bytes.NewReader(data))
	hdr, err := files.Next()
	if err != nil {
		return nil, nil, err
	}
	if hdr.Name != "genesis.bin" || !hdr.FileInfo().Mode().IsRegular() {
		return nil, nil, fmt.Errorf("first archive member must be a regular genesis.bin")
	}
	if hdr.Size < 0 || hdr.Size > MaxGenesisSize {
		return nil, nil, fmt.Errorf("genesis.bin too large")
	}
	raw, err := io.ReadAll(io.LimitReader(files, MaxGenesisSize+1))
	if err != nil {
		return nil, nil, err
	}
	// Other Agave ledger members are allowed but never extracted. Reject
	// duplicate genesis definitions and malformed remaining tar members.
	for {
		hdr, err := files.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, err
		}
		if hdr.Name == "genesis.bin" {
			return nil, nil, fmt.Errorf("duplicate genesis.bin archive member")
		}
	}
	return Decode(raw)
}

// WriteArchive emits a deterministic, standard bzip2 tar archive, without
// spawning external compressors or depending on Agave at runtime.
func WriteArchive(w io.Writer, raw []byte) error {
	if _, _, err := Decode(raw); err != nil {
		return err
	}
	bz, err := bzip2.NewWriter(w, &bzip2.WriterConfig{Level: 9})
	if err != nil {
		return err
	}
	t := tar.NewWriter(bz)
	if err = t.WriteHeader(&tar.Header{Name: "genesis.bin", Mode: 0644, Size: int64(len(raw)), ModTime: time.Unix(0, 0), Format: tar.FormatUSTAR}); err != nil {
		_ = bz.Close()
		return err
	}
	if _, err = t.Write(raw); err != nil {
		_ = bz.Close()
		return err
	}
	if err = t.Close(); err != nil {
		_ = bz.Close()
		return err
	}
	return bz.Close()
}
func syncDirectory(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// CreateFiles publishes a new output directory. An existing destination is
// never replaced; the result can be independently inspected before init.
func CreateFiles(ctx context.Context, c Config, output string) (*Genesis, error) {
	g, resolved, err := Build(ctx, c)
	if err != nil {
		return nil, err
	}
	bank, err := ConstructInitialBank(ctx, g)
	if err != nil {
		return nil, err
	}
	raw, err := Encode(g)
	if err != nil {
		return nil, err
	}
	var archive bytes.Buffer
	if err = WriteArchive(&archive, raw); err != nil {
		return nil, err
	}
	summary, err := marshalSummary(g, resolved, bank.Frozen.Metadata)
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if output == "" {
		return nil, fmt.Errorf("empty genesis output path")
	}
	if err = os.Mkdir(output, 0755); err != nil {
		return nil, fmt.Errorf("genesis output must not already exist: %w", err)
	}
	// Only these O_EXCL-created files belong to this call. Never remove the
	// directory recursively, since another writer may have added unrelated data.
	var owned []string
	complete := false
	defer func() {
		if !complete {
			for _, p := range owned {
				_ = os.Remove(p)
			}
			_ = os.Remove(output)
		}
	}()
	for _, f := range []struct {
		name string
		data []byte
	}{{"genesis.bin", raw}, {"genesis.tar.bz2", archive.Bytes()}, {"genesis-summary.json", summary}} {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		p := filepath.Join(output, f.name)
		file, err := os.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
		if err != nil {
			return nil, err
		}
		owned = append(owned, p)
		_, writeErr := file.Write(f.data)
		syncErr := file.Sync()
		closeErr := file.Close()
		if writeErr != nil {
			return nil, writeErr
		}
		if syncErr != nil {
			return nil, syncErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
	}
	if err = syncDirectory(output); err != nil {
		return nil, err
	}
	// Do not undo publication if syncing the containing directory is uncertain.
	complete = true
	if err = syncDirectory(filepath.Dir(output)); err != nil {
		return nil, err
	}
	return g, nil
}

package snapshot

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

func UnmarshalManifestFromSnapshot(filename string, accountsDbDir string) (*SnapshotManifest, *os.File, error) {
	manifest := new(SnapshotManifest)

	file, err := os.Open(filename)
	if err != nil {
		return nil, nil, err
	}

	if err = os.MkdirAll(accountsDbDir, 0775); err != nil {
		return nil, nil, err
	}
	tarReader, err := newSnapshotReader(file)
	if err != nil {
		panic(err)
	}

	writer := new(bytes.Buffer)

	for {
		header, err := tarReader.Next()
		if err != nil {
			return nil, nil, err
		}

		// identify manifest file, whose path is of the form "snapshots/SLOT/SLOT"
		if strings.Contains(header.Name, "snapshots/") {
			if strings.Count(header.Name, "/") == 2 {
				_, err := io.Copy(writer, tarReader)
				if err != nil {
					return nil, nil, err
				}
				err = os.WriteFile(filepath.Join(accountsDbDir, "manifest"), writer.Bytes(), 0644)
				if err != nil {
					mlog.Log.Errorf("err copying manifest file out: %s\n", err)
					return nil, nil, err
				}
				break
			}
		}
	}

	decoder := bin.NewBinDecoder(writer.Bytes())
	err = manifest.UnmarshalWithDecoder(decoder)

	return manifest, file, err
}

type appendVecCopyingTask struct {
	Filename                string
	TarBuffer               *bytes.Buffer
	FromIncrementalSnapshot bool
}

type indexEntryBuilderTask struct {
	FilePath string
	FileSize uint64
	Slot     uint64
	FileId   uint64
}

type indexEntryCommitterTask struct {
	IndexEntries []accountsdb.AccountIndexEntry
	Pubkeys      []solana.PublicKey
}

const (
	snapshotTypeZst = iota
	snapshotTypeLz4
)

var (
	ZstdDecoderConcurrency = runtime.NumCPU()
)

func readerForCompressionType(snapshotType int, file *os.File) (io.Reader, error) {
	var reader io.Reader

	bmr, err := NewBufMonReader(file)
	if err != nil {
		return nil, err
	}
	if snapshotType == snapshotTypeZst {
		zstdReader, err := zstd.NewReader(bmr, zstd.WithDecoderConcurrency(ZstdDecoderConcurrency))
		if err != nil {
			return nil, err
		}
		reader = zstdReader
	} else if snapshotType == snapshotTypeLz4 {
		reader = lz4.NewReader(bmr)
	} else {
		panic("unknown snapshot type")
	}

	return reader, nil
}

func parseSnapshotType(snapshotFileName string) int {
	var snapshotType int
	fileExt := filepath.Ext(snapshotFileName)

	if fileExt == ".zst" {
		snapshotType = snapshotTypeZst
	} else if fileExt == ".lz4" {
		snapshotType = snapshotTypeLz4
	} else {
		panic(fmt.Sprintf("unknown snapshot compression type - file ext: %s", fileExt))
	}

	return snapshotType
}

func newSnapshotReader(snapshotFile *os.File) (*tar.Reader, error) {
	snapshotFile.Seek(0, io.SeekStart)
	snapshotType := parseSnapshotType(snapshotFile.Name())
	reader, err := readerForCompressionType(snapshotType, snapshotFile)
	if err != nil {
		return nil, err
	}
	return tar.NewReader(reader), nil
}

func LoadManifestFromFile(filename string) (*SnapshotManifest, error) {
	manifestFile, err := os.Open(filename)
	if err != nil {
		mlog.Log.Errorf("failed to open %s\n", filename)
		return nil, err
	}
	manifestBytes, err := ioutil.ReadAll(manifestFile)
	if err != nil {
		return nil, err
	}

	manifest := new(SnapshotManifest)
	decoder := bin.NewBinDecoder(manifestBytes)
	err = manifest.UnmarshalWithDecoder(decoder)
	if err != nil {
		return nil, err
	}

	return manifest, nil
}

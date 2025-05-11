package snapshot

import (
	"archive/tar"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/statsd"
	"github.com/Overclock-Validator/sniper"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/klauspost/compress/zstd"
	"github.com/panjf2000/ants/v2"
	"github.com/pierrec/lz4/v4"
)

func UnmarshalManifestFromSnapshot(filename string, accountsDbDir string, snapshotType int) (*SnapshotManifest, *os.File, error) {
	manifest := new(SnapshotManifest)

	file, err := os.Open(filename)
	if err != nil {
		return nil, nil, err
	}

	manifestOutputFile := fmt.Sprintf("%s/manifest", accountsDbDir)
	if err = os.MkdirAll(accountsDbDir, 0775); err != nil {
		return nil, nil, err
	}
	manifestOut, err := os.Create(manifestOutputFile)
	if err != nil {
		return nil, nil, err
	}
	defer manifestOut.Close()

	reader, err := readerForCompressionType(snapshotType, file)
	if err != nil {
		panic(err)
	}

	tarReader := tar.NewReader(reader)
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
				_, err = io.Copy(manifestOut, bytes.NewBuffer(writer.Bytes()))
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
	Filename  string
	TarBuffer *bytes.Buffer
}

type indexEntryBuilderTask struct {
	Data     []byte
	FileSize uint64
	Slot     uint64
	FileId   uint64
}

type indexEntryCommitterTask struct {
	IndexEntries []*accountsdb.AccountIndexEntry
	Pubkeys      []solana.PublicKey
}

const (
	snapshotTypeZst = iota
	snapshotTypeLz4
)

var (
	tarBufferBytes = &atomic.Int64{}
)

func readerForCompressionType(snapshotType int, file *os.File) (io.Reader, error) {
	var reader io.Reader

	if snapshotType == snapshotTypeZst {
		zstdReader, err := zstd.NewReader(file)
		if err != nil {
			return nil, err
		}
		reader = zstdReader
	} else if snapshotType == snapshotTypeLz4 {
		reader = lz4.NewReader(file)
	} else {
		panic(fmt.Sprintf("unknown snapshot type"))
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

func BuildAccountsIndexFromSnapshot(snapshotFile string, accountsDbDir string) (*accountsdb.AccountsDb, *SnapshotManifest, error) {
	snapshotType := parseSnapshotType(snapshotFile)

	manifest, file, err := UnmarshalManifestFromSnapshot(snapshotFile, accountsDbDir, snapshotType)
	if err != nil {
		return nil, nil, err
	}

	defer file.Close()
	file.Seek(0, io.SeekStart)

	reader, err := readerForCompressionType(snapshotType, file)
	if err != nil {
		panic(err)
	}

	tarReader := tar.NewReader(reader)
	start := time.Now()

	appendVecsOutputDir := fmt.Sprintf("%s/accounts", accountsDbDir)
	if err = os.MkdirAll(appendVecsOutputDir, 0775); err != nil {
		return nil, nil, err
	}

	indexOutputDir := fmt.Sprintf("%s/index", accountsDbDir)
	if err = os.MkdirAll(indexOutputDir, 0775); err != nil {
		return nil, nil, err
	}

	db, err := sniper.Open(sniper.Dir(indexOutputDir), sniper.ChunksCollision(32))
	if err != nil {
		mlog.Log.Errorf("failed to open database: %s\n", err)
		return nil, nil, err
	}
	defer ants.Release()

	var largestFileId atomic.Uint64
	wg := sync.WaitGroup{}

	indexEntryCommiterPool, _ := ants.NewPoolWithFunc(500, func(i interface{}) {
		defer wg.Done()
		task := i.(indexEntryCommitterTask)
		writer := new(bytes.Buffer)

		for idx, entry := range task.IndexEntries {
			writer.Reset()
			encoder := bin.NewBinEncoder(writer)
			err = entry.MarshalWithEncoder(encoder)
			if err != nil {
				mlog.Log.Errorf("failed to encode index entry: %s\n", err)
				return
			}

			err = db.SetIfSlotHigher(task.Pubkeys[idx][:], writer.Bytes(), 0)
			if err != nil {
				mlog.Log.Errorf("error calling SetIfHigherSlot for %s: %s\n", task.Pubkeys[idx], err)
			}
		}
	})

	indexEntryBuilderPool, _ := ants.NewPoolWithFunc(500, func(i interface{}) {
		defer wg.Done()
		task := i.(indexEntryBuilderTask)
		pubkeys, entries, err := accountsdb.BuildIndexEntriesFromAppendVecs(task.Data, task.FileSize, task.Slot, task.FileId)
		if err != nil {
			mlog.Log.Errorf("%s\n", err)
			return
		}
		totalTarBytes := tarBufferBytes.Add(-int64(len(task.Data)))
		statsd.Gauge("accounts_index.tar_buffer_bytes", float64(totalTarBytes), nil, 1)

		commitTask := indexEntryCommitterTask{IndexEntries: entries, Pubkeys: pubkeys}
		wg.Add(1)
		err = indexEntryCommiterPool.Invoke(commitTask)
		if err != nil {
			mlog.Log.Errorf("error calling indexEntryCommiterPool.Invoke\n")
		}
	})

	appendVecCopyingPool, _ := ants.NewPoolWithFunc(500, func(i interface{}) {
		defer wg.Done()
		task := i.(appendVecCopyingTask)
		filename := task.Filename
		writer := task.TarBuffer

		// identify appendvec files, whose path is of the form "accounts/SLOT.ID"
		if strings.Contains(filename, "accounts/") {
			if !strings.Contains(filename, ".") {
				return
			}

			outFile, err := os.Create(fmt.Sprintf("%s/%s", accountsDbDir, filename))
			if err != nil {
				mlog.Log.Errorf("err creating new: %s\n", err)
				return
			}

			appendVecBytes := writer.Bytes()
			_, err = io.Copy(outFile, bytes.NewReader(appendVecBytes))
			if err != nil {
				mlog.Log.Errorf("err copying file out: %s\n", err)
				return
			}

			// parse slot and file ID out of filename
			_, after, found := strings.Cut(filename, "/")
			if !found {
				panic(fmt.Sprintf("invalid appendvec path format: %s", filename))
			}

			slotStr, idStr, found := strings.Cut(after, ".")
			slot, err := strconv.ParseUint(slotStr, 10, 64)
			if err != nil {
				mlog.Log.Errorf("invalid snapshot - unable to convert string to slot\n")
				panic("")
			}

			fileId, err := strconv.ParseUint(idStr, 10, 64)
			if err != nil {
				panic("invalid snapshot - unable to convert string to file id\n")
			}

			if fileId > largestFileId.Load() {
				largestFileId.Store(fileId)
			}

			// find the relevant appendvec storage info
			var fileSize uint64
			for _, av := range manifest.AccountsDb.Storages[slot].AcctVecs {
				if av.Id == fileId {
					fileSize = av.FileSize
					break
				}
			}

			if fileSize == 0 {
				panic("programming error - fileSize for appendvec was 0")
			}

			task := indexEntryBuilderTask{Data: appendVecBytes, FileSize: fileSize, Slot: slot, FileId: fileId}
			wg.Add(1)
			err = indexEntryBuilderPool.Invoke(task)
			if err != nil {
				mlog.Log.Errorf("error calling indexEntryBuilderPool.Invoke\n")
			}
		}
	})

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			mlog.Log.Errorf("err reading next tar: %s\n", err)
			return nil, nil, err
		}

		writer := new(bytes.Buffer)
		tarBytes, err := io.Copy(writer, tarReader)
		if err != nil {
			mlog.Log.Errorf("err copying data to reader: %s\n", err)
			return nil, nil, err
		}
		totalTarBytes := tarBufferBytes.Add(tarBytes)
		statsd.Gauge("accounts_index.tar_buffer_bytes", float64(totalTarBytes), nil, 1)

		task := appendVecCopyingTask{TarBuffer: writer, Filename: header.Name}
		wg.Add(1)
		err = appendVecCopyingPool.Invoke(task)
		if err != nil {
			mlog.Log.Errorf("error calling appendVecCopyingPool.Invoke\n")
		}
	}

	mlog.Log.Infof("done in %s. waiting for all tasks to complete.\n", time.Since(start))
	wg.Wait()
	mlog.Log.Infof("snapshot processed in %s.\n", time.Since(start))

	largestFileIdFile, err := os.Create(fmt.Sprintf("%s/largest_file_id", accountsDbDir))
	if err != nil {
		mlog.Log.Errorf("err creating new: %s\n", err)
		return nil, nil, err
	}

	largestFileIdBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(largestFileIdBytes, largestFileId.Load())

	numBytesWritten, err := largestFileIdFile.Write(largestFileIdBytes[:])
	if err != nil {
		mlog.Log.Errorf("error writing largest file ID to file: %s\n", err)
		return nil, nil, err
	} else if numBytesWritten != 8 {
		mlog.Log.Errorf("error writing largest file ID to file\n")
		return nil, nil, fmt.Errorf("error writing largest file ID to file, wrote %d bytes", numBytesWritten)
	}

	largestFileIdFile.Close()

	bankHashOutputFileName := fmt.Sprintf("%s/bank_hash", accountsDbDir)
	bankHashFile, err := os.Create(bankHashOutputFileName)
	if err != nil {
		mlog.Log.Errorf("err creating new: %s\n", err)
		return nil, nil, err
	}

	numBytesWritten, err = bankHashFile.Write(manifest.Bank.Hash[:])
	if err != nil {
		mlog.Log.Errorf("error writing bank hash to file: %s\n", err)
		return nil, nil, err
	} else if numBytesWritten != 32 {
		mlog.Log.Errorf("error writing bank hash to file\n")
		return nil, nil, fmt.Errorf("error writing bank hash to file, wrote %d bytes", numBytesWritten)
	}

	bankHashFile.Close()

	accountsDb := &accountsdb.AccountsDb{IndexDb: db, AcctsDir: appendVecsOutputDir, IndexDir: indexOutputDir}
	accountsDb.LargestFileId.Store(largestFileId.Load())
	copy(accountsDb.BankHashBytes[:], manifest.Bank.Hash[:])

	return accountsDb, manifest, nil
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

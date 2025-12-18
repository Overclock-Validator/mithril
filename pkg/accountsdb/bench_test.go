package accountsdb

import (
	"crypto/rand"
	"os"
	"testing"

	"github.com/gagliardetto/solana-go"
)

// BenchmarkBuildIndexEntries writes a real file to disk and parses it.
func BenchmarkBuildIndexEntries(b *testing.B) {
	numAccounts := 1000
	dataSize := 100 * 1024 // 100 KB per account
	tmpFile := "bench_appendvec.dat"

	// Create or truncate the file
	f, err := os.Create(tmpFile)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		f.Close()
		os.Remove(tmpFile) // clean up
	}()

	// Write dummy accounts to the file
	for i := 0; i < numAccounts; i++ {
		var pk, owner solana.PublicKey
		rand.Read(pk[:])
		rand.Read(owner[:])

		acct := AppendVecAccount{
			WriteVersion: 1,
			DataLen:      uint64(dataSize),
			Pubkey:       pk,
			Lamports:     100,
			RentEpoch:    0,
			Owner:        owner,
			Executable:   false,
			Data:         make([]byte, dataSize),
		}

		if err := acct.Marshal(f); err != nil {
			b.Fatal(err)
		}
	}

	fileInfo, _ := f.Stat()
	fileSize := uint64(fileInfo.Size())
	b.Logf("Benchmarking with file size: %.2f MB", float64(fileSize)/1024/1024)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Open the file for reading
		fRead, err := os.Open(tmpFile)
		// Open the file in bytes for testing
		//fileBytes, err := os.ReadFile(tmpFile)
		if err != nil {
			b.Fatal(err)
		}

		_, _, err = BuildIndexEntriesFromAppendVecs(fRead, fileSize, 100, 100)
		if err != nil {
			//	fRead.Close()
			b.Fatal(err)
		}

		//fRead.Close()
	}
}

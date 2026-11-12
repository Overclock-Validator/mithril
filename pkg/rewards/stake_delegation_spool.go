package rewards

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// StakeAccountVisitor consumes a delegation during a scan. The pointer is only
// valid for the call; streams may invoke the visitor concurrently.
type StakeAccountVisitor = func(solana.PublicKey, *sealevel.Delegation, uint64)

// StakeAccountStream must read one coherent parent account view. Capturing a
// stream does not make a concurrently changing AccountsDB view coherent.
type StakeAccountStream func(StakeAccountVisitor) (int, error)

// The temporary format retains every Delegation field and the account's separate
// credits-observed value. It is not an on-disk AccountsDB or snapshot format.
const stakeDelegationRecordSize = 112

// StakeDelegationSpool is an immutable, repeatable stake scan. Epoch transitions
// need the completed history before they can calculate entering-epoch weights,
// but need not reread and decode millions of accounts to perform that second
// calculation (or the subsequent reward calculation). Only a fixed I/O buffer
// is resident, regardless of stake count. Close removes the temporary file.
type StakeDelegationSpool struct {
	mu    sync.RWMutex
	file  *os.File
	size  int64
	count int
}

// CaptureStakeDelegations captures a single parent scan, optionally performing
// the outgoing-epoch aggregation in visit at the same time. source must return
// only after all its visitors have completed.
func CaptureStakeDelegations(dir string, source StakeAccountStream, visit StakeAccountVisitor) (_ *StakeDelegationSpool, err error) {
	file, err := os.CreateTemp(dir, "epoch-stake-delegations-*")
	if err != nil {
		return nil, err
	}
	spool := &StakeDelegationSpool{file: file}
	complete := false
	defer func() {
		if !complete {
			_ = spool.Close()
		}
	}()
	writer := bufio.NewWriterSize(file, 256*1024)
	var mu sync.Mutex
	var writeErr error
	_, err = source(func(pk solana.PublicKey, delegation *sealevel.Delegation, credits uint64) {
		var record [stakeDelegationRecordSize]byte
		copy(record[:32], pk[:])
		copy(record[32:64], delegation.VoterPubkey[:])
		binary.LittleEndian.PutUint64(record[64:72], delegation.StakeLamports)
		binary.LittleEndian.PutUint64(record[72:80], delegation.ActivationEpoch)
		binary.LittleEndian.PutUint64(record[80:88], delegation.DeactivationEpoch)
		binary.LittleEndian.PutUint64(record[88:96], math.Float64bits(delegation.WarmupCooldownRate))
		binary.LittleEndian.PutUint64(record[96:104], delegation.CreditsObserved)
		binary.LittleEndian.PutUint64(record[104:112], credits)
		mu.Lock()
		if writeErr == nil {
			_, writeErr = writer.Write(record[:])
			spool.count++
		}
		mu.Unlock()
		if visit != nil {
			visit(pk, delegation, credits)
		}
	})
	if err != nil {
		return nil, fmt.Errorf("capturing stake delegations: %w", err)
	}
	if writeErr != nil {
		return nil, fmt.Errorf("writing stake delegations: %w", writeErr)
	}
	if err = writer.Flush(); err != nil {
		return nil, fmt.Errorf("flushing stake delegations: %w", err)
	}
	spool.size = int64(spool.count) * stakeDelegationRecordSize
	complete = true
	return spool, nil
}

// Stream visits immutable records sequentially. Independent calls have their
// own read offsets and buffers, so replaying the stream never consumes it.
func (s *StakeDelegationSpool) Stream(visit StakeAccountVisitor) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.file == nil {
		return 0, fmt.Errorf("stake delegation spool is closed")
	}
	reader := bufio.NewReaderSize(io.NewSectionReader(s.file, 0, s.size), 256*1024)
	var record [stakeDelegationRecordSize]byte
	var delegation sealevel.Delegation
	for i := 0; i < s.count; i++ {
		if _, err := io.ReadFull(reader, record[:]); err != nil {
			return i, fmt.Errorf("reading stake delegation %d: %w", i, err)
		}
		var pk solana.PublicKey
		copy(pk[:], record[:32])
		delegation = sealevel.Delegation{
			StakeLamports:      binary.LittleEndian.Uint64(record[64:72]),
			ActivationEpoch:    binary.LittleEndian.Uint64(record[72:80]),
			DeactivationEpoch:  binary.LittleEndian.Uint64(record[80:88]),
			WarmupCooldownRate: math.Float64frombits(binary.LittleEndian.Uint64(record[88:96])),
			CreditsObserved:    binary.LittleEndian.Uint64(record[96:104]),
		}
		copy(delegation.VoterPubkey[:], record[32:64])
		visit(pk, &delegation, binary.LittleEndian.Uint64(record[104:112]))
	}
	return s.count, nil
}

// Close waits for active readers before removing the file; repeated closes are
// harmless. This spool is scratch data, never a saved reward-partition plan.
func (s *StakeDelegationSpool) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil {
		return nil
	}
	path := s.file.Name()
	err := s.file.Close()
	s.file = nil
	removeErr := os.Remove(path)
	if err != nil {
		return err
	}
	return removeErr
}

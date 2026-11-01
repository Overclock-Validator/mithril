package accountsdb

import (
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/cockroachdb/pebble/v2/vfs/errorfs"
)

// TestCloseDbReturnsStoreCloseErrors arms empty WALs only after both stores open,
// so the observed writes are Close's EOF trailers. It checks error propagation
// and Pebble's closed marker, not fsync failures or complete resource release.
func TestCloseDbReturnsStoreCloseErrors(t *testing.T) {
	indexErr := errors.New("index close-time wal write failure")
	bankErr := errors.New("bank close-time wal write failure")
	for _, tc := range []struct {
		name      string
		failIndex bool
		failBank  bool
	}{
		{name: "neither"},
		{name: "index", failIndex: true},
		{name: "bank", failBank: true},
		{name: "both", failIndex: true, failBank: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var armed atomic.Bool
			var indexWrites, bankWrites atomic.Int32
			openStore := func(name string, fail bool, injected error, writes *atomic.Int32) (*pebble.DB, error) {
				fs := errorfs.Wrap(vfs.NewMem(), errorfs.InjectorFunc(func(op errorfs.Op) error {
					if !armed.Load() || op.Kind != errorfs.OpFileWrite || !strings.HasSuffix(op.Path, ".log") {
						return nil
					}
					writes.Add(1)
					if fail {
						return injected
					}
					return nil
				}))
				return pebble.Open(name, &pebble.Options{FS: fs, Logger: silentLogger{}})
			}
			index, err := openStore("index", tc.failIndex, indexErr, &indexWrites)
			if err != nil {
				t.Fatalf("open index: %v", err)
			}
			bank, err := openStore("bank", tc.failBank, bankErr, &bankWrites)
			if err != nil {
				if closeErr := index.Close(); closeErr != nil {
					t.Errorf("close index after bank open failure: %v", closeErr)
				}
				t.Fatalf("open bank: %v", err)
			}

			armed.Store(true)
			got := closeDbResultForTest(&AccountsDb{Index: index, BankHashStore: bank})
			armed.Store(false)
			if !tc.failIndex && !tc.failBank && got != nil {
				t.Errorf("CloseDb() = %v, want nil", got)
			}
			for _, store := range []struct {
				name    string
				db      *pebble.DB
				writes  int32
				err     error
				wantErr bool
			}{
				{name: "index", db: index, writes: indexWrites.Load(), err: indexErr, wantErr: tc.failIndex},
				{name: "bank", db: bank, writes: bankWrites.Load(), err: bankErr, wantErr: tc.failBank},
			} {
				if store.writes != 1 {
					t.Errorf("%s close-time WAL write callbacks = %d, want 1", store.name, store.writes)
				}
				if errors.Is(got, store.err) != store.wantErr {
					t.Errorf("CloseDb() = %v; errors.Is(%s failure) = %t, want %t", got, store.name, errors.Is(got, store.err), store.wantErr)
				}
				if !closeDbStoreClosedForTest(t, store.db) {
					t.Errorf("%s still accepts Get after AccountsDb.CloseDb", store.name)
					// Close only a store proven open, never a second Close on a closed DB.
					if err := store.db.Close(); err != nil {
						t.Errorf("cleanup unclosed %s: %v", store.name, err)
					}
				}
			}
		})
	}
}

// closeDbResultForTest keeps the void baseline runnable, so lost errors fail
// behaviorally rather than through an incompatible method signature.
func closeDbResultForTest(db *AccountsDb) error {
	if closer, ok := any(db).(interface{ CloseDb() error }); ok {
		return closer.CloseDb()
	}
	db.CloseDb()
	return nil
}

func closeDbStoreClosedForTest(t *testing.T, db *pebble.DB) (closed bool) {
	t.Helper()
	defer func() {
		if value := recover(); value != nil {
			err, ok := value.(error)
			if !ok || !errors.Is(err, pebble.ErrClosed) {
				panic(value)
			}
			closed = true
		}
	}()
	_, closer, err := db.Get([]byte("close-state-probe"))
	if closer != nil {
		if closeErr := closer.Close(); closeErr != nil {
			t.Errorf("close probe value: %v", closeErr)
		}
	}
	if !errors.Is(err, pebble.ErrNotFound) {
		t.Errorf("open-store probe Get() = %v, want %v", err, pebble.ErrNotFound)
	}
	return false
}

// Package genesisinit adapts a native genesis bank to AccountsDB. It is
// reusable by a future live bootstrap path; it contains no network fallback.
package genesisinit

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/genesis"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/cockroachdb/pebble"
	"github.com/gagliardetto/solana-go"
)

type indexRow struct {
	key   solana.PublicKey
	entry accountsdb.AccountIndexEntry
}
type indexSource []indexRow

func syncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}
func writeNew(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	return errors.Join(err, f.Sync(), f.Close())
}

// Initialize constructs, persists and verifies the bank under AccountsDB's
// exclusive lock. Partial durable artifacts are intentionally retained without
// a ready marker after an error: retries refuse that occupied directory. Only
// owned temporary artifacts are removed by the writers that created them.
func Initialize(ctx context.Context, g *genesis.Genesis, root string) (*genesis.BankMetadata, error) {
	return initialize(ctx, g, root, nil)
}
func initialize(ctx context.Context, g *genesis.Genesis, root string, checkpoint func(string) error) (_ *genesis.BankMetadata, retErr error) {
	bank, err := genesis.ConstructInitialBank(ctx, g)
	if err != nil {
		return nil, err
	}
	raw, err := genesis.Encode(g)
	if err != nil {
		return nil, err
	}
	metadata, err := json.MarshalIndent(bank.Frozen.Metadata, "", "  ")
	if err != nil {
		return nil, err
	}
	check := func(stage string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if checkpoint != nil {
			return checkpoint(stage)
		}
		return nil
	}
	if err = check("before-directory"); err != nil {
		return nil, err
	}
	if root == "" {
		return nil, fmt.Errorf("empty accounts path")
	}
	if err = os.Mkdir(root, 0755); err != nil && !os.IsExist(err) {
		return nil, err
	}
	guard, err := accountsdb.AcquireExclusiveAccountsDbStore(root)
	if err != nil {
		return nil, err
	}
	defer func() { retErr = errors.Join(retErr, guard.Close()) }()
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if entry.Name() != accountsdb.AccountsDbLockFileName {
			return nil, fmt.Errorf("accounts destination is occupied (%s); use a new empty directory", entry.Name())
		}
	}
	if err = check("locked"); err != nil {
		return nil, err
	}
	// This durable intent protects interrupted initialization from node cleanup.
	if err = writeNew(filepath.Join(root, state.GenesisInitializingFileName), []byte("{\"origin\":\"genesis\",\"version\":1}\n")); err != nil {
		return nil, err
	}
	if err = syncDir(root); err != nil {
		return nil, err
	}
	if err = syncDir(filepath.Dir(root)); err != nil {
		return nil, err
	}
	if err = check("intent"); err != nil {
		return nil, err
	}
	accountsPath := filepath.Join(root, "accounts")
	if err = os.Mkdir(accountsPath, 0755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(accountsPath, "0.1"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	source := indexSource{}
	stakes := []accountsdb.StakeIndexEntry{}
	offset := uint64(0)
	for _, a := range bank.Frozen.Accounts {
		if err = ctx.Err(); err != nil {
			_ = f.Close()
			return nil, err
		}
		idx := accountsdb.AccountIndexEntry{Slot: 0, FileId: 1, Offset: offset}
		source = append(source, indexRow{a.Pubkey, idx})
		if a.Owner == solana.StakeProgramID {
			stakes = append(stakes, accountsdb.StakeIndexEntry{Pubkey: a.Pubkey, FileId: 1, Offset: offset})
		}
		av := accountsdb.AppendVecAccount{Pubkey: a.Pubkey, Lamports: a.Lamports, Owner: a.Owner, Executable: a.Executable, RentEpoch: a.RentEpoch, DataLen: uint64(len(a.Data)), Data: a.Data}
		n, writeErr := av.MarshalReturningLength(f)
		if writeErr != nil {
			_ = f.Close()
			return nil, writeErr
		}
		offset += uint64(n)
	}
	if err = errors.Join(f.Sync(), f.Close()); err != nil {
		return nil, err
	}
	if err = syncDir(accountsPath); err != nil {
		return nil, err
	}
	if err = accountsdb.WriteLargestFileID(root, 1); err != nil {
		return nil, err
	}
	if err = accountsdb.WriteBootstrapHighFileID(root, 1); err != nil {
		return nil, err
	}
	if err = accountsdb.WriteStakePubkeyIndex(filepath.Join(root, "stake_pubkeys.idx"), stakes); err != nil {
		return nil, err
	}
	stakeFile, err := os.OpenFile(filepath.Join(root, "stake_pubkeys.idx"), os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	if err = errors.Join(stakeFile.Sync(), stakeFile.Close()); err != nil {
		return nil, err
	}
	bankhash := solana.MustHashFromBase58(bank.Frozen.Metadata.BankHash)
	for _, file := range []struct {
		name string
		data []byte
	}{{"genesis.bin", raw}, {state.GenesisBankFileName, metadata}, {"bank_hash", bankhash[:]}} {
		if err = writeNew(filepath.Join(root, file.name), file.data); err != nil {
			return nil, err
		}
	}
	if err = syncDir(root); err != nil {
		return nil, err
	}
	if err = check("accounts"); err != nil {
		return nil, err
	}
	db, err := accountsdb.CreateDbWithStoreGuard(root, guard)
	if err != nil {
		return nil, err
	}
	defer func() { retErr = errors.Join(retErr, db.Shutdown(context.Background())) }()
	batch := db.Index.NewBatch()
	for _, row := range source {
		if err = ctx.Err(); err != nil {
			_ = batch.Close()
			return nil, err
		}
		var encoded [24]byte
		row.entry.Marshal(&encoded)
		if err = batch.Set(row.key[:], encoded[:], nil); err != nil {
			_ = batch.Close()
			return nil, err
		}
	}
	err = errors.Join(batch.Commit(pebble.Sync), batch.Close())
	if err != nil {
		return nil, err
	}
	if err = syncDir(root); err != nil {
		return nil, err
	}
	if err = check("index"); err != nil {
		return nil, err
	}
	var slot [8]byte
	if err = db.BankHashStore.Set(slot[:], bankhash[:], pebble.Sync); err != nil {
		return nil, err
	}
	if err = verifyAccounts(ctx, db, &bank.Frozen); err != nil {
		return nil, err
	}
	digest := sha256.Sum256(metadata)
	marker := &state.MithrilState{StateSchemaVersion: state.GenesisStateSchemaVersion, Origin: "genesis", Stage: "ready", BuildMode: "genesis", Cluster: "alpenglow", GenesisHash: bank.Frozen.Metadata.GenesisHash, BuildCompleted: time.Now().UTC(), Genesis: &state.GenesisOrigin{Version: 1, StorageFormat: state.GenesisStorageFormat, RootSlot: 0, BankHash: bank.Frozen.Metadata.BankHash, NextReplaySlot: 1, MetadataSHA256: hex.EncodeToString(digest[:])}}
	if err = marker.ValidateGenesisSidecars(root); err != nil {
		return nil, err
	}
	if err = marker.ValidateAgainstBankhashDB(db); err != nil {
		return nil, err
	}
	if err = check("verified"); err != nil {
		return nil, err
	}
	if err = os.Remove(filepath.Join(root, state.GenesisInitializingFileName)); err != nil {
		return nil, err
	}
	if err = syncDir(root); err != nil {
		return nil, err
	}
	// Save is the final validity publication. Never roll back dependencies on a
	// commit-decided fsync failure: the ready marker may already be visible.
	if err = marker.Save(root); err != nil {
		return nil, err
	}
	return &bank.Frozen.Metadata, nil
}
func verifyAccounts(ctx context.Context, db *accountsdb.AccountsDb, want *genesis.Bank) error {
	db.InitCaches()
	var count uint64
	if err := db.ScanKeysBetweenPrefixes(ctx, 0, ^uint64(0), func(solana.PublicKey) error { count++; return nil }); err != nil {
		return err
	}
	if count != uint64(len(want.Accounts)) {
		return fmt.Errorf("genesis account index count mismatch")
	}
	for _, a := range want.Accounts {
		if err := ctx.Err(); err != nil {
			return err
		}
		got, err := db.GetAccount(0, a.Pubkey)
		if err != nil {
			return err
		}
		if got == nil || got.Key != a.Pubkey || got.Lamports != a.Lamports || got.Owner != a.Owner || got.Executable != a.Executable || got.RentEpoch != a.RentEpoch || !bytes.Equal(got.Data, a.Data) {
			return fmt.Errorf("persisted genesis account mismatch: %s", a.Key)
		}
	}
	return nil
}

// Open validates and reopens a completed genesis-origin database. The caller
// owns the returned database and must call Shutdown. No replay is started.
func Open(ctx context.Context, root string) (*accountsdb.AccountsDb, *genesis.BankMetadata, error) {
	return open(ctx, root)
}
func open(ctx context.Context, root string) (_ *accountsdb.AccountsDb, _ *genesis.BankMetadata, retErr error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	guard, err := accountsdb.AcquireExclusiveAccountsDbStore(root)
	if err != nil {
		return nil, nil, err
	}
	defer func() { retErr = errors.Join(retErr, guard.Close()) }()
	s, err := state.LoadState(root)
	if err != nil {
		return nil, nil, err
	}
	if !s.HasGenesisRoot() || !s.IsReady() {
		return nil, nil, fmt.Errorf("no ready genesis-origin state")
	}
	if s.StateSchemaVersion == state.GenesisReplayStateSchemaVersion {
		return nil, nil, fmt.Errorf("genesis replay checkpoint store requires replay.OpenGenesisReplay")
	}
	if err = s.ValidateGenesisSidecars(root); err != nil {
		return nil, nil, err
	}
	g, _, err := genesis.ReadGenesisFromFile(filepath.Join(root, "genesis.bin"))
	if err != nil {
		return nil, nil, err
	}
	bank, err := genesis.ConstructInitialBank(ctx, g)
	if err != nil {
		return nil, nil, err
	}
	raw, err := os.ReadFile(filepath.Join(root, state.GenesisBankFileName))
	if err != nil {
		return nil, nil, err
	}
	var metadata genesis.BankMetadata
	if err = json.Unmarshal(raw, &metadata); err != nil {
		return nil, nil, err
	}
	if !reflect.DeepEqual(metadata, bank.Frozen.Metadata) {
		return nil, nil, fmt.Errorf("bootstrap metadata does not match reconstructed initial bank")
	}
	db, err := accountsdb.OpenDbWithStoreGuard(root, guard)
	if err != nil {
		return nil, nil, err
	}
	if err = errors.Join(verifyAccounts(ctx, db, &bank.Frozen), s.ValidateAgainstBankhashDB(db)); err != nil {
		return nil, nil, errors.Join(err, db.Shutdown(context.Background()))
	}
	return db, &metadata, nil
}

package state

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/mr-tron/base58"
)

const GenesisStorageFormat = "pebble-v1"

const GenesisBankFileName = "genesis_bank.json"
const GenesisInitializingFileName = "genesis_initializing.json"

// GenesisOrigin records an explicit durable root, including slot zero. The
// separately versioned metadata file contains the full bank bootstrap seed.
type GenesisOrigin struct {
	StorageFormat  string `json:"storage_format"`
	Version        uint32 `json:"version"`
	RootSlot       uint64 `json:"root_slot"`
	BankHash       string `json:"bank_hash"`
	NextReplaySlot uint64 `json:"next_replay_slot"`
	MetadataSHA256 string `json:"metadata_sha256"`
}

func (s *MithrilState) HasGenesisRoot() bool {
	return s != nil && (s.StateSchemaVersion == GenesisStateSchemaVersion || s.StateSchemaVersion == GenesisReplayStateSchemaVersion) && s.Origin == "genesis" && s.Genesis != nil
}

// HasDurableRoot distinguishes genesis's real slot-zero root from no watermark.
func (s *MithrilState) HasDurableRoot() bool {
	return s != nil && (s.HasGenesisRoot() || s.LastRootedSlot > 0)
}
func (s *MithrilState) ValidateGenesisOrigin() error {
	if !s.HasGenesisRoot() || s.Genesis.Version != 1 || s.Genesis.StorageFormat != GenesisStorageFormat || s.Genesis.RootSlot != 0 || s.Genesis.NextReplaySlot != 1 || s.SnapshotSlot != 0 || s.FullSnapshot != nil || s.IncrSnapshot != nil || s.LastSlot != 0 || s.LastRootedSlot != 0 {
		return fmt.Errorf("unsupported or malformed genesis-origin state")
	}
	for _, value := range []string{s.GenesisHash, s.Genesis.BankHash} {
		b, err := base58.Decode(value)
		if err != nil || len(b) != 32 || bytes.Equal(b, make([]byte, 32)) {
			return fmt.Errorf("invalid genesis-origin hash")
		}
	}
	digest, err := hex.DecodeString(s.Genesis.MetadataSHA256)
	if err != nil || len(digest) != 32 {
		return fmt.Errorf("invalid genesis metadata digest")
	}
	return nil
}
func (s *MithrilState) validateGenesisBankhash(db BankhashGetter) error {
	if err := s.ValidateGenesisOrigin(); err != nil {
		return err
	}
	h, err := db.GetBankHashForSlot(0)
	if err != nil {
		return err
	}
	if base58.Encode(h) != s.Genesis.BankHash {
		return fmt.Errorf("genesis slot-0 bank hash does not match database")
	}
	return nil
}
func (s *MithrilState) ValidateGenesisArtifacts(root string) error {
	if err := s.ValidateGenesisOrigin(); err != nil {
		return err
	}
	if err := accountsdb.ValidateBootstrapStoreArtifacts(root); err != nil {
		return err
	}
	return s.ValidateGenesisSidecars(root)
}

// ValidateGenesisSidecars may be called while holding the exclusive AccountsDB guard.
// The guarded database opener validates the account index itself.
func (s *MithrilState) ValidateGenesisSidecars(root string) error {
	if err := s.ValidateGenesisOrigin(); err != nil {
		return err
	}
	for _, name := range []string{"accounts", "bankhash_db"} {
		info, err := os.Lstat(filepath.Join(root, name))
		if err != nil {
			return err
		}
		if !info.IsDir() {
			return fmt.Errorf("genesis artifact %s is not a real directory", name)
		}
	}
	for _, name := range []string{GenesisBankFileName, "genesis.bin", "bank_hash"} {
		info, err := os.Lstat(filepath.Join(root, name))
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Size() > 10_000_000 {
			return fmt.Errorf("invalid genesis artifact %s", name)
		}
	}
	metadata, err := os.ReadFile(filepath.Join(root, GenesisBankFileName))
	if err != nil {
		return err
	}
	digest := sha256.Sum256(metadata)
	if hex.EncodeToString(digest[:]) != s.Genesis.MetadataSHA256 {
		return fmt.Errorf("genesis bootstrap metadata checksum mismatch")
	}
	raw, err := os.ReadFile(filepath.Join(root, "genesis.bin"))
	if err != nil {
		return err
	}
	hash := sha256.Sum256(raw)
	if base58.Encode(hash[:]) != s.GenesisHash {
		return fmt.Errorf("persisted genesis hash mismatch")
	}
	bankhash, err := os.ReadFile(filepath.Join(root, "bank_hash"))
	if err != nil {
		return err
	}
	if base58.Encode(bankhash) != s.Genesis.BankHash {
		return fmt.Errorf("genesis bank_hash mismatch")
	}
	if _, err = accountsdb.ValidateLargestFileID(root); err != nil {
		return err
	}
	if _, err = accountsdb.ReadBootstrapHighFileID(root); err != nil {
		return err
	}
	_, err = accountsdb.ValidateStakePubkeyIndex(filepath.Join(root, "stake_pubkeys.idx"))
	return err
}

// RejectGenesisLaunch runs before startup can clean a database or choose RPC.
// An interrupted bootstrap is also protected, even without its ready marker.
func RejectGenesisLaunch(root string) error {
	if root == "" {
		return nil
	}
	for _, name := range []string{GenesisInitializingFileName, GenesisBankFileName} {
		if _, err := os.Lstat(filepath.Join(root, name)); err == nil {
			return fmt.Errorf("genesis-origin AccountsDB detected: live genesis startup is not implemented; database preserved")
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	s, err := LoadState(root)
	if err != nil {
		// Ordinary snapshot replacement retains its existing error policy. A
		// recognized genesis or newer schema must never reach that cleanup path.
		raw, readErr := os.ReadFile(filepath.Join(root, StateFileName))
		if readErr != nil {
			return readErr
		}
		var header struct {
			Version uint32 `json:"state_schema_version"`
			Origin  string `json:"origin"`
		}
		if json.Unmarshal(raw, &header) == nil && (header.Version > CurrentStateSchemaVersion || header.Origin == "genesis") {
			return err
		}
		return nil
	}
	if s.HasGenesisRoot() {
		return fmt.Errorf("genesis-origin AccountsDB detected: live genesis startup is not implemented; database preserved")
	}
	return nil
}

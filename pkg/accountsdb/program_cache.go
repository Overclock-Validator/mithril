package accountsdb

import (
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/sbpf"
	"github.com/Overclock-Validator/mithril/pkg/sbpf/sbpfver"
	"github.com/gagliardetto/solana-go"
)

const programCacheFilename = "program_cache.gob"

// SerializableProgram is a gob-encodable version of sbpf.Program
type SerializableProgram struct {
	RO          []byte
	Text        []uint64 // sbpf.Slot is uint64
	TextVA      uint64
	Entrypoint  uint64
	Funcs       map[uint32]int64
	SbpfVersion uint32
}

// SerializableProgramCacheEntry is a gob-encodable version of ProgramCacheEntry
type SerializableProgramCacheEntry struct {
	Program        SerializableProgram
	DeploymentSlot uint64
}

// SerializableProgramCache holds all cache entries for serialization
type SerializableProgramCache struct {
	Entries map[solana.PublicKey]SerializableProgramCacheEntry
}

func init() {
	// Register types for gob encoding
	gob.Register(SerializableProgramCache{})
	gob.Register(solana.PublicKey{})
}

// toSerializable converts a ProgramCacheEntry to a serializable form
func toSerializable(entry *ProgramCacheEntry) SerializableProgramCacheEntry {
	prog := entry.Program
	text := make([]uint64, len(prog.Text))
	for i, slot := range prog.Text {
		text[i] = uint64(slot)
	}

	return SerializableProgramCacheEntry{
		Program: SerializableProgram{
			RO:          prog.RO,
			Text:        text,
			TextVA:      prog.TextVA,
			Entrypoint:  prog.Entrypoint,
			Funcs:       prog.Funcs,
			SbpfVersion: prog.SbpfVersion.Version,
		},
		DeploymentSlot: entry.DeploymentSlot,
	}
}

// fromSerializable converts a serializable entry back to ProgramCacheEntry
func fromSerializable(entry SerializableProgramCacheEntry) *ProgramCacheEntry {
	text := make([]sbpf.Slot, len(entry.Program.Text))
	for i, slot := range entry.Program.Text {
		text[i] = sbpf.Slot(slot)
	}

	return &ProgramCacheEntry{
		Program: &sbpf.Program{
			RO:          entry.Program.RO,
			Text:        text,
			TextVA:      entry.Program.TextVA,
			Entrypoint:  entry.Program.Entrypoint,
			Funcs:       entry.Program.Funcs,
			SbpfVersion: sbpfver.SbpfVersion{Version: entry.Program.SbpfVersion},
		},
		DeploymentSlot: entry.DeploymentSlot,
	}
}

// SaveProgramCache saves the program cache to disk
func (accountsDb *AccountsDb) SaveProgramCache() error {
	cacheFile := filepath.Join(filepath.Dir(accountsDb.AcctsDir), programCacheFilename)

	// Collect all entries from the otter cache
	cache := SerializableProgramCache{
		Entries: make(map[solana.PublicKey]SerializableProgramCacheEntry),
	}

	// Iterate over the cache - otter doesn't have a Range method,
	// so we need to track keys separately or use a different approach
	// For now, we'll save what we can access
	accountsDb.ProgramCache.Range(func(key solana.PublicKey, entry *ProgramCacheEntry) bool {
		cache.Entries[key] = toSerializable(entry)
		return true
	})

	if len(cache.Entries) == 0 {
		mlog.Log.Debugf("program cache is empty, skipping save")
		return nil
	}

	file, err := os.Create(cacheFile)
	if err != nil {
		return fmt.Errorf("failed to create program cache file: %w", err)
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(cache); err != nil {
		return fmt.Errorf("failed to encode program cache: %w", err)
	}

	mlog.Log.Infof("saved %d programs to cache file %s", len(cache.Entries), cacheFile)
	return nil
}

// LoadProgramCache loads the program cache from disk
func (accountsDb *AccountsDb) LoadProgramCache() error {
	cacheFile := filepath.Join(filepath.Dir(accountsDb.AcctsDir), programCacheFilename)

	file, err := os.Open(cacheFile)
	if err != nil {
		if os.IsNotExist(err) {
			mlog.Log.Debugf("no program cache file found at %s", cacheFile)
			return nil
		}
		return fmt.Errorf("failed to open program cache file: %w", err)
	}
	defer file.Close()

	var cache SerializableProgramCache
	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&cache); err != nil {
		mlog.Log.Infof("failed to decode program cache (may be from incompatible version), starting fresh: %v", err)
		return nil
	}

	loaded := 0
	for key, entry := range cache.Entries {
		accountsDb.ProgramCache.Set(key, fromSerializable(entry))
		loaded++
	}

	mlog.Log.Infof("loaded %d programs from cache file %s", loaded, cacheFile)
	return nil
}

// ClearProgramCacheFile removes the program cache file from disk
func (accountsDb *AccountsDb) ClearProgramCacheFile() error {
	cacheFile := filepath.Join(filepath.Dir(accountsDb.AcctsDir), programCacheFilename)
	err := os.Remove(cacheFile)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

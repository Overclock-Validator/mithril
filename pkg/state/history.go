package state

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// MaxHistoryEntries is the maximum number of history entries to retain.
const MaxHistoryEntries = 500

// HistoryEvent represents the type of state change event.
type HistoryEvent string

const (
	HistoryEventBootstrap HistoryEvent = "bootstrap"
	HistoryEventShutdown  HistoryEvent = "shutdown"
	HistoryEventCorrupted HistoryEvent = "corrupted"
	HistoryEventResume    HistoryEvent = "resume"
	HistoryEventRebuild   HistoryEvent = "rebuild" // AccountsDB being cleaned/rebuilt
)

// StateHistoryEntry represents a single entry in the state history log.
type StateHistoryEntry struct {
	Timestamp time.Time    `json:"ts"`
	Event     HistoryEvent `json:"event"`
	Slot      uint64       `json:"slot"`
	Bankhash  string       `json:"bankhash,omitempty"`
	RunID     string       `json:"run_id,omitempty"`
	Version   string       `json:"version,omitempty"`
	Commit    string       `json:"commit,omitempty"`
	Branch    string       `json:"branch,omitempty"` // git branch name (may be empty)
	Reason    string       `json:"reason,omitempty"` // shutdown reason or corruption reason
}

// AppendHistory appends a history entry to the state history file.
// The history file is append-only JSONL format.
func AppendHistory(accountsDbDir string, entry StateHistoryEntry) error {
	historyFile := filepath.Join(accountsDbDir, HistoryFileName)

	// Ensure timestamp is set
	if entry.Timestamp.IsZero() {
		entry.Timestamp = time.Now()
	}

	// Marshal to JSON (single line)
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal history entry: %w", err)
	}

	// Append to file
	f, err := os.OpenFile(historyFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open history file: %w", err)
	}
	defer f.Close()

	if _, err := f.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("failed to write history entry: %w", err)
	}

	return nil
}

// LoadHistory loads all history entries from the state history file.
// Returns empty slice if file doesn't exist.
func LoadHistory(accountsDbDir string) ([]StateHistoryEntry, error) {
	historyFile := filepath.Join(accountsDbDir, HistoryFileName)

	f, err := os.Open(historyFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to open history file: %w", err)
	}
	defer f.Close()

	var entries []StateHistoryEntry
	scanner := bufio.NewScanner(f)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var entry StateHistoryEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			// Skip malformed lines but continue
			continue
		}
		entries = append(entries, entry)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read history file: %w", err)
	}

	return entries, nil
}

// PruneHistory removes old entries if history exceeds MaxHistoryEntries.
// Should be called on startup to prevent unbounded growth.
func PruneHistory(accountsDbDir string) error {
	entries, err := LoadHistory(accountsDbDir)
	if err != nil {
		return err
	}

	// Nothing to prune
	if len(entries) <= MaxHistoryEntries {
		return nil
	}

	// Keep only the most recent entries
	entries = entries[len(entries)-MaxHistoryEntries:]

	// Rewrite the file
	historyFile := filepath.Join(accountsDbDir, HistoryFileName)
	tmpFile := historyFile + ".tmp"

	f, err := os.Create(tmpFile)
	if err != nil {
		return fmt.Errorf("failed to create temp history file: %w", err)
	}

	for _, entry := range entries {
		data, err := json.Marshal(entry)
		if err != nil {
			f.Close()
			os.Remove(tmpFile)
			return fmt.Errorf("failed to marshal history entry: %w", err)
		}
		if _, err := f.Write(append(data, '\n')); err != nil {
			f.Close()
			os.Remove(tmpFile)
			return fmt.Errorf("failed to write history entry: %w", err)
		}
	}

	if err := f.Close(); err != nil {
		os.Remove(tmpFile)
		return fmt.Errorf("failed to close temp history file: %w", err)
	}

	if err := os.Rename(tmpFile, historyFile); err != nil {
		os.Remove(tmpFile)
		return fmt.Errorf("failed to rename history file: %w", err)
	}

	return nil
}

// GetRecentHistory returns the N most recent history entries.
func GetRecentHistory(accountsDbDir string, n int) ([]StateHistoryEntry, error) {
	entries, err := LoadHistory(accountsDbDir)
	if err != nil {
		return nil, err
	}

	if n <= 0 || len(entries) <= n {
		return entries, nil
	}

	return entries[len(entries)-n:], nil
}

// RecordBootstrap records a bootstrap event in the history.
func RecordBootstrap(accountsDbDir string, slot uint64, bankhash, runID, version, commit, branch string) error {
	return AppendHistory(accountsDbDir, StateHistoryEntry{
		Event:    HistoryEventBootstrap,
		Slot:     slot,
		Bankhash: bankhash,
		RunID:    runID,
		Version:  version,
		Commit:   commit,
		Branch:   branch,
	})
}

// RecordShutdown records a shutdown event in the history.
func RecordShutdown(accountsDbDir string, slot uint64, bankhash, runID, version, commit, branch, reason string) error {
	return AppendHistory(accountsDbDir, StateHistoryEntry{
		Event:    HistoryEventShutdown,
		Slot:     slot,
		Bankhash: bankhash,
		RunID:    runID,
		Version:  version,
		Commit:   commit,
		Branch:   branch,
		Reason:   reason,
	})
}

// RecordCorrupted records a corruption event in the history.
func RecordCorrupted(accountsDbDir string, slot uint64, bankhash, runID, version, commit, branch, reason string) error {
	return AppendHistory(accountsDbDir, StateHistoryEntry{
		Event:    HistoryEventCorrupted,
		Slot:     slot,
		Bankhash: bankhash,
		RunID:    runID,
		Version:  version,
		Commit:   commit,
		Branch:   branch,
		Reason:   reason,
	})
}

// RecordResume records a resume event in the history.
func RecordResume(accountsDbDir string, slot uint64, bankhash, runID, version, commit, branch string) error {
	return AppendHistory(accountsDbDir, StateHistoryEntry{
		Event:    HistoryEventResume,
		Slot:     slot,
		Bankhash: bankhash,
		RunID:    runID,
		Version:  version,
		Commit:   commit,
		Branch:   branch,
	})
}

// RecordRebuild records when AccountsDB is about to be cleaned/rebuilt.
// This should be called BEFORE CleanAccountsDbDir to preserve the history.
func RecordRebuild(accountsDbDir string, slot uint64, bankhash, version, commit, branch, reason string) error {
	return AppendHistory(accountsDbDir, StateHistoryEntry{
		Event:    HistoryEventRebuild,
		Slot:     slot,
		Bankhash: bankhash,
		Version:  version,
		Commit:   commit,
		Branch:   branch,
		Reason:   reason, // e.g., "new-snapshot", "snapshot mode", "user requested rebuild"
	})
}

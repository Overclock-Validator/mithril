package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	maxArtifactBytes            = 512 * 1024 // 512 KB per artifact (real ones are a few KB)
	maxDivergenceArtifacts      = 64
	maxDivergenceEntries        = 10_000
	divergencePrefix            = "bankhash_mismatch_slot_"
	maxDivergenceStringBytes    = 1024
	maxDivergenceExtraNames     = 16
	maxDivergenceExtraNameBytes = 128
	divergenceRecoveryGuidance  = "stop the node, preserve the existing AccountsDB for review, " +
		"and rebuild from a snapshot into a distinct empty AccountsDB root"
)

// DivergenceArtifact is a parsed bank-hash divergence artifact. Key fields are
// named; every other field is preserved in Extra for forward compatibility.
type DivergenceArtifact struct {
	ArtifactType    *string `json:"type,omitempty"`
	CheckedSlot     *uint64 `json:"checked_slot,omitempty"`
	OurBankhash     *string `json:"our_bankhash,omitempty"`
	WinningBankhash *string `json:"winning_bankhash,omitempty"`
	Policy          *string `json:"policy,omitempty"`
	RunID           *string `json:"run_id,omitempty"`
	CreatedAt       *string `json:"created_at,omitempty"`

	Extra map[string]json.RawMessage `json:"-"`
}

var knownDivergenceKeys = map[string]bool{
	"type": true, "checked_slot": true, "our_bankhash": true,
	"winning_bankhash": true, "policy": true, "run_id": true, "created_at": true,
}

type divergenceAlias DivergenceArtifact

func (d *DivergenceArtifact) UnmarshalJSON(data []byte) error {
	var a divergenceAlias
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	*d = DivergenceArtifact(a)
	for _, value := range []**string{&d.ArtifactType, &d.OurBankhash, &d.WinningBankhash, &d.Policy, &d.RunID, &d.CreatedAt} {
		if *value != nil {
			bounded, _ := truncateUTF8Bytes(redactUntrustedText(**value), maxDivergenceStringBytes)
			*value = &bounded
		}
	}
	extra, err := splitExtra(data, knownDivergenceKeys)
	if err != nil {
		return err
	}
	for key, raw := range extra {
		extra[key] = redactRawJSON(raw)
	}
	d.Extra = extra
	return nil
}

func (d DivergenceArtifact) MarshalJSON() ([]byte, error) {
	named, err := json.Marshal(divergenceAlias(d))
	if err != nil {
		return nil, err
	}
	return mergeExtra(named, d.Extra)
}

// resolveConsensusDir finds the consensus artifact directory in the active or
// legacy log layout.
func resolveConsensusDir(logDir string) (dir, layout string, found bool, err error) {
	return resolveActiveOrFlatDir(logDir, "consensus")
}

// slotFromArtifactPath parses the slot out of a bankhash_mismatch_slot_<N>.json
// filename. A malformed suffix is invalid evidence, not slot-zero evidence.
func slotFromArtifactPath(p string) (uint64, bool) {
	name := filepath.Base(p)
	name = strings.TrimPrefix(name, divergencePrefix)
	name = strings.TrimSuffix(name, ".json")
	n, err := strconv.ParseUint(name, 10, 64)
	return n, err == nil && name != ""
}

func validateDivergenceArtifact(artifact *DivergenceArtifact, filenameSlot uint64) error {
	if artifact.ArtifactType == nil || *artifact.ArtifactType != "bankhash_mismatch" {
		return fmt.Errorf("artifact type is not bankhash_mismatch")
	}
	if artifact.CheckedSlot == nil || *artifact.CheckedSlot != filenameSlot {
		return fmt.Errorf("checked_slot does not match filename slot")
	}
	if artifact.OurBankhash == nil || artifact.WinningBankhash == nil {
		return fmt.Errorf("artifact is missing bank hashes")
	}
	if err := validateHash(*artifact.OurBankhash); err != nil {
		return fmt.Errorf("our_bankhash is invalid")
	}
	if err := validateHash(*artifact.WinningBankhash); err != nil {
		return fmt.Errorf("winning_bankhash is invalid")
	}
	if *artifact.OurBankhash == *artifact.WinningBankhash {
		return fmt.Errorf("artifact bank hashes are identical")
	}
	if artifact.Policy == nil || (*artifact.Policy != "halt" && *artifact.Policy != "warn") {
		return fmt.Errorf("artifact policy is invalid")
	}
	if artifact.RunID == nil || strings.TrimSpace(*artifact.RunID) == "" {
		return fmt.Errorf("artifact run_id is missing")
	}
	if artifact.CreatedAt == nil {
		return fmt.Errorf("artifact created_at is missing")
	}
	if _, err := time.Parse(time.RFC3339Nano, *artifact.CreatedAt); err != nil {
		return fmt.Errorf("artifact created_at is invalid")
	}
	return nil
}

type DivergenceMeta struct {
	SourceLayout  string `json:"source_layout,omitempty"`
	Scanned       int    `json:"scanned_entries"`
	ScanTruncated bool   `json:"scan_truncated"`
	Candidates    int    `json:"candidates"`
	Returned      int    `json:"returned"`
	Invalid       int    `json:"invalid"`
	Oversized     int    `json:"oversized"`
	Unreadable    int    `json:"unreadable"`
	Truncated     bool   `json:"truncated"`
}

// DivergenceArtifactSummary keeps mismatch evidence and omits arbitrary values.
type DivergenceArtifactSummary struct {
	ArtifactType           *string  `json:"type,omitempty"`
	CheckedSlot            *uint64  `json:"checked_slot,omitempty"`
	OurBankhash            *string  `json:"our_bankhash,omitempty"`
	WinningBankhash        *string  `json:"winning_bankhash,omitempty"`
	Policy                 *string  `json:"policy,omitempty"`
	RunID                  *string  `json:"run_id,omitempty"`
	CreatedAt              *string  `json:"created_at,omitempty"`
	OmittedExtraFieldCount int      `json:"omitted_extra_field_count"`
	OmittedExtraBytes      int64    `json:"omitted_extra_bytes"`
	OmittedExtraFields     []string `json:"omitted_extra_fields"`
	ExtraFieldsTruncated   bool     `json:"extra_fields_truncated"`
}

func summarizeDivergenceArtifact(artifact DivergenceArtifact) DivergenceArtifactSummary {
	keys, omittedBytes, truncated := boundedExtraMetadata(artifact.Extra, maxDivergenceExtraNames, maxDivergenceExtraNameBytes)
	return DivergenceArtifactSummary{
		ArtifactType: artifact.ArtifactType, CheckedSlot: artifact.CheckedSlot,
		OurBankhash: artifact.OurBankhash, WinningBankhash: artifact.WinningBankhash,
		Policy: artifact.Policy, RunID: artifact.RunID, CreatedAt: artifact.CreatedAt,
		OmittedExtraFieldCount: len(artifact.Extra), OmittedExtraBytes: omittedBytes,
		OmittedExtraFields: keys, ExtraFieldsTruncated: truncated,
	}
}

func summarizeDivergenceArtifacts(artifacts []DivergenceArtifact) []DivergenceArtifactSummary {
	out := make([]DivergenceArtifactSummary, 0, len(artifacts))
	for _, artifact := range artifacts {
		out = append(out, summarizeDivergenceArtifact(artifact))
	}
	return out
}

func readDivergenceArtifactsContext(ctx context.Context, logDir string) ([]DivergenceArtifact, DivergenceMeta, error) {
	dir, layout, found, err := resolveConsensusDir(logDir)
	if err != nil {
		return nil, DivergenceMeta{}, err
	}
	meta := DivergenceMeta{SourceLayout: layout}
	if !found {
		return []DivergenceArtifact{}, meta, nil
	}
	root, err := openConfinedRoot(dir, logDir)
	if err != nil {
		return nil, meta, err
	}
	defer root.Close()
	directory, err := root.Open(".")
	if err != nil {
		return nil, meta, err
	}
	defer directory.Close()
	type candidate struct {
		name string
		slot uint64
	}
	var candidates []candidate
	scanTruncated := false
	for scanned := 0; ; {
		if err := ctx.Err(); err != nil {
			return nil, meta, err
		}
		entries, readErr := directory.ReadDir(256)
		for _, entry := range entries {
			if scanned == maxDivergenceEntries {
				scanTruncated = true
				break
			}
			scanned++
			meta.Scanned = scanned
			name := entry.Name()
			if strings.HasSuffix(name, ".json") && strings.HasPrefix(name, divergencePrefix) {
				meta.Candidates++
				slot, ok := slotFromArtifactPath(name)
				if !ok {
					meta.Invalid++
					continue
				}
				candidates = append(candidates, candidate{name: name, slot: slot})
			}
		}
		if scanTruncated {
			break
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return nil, meta, readErr
		}
	}
	meta.ScanTruncated = scanTruncated
	meta.Truncated = scanTruncated
	// Newest evidence is operationally most relevant. Filenames use unpadded
	// decimal slots, so sort numerically rather than lexicographically.
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].slot > candidates[j].slot
	})

	out := []DivergenceArtifact{}
	for i, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return nil, meta, err
		}
		if i >= maxDivergenceArtifacts {
			meta.Truncated = true
			break
		}
		p := candidate.name
		info, err := root.Lstat(p)
		if err != nil {
			meta.Unreadable++
			continue
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			meta.Invalid++
			continue
		}
		if info.Size() > maxArtifactBytes {
			meta.Oversized++
			continue
		}
		data, readErr := readRootFile(ctx, root, p, maxArtifactBytes)
		if readErr != nil {
			meta.Unreadable++
			continue
		}
		if len(data) > maxArtifactBytes {
			meta.Oversized++
			continue
		}
		var a DivergenceArtifact
		if err := json.Unmarshal(data, &a); err != nil {
			meta.Invalid++
			continue
		}
		if err := validateDivergenceArtifact(&a, candidate.slot); err != nil {
			meta.Invalid++
			continue
		}
		out = append(out, a)
	}
	meta.Returned = len(out)
	return out, meta, nil
}

type divergenceInput struct{}

type divergenceOutput struct {
	LogDir       string                      `json:"log_dir"`
	Diverged     bool                        `json:"diverged"`
	Count        int                         `json:"count"`
	ScanComplete bool                        `json:"scan_complete"`
	Meta         DivergenceMeta              `json:"meta"`
	Artifacts    []DivergenceArtifactSummary `json:"artifacts"`
}

func registerDivergenceTool(server *mcpsdk.Server, cfg Config) {
	addTool(server, cfg, &mcpsdk.Tool{
		Name:        "mithril_read_divergence",
		Annotations: annReadOnlyLocal,
		Description: "Read bank-hash mismatch artifacts. If a valid artifact is present, " + divergenceRecoveryGuidance + ". At most 10,000 directory entries are scanned; scan_truncated means returned artifacts are newest only within that partial scan. An empty result does not prove verification ran.",
	}, func(ctx context.Context, _ *mcpsdk.CallToolRequest, _ divergenceInput) (*mcpsdk.CallToolResult, divergenceOutput, error) {
		logDir, err := requireConfiguredPath(cfg.LogDir, "MITHRIL_LOG_DIR is not configured")
		if err != nil {
			return nil, divergenceOutput{}, err
		}
		artifacts, meta, err := readDivergenceArtifactsContext(ctx, logDir)
		if err != nil {
			return nil, divergenceOutput{}, err
		}
		return nil, divergenceOutput{
			LogDir:       logDir,
			Diverged:     len(artifacts) > 0,
			Count:        len(artifacts),
			ScanComplete: meta.Invalid == 0 && meta.Oversized == 0 && meta.Unreadable == 0 && !meta.Truncated && !meta.ScanTruncated,
			Meta:         meta,
			Artifacts:    summarizeDivergenceArtifacts(artifacts),
		}, nil
	})
}

// Command sigverifytrace replays cache and SIMD policies over an exact
// Mithril sigverify telemetry export. It never contacts a running node.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/Overclock-Validator/mithril/pkg/sigverifytrace"
)

type output struct {
	Schema                string                               `json:"schema"`
	InputSHA256           string                               `json:"input_sha256"`
	Policy                policyConfiguration                  `json:"policy"`
	CollectionMode        sigverifytrace.CollectionMode        `json:"collection_mode"`
	TraceCapacity         uint64                               `json:"trace_capacity"`
	RetainedVerifications int                                  `json:"retained_verifications"`
	RetainedDispatches    int                                  `json:"retained_dispatches"`
	TruncatedPrefix       bool                                 `json:"truncated_prefix"`
	FirstSequence         uint64                               `json:"first_sequence"`
	LastSequence          uint64                               `json:"last_sequence"`
	ValidCompletions      uint64                               `json:"valid_completions"`
	InvalidCompletions    uint64                               `json:"invalid_completions"`
	UnknownCompletions    uint64                               `json:"unknown_completions"`
	KeyCache              sigverifytrace.KeyCacheStats         `json:"key_cache"`
	ExactDuplicateLRU     sigverifytrace.DuplicateStats        `json:"exact_duplicate_lru"`
	Attempts              []sigverifytrace.AttemptDecision     `json:"attempts,omitempty"`
	SIMDInference         string                               `json:"simd_inference"`
	SIMD                  map[string]sigverifytrace.SIMDReport `json:"simd,omitempty"`
}

type policyConfiguration struct {
	MaxKeyEntries     int    `json:"max_key_entries"`
	MaxTableBytes     uint64 `json:"max_table_bytes"`
	TableBytesPerKey  uint64 `json:"table_bytes_per_key"`
	AdmitAfterValid   uint64 `json:"admit_after_valid"`
	DuplicateCapacity int    `json:"duplicate_capacity"`
	SIMDWidths        []int  `json:"simd_widths"`
	Details           bool   `json:"details"`
}

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("sigverifytrace", flag.ContinueOnError)
	flags.SetOutput(stderr)
	inputPath := flags.String("input", "", "v3 JSONL telemetry trace")
	outputPath := flags.String("output", "", "optional aggregate JSON output (mode 0600)")
	maxKeyEntries := flags.Int("key-entries", 0, "maximum retained public-key tables; 0 means no entry limit")
	maxTableBytes := flags.Uint64("key-bytes", 0, "maximum bytes retained by public-key tables; 0 means no byte limit")
	tableBytes := flags.Uint64("table-bytes", 0, "bytes per retained backend-native public-key table")
	admitAfter := flags.Uint64("admit-after", 8, "valid miss completions required before retaining a key table")
	duplicateCapacity := flags.Int("duplicate-entries", 0, "exact tuple LRU capacity; 0 disables it")
	simdWidths := flags.String("simd", "4,8", "comma-separated SIMD widths to reconstruct (4,8), or empty")
	details := flags.Bool("details", false, "include per-attempt and per-group detail")
	pretty := flags.Bool("pretty", true, "indent JSON output")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *inputPath == "" {
		if flags.NArg() == 1 {
			*inputPath = flags.Arg(0)
		} else {
			return errors.New("sigverifytrace: provide exactly one trace with -input or as a positional argument")
		}
	} else if flags.NArg() != 0 {
		return errors.New("sigverifytrace: provide exactly one trace with -input or as a positional argument")
	}
	if *outputPath != "" {
		if err := validateOutputPath(*inputPath, *outputPath); err != nil {
			return err
		}
	}
	input, err := os.Open(*inputPath)
	if err != nil {
		return fmt.Errorf("sigverifytrace: open input: %w", err)
	}
	inputHash := sha256.New()
	trace, parseErr := sigverifytrace.Parse(io.TeeReader(input, inputHash))
	closeErr := input.Close()
	if parseErr != nil {
		return parseErr
	}
	if closeErr != nil {
		return fmt.Errorf("sigverifytrace: close input: %w", closeErr)
	}
	widths, err := parseWidths(*simdWidths)
	if err != nil {
		return err
	}
	replayConfig := sigverifytrace.ReplayConfig{
		MaxKeyEntries:     *maxKeyEntries,
		MaxTableBytes:     *maxTableBytes,
		TableBytesPerKey:  *tableBytes,
		AdmitAfterValid:   *admitAfter,
		DuplicateCapacity: *duplicateCapacity,
	}
	policy, err := sigverifytrace.Replay(trace, replayConfig)
	if err != nil {
		return err
	}
	result := output{
		Schema:      trace.Summary.Schema,
		InputSHA256: hex.EncodeToString(inputHash.Sum(nil)),
		Policy: policyConfiguration{
			MaxKeyEntries:     replayConfig.MaxKeyEntries,
			MaxTableBytes:     replayConfig.MaxTableBytes,
			TableBytesPerKey:  replayConfig.TableBytesPerKey,
			AdmitAfterValid:   replayConfig.AdmitAfterValid,
			DuplicateCapacity: replayConfig.DuplicateCapacity,
			SIMDWidths:        append([]int(nil), widths...),
			Details:           *details,
		},
		CollectionMode:        trace.Summary.Mode,
		TraceCapacity:         trace.Summary.Capacity,
		RetainedVerifications: len(trace.Verifications),
		RetainedDispatches:    len(trace.Dispatches),
		TruncatedPrefix:       policy.TruncatedPrefix,
		FirstSequence:         policy.FirstSequence,
		LastSequence:          policy.LastSequence,
		ValidCompletions:      policy.ValidCompletions,
		InvalidCompletions:    policy.InvalidCompletions,
		UnknownCompletions:    policy.UnknownCompletions,
		KeyCache:              policy.Keys,
		ExactDuplicateLRU:     policy.Duplicates,
		SIMD:                  make(map[string]sigverifytrace.SIMDReport),
	}
	if *details {
		result.Attempts = policy.Attempts
	}
	if len(widths) == 0 {
		result.SIMDInference = "not_requested"
	} else {
		result.SIMDInference = "available"
		for _, width := range widths {
			report, inferErr := sigverifytrace.InferSIMD(trace, width, &policy)
			if errors.Is(inferErr, sigverifytrace.ErrBatchInferenceUnavailable) {
				result.SIMDInference = "unavailable_for_passive_trace"
				result.SIMD = nil
				break
			}
			if inferErr != nil {
				return inferErr
			}
			if !*details {
				report.Groups = nil
			}
			result.SIMD["x"+strconv.Itoa(width)] = report
		}
	}
	var destination io.Writer = stdout
	var outputFile *os.File
	if *outputPath != "" {
		outputFile, err = os.OpenFile(*outputPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return fmt.Errorf("sigverifytrace: open output: %w", err)
		}
		if err := outputFile.Chmod(0o600); err != nil {
			_ = outputFile.Close()
			return fmt.Errorf("sigverifytrace: protect output: %w", err)
		}
		destination = outputFile
	}
	encoder := json.NewEncoder(destination)
	encoder.SetEscapeHTML(false)
	if *pretty {
		encoder.SetIndent("", "  ")
	}
	if err := encoder.Encode(result); err != nil {
		if outputFile != nil {
			_ = outputFile.Close()
		}
		return fmt.Errorf("sigverifytrace: encode output: %w", err)
	}
	if outputFile != nil {
		if err := outputFile.Close(); err != nil {
			return fmt.Errorf("sigverifytrace: close output: %w", err)
		}
	}
	return nil
}

func validateOutputPath(inputPath, outputPath string) error {
	inputAbsolute, err := filepath.Abs(inputPath)
	if err != nil {
		return fmt.Errorf("sigverifytrace: resolve input path: %w", err)
	}
	outputAbsolute, err := filepath.Abs(outputPath)
	if err != nil {
		return fmt.Errorf("sigverifytrace: resolve output path: %w", err)
	}
	inputInfo, err := os.Stat(inputAbsolute)
	if err != nil {
		return fmt.Errorf("sigverifytrace: stat input: %w", err)
	}
	outputInfo, outputErr := os.Stat(outputAbsolute)
	if outputErr == nil {
		if os.SameFile(inputInfo, outputInfo) {
			return errors.New("sigverifytrace: output path resolves to the input trace")
		}
		return errors.New("sigverifytrace: output path already exists; refusing to overwrite evidence")
	}
	if !errors.Is(outputErr, os.ErrNotExist) {
		return fmt.Errorf("sigverifytrace: stat output: %w", outputErr)
	}
	if filepath.Clean(inputAbsolute) == filepath.Clean(outputAbsolute) {
		return errors.New("sigverifytrace: output path resolves to the input trace")
	}
	return nil
}

func parseWidths(raw string) ([]int, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	seen := make(map[int]bool)
	var result []int
	for _, field := range strings.Split(raw, ",") {
		width, err := strconv.Atoi(strings.TrimSpace(field))
		if err != nil || width != 4 && width != 8 {
			return nil, fmt.Errorf("sigverifytrace: invalid SIMD width %q (use 4 and/or 8)", field)
		}
		if !seen[width] {
			seen[width] = true
			result = append(result, width)
		}
	}
	return result, nil
}

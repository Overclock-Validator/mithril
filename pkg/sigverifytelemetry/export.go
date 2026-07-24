package sigverifytelemetry

import (
	"bufio"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	envOutputPath = "MITHRIL_SIGVERIFY_TELEMETRY_OUTPUT"
	traceSchema   = "mithril-sigverify-v3"
)

type traceSummary struct {
	Type                     string         `json:"type"`
	Schema                   string         `json:"schema"`
	Enabled                  bool           `json:"enabled"`
	Mode                     CollectionMode `json:"collection_mode"`
	Capacity                 uint64         `json:"capacity"`
	Transactions             uint64         `json:"transactions"`
	Signatures               uint64         `json:"signatures"`
	MessageBytes             uint64         `json:"message_bytes"`
	VerificationAttempts     uint64         `json:"verification_attempts"`
	ObservedEvents           uint64         `json:"observed_events"`
	RetainedVerifications    int            `json:"retained_verifications"`
	RetainedDispatches       int            `json:"retained_dispatches"`
	ExactDuplicateHits       uint64         `json:"exact_duplicate_hits"`
	PublicKeyReuseHits       uint64         `json:"public_key_reuse_hits"`
	QueueSamples             uint64         `json:"queue_samples"`
	QueueOccupancySum        uint64         `json:"queue_occupancy_sum"`
	QueueOccupancyMax        uint64         `json:"queue_occupancy_max"`
	BatchSamples             uint64         `json:"batch_samples"`
	NaturalBatchWidthSum     uint64         `json:"natural_batch_width_sum"`
	NaturalBatchWidthMax     uint64         `json:"natural_batch_width_max"`
	SignaturesPerTransaction [19]uint64     `json:"signatures_per_transaction"`
	MessageSize              [9]uint64      `json:"message_size"`
	ReuseDistance            [19]uint64     `json:"reuse_distance"`
	QueueOccupancy           [16]uint64     `json:"queue_occupancy"`
	NaturalBatchWidth        [20]uint64     `json:"natural_batch_width"`
}

type traceVerification struct {
	Type                    string `json:"type"`
	Sequence                uint64 `json:"sequence"`
	BeginEventSequence      uint64 `json:"begin_event_sequence"`
	CompletionEventSequence uint64 `json:"completion_event_sequence"`
	Source                  Source `json:"source"`
	PublicKey               string `json:"public_key"`
	Signature               string `json:"signature"`
	Message                 string `json:"message_base64"`
	ExactDuplicate          bool   `json:"exact_duplicate"`
	PublicKeyReused         bool   `json:"public_key_reused"`
	ReuseDistance           uint64 `json:"reuse_distance"`
	Outcome                 string `json:"outcome"`
	DispatchID              uint64 `json:"dispatch_id"`
	JobIndex                uint32 `json:"job_index"`
	LaneIndex               uint32 `json:"lane_index"`
}

type traceDispatch struct {
	Type               string         `json:"type"`
	DispatchID         uint64         `json:"dispatch_id"`
	ClaimEventSequence uint64         `json:"claim_event_sequence"`
	ReadyEventSequence uint64         `json:"ready_event_sequence"`
	Mode               CollectionMode `json:"dispatch_mode"`
	Source             Source         `json:"source"`
	QueuedItemsBefore  uint64         `json:"queued_items_before"`
	QueuedItemsAfter   uint64         `json:"queued_items_after"`
	SignatureLanes     uint64         `json:"signature_lanes"`
	JobSignatures      []uint16       `json:"job_signatures"`
}

// ConfiguredOutputPath returns the optional shutdown-export path. Keeping the
// output opt-in prevents exact public keys, signatures, and messages from
// being written during ordinary node operation.
func ConfiguredOutputPath() string { return os.Getenv(envOutputPath) }

// WriteJSONL writes one summary record followed by the retained exact
// verification window in attempt order and dispatch window in dispatch order.
// Their event fields form the merged timeline. Hex is used for fixed-size
// cryptographic inputs and base64 for the arbitrary binary message so an
// offline replay can reconstruct every byte without JSON array overhead.
func WriteJSONL(w io.Writer) error {
	if w == nil {
		return errors.New("sigverifytelemetry: nil trace writer")
	}
	snapshot := Current()
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(traceSummary{
		Type:                     "summary",
		Schema:                   traceSchema,
		Enabled:                  snapshot.Enabled,
		Mode:                     snapshot.Mode,
		Capacity:                 snapshot.Capacity,
		Transactions:             snapshot.Transactions,
		Signatures:               snapshot.Signatures,
		MessageBytes:             snapshot.MessageBytes,
		VerificationAttempts:     snapshot.VerificationAttempts,
		ObservedEvents:           snapshot.ObservedEvents,
		RetainedVerifications:    len(snapshot.VerificationTrace),
		RetainedDispatches:       len(snapshot.DispatchTrace),
		ExactDuplicateHits:       snapshot.ExactDuplicateHits,
		PublicKeyReuseHits:       snapshot.PublicKeyReuseHits,
		QueueSamples:             snapshot.QueueSamples,
		QueueOccupancySum:        snapshot.QueueOccupancySum,
		QueueOccupancyMax:        snapshot.QueueOccupancyMax,
		BatchSamples:             snapshot.BatchSamples,
		NaturalBatchWidthSum:     snapshot.NaturalBatchWidthSum,
		NaturalBatchWidthMax:     snapshot.NaturalBatchWidthMax,
		SignaturesPerTransaction: snapshot.SignaturesPerTransaction,
		MessageSize:              snapshot.MessageSize,
		ReuseDistance:            snapshot.ReuseDistance,
		QueueOccupancy:           snapshot.QueueOccupancy,
		NaturalBatchWidth:        snapshot.NaturalBatchWidth,
	}); err != nil {
		return fmt.Errorf("sigverifytelemetry: encode summary: %w", err)
	}
	for _, entry := range snapshot.VerificationTrace {
		if err := encoder.Encode(traceVerification{
			Type:                    "verification",
			Sequence:                entry.Sequence,
			BeginEventSequence:      entry.BeginEventSequence,
			CompletionEventSequence: entry.CompletionEventSequence,
			Source:                  entry.Source,
			PublicKey:               hex.EncodeToString(entry.PublicKey[:]),
			Signature:               hex.EncodeToString(entry.Signature[:]),
			Message:                 base64.StdEncoding.EncodeToString(entry.Message),
			ExactDuplicate:          entry.ExactDuplicate,
			PublicKeyReused:         entry.PublicKeyReused,
			ReuseDistance:           entry.ReuseDistance,
			Outcome:                 outcomeLabel(entry.Outcome),
			DispatchID:              entry.DispatchID,
			JobIndex:                entry.JobIndex,
			LaneIndex:               entry.LaneIndex,
		}); err != nil {
			return fmt.Errorf("sigverifytelemetry: encode verification %d: %w", entry.Sequence, err)
		}
	}
	for _, entry := range snapshot.DispatchTrace {
		if err := encoder.Encode(traceDispatch{
			Type:               "dispatch",
			DispatchID:         entry.DispatchID,
			ClaimEventSequence: entry.ClaimEventSequence,
			ReadyEventSequence: entry.ReadyEventSequence,
			Mode:               entry.Mode,
			Source:             entry.Source,
			QueuedItemsBefore:  entry.QueuedItemsBefore,
			QueuedItemsAfter:   entry.QueuedItemsAfter,
			SignatureLanes:     entry.SignatureLanes,
			JobSignatures:      entry.JobSignatures,
		}); err != nil {
			return fmt.Errorf("sigverifytelemetry: encode dispatch %d: %w", entry.Sequence, err)
		}
	}
	return nil
}

// WriteJSONLFile atomically replaces path with a mode-0600 trace. A temporary
// file in the same directory prevents an interrupted shutdown from publishing
// a partially written replay corpus.
func WriteJSONLFile(path string) (err error) {
	if path == "" {
		return errors.New("sigverifytelemetry: empty trace output path")
	}
	path = filepath.Clean(path)
	dir, base := filepath.Dir(path), filepath.Base(path)
	temporary, err := os.CreateTemp(dir, "."+base+".tmp-*")
	if err != nil {
		return fmt.Errorf("sigverifytelemetry: create temporary trace: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() {
		if temporary != nil {
			_ = temporary.Close()
		}
		if err != nil {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err = temporary.Chmod(0o600); err != nil {
		return fmt.Errorf("sigverifytelemetry: protect temporary trace: %w", err)
	}
	buffered := bufio.NewWriterSize(temporary, 256*1024)
	if err = WriteJSONL(buffered); err != nil {
		return err
	}
	if err = buffered.Flush(); err != nil {
		return fmt.Errorf("sigverifytelemetry: flush trace: %w", err)
	}
	if err = temporary.Sync(); err != nil {
		return fmt.Errorf("sigverifytelemetry: sync trace: %w", err)
	}
	if err = temporary.Close(); err != nil {
		return fmt.Errorf("sigverifytelemetry: close trace: %w", err)
	}
	temporary = nil
	if err = os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("sigverifytelemetry: publish trace: %w", err)
	}
	return nil
}

func outcomeLabel(outcome VerificationOutcome) string {
	switch outcome {
	case VerificationOutcomeValid:
		return "valid"
	case VerificationOutcomeInvalid:
		return "invalid"
	default:
		return "unknown"
	}
}

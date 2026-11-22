package dashboard

import (
	"bytes"
	"encoding/json"
	"errors"
	"time"
	"unicode/utf8"
)

// MaxProductionDetailBytes bounds a complete local-production document.
const MaxProductionDetailBytes = 6144

// ProductionDetailFreshFor bounds a document lease, not the age of its terminal.
const ProductionDetailFreshFor = 30 * time.Second

// ProductionDetailBinding identifies a chain-bound node and producer instance.
type ProductionDetailBinding struct {
	RunID              string    `json:"run_id"`
	NodeStartedAt      time.Time `json:"node_started_at"`
	GenesisHash        string    `json:"genesis_hash"`
	ProducerGeneration uint64    `json:"producer_generation,string"`
	ProducerStartedAt  time.Time `json:"producer_started_at"`
}

// ProductionDetail contains the latest observed local terminal, not a complete
// attempt history. Running means the producer has not returned, not liveness.
type ProductionDetail struct {
	SchemaVersion   int                          `json:"schema_version"`
	Family          string                       `json:"family"`
	Scope           string                       `json:"scope"`
	State           string                       `json:"state"`
	Reason          string                       `json:"reason"`
	Binding         ProductionDetailBinding      `json:"binding"`
	CapturedAt      time.Time                    `json:"captured_at"`
	ProducerState   string                       `json:"producer_state"`
	LastTerminal    *ProductionTerminalDetail    `json:"last_terminal"`
	Engine          *ProductionEngineDetail      `json:"engine,omitempty"`
	SchedulerBudget *ProductionSchedulerDetail   `json:"scheduler_budget,omitempty"`
	ParentReady     *ProductionParentReadyDetail `json:"parent_ready,omitempty"`
}

// ProductionParentReadyDetail preserves the latest selected own-window parent
// notification. It is not a replay reset, bank-open result or current identity.
type ProductionParentReadyDetail struct {
	WindowStartSlot uint64    `json:"window_start_slot,string"`
	ParentSlot      uint64    `json:"parent_slot,string"`
	ParentBlockID   string    `json:"parent_block_id"`
	ReadyAt         time.Time `json:"ready_at"`
	ObservedAt      time.Time `json:"observed_at"`
	Activation      string    `json:"activation"`
}

// ProductionTerminalDetail preserves an original terminal observation date.
// Broadcast does not establish peer delivery, canonical inclusion or finality.
type ProductionTerminalDetail struct {
	Slot         uint64                        `json:"slot,string"`
	ObservedAt   time.Time                     `json:"observed_at"`
	Outcome      string                        `json:"outcome"`
	Reason       string                        `json:"reason"`
	Terminal     string                        `json:"terminal"`
	Cause        string                        `json:"cause"`
	Bank         *ProductionBankDetail         `json:"bank"`
	Costs        *ProductionCostDetail         `json:"costs"`
	Finalization *ProductionFinalizationDetail `json:"finalization,omitempty"`
}

// ProductionBankDetail is historical exact-slot bank context, not the current
// identity holder or evidence that the bank was selected by the chain.
type ProductionBankDetail struct {
	Leader         string `json:"leader"`
	ParentSlot     uint64 `json:"parent_slot,string"`
	ParentBankhash string `json:"parent_bankhash"`
}

// ProductionCostDetail reports accepted bank contents after native rebates,
// not execution work spent on rejected or discarded attempts.
type ProductionCostDetail struct {
	BlockCost                   uint64               `json:"block_cost,string"`
	AllocatedDataSizeDeltaBytes uint64               `json:"allocated_data_size_delta_bytes,string"`
	SerializedEntryBytes        uint64               `json:"serialized_entry_bytes,string"`
	Transactions                uint64               `json:"transactions,string"`
	Limits                      ProductionCostLimits `json:"limits"`
}

// ProductionCostLimits preserves the bank's effective native budgets. Cost
// units are not execution-only CU; entry bytes are not shred or wire bytes.
type ProductionCostLimits struct {
	BlockCost                   uint64 `json:"block_cost,string"`
	WritableAccountCost         uint64 `json:"writable_account_cost,string"`
	AllocatedDataSizeDeltaBytes uint64 `json:"allocated_data_size_delta_bytes,string"`
	MaxBatchBytes               uint64 `json:"max_batch_bytes,string"`
	MaxEntryBytes               uint64 `json:"max_entry_bytes,string"`
}

// ProductionDetailCapture retains the original monotonic document deadline.
// FreshUntil is not serialized or reconstructed from any wall date.
type ProductionDetailCapture struct {
	Detail     ProductionDetail
	FreshUntil time.Time
}

// ProductionDetailSampler returns a detached bounded owner copy without I/O.
// It is intended for the existing Feed cycle, never an HTTP request.
type ProductionDetailSampler func(now time.Time) (ProductionDetailCapture, error)

func validateProductionDetail(d ProductionDetail) error {
	b := d.Binding
	if d.SchemaVersion != 1 || d.Family != "production" || d.Scope != "producer_session_latest_terminal" || d.State != "fresh" || d.Reason != "" ||
		!utf8.ValidString(b.RunID) || b.RunID == "" || len(b.RunID) > 128 || b.ProducerGeneration == 0 || b.ProducerGeneration == ^uint64(0) ||
		!validReplayDetailHash(b.GenesisHash, true) || !validReplayDetailTime(b.NodeStartedAt) ||
		!validReplayDetailTime(b.ProducerStartedAt) || !validReplayDetailTime(d.CapturedAt) ||
		d.ProducerState != "running" && d.ProducerState != "stopped" || !validProductionEngine(d.Engine, d.CapturedAt) || !validProductionScheduler(d.SchedulerBudget, d.CapturedAt) {
		return errors.New("source_invalid")
	}
	if p := d.ParentReady; p != nil && (p.WindowStartSlot%4 != 0 || p.WindowStartSlot > ^uint64(0)-3 ||
		p.ParentSlot >= p.WindowStartSlot || !validReplayDetailHash(p.ParentBlockID, true) ||
		!validReplayDetailTime(p.ReadyAt) || !validReplayDetailTime(p.ObservedAt) ||
		p.Activation != "initial" && p.Activation != "handover") {
		return errors.New("source_invalid")
	}
	t := d.LastTerminal
	if t == nil {
		return nil
	}
	if t.Slot == 0 || !validReplayDetailTime(t.ObservedAt) {
		return errors.New("source_invalid")
	}
	switch t.Outcome {
	case "broadcast":
		if t.Reason != "complete" || t.Terminal != "complete" || t.Cause != "complete" {
			return errors.New("source_invalid")
		}
	case "missed":
		if !validProductionFailure(t.Reason) || !validProductionFailure(t.Cause) ||
			t.Terminal != "deadline_elapsed" && !validProductionFailure(t.Terminal) {
			return errors.New("source_invalid")
		}
	default:
		return errors.New("source_invalid")
	}
	if t.Bank != nil && (!validReplayDetailHash(t.Bank.Leader, false) || !validReplayDetailHash(t.Bank.ParentBankhash, false)) ||
		t.Costs != nil && (t.Bank == nil || t.Outcome != "broadcast") || !validProductionFinalization(t) {
		return errors.New("source_invalid")
	}
	// Wall dates do not establish elapsed-time order or make an old event unhealthy.
	return nil
}

// validProductionFailure mirrors the finite failure classifications in blockprod.
func validProductionFailure(reason string) bool {
	switch reason {
	case "live_clock_advanced", "already_resolved", "replay_not_ready", "parent_ready_not_ready", "parent_ready_missed_window",
		"parent_ready_invalid", "parent_block_id_mismatch", "parent_block_id_missing", "parent_chained_root_missing", "parent_context_not_ready",
		"parent_changed_before_freeze", "epoch_transition", "epoch_rewards", "slot_context_failed", "sysvar_prepare_failed", "header_broadcast_failed",
		"entry_broadcast_failed", "freeze_failed", "finalization_unavailable", "footer_broadcast_failed", "ending_tick_broadcast_failed",
		"window_deadline_elapsed", "previous_slot_missed":
		return true
	default:
		return false
	}
}

// decodeProductionDetail accepts only this producer's canonical typed JSON.
func decodeProductionDetail(body []byte) (ProductionDetail, error) {
	var d ProductionDetail
	if len(body) > MaxProductionDetailBytes || json.Unmarshal(body, &d) != nil || validateProductionDetail(d) != nil {
		return ProductionDetail{}, errors.New("source_invalid")
	}
	canonical, err := json.Marshal(d)
	if err != nil || !bytes.Equal(body, canonical) {
		return ProductionDetail{}, errors.New("source_invalid")
	}
	return d, nil
}

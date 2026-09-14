package alpenglow

import (
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"

	"github.com/gagliardetto/solana-go"
)

const voteHistoryVersion = 1
const reservedVoteHistoryVersion = 2

var ErrVoteHistoryNotFound = errors.New("alpenglow vote history not found")

// VoteHistory is the safety record for votes emitted by one validator
// identity. It deliberately includes the local consensus observations used by
// Agave when resuming (notarized blocks and ParentReady edges), in addition to
// the anti-equivocation vote sets.
type VoteHistory struct {
	ReservationRequired  bool                            `json:"reservation_required,omitempty"`
	Version              uint32                          `json:"version"`
	NodePubkey           solana.PublicKey                `json:"node_pubkey"`
	Root                 uint64                          `json:"root"`
	Voted                map[uint64]bool                 `json:"voted"`
	VotedNotar           map[uint64]solana.Hash          `json:"voted_notar"`
	VotedNotarFallback   map[uint64]map[solana.Hash]bool `json:"voted_notar_fallback"`
	VotedSkipFallback    map[uint64]bool                 `json:"voted_skip_fallback"`
	Skipped              map[uint64]bool                 `json:"skipped"`
	ItsOver              map[uint64]bool                 `json:"its_over"`
	VotesCast            map[uint64][]Vote               `json:"votes_cast"`
	NotarizedBlocks      map[BlockID]bool                `json:"-"`
	ParentReadyBySlot    map[uint64]map[BlockID]bool     `json:"-"`
	PersistedNotarized   []BlockID                       `json:"notarized_blocks,omitempty"`
	PersistedParentReady []persistedParentReady          `json:"parent_ready,omitempty"`
}

type persistedParentReady struct {
	Slot    uint64    `json:"slot"`
	Parents []BlockID `json:"parents"`
}

type savedVoteHistory struct {
	Version   uint32           `json:"version"`
	Node      solana.PublicKey `json:"node_pubkey"`
	Data      json.RawMessage  `json:"data"`
	Signature []byte           `json:"signature"`
}

func NewVoteHistory(node solana.PublicKey, root uint64) *VoteHistory {
	h := &VoteHistory{Version: voteHistoryVersion, NodePubkey: node, Root: root}
	h.ensureMaps()
	return h
}

func (h *VoteHistory) ensureMaps() {
	if h.Voted == nil {
		h.Voted = make(map[uint64]bool)
	}
	if h.VotedNotar == nil {
		h.VotedNotar = make(map[uint64]solana.Hash)
	}
	if h.VotedNotarFallback == nil {
		h.VotedNotarFallback = make(map[uint64]map[solana.Hash]bool)
	}
	if h.VotedSkipFallback == nil {
		h.VotedSkipFallback = make(map[uint64]bool)
	}
	if h.Skipped == nil {
		h.Skipped = make(map[uint64]bool)
	}
	if h.ItsOver == nil {
		h.ItsOver = make(map[uint64]bool)
	}
	if h.VotesCast == nil {
		h.VotesCast = make(map[uint64][]Vote)
	}
	if h.NotarizedBlocks == nil {
		h.NotarizedBlocks = make(map[BlockID]bool)
	}
	if h.ParentReadyBySlot == nil {
		h.ParentReadyBySlot = make(map[uint64]map[BlockID]bool)
	}
}

func (h *VoteHistory) VotedAt(slot uint64) bool {
	return slot >= h.Root && h.Voted[slot]
}

func (h *VoteHistory) NotarizedVote(slot uint64) (solana.Hash, bool) {
	if slot < h.Root {
		return solana.Hash{}, false
	}
	hash, ok := h.VotedNotar[slot]
	return hash, ok
}

func (h *VoteHistory) HasNotarFallback(block BlockID) bool {
	return slotHashSetContains(h.VotedNotarFallback, block)
}

func (h *VoteHistory) HasSkipFallback(slot uint64) bool {
	return slot >= h.Root && h.VotedSkipFallback[slot]
}

func (h *VoteHistory) HasSkipped(slot uint64) bool {
	return slot >= h.Root && h.Skipped[slot]
}

func (h *VoteHistory) IsOver(slot uint64) bool {
	return slot >= h.Root && h.ItsOver[slot]
}

func (h *VoteHistory) BadWindow(slot uint64) bool {
	return h.HasSkipped(slot) || len(h.VotedNotarFallback[slot]) != 0 || h.HasSkipFallback(slot)
}

func (h *VoteHistory) IsBlockNotarized(block BlockID) bool {
	return h.NotarizedBlocks[block]
}

func (h *VoteHistory) IsParentReady(slot uint64, parent BlockID) bool {
	return h.ParentReadyBySlot[slot][parent]
}

func (h *VoteHistory) ValidateVote(vote Vote) error {
	if err := vote.ValidateBasic(); err != nil {
		return err
	}
	if vote.Slot < h.Root {
		return fmt.Errorf("vote slot %d is behind vote-history root %d", vote.Slot, h.Root)
	}
	switch vote.Type {
	case VoteTypeNotarize:
		if h.Voted[vote.Slot] {
			return fmt.Errorf("slot %d already has a notarize-or-skip vote", vote.Slot)
		}
	case VoteTypeFinalize:
		if h.Skipped[vote.Slot] {
			return fmt.Errorf("slot %d has a skip-family vote and cannot be finalized", vote.Slot)
		}
		if h.ItsOver[vote.Slot] {
			return fmt.Errorf("slot %d already has a finalization vote", vote.Slot)
		}
	case VoteTypeSkip:
		if h.Voted[vote.Slot] {
			return fmt.Errorf("slot %d already has a notarize-or-skip vote", vote.Slot)
		}
	case VoteTypeNotarizeFallback:
		if !h.Voted[vote.Slot] || h.ItsOver[vote.Slot] {
			return fmt.Errorf("slot %d is not eligible for notarize-fallback", vote.Slot)
		}
		if h.VotedNotarFallback[vote.Slot][vote.BlockHash] {
			return fmt.Errorf("slot %d already has notarize-fallback for %s", vote.Slot, vote.BlockHash)
		}
	case VoteTypeSkipFallback:
		if !h.Voted[vote.Slot] || h.ItsOver[vote.Slot] || h.VotedSkipFallback[vote.Slot] {
			return fmt.Errorf("slot %d is not eligible for skip-fallback", vote.Slot)
		}
	case VoteTypeGenesis:
		return fmt.Errorf("genesis votes are managed by migration, not the live voter")
	}
	return nil
}

func (h *VoteHistory) AddVote(vote Vote) error {
	h.ensureMaps()
	if err := h.ValidateVote(vote); err != nil {
		return err
	}
	switch vote.Type {
	case VoteTypeNotarize:
		h.Voted[vote.Slot] = true
		h.VotedNotar[vote.Slot] = vote.BlockHash
	case VoteTypeFinalize:
		h.ItsOver[vote.Slot] = true
	case VoteTypeSkip:
		h.Voted[vote.Slot] = true
		h.Skipped[vote.Slot] = true
	case VoteTypeNotarizeFallback:
		h.Skipped[vote.Slot] = true
		if h.VotedNotarFallback[vote.Slot] == nil {
			h.VotedNotarFallback[vote.Slot] = make(map[solana.Hash]bool)
		}
		h.VotedNotarFallback[vote.Slot][vote.BlockHash] = true
	case VoteTypeSkipFallback:
		h.Skipped[vote.Slot] = true
		h.VotedSkipFallback[vote.Slot] = true
	}
	h.VotesCast[vote.Slot] = append(h.VotesCast[vote.Slot], vote)
	return nil
}

func (h *VoteHistory) AddBlockNotarized(block BlockID) {
	h.ensureMaps()
	if block.Slot >= h.Root {
		h.NotarizedBlocks[block] = true
	}
}

// AddParentReady returns true for the first ParentReady observation at slot.
func (h *VoteHistory) AddParentReady(slot uint64, parent BlockID) bool {
	h.ensureMaps()
	if slot < h.Root || parent.Slot >= slot || slot-parent.Slot > maxParentReadyRestoreGap || !parent.HasHash() {
		return false
	}
	first := len(h.ParentReadyBySlot[slot]) == 0
	if h.ParentReadyBySlot[slot] == nil {
		h.ParentReadyBySlot[slot] = make(map[BlockID]bool)
	}
	h.ParentReadyBySlot[slot][parent] = true
	return first
}

func (h *VoteHistory) HighestParentReady() (uint64, BlockID, bool) {
	return h.HighestParentReadyMatching(nil)
}

// HighestParentReadyMatching selects the latest persisted ParentReady edge
// accepted by eligible. A nil predicate accepts every historical edge.
func (h *VoteHistory) HighestParentReadyMatching(eligible func(BlockID) bool) (uint64, BlockID, bool) {
	var bestSlot uint64
	var best BlockID
	found := false
	for slot, parents := range h.ParentReadyBySlot {
		for parent := range parents {
			if eligible != nil && !eligible(parent) {
				continue
			}
			if !found || slot > bestSlot || (slot == bestSlot && blockLess(parent, best)) {
				bestSlot, best, found = slot, parent, true
			}
		}
	}
	return bestSlot, best, found
}

func (h *VoteHistory) VotesAfter(slot uint64) []Vote {
	slots := make([]uint64, 0, len(h.VotesCast))
	for voteSlot := range h.VotesCast {
		if voteSlot > slot {
			slots = append(slots, voteSlot)
		}
	}
	sort.Slice(slots, func(i, j int) bool { return slots[i] < slots[j] })
	var votes []Vote
	for _, voteSlot := range slots {
		votes = append(votes, h.VotesCast[voteSlot]...)
	}
	return votes
}

func (h *VoteHistory) SetRoot(root uint64) {
	if root <= h.Root {
		return
	}
	h.Root = root
	pruneSlotMap(h.Voted, root)
	pruneSlotMap(h.VotedNotar, root)
	pruneSlotMap(h.VotedNotarFallback, root)
	pruneSlotMap(h.VotedSkipFallback, root)
	pruneSlotMap(h.Skipped, root)
	pruneSlotMap(h.ItsOver, root)
	pruneSlotMap(h.VotesCast, root)
	pruneSlotMap(h.ParentReadyBySlot, root)
	for block := range h.NotarizedBlocks {
		if block.Slot < root {
			delete(h.NotarizedBlocks, block)
		}
	}
}

func pruneSlotMap[T any](m map[uint64]T, root uint64) {
	for slot := range m {
		if slot < root {
			delete(m, slot)
		}
	}
}

func slotHashSetContains(m map[uint64]map[solana.Hash]bool, block BlockID) bool {
	return m[block.Slot][block.Hash]
}

func (h *VoteHistory) preparePersistedViews() error {
	for block, present := range h.NotarizedBlocks {
		if !present {
			return fmt.Errorf("vote-history notarized block %s has false membership", block)
		}
	}
	for slot, parents := range h.ParentReadyBySlot {
		for parent, present := range parents {
			if !present {
				return fmt.Errorf("vote-history ParentReady slot %d parent %s has false membership", slot, parent)
			}
		}
	}
	h.PersistedNotarized = h.PersistedNotarized[:0]
	for block := range h.NotarizedBlocks {
		h.PersistedNotarized = append(h.PersistedNotarized, block)
	}
	sort.Slice(h.PersistedNotarized, func(i, j int) bool { return blockLess(h.PersistedNotarized[i], h.PersistedNotarized[j]) })
	h.PersistedParentReady = h.PersistedParentReady[:0]
	slots := make([]uint64, 0, len(h.ParentReadyBySlot))
	for slot := range h.ParentReadyBySlot {
		slots = append(slots, slot)
	}
	sort.Slice(slots, func(i, j int) bool { return slots[i] < slots[j] })
	for _, slot := range slots {
		parents := make([]BlockID, 0, len(h.ParentReadyBySlot[slot]))
		for parent := range h.ParentReadyBySlot[slot] {
			parents = append(parents, parent)
		}
		sort.Slice(parents, func(i, j int) bool { return blockLess(parents[i], parents[j]) })
		h.PersistedParentReady = append(h.PersistedParentReady, persistedParentReady{Slot: slot, Parents: parents})
	}
	return nil
}

func (h *VoteHistory) restorePersistedViews() {
	h.ensureMaps()
	for _, block := range h.PersistedNotarized {
		h.NotarizedBlocks[block] = true
	}
	for _, ready := range h.PersistedParentReady {
		for _, parent := range ready.Parents {
			h.AddParentReady(ready.Slot, parent)
		}
	}
	// These are transport-only views; avoid retaining duplicate state in RAM.
	h.PersistedNotarized = nil
	h.PersistedParentReady = nil
}

// validatePersistedState makes VotesCast the canonical anti-equivocation
// record and rejects signed files whose redundant acceleration maps disagree.
// It also bounds restart-only graph reconstruction before Restore can allocate
// one status per skipped slot.
func (h *VoteHistory) validatePersistedState() error {
	h.ensureMaps()
	rebuilt := NewVoteHistory(h.NodePubkey, h.Root)
	slots := make([]uint64, 0, len(h.VotesCast))
	for slot := range h.VotesCast {
		slots = append(slots, slot)
	}
	sort.Slice(slots, func(i, j int) bool { return slots[i] < slots[j] })
	for _, slot := range slots {
		if slot < h.Root {
			return fmt.Errorf("vote-history slot %d is behind root %d", slot, h.Root)
		}
		for index, vote := range h.VotesCast[slot] {
			if vote.Slot != slot {
				return fmt.Errorf("vote-history slot key %d contains vote[%d] for slot %d", slot, index, vote.Slot)
			}
			if err := rebuilt.AddVote(vote); err != nil {
				return fmt.Errorf("vote-history vote[%d] at slot %d: %w", index, slot, err)
			}
		}
	}
	if !reflect.DeepEqual(h.Voted, rebuilt.Voted) {
		return fmt.Errorf("vote-history voted map disagrees with votes_cast")
	}
	if !reflect.DeepEqual(h.VotedNotar, rebuilt.VotedNotar) {
		return fmt.Errorf("vote-history notarize map disagrees with votes_cast")
	}
	if !reflect.DeepEqual(h.VotedNotarFallback, rebuilt.VotedNotarFallback) {
		return fmt.Errorf("vote-history notarize-fallback map disagrees with votes_cast")
	}
	if !reflect.DeepEqual(h.VotedSkipFallback, rebuilt.VotedSkipFallback) {
		return fmt.Errorf("vote-history skip-fallback map disagrees with votes_cast")
	}
	if !reflect.DeepEqual(h.Skipped, rebuilt.Skipped) {
		return fmt.Errorf("vote-history skipped map disagrees with votes_cast")
	}
	if !reflect.DeepEqual(h.ItsOver, rebuilt.ItsOver) {
		return fmt.Errorf("vote-history finalization map disagrees with votes_cast")
	}
	if !reflect.DeepEqual(h.VotesCast, rebuilt.VotesCast) {
		return fmt.Errorf("vote-history votes_cast contains non-canonical empty entries")
	}

	seenNotarized := make(map[BlockID]struct{}, len(h.PersistedNotarized))
	for _, block := range h.PersistedNotarized {
		if !block.HasHash() || block.Slot < h.Root {
			return fmt.Errorf("vote-history has invalid notarized block %s at root %d", block, h.Root)
		}
		if _, duplicate := seenNotarized[block]; duplicate {
			return fmt.Errorf("vote-history repeats notarized block %s", block)
		}
		seenNotarized[block] = struct{}{}
	}
	seenReadySlots := make(map[uint64]struct{}, len(h.PersistedParentReady))
	for _, ready := range h.PersistedParentReady {
		if ready.Slot < h.Root || len(ready.Parents) == 0 {
			return fmt.Errorf("vote-history has invalid ParentReady slot %d at root %d", ready.Slot, h.Root)
		}
		if _, duplicate := seenReadySlots[ready.Slot]; duplicate {
			return fmt.Errorf("vote-history repeats ParentReady slot %d", ready.Slot)
		}
		seenReadySlots[ready.Slot] = struct{}{}
		seenParents := make(map[BlockID]struct{}, len(ready.Parents))
		for _, parent := range ready.Parents {
			if !parent.HasHash() || parent.Slot >= ready.Slot || ready.Slot-parent.Slot > maxParentReadyRestoreGap {
				return fmt.Errorf("vote-history ParentReady slot %d has invalid parent %s", ready.Slot, parent)
			}
			if _, duplicate := seenParents[parent]; duplicate {
				return fmt.Errorf("vote-history ParentReady slot %d repeats parent %s", ready.Slot, parent)
			}
			seenParents[parent] = struct{}{}
		}
	}
	return nil
}

func VoteHistoryFilename(dir string, node solana.PublicKey) string {
	return filepath.Join(dir, fmt.Sprintf("vote_history-%s.mithril.json", node))
}

// SaveVoteHistory signs the exact serialized history with the validator
// identity and atomically replaces the previous file before a vote can be
// admitted to consensus or sent to the network.
func SaveVoteHistory(dir string, h *VoteHistory, identity ed25519.PrivateKey) error {
	return saveVoteHistory(dir, h, identity, true)
}

// SaveReservedVoteHistory writes and renames without per-vote sync. The caller
// must enforce an independently durable signing reservation before using it.
func SaveReservedVoteHistory(dir string, h *VoteHistory, identity ed25519.PrivateKey) error {
	if h == nil || !h.ReservationRequired {
		return fmt.Errorf("reserved history requires durable reservation enrollment")
	}
	return saveVoteHistory(dir, h, identity, false)
}

// VoteHistorySnapshot owns signed, immutable bytes. It retains no reference to
// the voter's mutable maps or signing key and can be saved by a worker.
type VoteHistorySnapshot struct {
	node    solana.PublicKey
	encoded []byte
}

// PrepareReservedVoteHistory validates and signs the complete current history
// without filesystem access. Only the owner of h may call this while mutating h.
func PrepareReservedVoteHistory(h *VoteHistory, identity ed25519.PrivateKey) (*VoteHistorySnapshot, error) {
	if h == nil || !h.ReservationRequired {
		return nil, fmt.Errorf("reserved history requires durable reservation enrollment")
	}
	encoded, err := encodeVoteHistory(h, identity)
	if err != nil {
		return nil, err
	}
	return &VoteHistorySnapshot{node: h.NodePubkey, encoded: encoded}, nil
}

// SaveReservedVoteHistorySnapshot replaces history without an explicit sync.
// Callers must serialize writes and enforce the independent durable reservation.
func SaveReservedVoteHistorySnapshot(dir string, snapshot *VoteHistorySnapshot) error {
	if snapshot == nil || len(snapshot.encoded) == 0 {
		return fmt.Errorf("save reserved history: empty snapshot")
	}
	if err := ensureDurableVoteHistoryDirectory(dir); err != nil {
		return err
	}
	return replaceVoteHistoryFile(dir, VoteHistoryFilename(dir, snapshot.node), snapshot.encoded, false)
}

func saveVoteHistory(dir string, h *VoteHistory, identity ed25519.PrivateKey, durable bool) error {
	encoded, err := encodeVoteHistory(h, identity)
	if err != nil {
		return err
	}
	if err := ensureDurableVoteHistoryDirectory(dir); err != nil {
		return err
	}
	return replaceVoteHistoryFile(dir, VoteHistoryFilename(dir, h.NodePubkey), encoded, durable)
}

func encodeVoteHistory(h *VoteHistory, identity ed25519.PrivateKey) ([]byte, error) {
	if h == nil {
		return nil, fmt.Errorf("save vote history: nil history")
	}
	if len(identity) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("save vote history: invalid identity key size %d", len(identity))
	}
	node := solana.PublicKey(identity.Public().(ed25519.PublicKey))
	if node != h.NodePubkey {
		return nil, fmt.Errorf("save vote history: identity %s does not match history %s", node, h.NodePubkey)
	}
	h.Version = voteHistoryVersion
	if h.ReservationRequired {
		h.Version = reservedVoteHistoryVersion
	}
	if err := h.preparePersistedViews(); err != nil {
		return nil, fmt.Errorf("save vote history: %w", err)
	}
	defer func() {
		h.PersistedNotarized = nil
		h.PersistedParentReady = nil
	}()
	if err := h.validatePersistedState(); err != nil {
		return nil, fmt.Errorf("save vote history: %w", err)
	}
	data, err := json.Marshal(h)
	if err != nil {
		return nil, fmt.Errorf("serialize vote history: %w", err)
	}
	envelope := savedVoteHistory{
		Version:   h.Version,
		Node:      node,
		Data:      data,
		Signature: ed25519.Sign(identity, data),
	}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("serialize saved vote history: %w", err)
	}
	return encoded, nil
}

// ensureDurableVoteHistoryDirectory creates each missing path component and
// syncs its parent before proceeding. This makes first-time initialization as
// durable as later atomic file replacement, including nested HistoryDir paths.
func ensureDurableVoteHistoryDirectory(dir string) error {
	current := filepath.Clean(dir)
	var missing []string
	for {
		info, err := os.Stat(current)
		if err == nil {
			if !info.IsDir() {
				return fmt.Errorf("create vote-history directory: %s is not a directory", current)
			}
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("inspect vote-history directory %s: %w", current, err)
		}
		missing = append(missing, current)
		parent := filepath.Dir(current)
		if parent == current {
			return fmt.Errorf("create vote-history directory: no existing ancestor for %s", dir)
		}
		current = parent
	}
	for i := len(missing) - 1; i >= 0; i-- {
		path := missing[i]
		if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("create vote-history directory %s: %w", path, err)
		}
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("inspect created vote-history directory %s: %w", path, err)
		}
		if !info.IsDir() {
			return fmt.Errorf("create vote-history directory: %s is not a directory", path)
		}
		if err := syncVoteHistoryDirectory(filepath.Dir(path)); err != nil {
			return fmt.Errorf("persist vote-history directory %s: %w", path, err)
		}
	}
	return nil
}

// persistVoteHistoryFile makes the signed vote record durable before its
// caller can publish a vote. The temporary file lives beside the destination
// so rename is atomic. Syncing both the file and directory is required: rename
// alone does not guarantee that either the bytes or the new directory entry
// survives a crash.
func persistVoteHistoryFile(dir, filename string, encoded []byte) error {
	return replaceVoteHistoryFile(dir, filename, encoded, true)
}

func replaceVoteHistoryFile(dir, filename string, encoded []byte, durable bool) error {
	temporary, err := os.CreateTemp(dir, "."+filepath.Base(filename)+".tmp-")
	if err != nil {
		return fmt.Errorf("create temporary vote history: %w", err)
	}
	temporaryName := temporary.Name()
	closed := false
	renamed := false
	defer func() {
		if !closed {
			_ = temporary.Close()
		}
		if !renamed {
			_ = os.Remove(temporaryName)
		}
	}()

	n, err := temporary.Write(encoded)
	if err != nil {
		return fmt.Errorf("write temporary vote history: %w", err)
	}
	if n != len(encoded) {
		return fmt.Errorf("write temporary vote history: %w", io.ErrShortWrite)
	}
	if durable {
		if err := temporary.Sync(); err != nil {
			return fmt.Errorf("sync temporary vote history: %w", err)
		}
	}
	closeErr := temporary.Close()
	closed = true
	if closeErr != nil {
		return fmt.Errorf("close temporary vote history: %w", closeErr)
	}
	if err := os.Rename(temporaryName, filename); err != nil {
		return fmt.Errorf("replace vote history: %w", err)
	}
	renamed = true

	if durable {
		return syncVoteHistoryDirectory(dir)
	}
	return nil
}

func syncVoteHistoryDirectory(dir string) error {
	directory, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open vote-history directory %s for sync: %w", dir, err)
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil {
		return fmt.Errorf("sync vote-history directory %s: %w", dir, syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close vote-history directory %s: %w", dir, closeErr)
	}
	return nil
}

func LoadVoteHistory(dir string, node solana.PublicKey) (*VoteHistory, error) {
	filename := VoteHistoryFilename(dir, node)
	encoded, err := os.ReadFile(filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", ErrVoteHistoryNotFound, filename)
		}
		return nil, fmt.Errorf("read vote history: %w", err)
	}
	var envelope savedVoteHistory
	if err := json.Unmarshal(encoded, &envelope); err != nil {
		return nil, fmt.Errorf("decode saved vote history: %w", err)
	}
	if (envelope.Version != voteHistoryVersion && envelope.Version != reservedVoteHistoryVersion) || envelope.Node != node {
		return nil, fmt.Errorf("saved vote history identity/version mismatch")
	}
	if !ed25519.Verify(ed25519.PublicKey(node[:]), envelope.Data, envelope.Signature) {
		return nil, fmt.Errorf("saved vote history signature is invalid")
	}
	var h VoteHistory
	if err := json.Unmarshal(envelope.Data, &h); err != nil {
		return nil, fmt.Errorf("decode vote history: %w", err)
	}
	if h.Version != envelope.Version || h.NodePubkey != node || h.ReservationRequired != (h.Version == reservedVoteHistoryVersion) {
		return nil, fmt.Errorf("vote history identity/version mismatch")
	}
	if err := h.validatePersistedState(); err != nil {
		return nil, fmt.Errorf("validate vote history: %w", err)
	}
	h.restorePersistedViews()
	return &h, nil
}

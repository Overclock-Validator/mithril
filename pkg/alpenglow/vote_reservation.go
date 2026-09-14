package alpenglow

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/gagliardetto/solana-go"
	"golang.org/x/sys/unix"
)

// VoteReservation is independent of AccountsDB checkpoints. Never restore an
// older copy of this record when restoring a snapshot. A clean digest is valid
// only until a durably written dirty successor consumes it before signing.
type VoteReservation struct {
	Version            uint32           `json:"version"`
	Node               solana.PublicKey `json:"node"`
	VoteAccount        solana.PublicKey `json:"vote_account"`
	AuthorizedVoter    solana.PublicKey `json:"authorized_voter"`
	Genesis            solana.Hash      `json:"genesis"`
	ShredVersion       uint16           `json:"shred_version"`
	Generation         uint64           `json:"generation"`
	Through            uint64           `json:"through"`
	CleanHistoryDigest []byte           `json:"clean_history_digest,omitempty"`
}

func VoteReservationFilename(dir string, node solana.PublicKey) string {
	return filepath.Join(dir, fmt.Sprintf("vote_reservation-%s.mithril.json", node))
}

// LockVoteHistory excludes concurrent owners of this directory. Operators must
// still fence copies of the same identity on other hosts or in other paths.
func LockVoteHistory(dir string, node solana.PublicKey) (*os.File, error) {
	if err := ensureDurableVoteHistoryDirectory(dir); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, ".vote_history-"+node.String()+".lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("vote history already owned: %w", err)
	}
	return f, nil
}

func LoadVoteReservation(dir string, node solana.PublicKey) (VoteReservation, error) {
	var r VoteReservation
	encoded, err := os.ReadFile(VoteReservationFilename(dir, node))
	if err != nil {
		return r, err
	}
	var envelope savedVoteHistory
	if err := json.Unmarshal(encoded, &envelope); err != nil {
		return r, err
	}
	if envelope.Version != 1 || envelope.Node != node || !ed25519.Verify(ed25519.PublicKey(node[:]), envelope.Data, envelope.Signature) {
		return r, errors.New("invalid vote reservation signature/version/identity")
	}
	if err := json.Unmarshal(envelope.Data, &r); err != nil {
		return r, err
	}
	if r.Version != 1 || r.Node != node || r.Generation == 0 || (len(r.CleanHistoryDigest) != 0 && len(r.CleanHistoryDigest) != sha256.Size) {
		return r, errors.New("invalid vote reservation record")
	}
	return r, nil
}

func SaveVoteReservation(dir string, r VoteReservation, identity ed25519.PrivateKey) error {
	if len(identity) != ed25519.PrivateKeySize || solana.PublicKey(identity.Public().(ed25519.PublicKey)) != r.Node || r.Version != 1 || r.Generation == 0 {
		return errors.New("invalid vote reservation signer/record")
	}
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(savedVoteHistory{Version: 1, Node: r.Node, Data: data, Signature: ed25519.Sign(identity, data)})
	if err != nil {
		return err
	}
	if err := ensureDurableVoteHistoryDirectory(dir); err != nil {
		return err
	}
	return persistVoteHistoryFile(dir, VoteReservationFilename(dir, r.Node), encoded)
}

func VoteHistoryDigest(dir string, node solana.PublicKey) ([]byte, error) {
	data, err := os.ReadFile(VoteHistoryFilename(dir, node))
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(data)
	return digest[:], nil
}

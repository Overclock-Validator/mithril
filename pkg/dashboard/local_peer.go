package dashboard

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/gagliardetto/solana-go"
)

const (
	// MaxLocalPeerBytes bounds a complete canonical, endpoint-free publication.
	MaxLocalPeerBytes = 2 << 20
	// LocalPeerCapacity is the maximum retained local signed-contact identities.
	LocalPeerCapacity = 4096
)

// LocalPeerVersion is self-reported contact metadata, not an authenticated build.
// Unknown numeric client IDs remain representable; no client name is inferred.
type LocalPeerVersion struct {
	Major      uint16 `json:"major"`
	Minor      uint16 `json:"minor"`
	Patch      uint16 `json:"patch"`
	Commit     uint32 `json:"commit"`
	FeatureSet uint32 `json:"feature_set"`
	Client     uint16 `json:"client"`
}

// LocalPeerRow is an accepted contact's allowlist, without endpoints or payloads.
// ObservedAt is the local acceptance date, not UDP receipt or remote wallclock.
type LocalPeerRow struct {
	Identity     string           `json:"identity"`
	ObservedAt   time.Time        `json:"observed_at"`
	Outset       uint64           `json:"outset,string"`
	Wallclock    uint64           `json:"wallclock,string"`
	Version      LocalPeerVersion `json:"version"`
	ShredVersion uint16           `json:"shred_version"`
	Repair       bool             `json:"repair"`
	TVU          bool             `json:"tvu"`
	Alpenglow    bool             `json:"alpenglow"`
}

// LocalPeerDetail describes one cached local observation, not a cluster census
// or a reachability claim. Client and socket-Run lifetimes are independent of
// replay. Counters are advert occurrences and can already include concurrent
// accepted adverts after SampledAt; they are not an atomic as-of partition.
type LocalPeerDetail struct {
	SchemaVersion       int                `json:"schema_version"`
	Family              string             `json:"family"`
	Scope               string             `json:"scope"`
	Binding             NodeNetworkBinding `json:"binding"`
	CapturedAt          time.Time          `json:"captured_at"`
	ClientGeneration    uint64             `json:"client_generation,string"`
	ClientStartedAt     time.Time          `json:"client_started_at"`
	RunGeneration       uint64             `json:"run_generation,string"`
	RunStartedAt        time.Time          `json:"run_started_at"`
	PublicationSequence uint64             `json:"publication_sequence,string"`
	SampledAt           time.Time          `json:"sampled_at"`
	Capacity            int                `json:"capacity"`
	Limited             bool               `json:"limited"`
	CountersSaturated   bool               `json:"counters_saturated"`
	AcceptedAdverts     uint64             `json:"accepted_adverts,string"`
	RetainedAdverts     uint64             `json:"retained_adverts,string"`
	CapacityRefusals    uint64             `json:"capacity_refusals,string"`
	Rows                []LocalPeerRow     `json:"rows"`
}

// PreparedLocalPeers owns an immutable canonical body and its detached binding.
// Preparation does not establish a lease, publish a route or renew source dates.
type PreparedLocalPeers struct {
	body, digest string
	binding      NodeNetworkBinding
}

// Body returns the immutable canonical encoding without a per-request copy.
func (p *PreparedLocalPeers) Body() string {
	if p == nil {
		return ""
	}
	return p.body
}

// Digest returns the body's SHA-256, not either source-lifetime generation.
func (p *PreparedLocalPeers) Digest() string {
	if p == nil {
		return ""
	}
	return p.digest
}

// Bytes returns the canonical body's encoded length.
func (p *PreparedLocalPeers) Bytes() int {
	if p == nil {
		return 0
	}
	return len(p.body)
}

// Binding returns a detached copy of the binding actually encoded in Body.
func (p *PreparedLocalPeers) Binding() NodeNetworkBinding {
	if p == nil {
		return NodeNetworkBinding{}
	}
	return cloneLocalPeerBinding(p.binding)
}

// PrepareLocalPeers validates and seals a bounded source projection. It retains
// no row slice or source pointer. Rows must be ordered by decoded identity bytes.
func PrepareLocalPeers(d LocalPeerDetail) (*PreparedLocalPeers, error) {
	if err := validateLocalPeers(d); err != nil {
		return nil, err
	}
	body, err := json.Marshal(d)
	if err != nil || len(body) > MaxLocalPeerBytes {
		return nil, errors.New("local peer response limit")
	}
	sum := sha256.Sum256(body)
	return &PreparedLocalPeers{body: string(body), digest: hex.EncodeToString(sum[:]), binding: cloneLocalPeerBinding(d.Binding)}, nil
}

// DecodeLocalPeers accepts only the exact canonical typed encoding. The row
// preflight bounds allocation even for a small malicious array of empty objects.
func DecodeLocalPeers(body []byte) (LocalPeerDetail, error) {
	var d LocalPeerDetail
	if len(body) == 0 || len(body) > MaxLocalPeerBytes || !boundedLocalPeerRows(body) || json.Unmarshal(body, &d) != nil {
		return LocalPeerDetail{}, errors.New("invalid local peers")
	}
	if err := validateLocalPeers(d); err != nil {
		return LocalPeerDetail{}, err
	}
	canonical, err := json.Marshal(d)
	if err != nil || !bytes.Equal(body, canonical) {
		return LocalPeerDetail{}, errors.New("noncanonical local peers")
	}
	return d, nil
}

func validateLocalPeers(d LocalPeerDetail) error {
	if d.SchemaVersion != 1 || d.Family != "local_peers" || d.Scope != "local_signed_contacts" ||
		!validNodeNetworkBinding(d.Binding) || d.Binding.GenesisHash == nil || !validReplayDetailHash(*d.Binding.GenesisHash, true) ||
		!validReplayDetailTime(d.CapturedAt) ||
		d.ClientGeneration == 0 || d.RunGeneration == 0 || d.PublicationSequence == 0 || !validReplayDetailTime(d.ClientStartedAt) ||
		!validReplayDetailTime(d.RunStartedAt) || !validReplayDetailTime(d.SampledAt) ||
		d.Capacity != LocalPeerCapacity || d.Rows == nil || len(d.Rows) > LocalPeerCapacity || d.CountersSaturated && !d.Limited {
		return errors.New("invalid local peer metadata")
	}
	var previous solana.PublicKey
	for i, row := range d.Rows {
		if len(row.Identity) < 32 || len(row.Identity) > 44 {
			return errors.New("invalid local peer identity")
		}
		key, err := solana.PublicKeyFromBase58(row.Identity)
		if err != nil || key.IsZero() || key.String() != row.Identity ||
			!validReplayDetailTime(row.ObservedAt) || i > 0 && bytes.Compare(previous[:], key[:]) >= 0 {
			return errors.New("invalid local peer row")
		}
		previous = key
	}
	return nil
}

func cloneLocalPeerBinding(b NodeNetworkBinding) NodeNetworkBinding {
	if b.GenesisHash != nil {
		v := *b.GenesisHash
		b.GenesisHash = &v
	}
	return b
}

func boundedLocalPeerRows(body []byte) bool {
	var header struct {
		Rows json.RawMessage `json:"rows"`
	}
	if json.Unmarshal(body, &header) != nil {
		return false
	}
	d := json.NewDecoder(bytes.NewReader(header.Rows))
	token, err := d.Token()
	if err != nil || token != json.Delim('[') {
		return false
	}
	for count := 0; d.More(); count++ {
		if count == LocalPeerCapacity {
			return false
		}
		var row json.RawMessage
		if d.Decode(&row) != nil {
			return false
		}
	}
	_, err = d.Token()
	return err == nil
}

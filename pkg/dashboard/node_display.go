package dashboard

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Overclock-Validator/mithril/pkg/dashboard/cluster"
	"github.com/gagliardetto/solana-go"
)

const (
	nodeDisplayBytes      = 4 << 10
	nodeDisplayPublicPath = "/api/v1/node-display"
)

// nodeDisplay is attributed directory metadata, not a native validator fact.
// It is temporary preparation state; published frames never retain this DTO.
type nodeDisplay struct {
	SchemaVersion   int        `json:"schema_version"`
	Scope           string     `json:"scope"`
	RunID           string     `json:"run_id"`
	Identity        *string    `json:"identity"`
	GenesisHash     *string    `json:"genesis_hash"`
	Profile         string     `json:"profile"`
	Epoch           *string    `json:"epoch"`
	State           string     `json:"state"`
	Reason          string     `json:"reason"`
	Name            *string    `json:"name"`
	DirectorySource *string    `json:"directory_source"`
	SourceAt        *time.Time `json:"source_at"`
	CapturedAt      *time.Time `json:"captured_at"`
	IconURL         *string    `json:"icon_url"`
	IconState       string     `json:"icon_state"`
}

// nodeDisplayFrame retains only encoded bytes and their admission fence. It
// never pins the node snapshot or external directory used during preparation.
type nodeDisplayFrame struct {
	body     string
	deadline time.Time
	revision uint64
	parent   *statusFrameLease
}

// selectNodeDisplay runs on the collection owner, never on a viewer request.
// Its view is the existing validated external directory; no provider is fetched.
func selectNodeDisplay(s *Snapshot, state string, view *clusterView, now time.Time) nodeDisplay {
	v := nodeDisplay{SchemaVersion: 1, Scope: "linked_node_external_display", Profile: "alpenglow",
		State: "unavailable", Reason: "waiting_source", IconState: "not_supplied"}
	if s == nil {
		return v
	}
	if s.RunID == "" || len(s.RunID) > 128 {
		v.Reason = "invalid_context"
		return v
	}
	v.RunID = s.RunID
	for i, key := range []*string{s.IdentityKey, s.GenesisHash} {
		if key == nil {
			v.Reason = "unknown_binding"
			return v
		}
		parsed, err := solana.PublicKeyFromBase58(*key)
		if err != nil || parsed.String() != *key {
			v.Reason = "invalid_context"
			return v
		}
		copied := *key
		if i == 0 {
			v.Identity = &copied
		} else {
			v.GenesisHash = &copied
		}
	}
	if state != "fresh" || !validPublicTime(s.SampledAt, now) || now.Sub(s.SampledAt) > StaleAfter {
		v.Reason = "status_stale"
		return v
	}
	if !s.Alpenglow || *s.GenesisHash != cluster.GenesisHash {
		v.Reason = "unsupported_profile"
		return v
	}
	if s.Replay == nil || s.Replay.Completed == nil || !validPublicTime(s.Replay.Completed.At, now) ||
		s.Replay.Completed.At.Before(s.StartedAt) || s.Replay.Completed.At.After(s.SampledAt.Add(time.Second)) {
		v.Reason = "unknown_epoch"
		return v
	}
	epoch := strconv.FormatUint(s.Replay.Completed.Epoch, 10)
	v.Epoch = &epoch
	if view == nil || view.metadata.State != "fresh" || view.metadata.Profile != v.Profile ||
		view.metadata.GenesisHash == nil || *view.metadata.GenesisHash != *s.GenesisHash ||
		view.metadata.Epoch == nil || string(*view.metadata.Epoch) != epoch ||
		!nodeDisplayDate(view.metadata.CapturedAt, now) {
		v.Reason = "directory_unavailable"
		return v
	}
	// ponytail: at most2,048 existing rows and five providers, once per collection
	// tick; add an identity index only if this bounded preparation is measured hot.
	for _, row := range view.rows {
		if row.Identity != *s.IdentityKey || row.Name == "" {
			continue
		}
		if len(row.Name) > 128 || !utf8.ValidString(row.Name) || strings.IndexFunc(row.Name, unicode.IsControl) >= 0 {
			continue
		}
		var provider *cluster.ProviderStatus
		for i := range view.metadata.Providers {
			p := &view.metadata.Providers[i]
			if p.ID == row.DirectorySource && (p.ID == "solstat" || p.ID == "titan") && p.State == "fresh" &&
				p.Reason == "" && p.GenesisHash == *s.GenesisHash && p.Epoch != nil && string(*p.Epoch) == epoch &&
				nodeDisplayDate(p.SourceAt, now) && nodeDisplayDate(&p.CapturedAt, now) {
				provider = p
				break
			}
		}
		if provider == nil {
			continue
		}
		if v.Name != nil && *v.Name != row.Name {
			v.State, v.Reason = "conflict", "name_conflict"
			v.Name, v.DirectorySource, v.SourceAt, v.CapturedAt = nil, nil, nil, nil
			return v
		}
		if v.Name == nil || row.DirectorySource == "solstat" && *v.DirectorySource != "solstat" {
			name, source, sourceAt, capturedAt := row.Name, row.DirectorySource, provider.SourceAt.UTC(), provider.CapturedAt.UTC()
			v.Name, v.DirectorySource, v.SourceAt, v.CapturedAt = &name, &source, &sourceAt, &capturedAt
		}
	}
	if v.Name == nil {
		v.Reason = "name_unavailable"
		return v
	}
	v.State, v.Reason = "fresh", ""
	return v
}

func nodeDisplayDate(at *time.Time, now time.Time) bool {
	return at != nil && !at.IsZero() && at.Year() >= 1 && at.Year() <= 9999 &&
		!at.After(now.Add(5*time.Second)) && now.Sub(*at) <= cluster.MaxAge
}

func encodeNodeDisplay(v nodeDisplay) (nodeDisplayFrame, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nodeDisplayFrame{}, err
	}
	if len(body) > nodeDisplayBytes {
		return nodeDisplayFrame{}, errors.New("node display response limit exceeded")
	}
	return nodeDisplayFrame{body: string(body)}, nil
}

package dashboard

import (
	"bytes"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/gagliardetto/solana-go"
)

// MaxReceptionCanonicalChunkBytes is the largest legal chunk encoding: its fixed
// columns and 128 widest tuples. The outer transport limit remains 64 KiB.
const MaxReceptionCanonicalChunkBytes = 26963

var receptionChunkColumns = [10]string{"sequence", "at", "source", "slot", "index", "fec_set_index", "version", "variant", "origin", "authenticated_fec_root"}
var receptionChunkOrigins = [3]string{"tvu", "repair_matched", "repair_socket_unmatched"}

// receptionChunkRow is parsing scratch or a reader-owned fixed scalar row,
// never part of the node body cache.
// Root is an authenticated FEC root, including zero, not a bank or block hash.
type receptionChunkRow struct {
	Sequence uint64
	At       time.Time
	Source   uint64
	Slot     uint64
	Index    uint32
	FEC      uint32
	Version  uint16
	Variant  uint8
	Origin   uint8
	Root     solana.Hash
}

func (r receptionChunkRow) MarshalJSON() ([]byte, error) {
	if r.Origin < 1 || r.Origin > 3 {
		return nil, errors.New("source_invalid")
	}
	return json.Marshal([10]any{strconv.FormatUint(r.Sequence, 10), r.At, strconv.FormatUint(r.Source, 10),
		strconv.FormatUint(r.Slot, 10), r.Index, r.FEC, r.Version, r.Variant, receptionChunkOrigins[r.Origin-1], r.Root.String()})
}

func (r *receptionChunkRow) UnmarshalJSON(body []byte) error {
	parts, err := receptionTuple(body, 10)
	if err != nil {
		return err
	}
	var out receptionChunkRow
	var origin, root string
	if receptionUint(parts[0], &out.Sequence) != nil || json.Unmarshal(parts[1], &out.At) != nil ||
		receptionUint(parts[2], &out.Source) != nil || receptionUint(parts[3], &out.Slot) != nil ||
		json.Unmarshal(parts[4], &out.Index) != nil || json.Unmarshal(parts[5], &out.FEC) != nil ||
		json.Unmarshal(parts[6], &out.Version) != nil || json.Unmarshal(parts[7], &out.Variant) != nil ||
		json.Unmarshal(parts[8], &origin) != nil || json.Unmarshal(parts[9], &root) != nil ||
		out.Sequence == 0 || out.Source == 0 || !validReplayDetailTime(out.At) || len(root) < 32 || len(root) > 44 {
		return errors.New("source_invalid")
	}
	out.Root, err = solana.HashFromBase58(root)
	if err != nil || out.Root.String() != root {
		return errors.New("source_invalid")
	}
	for i, known := range receptionChunkOrigins {
		if origin == known {
			out.Origin = uint8(i + 1)
		}
	}
	if out.Origin == 0 {
		return errors.New("source_invalid")
	}
	*r = out
	return nil
}

// A fixed array bounds row storage before decoding an untrusted array. Decode
// each row through its exact ten-element tuple check, not a fixed JSON array
// which encoding/json would silently truncate or zero-fill.
type receptionChunkRows struct {
	values [128]receptionChunkRow
	count  int
}

func (r receptionChunkRows) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.values[:r.count])
}

func (r *receptionChunkRows) UnmarshalJSON(body []byte) error {
	d := json.NewDecoder(bytes.NewReader(body))
	start, err := d.Token()
	if err != nil || start != json.Delim('[') {
		return errors.New("source_invalid")
	}
	var out receptionChunkRows
	for d.More() {
		if out.count == len(out.values) || d.Decode(&out.values[out.count]) != nil {
			return errors.New("source_invalid")
		}
		out.count++
	}
	end, err := d.Token()
	if err != nil || end != json.Delim(']') || out.count == 0 {
		return errors.New("source_invalid")
	}
	*r = out
	return nil
}

type receptionChunkWire struct {
	First   uint64             `json:"first_sequence,string"`
	Last    uint64             `json:"last_sequence,string"`
	Columns []string           `json:"columns"`
	Rows    receptionChunkRows `json:"rows"`
}

// PreparedReceptionChunk is an immutable validated body plus its detached
// descriptor. It contains no native rows, source pointer or enclosing cache.
type PreparedReceptionChunk struct {
	descriptor ReceptionChunkDescriptor
	body       string
}

// Descriptor returns one detached scalar descriptor.
func (p PreparedReceptionChunk) Descriptor() ReceptionChunkDescriptor { return p.descriptor }

// Body returns the original canonical bytes, without re-encoding or copying.
func (p PreparedReceptionChunk) Body() string { return p.body }

// PrepareReceptionHistoryChunk validates the original chunk body and every tuple
// against its descriptor. Recording ownership is checked separately by the
// cache; a valid hash is not permission to import another recording's chunk.
func PrepareReceptionHistoryChunk(d ReceptionChunkDescriptor, body string) (PreparedReceptionChunk, error) {
	p, _, err := decodeReceptionHistoryChunk(d, body)
	return p, err
}

// decodeReceptionHistoryChunk is shared by node preparation and reader import.
// Rows are a detached fixed value; PreparedReceptionChunk remains encoded-only.
func decodeReceptionHistoryChunk(d ReceptionChunkDescriptor, body string) (PreparedReceptionChunk, receptionChunkRows, error) {
	if !validReceptionChunk(d) || len(body) > MaxReceptionCanonicalChunkBytes || d.Bytes != uint64(len(body)) || d.Digest != receptionDigest([]byte(body)) {
		return PreparedReceptionChunk{}, receptionChunkRows{}, errors.New("source_invalid")
	}
	var wire receptionChunkWire
	if json.Unmarshal([]byte(body), &wire) != nil || wire.First != d.FirstSequence || wire.Last != d.LastSequence ||
		uint64(wire.Rows.count) != d.Count || len(wire.Columns) != len(receptionChunkColumns) {
		return PreparedReceptionChunk{}, receptionChunkRows{}, errors.New("source_invalid")
	}
	for i, column := range receptionChunkColumns {
		if wire.Columns[i] != column {
			return PreparedReceptionChunk{}, receptionChunkRows{}, errors.New("source_invalid")
		}
	}
	minAt, maxAt := wire.Rows.values[0].At, wire.Rows.values[0].At
	for i := 0; i < wire.Rows.count; i++ {
		row := wire.Rows.values[i]
		// Descriptor validation already proved this addition cannot overflow.
		if row.Sequence != d.FirstSequence+uint64(i) {
			return PreparedReceptionChunk{}, receptionChunkRows{}, errors.New("source_invalid")
		}
		if row.At.Before(minAt) {
			minAt = row.At
		}
		if row.At.After(maxAt) {
			maxAt = row.At
		}
	}
	if !minAt.Equal(d.MinAt) || !maxAt.Equal(d.MaxAt) || !receptionCanonical([]byte(body), wire) {
		return PreparedReceptionChunk{}, receptionChunkRows{}, errors.New("source_invalid")
	}
	return PreparedReceptionChunk{descriptor: d, body: body}, wire.Rows, nil
}

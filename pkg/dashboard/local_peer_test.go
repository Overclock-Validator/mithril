package dashboard

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
)

func localPeerTestDetail(count int) LocalPeerDetail {
	at := time.Date(2026, 11, 3, 12, 0, 0, 999999999, time.UTC)
	genesis := solana.Hash{255}.String()
	d := LocalPeerDetail{SchemaVersion: 1, Family: "local_peers", Scope: "local_signed_contacts",
		Binding:    NodeNetworkBinding{RunID: strings.Repeat("\x01", 128), NodeStartedAt: at, GenesisHash: &genesis},
		CapturedAt: at, ClientGeneration: math.MaxUint64, ClientStartedAt: at,
		RunGeneration: math.MaxUint64, RunStartedAt: at, PublicationSequence: math.MaxUint64, SampledAt: at,
		Capacity: LocalPeerCapacity, Limited: true, CountersSaturated: true,
		AcceptedAdverts: math.MaxUint64, RetainedAdverts: math.MaxUint64, CapacityRefusals: math.MaxUint64,
		Rows: make([]LocalPeerRow, count)}
	for i := range d.Rows {
		key := solana.PublicKey{255}
		binary.BigEndian.PutUint64(key[24:], uint64(i))
		d.Rows[i] = LocalPeerRow{Identity: key.String(), ObservedAt: at,
			Outset: math.MaxUint64, Wallclock: math.MaxUint64,
			Version: LocalPeerVersion{Major: math.MaxUint16, Minor: math.MaxUint16, Patch: math.MaxUint16,
				Commit: math.MaxUint32, FeatureSet: math.MaxUint32, Client: math.MaxUint16},
			ShredVersion: math.MaxUint16} // false is the longest bool encoding.
	}
	return d
}

func TestLocalPeerDetail(t *testing.T) {
	t.Run("maximum_canonical_and_detached", func(t *testing.T) {
		d := localPeerTestDetail(LocalPeerCapacity)
		prepared, err := PrepareLocalPeers(d)
		if err != nil {
			t.Fatal(err)
		}
		body := prepared.Body()
		if prepared.Bytes() != len(body) || len(body) > MaxLocalPeerBytes {
			t.Fatalf("body size = %d, limit %d", len(body), MaxLocalPeerBytes)
		}
		decoded, err := DecodeLocalPeers([]byte(body))
		if err != nil || len(decoded.Rows) != LocalPeerCapacity {
			t.Fatalf("maximum roundtrip: rows=%d, err=%v", len(decoded.Rows), err)
		}
		sum := sha256.Sum256([]byte(body))
		if prepared.Digest() != hex.EncodeToString(sum[:]) {
			t.Fatal("digest does not cover the canonical body")
		}
		originalGenesis := *prepared.Binding().GenesisHash
		*d.Binding.GenesisHash = solana.Hash{1}.String()
		d.Rows[0].Identity = "changed"
		b := prepared.Binding()
		*b.GenesisHash = "changed"
		if prepared.Body() != body || *prepared.Binding().GenesisHash != originalGenesis {
			t.Fatal("source or returned binding aliases prepared data")
		}
		t.Logf("maximum canonical body: %d/%d bytes (%d rows)", len(body), MaxLocalPeerBytes, len(decoded.Rows))
	})
	t.Run("known_empty_and_independent_clocks", func(t *testing.T) {
		d := localPeerTestDetail(0)
		d.CountersSaturated, d.Limited = false, false
		// No UTC ordering is invented for a monotonic-source publication. Totals
		// can include post-sample adverts and refusals from expired windows.
		d.SampledAt = d.CapturedAt.Add(-time.Hour)
		d.RunStartedAt = d.CapturedAt.Add(time.Hour)
		p, err := PrepareLocalPeers(d)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = DecodeLocalPeers([]byte(p.Body())); err != nil || !strings.HasSuffix(p.Body(), `"rows":[]}`) {
			t.Fatalf("known empty rows: %v", err)
		}
		var nilPrepared *PreparedLocalPeers
		if nilPrepared.Body() != "" || nilPrepared.Digest() != "" || nilPrepared.Bytes() != 0 || nilPrepared.Binding().RunID != "" {
			t.Fatal("nil preparation must be unavailable")
		}
	})
	t.Run("invalid_metadata_and_rows", func(t *testing.T) {
		changes := map[string]func(*LocalPeerDetail){
			"schema":        func(d *LocalPeerDetail) { d.SchemaVersion++ },
			"family":        func(d *LocalPeerDetail) { d.Family = "peers" },
			"scope":         func(d *LocalPeerDetail) { d.Scope = "cluster" },
			"binding":       func(d *LocalPeerDetail) { d.Binding.RunID = "" },
			"no_genesis":    func(d *LocalPeerDetail) { d.Binding.GenesisHash = nil },
			"zero_genesis":  func(d *LocalPeerDetail) { v := (solana.Hash{}).String(); d.Binding.GenesisHash = &v },
			"capacity":      func(d *LocalPeerDetail) { d.Capacity-- },
			"client_zero":   func(d *LocalPeerDetail) { d.ClientGeneration = 0 },
			"run_zero":      func(d *LocalPeerDetail) { d.RunGeneration = 0 },
			"sequence_zero": func(d *LocalPeerDetail) { d.PublicationSequence = 0 },
			"nil_rows":      func(d *LocalPeerDetail) { d.Rows = nil },
			"over_rows":     func(d *LocalPeerDetail) { d.Rows = make([]LocalPeerRow, LocalPeerCapacity+1) },
			"saturation":    func(d *LocalPeerDetail) { d.Limited = false },
			"date":          func(d *LocalPeerDetail) { d.SampledAt = time.Time{} },
			"offset":        func(d *LocalPeerDetail) { d.Rows[0].ObservedAt = d.SampledAt.In(time.FixedZone("offset", 60)) },
			"identity":      func(d *LocalPeerDetail) { d.Rows[0].Identity = "127.0.0.1:8001" },
			"zero_key":      func(d *LocalPeerDetail) { d.Rows[0].Identity = (solana.PublicKey{}).String() },
			"duplicate":     func(d *LocalPeerDetail) { d.Rows[1] = d.Rows[0] },
			"order":         func(d *LocalPeerDetail) { d.Rows[0], d.Rows[1] = d.Rows[1], d.Rows[0] },
		}
		for name, change := range changes {
			t.Run(name, func(t *testing.T) {
				d := localPeerTestDetail(2)
				change(&d)
				if _, err := PrepareLocalPeers(d); err == nil {
					t.Fatal("accepted invalid source projection")
				}
			})
		}
	})
	t.Run("strict_wire", func(t *testing.T) {
		p, err := PrepareLocalPeers(localPeerTestDetail(1))
		if err != nil {
			t.Fatal(err)
		}
		body := p.Body()
		bad := []string{
			" " + body,
			body + " ",
			strings.Replace(body, `"schema_version":1`, `"schema_version":1,"schema_version":1`, 1),
			strings.Replace(body, `"schema_version":1`, `"endpoint":"127.0.0.1:8001","schema_version":1`, 1),
			strings.Replace(body, `"repair":false`, `"repair":null`, 1),
			strings.Replace(body, `"repair":false,`, "", 1),
			strings.Replace(body, `"outset":"18446744073709551615"`, `"outset":"01"`, 1),
			strings.Replace(body, `"outset":"18446744073709551615"`, `"outset":1`, 1),
			strings.Replace(body, `"outset":"18446744073709551615"`, `"outset":"18446744073709551616"`, 1),
			strings.Replace(body, `"publication_sequence":"18446744073709551615",`, "", 1),
			strings.Replace(body, `"publication_sequence":"18446744073709551615"`, `"publication_sequence":"0"`, 1),
			strings.Replace(body, `"publication_sequence":"18446744073709551615"`, `"publication_sequence":"01"`, 1),
			strings.Replace(body, `"publication_sequence":"18446744073709551615"`, `"publication_sequence":1`, 1),
			strings.Replace(body, `"publication_sequence":"18446744073709551615"`, `"publication_sequence":"18446744073709551616"`, 1),
			strings.Replace(body, `"major":65535`, `"major":65536`, 1),
			string(bytes.Repeat([]byte(" "), MaxLocalPeerBytes+1)),
		}
		for i, raw := range bad {
			if _, err := DecodeLocalPeers([]byte(raw)); err == nil {
				t.Fatalf("accepted malformed wire case %d", i)
			}
		}
		var envelope map[string]json.RawMessage
		if err = json.Unmarshal([]byte(body), &envelope); err != nil {
			t.Fatal(err)
		}
		envelope["rows"] = json.RawMessage("[" + strings.Repeat("{},", LocalPeerCapacity) + "{}]")
		raw, err := json.Marshal(envelope)
		if err != nil {
			t.Fatal(err)
		}
		if boundedLocalPeerRows(raw) {
			t.Fatal("row preflight accepted capacity+1 empty objects")
		}
	})
}

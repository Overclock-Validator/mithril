package genesis

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"time"
	"unicode/utf8"

	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

// Genesis uses Solana Account, not Mithril's internal Account serialization
// (which additionally contains a slot and a second public key).
func (a *AccountEntry) UnmarshalWithDecoder(d *bin.Decoder) error {
	for _, v := range []any{&a.Pubkey, &a.Lamports} {
		if err := d.Decode(v); err != nil {
			return err
		}
	}
	n, err := d.ReadUint64(bin.LE)
	if err != nil {
		return err
	}
	if n > uint64(d.Remaining()) {
		return io.ErrUnexpectedEOF
	}
	data, err := d.ReadNBytes(int(n))
	if err != nil {
		return err
	}
	a.Data = bytes.Clone(data)
	for _, v := range []any{&a.Owner, &a.Executable, &a.RentEpoch} {
		if err := d.Decode(v); err != nil {
			return err
		}
	}
	a.Key = solana.PublicKey(a.Pubkey)
	return nil
}
func (a *AccountEntry) MarshalWithEncoder(e *bin.Encoder) error {
	for _, v := range []any{a.Pubkey, a.Lamports, uint64(len(a.Data))} {
		if err := e.Encode(v); err != nil {
			return err
		}
	}
	if err := e.WriteBytes(a.Data, false); err != nil {
		return err
	}
	for _, v := range []any{a.Owner, a.Executable, a.RentEpoch} {
		if err := e.Encode(v); err != nil {
			return err
		}
	}
	return nil
}
func (a *AccountEntry) MarshalWihEncoder(e *bin.Encoder) error { return a.MarshalWithEncoder(e) }

func readCount(d *bin.Decoder, minimum uint64) (int, error) {
	n, err := d.ReadUint64(bin.LE)
	if err != nil {
		return 0, err
	}
	if n > uint64(d.Remaining())/minimum {
		return 0, io.ErrUnexpectedEOF
	}
	return int(n), nil
}
func readAccounts(d *bin.Decoder) ([]AccountEntry, error) {
	n, err := readCount(d, 89)
	if err != nil {
		return nil, err
	}
	rows := make([]AccountEntry, n)
	seen := make(map[[32]byte]bool, n)
	for i := range rows {
		if err := rows[i].UnmarshalWithDecoder(d); err != nil {
			return nil, err
		}
		if seen[rows[i].Pubkey] {
			return nil, fmt.Errorf("duplicate genesis account %s", rows[i].Key)
		}
		seen[rows[i].Pubkey] = true
	}
	return rows, nil
}
func writeAccounts(e *bin.Encoder, in []AccountEntry) error {
	rows := append([]AccountEntry(nil), in...)
	sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i].Pubkey[:], rows[j].Pubkey[:]) < 0 })
	if err := e.WriteUint64(uint64(len(rows)), bin.LE); err != nil {
		return err
	}
	for i := range rows {
		if i > 0 && rows[i].Pubkey == rows[i-1].Pubkey {
			return fmt.Errorf("duplicate genesis account %s", solana.PublicKey(rows[i].Pubkey))
		}
		if err := rows[i].MarshalWithEncoder(e); err != nil {
			return err
		}
	}
	return nil
}
func (g *Genesis) UnmarshalWithDecoder(d *bin.Decoder) error {
	*g = Genesis{}
	ts, err := d.ReadInt64(bin.LE)
	if err != nil {
		return err
	}
	g.CreationTime = time.Unix(ts, 0).UTC()
	if g.Accounts, err = readAccounts(d); err != nil {
		return err
	}
	n, err := readCount(d, 40)
	if err != nil {
		return err
	}
	g.Builtins = make([]BuiltinProgram, n)
	for i := range g.Builtins {
		if err := g.Builtins[i].UnmarshalWithDecoder(d); err != nil {
			return err
		}
	}
	if g.RewardPools, err = readAccounts(d); err != nil {
		return err
	}
	for _, v := range []any{&g.TicksPerSlot, &g.Unused, &g.PohParams, &g.Unused2, &g.Fees, &g.Rent, &g.Inflation, &g.EpochSchedule, &g.ClusterID} {
		if err := d.Decode(v); err != nil {
			return err
		}
	}
	if g.ClusterID > 3 {
		return fmt.Errorf("invalid genesis cluster type %d", g.ClusterID)
	}
	return nil
}
func (g *Genesis) MarshalWithEncoder(e *bin.Encoder) error {
	if g.CreationTime.Nanosecond() != 0 {
		return fmt.Errorf("genesis creation time must have whole-second precision")
	}
	if g.ClusterID > 3 {
		return fmt.Errorf("invalid genesis cluster type %d", g.ClusterID)
	}
	if err := e.WriteInt64(g.CreationTime.Unix(), bin.LE); err != nil {
		return err
	}
	if err := writeAccounts(e, g.Accounts); err != nil {
		return err
	}
	if err := e.WriteUint64(uint64(len(g.Builtins)), bin.LE); err != nil {
		return err
	}
	for i := range g.Builtins {
		if err := g.Builtins[i].MarshalWithEncoder(e); err != nil {
			return err
		}
	}
	if err := writeAccounts(e, g.RewardPools); err != nil {
		return err
	}
	for _, v := range []any{g.TicksPerSlot, g.Unused, &g.PohParams, g.Unused2, g.Fees, g.Rent, g.Inflation, g.EpochSchedule, g.ClusterID} {
		if err := e.Encode(v); err != nil {
			return err
		}
	}
	return nil
}
func (b *BuiltinProgram) UnmarshalWithDecoder(d *bin.Decoder) error {
	n, err := d.ReadUint64(bin.LE)
	if err != nil {
		return err
	}
	if n > uint64(d.Remaining()) {
		return io.ErrUnexpectedEOF
	}
	raw, err := d.ReadNBytes(int(n))
	if err != nil {
		return err
	}
	if !utf8.Valid(raw) {
		return fmt.Errorf("invalid UTF-8 builtin name")
	}
	b.Key = string(raw)
	return d.Decode(&b.Pubkey)
}
func (b *BuiltinProgram) MarshalWithEncoder(e *bin.Encoder) error {
	if !utf8.ValidString(b.Key) {
		return fmt.Errorf("invalid UTF-8 builtin name")
	}
	if err := e.WriteUint64(uint64(len(b.Key)), bin.LE); err != nil {
		return err
	}
	if err := e.WriteBytes([]byte(b.Key), false); err != nil {
		return err
	}
	return e.WriteBytes(b.Pubkey[:], false)
}
func (b *BuiltinProgram) MarshalWihEncoder(e *bin.Encoder) error { return b.MarshalWithEncoder(e) }

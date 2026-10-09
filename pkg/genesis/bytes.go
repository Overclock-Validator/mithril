package genesis

import (
	"bytes"
	"crypto/sha256"
	"fmt"

	bin "github.com/gagliardetto/binary"
)

const MaxGenesisSize = 10_000_000

func Encode(g *Genesis) ([]byte, error) {
	if g == nil {
		return nil, fmt.Errorf("nil genesis")
	}
	var b bytes.Buffer
	if err := g.MarshalWithEncoder(bin.NewBinEncoder(&b)); err != nil {
		return nil, err
	}
	if b.Len() > MaxGenesisSize {
		return nil, fmt.Errorf("genesis.bin too large")
	}
	return b.Bytes(), nil
}
func Decode(raw []byte) (*Genesis, *[32]byte, error) {
	if len(raw) > MaxGenesisSize {
		return nil, nil, fmt.Errorf("genesis.bin too large")
	}
	g := new(Genesis)
	d := bin.NewBinDecoder(raw)
	if err := g.UnmarshalWithDecoder(d); err != nil {
		return nil, nil, err
	}
	if d.HasRemaining() {
		return nil, nil, fmt.Errorf("genesis.bin contains %d trailing bytes", d.Remaining())
	}
	canonical, err := Encode(g)
	if err != nil {
		return nil, nil, err
	}
	if !bytes.Equal(raw, canonical) {
		return nil, nil, fmt.Errorf("genesis.bin is not canonically encoded")
	}
	hash := sha256.Sum256(raw)
	return g, &hash, nil
}

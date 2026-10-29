package rewards

import bin "github.com/gagliardetto/binary"

type Inflation struct {
	Initial        float64
	Terminal       float64
	Taper          float64
	FoundationVal  float64
	FoundationTerm float64
	Unused         float64
}

func (inflation *Inflation) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error

	inflation.Initial, err = decoder.ReadFloat64(bin.LE)
	if err != nil {
		return err
	}

	inflation.Terminal, err = decoder.ReadFloat64(bin.LE)
	if err != nil {
		return err
	}

	inflation.Taper, err = decoder.ReadFloat64(bin.LE)
	if err != nil {
		return err
	}

	inflation.FoundationVal, err = decoder.ReadFloat64(bin.LE)
	if err != nil {
		return err
	}

	inflation.FoundationTerm, err = decoder.ReadFloat64(bin.LE)
	if err != nil {
		return err
	}

	inflation.Unused, err = decoder.ReadFloat64(bin.LE)
	return err
}

func (inflation *Inflation) Total(year float64) float64 {
	tapered := inflation.Initial * inflationPow(1.0-inflation.Taper, year)
	if tapered > inflation.Terminal {
		return tapered
	} else {
		return inflation.Terminal
	}
}

func (inflation *Inflation) Foundation(year float64) float64 {
	if year < inflation.FoundationTerm {
		return inflation.Total(year) * inflation.FoundationVal
	} else {
		return 0.0
	}
}

func (inflation *Inflation) Validator(year float64) float64 {
	return inflation.Total(year) - inflation.Foundation(year)
}

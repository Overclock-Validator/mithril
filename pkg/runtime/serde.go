package runtime

import (
	"fmt"
	"math"
	"time"

	bin "github.com/gagliardetto/binary"
)

// Dumping ground for handwritten serialization boilerplate.
// To be removed when switching over to serde-generate.

func (a *PohParams) UnmarshalWithDecoder(decoder *bin.Decoder) (err error) {
	*a = PohParams{}
	var tickDuration serdeDuration
	if err = decoder.Decode(&tickDuration); err != nil {
		return err
	}
	if a.TickDuration, err = tickDuration.Duration(); err != nil {
		return err
	}
	if a.HasTickCount, err = decoder.ReadBool(); err != nil {
		return err
	}
	if a.HasTickCount {
		if a.TickCount, err = decoder.ReadUint64(bin.LE); err != nil {
			return err
		}
	}
	if a.HasHashesPerTick, err = decoder.ReadBool(); err != nil {
		return err
	}
	if a.HasHashesPerTick {
		if a.HashesPerTick, err = decoder.ReadUint64(bin.LE); err != nil {
			return err
		}
	}
	return nil
}

func (a *PohParams) MarshalWithEncoder(encoder *bin.Encoder) (err error) {
	if a.TickDuration < 0 {
		return fmt.Errorf("negative tick duration")
	}
	tickDuration := newSerdeDuration(a.TickDuration)
	if err = encoder.Encode(&tickDuration); err != nil {
		return err
	}
	if err = encoder.WriteBool(a.HasTickCount); err != nil {
		return err
	}
	if a.HasTickCount {
		if err = encoder.WriteUint64(a.TickCount, bin.LE); err != nil {
			return err
		}
	}
	if err = encoder.WriteBool(a.HasHashesPerTick); err != nil {
		return err
	}
	if a.HasHashesPerTick {
		if err = encoder.WriteUint64(a.HashesPerTick, bin.LE); err != nil {
			return err
		}
	}
	return nil
}

// MarshalWithDecoder is retained for callers of the historically misnamed method.
func (a *PohParams) MarshalWithDecoder(encoder *bin.Encoder) error {
	return a.MarshalWithEncoder(encoder)
}

// serdeDuration implements the bincode serialization of std::time::Duration.
type serdeDuration struct {
	Secs  uint64
	Nanos uint32
}

func newSerdeDuration(d time.Duration) serdeDuration {
	if d < 0 {
		panic("negative duration")
	}
	return serdeDuration{
		Secs:  uint64(d / time.Second),
		Nanos: uint32(d % time.Second),
	}
}

func (s serdeDuration) Duration() (time.Duration, error) {
	if time.Duration(s.Nanos) >= time.Second {
		return 0, fmt.Errorf("malformed serde duration")
	}
	if s.Secs > uint64(time.Duration(math.MaxInt64)/time.Second) {
		return 0, fmt.Errorf("malformed serde duration")
	}
	seconds := time.Duration(s.Secs) * time.Second
	if seconds > time.Duration(math.MaxInt64)-time.Duration(s.Nanos) {
		return 0, fmt.Errorf("malformed serde duration")
	}
	d := time.Duration(s.Nanos) + seconds
	return d, nil
}

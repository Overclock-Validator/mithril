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

func (a *PohParams) MarshalWithDecoder(encoder *bin.Encoder) (err error) {
	tickDuration := newSerdeDuration(a.TickDuration)
	_ = encoder.Encode(&tickDuration)
	_ = encoder.WriteBool(a.HasTickCount)
	if a.HasTickCount {
		_ = encoder.WriteUint64(a.TickCount, bin.LE)
	}
	_ = encoder.WriteBool(a.HasHashesPerTick)
	if a.HasHashesPerTick {
		_ = encoder.WriteUint64(a.HashesPerTick, bin.LE)
	}
	return nil
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
	if time.Duration(s.Nanos) > time.Second {
		return 0, fmt.Errorf("malformed serde duration")
	}
	if s.Secs > uint64(time.Duration(math.MaxInt64)/time.Second) {
		return 0, fmt.Errorf("malformed serde duration")
	}
	d := time.Duration(s.Nanos) + (time.Duration(s.Secs) * time.Second)
	return d, nil
}

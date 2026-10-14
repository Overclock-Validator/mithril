package runtime

import (
	"bytes"
	"errors"
	bin "github.com/gagliardetto/binary"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
	"time"
)

type failingWriter struct{ n int }

func (w *failingWriter) Write(p []byte) (int, error) {
	if w.n < len(p) {
		return 0, errWrite
	}
	w.n -= len(p)
	return len(p), nil
}

var errWrite = errors.New("write failed")

func TestPohEncodingAndErrors(t *testing.T) {
	p := PohParams{TickDuration: time.Second + 3, HasTickCount: true, TickCount: 42, HasHashesPerTick: true, HashesPerTick: 7}
	var b bytes.Buffer
	require.NoError(t, p.MarshalWithEncoder(bin.NewBinEncoder(&b)))
	for n := 0; n < b.Len(); n++ {
		require.ErrorIs(t, p.MarshalWithEncoder(bin.NewBinEncoder(&failingWriter{n})), errWrite)
	}
	var out PohParams
	require.NoError(t, out.UnmarshalWithDecoder(bin.NewBinDecoder(b.Bytes())))
	require.Equal(t, p, out)
	p.HasTickCount = false
	p.HasHashesPerTick = false
	b.Reset()
	require.NoError(t, p.MarshalWithEncoder(bin.NewBinEncoder(&b)))
	require.NoError(t, out.UnmarshalWithDecoder(bin.NewBinDecoder(b.Bytes())))
	require.Zero(t, out.TickCount)
	require.Zero(t, out.HashesPerTick)
	p.TickDuration = -1
	require.Error(t, p.MarshalWithEncoder(bin.NewBinEncoder(&b)))
	for _, d := range []serdeDuration{{Nanos: 1_000_000_000}, {Secs: math.MaxUint64}, {Secs: 9223372036, Nanos: 854775808}} {
		_, err := d.Duration()
		require.Error(t, err)
	}
	got, err := (serdeDuration{Secs: 9223372036, Nanos: 854775807}).Duration()
	require.NoError(t, err)
	require.Equal(t, time.Duration(math.MaxInt64), got)
}

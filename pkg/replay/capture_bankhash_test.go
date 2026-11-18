package replay

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVerifyCaptureBankHash(t *testing.T) {
	previous := CaptureBankHashes
	defer func() { CaptureBankHashes = previous }()

	CaptureBankHashes = nil
	require.NoError(t, verifyCaptureBankHash(10, []byte{1}), "a normal node checks nothing")

	good := [32]byte{7}
	CaptureBankHashes = map[uint64][32]byte{10: good}
	require.NoError(t, verifyCaptureBankHash(10, good[:]))

	err := verifyCaptureBankHash(10, make([]byte, 32))
	require.True(t, errors.Is(err, ErrCaptureBankHash))
	require.ErrorContains(t, err, "Bank hash mismatch! slot=10 expected=")

	err = verifyCaptureBankHash(11, good[:])
	require.True(t, errors.Is(err, ErrCaptureBankHash))
	require.ErrorContains(t, err, "no bank hash available for slot 11")
}

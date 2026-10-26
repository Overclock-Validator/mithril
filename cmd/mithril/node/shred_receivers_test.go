package node

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseShredReceivers(t *testing.T) {
	got, err := parseShredReceivers("turbine.shred_receiver_addresses", []string{"64.130.42.198:8003", " ", "127.0.0.1:9000"})
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, "64.130.42.198:8003", got[0].String())
	assert.Equal(t, "127.0.0.1:9000", got[1].String())

	none, err := parseShredReceivers("turbine.shred_receiver_addresses", nil)
	require.NoError(t, err)
	assert.Empty(t, none)

	for _, bad := range []string{"64.130.42.198", "64.130.42.198:0", "0.0.0.0:8003", "not a host:8003"} {
		_, err := parseShredReceivers("turbine.shred_receiver_addresses", []string{bad})
		assert.Errorf(t, err, "%q", bad)
	}
}

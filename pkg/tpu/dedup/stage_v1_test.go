package dedup

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/stretchr/testify/require"
)

func TestFilterWireDeduplicatesV1ByTrailingSignature(t *testing.T) {
	cache := NewCache(8)
	first := txfixture.MustSignedV1Wire(1, 16)
	second := txfixture.MustSignedV1Wire(2, 16)

	require.True(t, FilterWire(first, cache, nil, nil))
	require.False(t, FilterWire(first, cache, nil, nil))
	require.True(t, FilterWire(second, cache, nil, nil))
}

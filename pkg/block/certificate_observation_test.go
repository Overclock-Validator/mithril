package block

import (
	"encoding/json"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestCertificateObservationIsPrivateAndCopied(t *testing.T) {
	var absent *Block
	_, ok := absent.CertificateObservation()
	require.False(t, ok)
	block := &Block{Slot: 12}
	require.Zero(t, testing.AllocsPerRun(10, func() { block.EnableCertificateObservation(solana.PublicKey{}) }))
	before, err := json.Marshal(block)
	require.NoError(t, err)
	subject := solana.PublicKey{3}
	block.EnableCertificateObservation(subject)
	_, ok = block.CertificateObservation()
	require.False(t, ok)
	value := CertificateObservation{VoteAccount: subject, Reward: CertificateMembership{TargetSlot: 4, Included: true}}
	block.RecordCertificateObservation(value)
	got, ok := block.CertificateObservation()
	require.True(t, ok)
	got.Reward.Included = false
	again, _ := block.CertificateObservation()
	require.True(t, again.Reward.Included)
	after, err := json.Marshal(block)
	require.NoError(t, err)
	require.Equal(t, before, after)
	var decoded Block
	require.NoError(t, json.Unmarshal([]byte(`{"Slot":12,"certificateObservation":{"ready":true},"CertificateObservation":{"VoteAccount":[3]}}`), &decoded))
	_, ok = decoded.CertificateObservation()
	require.False(t, ok, "wire data cannot create a trusted in-memory result")
	block.EnableCertificateObservation(solana.PublicKey{4})
	block.RecordCertificateObservation(value)
	_, ok = block.CertificateObservation()
	require.False(t, ok, "a result for another subject is not accepted")
}

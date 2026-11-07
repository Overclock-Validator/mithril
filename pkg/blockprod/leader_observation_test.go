package blockprod

import (
	"errors"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestLeaderCertificateObservationRemainsPrivateUntilHandoff(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "handoff", true: "failed_freeze"}[fail], func(t *testing.T) {
			subject := solana.PublicKey{7}
			value := block.CertificateObservation{VoteAccount: subject, Reward: block.CertificateMembership{Present: true, Included: true}}
			loop, _, _, _ := newFinishingLeaderLoop(t, 106, &recordingHooks{}, func(_ *ParentContext, in replay.CommitLeaderInput) error {
				got, ok := in.Block.CertificateObservationSubject()
				require.True(t, ok)
				require.Equal(t, subject, got)
				require.False(t, in.Block.HasAlpenglowBlockID, "carrier identity is assigned only after speculative apply")
				in.Block.RecordCertificateObservation(value)
				if fail {
					return errors.New("test freeze failure")
				}
				return nil
			})
			loop.certificateObservationVoteAccount = subject
			var delivered *block.Block
			loop.onBlock = func(b *block.Block) { delivered = b }
			loop.finishActiveSlotLocked()
			if fail {
				require.Nil(t, delivered, "a failed speculative bank must not publish a carrier")
				return
			}
			require.NotNil(t, delivered)
			require.True(t, delivered.HasAlpenglowBlockID)
			got, ok := delivered.CertificateObservation()
			require.True(t, ok)
			require.Equal(t, value, got, "the exact in-memory block carries detached values through handoff")
		})
	}
}

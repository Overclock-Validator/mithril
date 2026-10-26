package node

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/stretchr/testify/require"
)

type rpcExecutionPublicationProbe struct {
	root uint64
	bank *replay.ChainTipSnapshot
}

func (*rpcExecutionPublicationProbe) SetSlotCtx(*sealevel.SlotCtx) {}
func (p *rpcExecutionPublicationProbe) SetRootedBankState(slot, _, _ uint64) {
	p.root, p.bank = slot, nil
}
func (p *rpcExecutionPublicationProbe) SetRootedExecutionBank(bank *replay.ChainTipSnapshot) {
	p.bank = bank
}

func TestRPCExecutionReconstructionFailurePreservesScalarPublication(t *testing.T) {
	server := &rpcExecutionPublicationProbe{bank: &replay.ChainTipSnapshot{Slot: 9}}
	s := &state.MithrilState{ManifestParentSlot: 10, ManifestBlockHeight: 7}
	publishRPCBankState(server, s)
	publishRPCExecutionBank(t.Context(), server, nil, s, nil, false)
	require.Equal(t, uint64(10), server.root)
	require.Nil(t, server.bank, "unavailable reconstruction must not resurrect the previous execution view")
	publishRPCExecutionBank(t.Context(), nil, nil, s, nil, false)
}

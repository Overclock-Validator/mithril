package replay

import (
	consensusengine "github.com/Overclock-Validator/mithril/pkg/consensus"
)

// ConsensusOpts carries the Alpenglow consensus engine into replay.
// Nil (or a nil Engine) runs replay without certificate finality — promotion
// then relies solely on delegated (RPC-attested) finality.
type ConsensusOpts struct {
	Engine consensusengine.Engine
}

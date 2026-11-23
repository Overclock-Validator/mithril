package node

import (
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/blockstream"
)

// nodePrewarmLifetime serializes manifest callbacks and replay handover.
type nodePrewarmLifetime struct {
	mu        sync.Mutex
	collector *blockstream.TurbinePrewarm
	closing   bool
}

func (p *nodePrewarmLifetime) forReplay(closeAdmission bool) *blockstream.TurbinePrewarm {
	p.mu.Lock()
	defer p.mu.Unlock()
	if closeAdmission {
		p.closing = true
	}
	return p.collector
}

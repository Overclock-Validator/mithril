package blockstream

import (
	"context"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
)

type Prefetcher struct {
	src        *BlockSource
	accountsDb *accountsdb.AccountsDb
	out        chan *block.Block
}

func NewPrefetcher(
	ctx context.Context,
	b *BlockSource,
	a *accountsdb.AccountsDb,
) *Prefetcher {
	p := &Prefetcher{b, a, make(chan *block.Block)}
	go p.prefetchWorker(ctx)
	return p
}

func (p *Prefetcher) NextBlock() *block.Block {
	return <-p.out
}

func (p *Prefetcher) prefetchWorker(ctx context.Context) {
	defer close(p.out)
	for {
		b := p.src.NextBlock()
		if b == nil {
			return
		}
		select {
		case <-ctx.Done():
			mlog.Log.Infof("prefetch worker exiting: %v", ctx.Err())
			return
		case p.out <- b: // Caller was waiting on NextBlock so return it immediately.
			continue
		default:
		}

		// TODO account for ALTs
		p.accountsDb.Prefetch(b.UniqueAccounts())

		select {
		case <-ctx.Done():
			mlog.Log.Infof("prefetch worker exiting: %v", ctx.Err())
			return
		case p.out <- b:
		}
	}
}

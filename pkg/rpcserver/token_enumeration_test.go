package rpcserver

import (
	"context"
	"encoding/binary"
	"sort"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/rpcindex"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	solanarpc "github.com/gagliardetto/solana-go/rpc"
	"github.com/stretchr/testify/require"
)

// scannableAccounts adds an account-store scan to viewAccounts. The scan
// waits for gate, so a test can observe a wallet before it is enumerated.
type scannableAccounts struct {
	*viewAccounts
	gate chan struct{}
}

func (s *scannableAccounts) ScanAccounts(ctx context.Context, cfg accountsdb.ScanRecordsConfig, fn func(pubkey, owner [32]byte, lamports uint64, data []byte)) error {
	select {
	case <-s.gate:
	case <-ctx.Done():
		return ctx.Err()
	}
	programs := map[[32]byte]bool{}
	for _, p := range cfg.Programs {
		programs[p] = true
	}
	visit := func(a *accounts.Account) {
		if programs[a.Owner] {
			fn(a.Key, a.Owner, a.Lamports, a.Data)
		}
	}
	s.ws.RangeVersions(func(_ uint64, a *accounts.Account) { visit(a) })
	for _, a := range s.durable {
		visit(a)
	}
	return nil
}

func splAccount(key, mint, owner solana.PublicKey, amount uint64) *accounts.Account {
	data := make([]byte, 165)
	copy(data[0:32], mint[:])
	copy(data[32:64], owner[:])
	binary.LittleEndian.PutUint64(data[64:72], amount)
	data[108] = 1
	return &accounts.Account{Key: key, Lamports: 2_039_280, Owner: [32]byte(rpcindex.SPLTokenProgram), Data: data}
}

func TestGetTokenAccountsByOwnerListsIdleAccountsAfterEnumeration(t *testing.T) {
	server, client, plain := newViewServer(t, sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32, LeaderScheduleSlotOffset: 32})
	accts := &scannableAccounts{viewAccounts: plain, gate: make(chan struct{})}
	views := server.BankViews()
	views.Reset(accts)

	wallet, buyer, mint := solana.PublicKey{1}, solana.PublicKey{2}, solana.PublicKey{5}
	idle, sold, fresh := solana.PublicKey{10}, solana.PublicKey{11}, solana.PublicKey{12}
	// Before registration the wallet holds idle and sold; neither is touched
	// again until sold changes hands in slot 20.
	plain.durable[idle] = splAccount(idle, mint, wallet, 1)
	plain.durable[sold] = splAccount(sold, mint, wallet, 2)
	require.NoError(t, server.EnableHistory(t.TempDir(), solana.Hash{1}, []solana.PublicKey{wallet}, 0))
	ix := server.RPCIndexer()

	views.PublishProcessed(testView(t, 10, 10))
	views.MarkFinalized(10)
	// Slot 20 creates fresh for the wallet (discovered at commit) and moves
	// sold to another owner.
	plain.ws.Add(20, []*accounts.Account{splAccount(fresh, mint, wallet, 3), splAccount(sold, mint, buyer, 2)})
	ix.CompletedBlock(20, solana.Hash{20}, &b.Block{Slot: 20}, nil, []rpcindex.TokenAccountChange{
		{Owner: wallet, Mint: mint, Account: fresh, Program: rpcindex.SPLTokenProgram},
	})
	views.PublishProcessed(testView(t, 20, 20))

	ctx := t.Context()
	list := func(commitment solanarpc.CommitmentType) ([]string, uint64, error) {
		out, err := client.GetTokenAccountsByOwner(ctx, wallet, &solanarpc.GetTokenAccountsConfig{Mint: &mint},
			&solanarpc.GetTokenAccountsOpts{Commitment: commitment, Encoding: "base64"})
		if err != nil {
			return nil, 0, err
		}
		var keys []string
		for _, a := range out.Value {
			keys = append(keys, a.Pubkey.String())
		}
		sort.Strings(keys)
		return keys, out.Context.Slot, nil
	}

	_, _, err := list(solanarpc.CommitmentProcessed)
	require.ErrorContains(t, err, "still being enumerated")

	close(accts.gate)
	require.Eventually(t, func() bool { _, _, err := list(solanarpc.CommitmentProcessed); return err == nil }, 5*time.Second, time.Millisecond)

	got, slot, err := list(solanarpc.CommitmentProcessed)
	require.NoError(t, err)
	require.EqualValues(t, 20, slot)
	require.Equal(t, sortedKeys(idle, fresh), got, "slot 20: sold has a new owner, fresh is new")

	got, slot, err = list(solanarpc.CommitmentFinalized)
	require.NoError(t, err)
	require.EqualValues(t, 10, slot)
	require.Equal(t, sortedKeys(idle, sold), got, "slot 10: before the sale and before fresh existed")

	_, err = client.GetTokenAccountsByOwner(ctx, buyer, &solanarpc.GetTokenAccountsConfig{Mint: &mint}, nil)
	require.ErrorContains(t, err, "only for watched wallets")
}

func sortedKeys(keys ...solana.PublicKey) []string {
	out := make([]string, len(keys))
	for i, k := range keys {
		out[i] = k.String()
	}
	sort.Strings(out)
	return out
}

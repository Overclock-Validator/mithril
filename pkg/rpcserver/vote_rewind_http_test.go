package rpcserver

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/cockroachdb/pebble/v2"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestVoteAccountsHTTPAfterLegacyIndexRewind(t *testing.T) {
	key := solana.PublicKey{71}
	state := sealevel.VoteStateVersions{Type: sealevel.VoteStateVersionV4}
	state.V4.NodePubkey = solana.PublicKey{72}
	state.V4.AuthorizedVoters.AuthorizedVoters.Set(0, state.V4.NodePubkey)
	state.V4.Votes.PushBack(sealevel.LandedVote{Lockout: sealevel.VoteLockout{Slot: 9}})
	s := seedStakeCacheServer(t, voteAccountForRPC(t, key, &state))
	commitCacheAccounts(t, s, 20, &accounts.Account{Key: key})
	legacyKey := append([]byte("\x00mithril.vote."), key[:]...)
	require.NoError(t, s.acctsDb.Index.Delete(legacyKey, pebble.Sync))
	keys, err := s.acctsDb.VoteAccountPubkeys(t.Context())
	require.NoError(t, err)
	require.Empty(t, keys)
	_, err = s.acctsDb.RewindToBatchBoundaryAndPersist(10, func(accountsdb.RewindResult) error { return nil })
	require.NoError(t, err)
	previous, hadPrevious := global.EpochStakesSnapshot(0)
	global.PutEpochStakes(0, map[solana.PublicKey]uint64{}, map[solana.PublicKey]*epochstakes.VoteAccount{}, 0)
	t.Cleanup(func() {
		if hadPrevious {
			global.PutEpochStakes(0, previous.Stakes, previous.VoteAccounts, previous.TotalStake)
		} else {
			global.ClearEpochStakes(0)
		}
	})
	server := NewRpcServer(s.acctsDb, 0, &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 100}, solana.Hash{1})
	require.NoError(t, server.listener.Close())
	server.SetRootedBankState(10, 10, 0)
	call := func(config string) GetVoteAccountsResp {
		t.Helper()
		body := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"getVoteAccounts","params":[%s]}`, config)
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body)))
		var got struct {
			Result GetVoteAccountsResp `json:"result"`
			Error  interface{}         `json:"error"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
		require.Nil(t, got.Error, rec.Body.String())
		return got.Result
	}
	filtered := call(fmt.Sprintf(`{"votePubkey":%q,"keepUnstakedDelinquents":true}`, key.String()))
	require.Len(t, filtered.Current, 1)
	all := call(`{"keepUnstakedDelinquents":true}`)
	require.Len(t, all.Current, 1)
	require.Equal(t, key.String(), all.Current[0].VotePubkey)
}

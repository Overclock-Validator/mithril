package sealevel

import (
	"bytes"
	"testing"

	"github.com/Overclock-Validator/mithril/fixtures"
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func preloadUpgradeableFixture(t *testing.T, elf []byte, deploySlot uint64) (accounts.Account, *accounts.Account) {
	t.Helper()
	dataKey := benchPubkey(0x93)
	var encoded bytes.Buffer
	state := UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgram,
		Program: UpgradeableLoaderStateProgram{ProgramDataAddress: dataKey}}
	require.NoError(t, state.MarshalWithEncoder(bin.NewBinEncoder(&encoded)))
	program := accounts.Account{Key: benchPubkey(0x94), Owner: a.BpfLoaderUpgradeableAddr,
		Executable: true, Lamports: 1, Data: bytes.Clone(encoded.Bytes())}
	encoded.Reset()
	state = UpgradeableLoaderState{Type: UpgradeableLoaderStateTypeProgramData,
		ProgramData: UpgradeableLoaderStateProgramData{Slot: deploySlot}}
	require.NoError(t, state.MarshalWithEncoder(bin.NewBinEncoder(&encoded)))
	data := make([]byte, int(upgradeableLoaderSizeOfProgramDataMetaData)+len(elf))
	copy(data, encoded.Bytes())
	copy(data[upgradeableLoaderSizeOfProgramDataMetaData:], elf)
	return program, &accounts.Account{Key: dataKey, Owner: a.BpfLoaderUpgradeableAddr, Lamports: 1, Data: data}
}

func TestPreloadSourceResolvesLikeExecution(t *testing.T) {
	elf := fixtures.Load(t, "sbpf", "noop_aligned.so")
	program, programData := preloadUpgradeableFixture(t, elf, 100)
	read := func(key solana.PublicKey) *accounts.Account {
		if key == programData.Key {
			return programData
		}
		return nil
	}

	key, source, ok := PreloadSource(program.Key, &program, 101, read)
	require.True(t, ok)
	assert.Equal(t, programData.Key, key, "upgradeable programs cache under their programdata address")
	assert.Equal(t, elf, source)

	_, _, ok = PreloadSource(program.Key, &program, 100, read)
	assert.False(t, ok, "programdata deployed in the bank's own slot is not executable yet")

	v2 := accounts.Account{Key: benchPubkey(0x95), Owner: a.BpfLoader2Addr, Executable: true, Lamports: 1, Data: elf}
	key, source, ok = PreloadSource(v2.Key, &v2, 101, read)
	require.True(t, ok)
	assert.Equal(t, v2.Key, key)
	assert.Equal(t, elf, source)

	for name, acct := range map[string]*accounts.Account{
		"missing":        nil,
		"not executable": {Owner: a.BpfLoader2Addr, Lamports: 1, Data: elf},
		"closed":         {Owner: a.BpfLoader2Addr, Executable: true, Data: elf},
		"builtin":        {Owner: a.NativeLoaderAddr, Executable: true, Lamports: 1, Data: []byte("system")},
	} {
		_, _, ok := PreloadSource(solana.PublicKey{}, acct, 101, read)
		assert.False(t, ok, name)
	}
}

func TestPreloadProgramCachesOnceAndMatchesExecution(t *testing.T) {
	elf := fixtures.Load(t, "sbpf", "noop_aligned.so")
	cache := accountsdb.NewWeightedCache[solana.PublicKey, *accountsdb.ProgramCacheEntry](64, func(solana.PublicKey, *accountsdb.ProgramCacheEntry) uint32 { return 1 })
	t.Cleanup(cache.Close)
	db := &accountsdb.AccountsDb{ProgramCache: cache}
	f := features.NewFeaturesDefault()
	key := benchPubkey(0x96)

	require.True(t, PreloadProgram(db, key, elf, f))
	entry, ok := db.MaybeGetProgramFromCache(key)
	require.True(t, ok)
	assert.True(t, entry.MatchesSource(elf, f), "execution's own match check accepts the preloaded entry")
	assert.False(t, PreloadProgram(db, key, elf, f), "a matching entry is not rebuilt")

	other := features.NewFeaturesDefault()
	other.EnableFeature(features.VirtualAddressSpaceAdjustments, 0)
	assert.False(t, entry.MatchesSource(elf, other), "a different feature set does not match the preload")

	changed := bytes.Clone(elf)
	changed[len(changed)-1] ^= 0xff
	assert.False(t, entry.MatchesSource(changed, f), "different source bytes do not match the preload")
	assert.False(t, PreloadProgram(db, benchPubkey(0x97), []byte("not an elf"), f), "an invalid program is not cached")
	_, ok = db.MaybeGetProgramFromCache(benchPubkey(0x97))
	assert.False(t, ok)
}

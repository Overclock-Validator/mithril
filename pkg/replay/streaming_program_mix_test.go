package replay

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/Overclock-Validator/mithril/fixtures"
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/programs/system"
	"github.com/stretchr/testify/require"
)

func signLifecycleInstructions(t *testing.T, instructions ...solana.Instruction) *solana.Transaction {
	t.Helper()
	tx, err := solana.NewTransaction(instructions, txfixture.TestBlockhash(), solana.TransactionPayer(txfixture.PayerPubkey()))
	require.NoError(t, err)
	key := txfixture.PayerPrivateKey()
	_, err = tx.Sign(func(pk solana.PublicKey) *solana.PrivateKey {
		if pk == key.PublicKey() {
			return &key
		}
		return nil
	})
	require.NoError(t, err)
	wire, err := tx.MarshalBinary()
	require.NoError(t, err)
	decoded, err := solana.TransactionFromBytes(wire)
	require.NoError(t, err)
	return decoded
}

// Each case has a real write consumed or replaced in a subsequent group. Fresh
// wire decoding avoids accidentally sharing resolved transactions across paths.
func TestStreamingLifecycleProgramMutations(t *testing.T) {
	previous := StreamingExecutionCfg
	StreamingExecutionCfg = StreamingExecutionConfig{Enabled: true}
	t.Cleanup(func() { StreamingExecutionCfg = previous })
	payer := txfixture.PayerPubkey()
	key := solana.PublicKey{0xC1}
	previousVote := global.VoteCacheItem(key)
	t.Cleanup(func() {
		if previousVote == nil {
			global.DeleteVoteCacheItem(key)
		} else {
			global.PutVoteCacheItem(key, previousVote)
		}
	})
	for _, kind := range []string{"nonce", "lookup extension", "vote commission", "program upgrade", "program deployment"} {
		t.Run(kind, func(t *testing.T) {
			setup := func() (*lifecycleEnv, []*solana.Transaction) {
				env := newLifecycleEnv(t)
				env.acctsDb.InitCaches()
				t.Cleanup(env.acctsDb.ProgramCache.Close)
				t.Cleanup(env.acctsDb.VoteAcctCache.Close)
				t.Cleanup(env.acctsDb.CommonAcctsCache.Close)
				put := func(pk solana.PublicKey, owner solana.PublicKey, data []byte, executable bool) {
					require.NoError(t, env.durable.SetAccountWithoutLock(pk, &accounts.Account{Key: pk, Owner: owner, Lamports: 100_000_000, Data: data, Executable: executable, RentEpoch: ^uint64(0)}))
				}
				put(payer, addresses.SystemProgramAddr, nil, false)
				native := func(pk solana.PublicKey) { put(pk, addresses.NativeLoaderAddr, nil, true) }
				var txs []*solana.Transaction
				switch kind {
				case "nonce":
					state := sealevel.NonceStateVersions{Type: sealevel.NonceVersionCurrent, Current: sealevel.NonceData{IsInitialized: true, Authority: payer, DurableNonce: [32]byte{0xAA}, FeeCalculator: sealevel.FeeCalculator{LamportsPerSignature: 5000}}}
					data, err := state.Marshal()
					require.NoError(t, err)
					put(key, addresses.SystemProgramAddr, data, false)
					// Repeated advances have different messages but the same nonce account;
					// only the first may advance in this bank. Later instructions must see it.
					for i := uint64(1); i <= 3; i++ {
						txs = append(txs, signLifecycleInstructions(t, system.NewAdvanceNonceAccountInstruction(key, solana.SysVarRecentBlockHashesPubkey, payer).Build(), system.NewTransferInstruction(i, payer, txfixture.DestPubkey()).Build()))
					}
				case "lookup extension":
					native(addresses.AddressLookupTableAddr)
					table := lookupTableAccount(t, txfixture.DestPubkey())
					table.Lamports = 100_000_000
					require.NoError(t, env.durable.SetAccountWithoutLock(lifecycleTableKey, table))
					for i := byte(1); i <= 3; i++ {
						instruction := sealevel.AddrLookupTableInstrExtendLookupTable{NewAddresses: []solana.PublicKey{{0xD2, i}}}
						var b bytes.Buffer
						require.NoError(t, instruction.MarshalWithEncoder(bin.NewBinEncoder(&b)))
						txs = append(txs, signLifecycleInstructions(t, solana.NewInstruction(addresses.AddressLookupTableAddr, solana.AccountMetaSlice{solana.Meta(lifecycleTableKey).WRITE(), solana.Meta(payer).SIGNER()}, b.Bytes())))
					}
				case "vote commission":
					global.DeleteVoteCacheItem(key)
					native(addresses.VoteProgramAddr)
					state := sealevel.VoteStateVersions{Type: sealevel.VoteStateVersionCurrent, Current: sealevel.VoteState{AuthorizedWithdrawer: payer, Commission: 30}}
					var b bytes.Buffer
					require.NoError(t, state.MarshalWithEncoder(bin.NewBinEncoder(&b)))
					data := make([]byte, sealevel.VoteStateV3Size)
					copy(data, b.Bytes())
					put(key, addresses.VoteProgramAddr, data, false)
					for _, commission := range []byte{20, 10, 5} {
						data := binary.LittleEndian.AppendUint32(nil, sealevel.VoteProgramInstrTypeUpdateCommission)
						data = append(data, commission)
						txs = append(txs, signLifecycleInstructions(t, solana.NewInstruction(addresses.VoteProgramAddr, solana.AccountMetaSlice{solana.Meta(key).WRITE(), solana.Meta(payer).SIGNER()}, data)))
					}
				case "program upgrade", "program deployment":
					native(addresses.BpfLoaderUpgradeableAddr)
					programDataKey := solana.PublicKey{0xC2}
					bufferKey := solana.PublicKey{0xC3}
					elf := fixtures.Load(t, "sbpf", "noop_aligned.so")
					encode := func(state sealevel.UpgradeableLoaderState, size int) []byte {
						var b bytes.Buffer
						require.NoError(t, state.MarshalWithEncoder(bin.NewBinEncoder(&b)))
						out := make([]byte, size)
						copy(out, b.Bytes())
						return out
					}
					put(key, addresses.BpfLoaderUpgradeableAddr, encode(sealevel.UpgradeableLoaderState{Type: sealevel.UpgradeableLoaderStateTypeProgram, Program: sealevel.UpgradeableLoaderStateProgram{ProgramDataAddress: programDataKey}}, 36), true)
					pd := encode(sealevel.UpgradeableLoaderState{Type: sealevel.UpgradeableLoaderStateTypeProgramData, ProgramData: sealevel.UpgradeableLoaderStateProgramData{Slot: 1, UpgradeAuthorityAddress: &payer}}, 45+len(elf))
					copy(pd[45:], elf)
					put(programDataKey, addresses.BpfLoaderUpgradeableAddr, pd, false)
					buf := encode(sealevel.UpgradeableLoaderState{Type: sealevel.UpgradeableLoaderStateTypeBuffer, Buffer: sealevel.UpgradeableLoaderStateBuffer{AuthorityAddress: &payer}}, 37+len(elf))
					copy(buf[37:], elf)
					put(bufferKey, addresses.BpfLoaderUpgradeableAddr, buf, false)
					if kind == "program deployment" {
						programDataKey, _, err := solana.FindProgramAddress([][]byte{key[:]}, addresses.BpfLoaderUpgradeableAddr)
						require.NoError(t, err)
						put(key, addresses.BpfLoaderUpgradeableAddr, make([]byte, 36), false)
						// No pre-funded PDA: Deploy creates it with a signed CPI.
						require.NoError(t, env.durable.SetAccountWithoutLock(programDataKey, &accounts.Account{Key: programDataKey, Owner: addresses.SystemProgramAddr}))
						write := sealevel.UpgradeableLoaderInstrWrite{Offset: 0, Bytes: elf[:8]}
						var b bytes.Buffer
						require.NoError(t, write.MarshalWithEncoder(bin.NewBinEncoder(&b)))
						txs = append(txs, signLifecycleInstructions(t, solana.NewInstruction(addresses.BpfLoaderUpgradeableAddr, solana.AccountMetaSlice{solana.Meta(bufferKey).WRITE(), solana.Meta(payer).SIGNER()}, b.Bytes())))
						deploy := sealevel.UpgradeableLoaderInstrDeployWithMaxDataLen{MaxDataLen: uint64(len(elf))}
						b.Reset()
						require.NoError(t, deploy.MarshalWithEncoder(bin.NewBinEncoder(&b)))
						txs = append(txs, signLifecycleInstructions(t, solana.NewInstruction(addresses.BpfLoaderUpgradeableAddr, solana.AccountMetaSlice{solana.Meta(payer).WRITE().SIGNER(), solana.Meta(programDataKey).WRITE(), solana.Meta(key).WRITE(), solana.Meta(bufferKey).WRITE(), solana.Meta(sealevel.SysvarRentAddr), solana.Meta(sealevel.SysvarClockAddr), solana.Meta(addresses.SystemProgramAddr), solana.Meta(payer).SIGNER()}, b.Bytes())))
						txs = append(txs, signLifecycleInstructions(t, solana.NewInstruction(key, nil, []byte{2})))
						break
					}

					txs = append(txs, signLifecycleInstructions(t, solana.NewInstruction(key, nil, []byte{1})))
					txs = append(txs, signLifecycleInstructions(t, solana.NewInstruction(addresses.BpfLoaderUpgradeableAddr, solana.AccountMetaSlice{solana.Meta(programDataKey).WRITE(), solana.Meta(key).WRITE(), solana.Meta(bufferKey).WRITE(), solana.Meta(payer).WRITE(), solana.Meta(sealevel.SysvarRentAddr), solana.Meta(sealevel.SysvarClockAddr), solana.Meta(payer).SIGNER()}, binary.LittleEndian.AppendUint32(nil, sealevel.UpgradeableLoaderInstrTypeUpgrade))))
					txs = append(txs, signLifecycleInstructions(t, solana.NewInstruction(key, nil, []byte{2})))
				}
				return env, txs
			}
			env, txs := setup()
			whole := lifecycleWholeBlock(t, env, txs, 2)
			switch kind {
			case "nonce":
				require.Contains(t, whole.delta, key)
				state, err := sealevel.UnmarshalNonceStateVersions(whole.delta[key].Data)
				require.NoError(t, err)
				require.NotEqual(t, [32]byte{0xAA}, state.State().DurableNonce)
			case "lookup extension":
				require.Contains(t, whole.delta, lifecycleTableKey)
				require.Len(t, whole.delta[lifecycleTableKey].Data, sealevel.AddressLookupTableMetaSize+4*32)
			case "vote commission":
				require.Contains(t, whole.delta, key)
				state, err := sealevel.UnmarshalVersionedVoteState(whole.delta[key].Data)
				require.NoError(t, err)
				require.Equal(t, byte(5), state.ConvertToCurrent().Commission)
			case "program upgrade", "program deployment":
				pk := solana.PublicKey{0xC2}
				if kind == "program deployment" {
					var err error
					pk, _, err = solana.FindProgramAddress([][]byte{key[:]}, addresses.BpfLoaderUpgradeableAddr)
					require.NoError(t, err)
					require.True(t, whole.delta[key].Executable)
				}
				require.Contains(t, whole.delta, pk)
				state, err := sealevel.UnmarshalUpgradeableLoaderState(whole.delta[pk].Data)
				require.NoError(t, err)
				require.Equal(t, lifecycleSlot, state.ProgramData.Slot)
			}
			for _, workers := range []int{1, 4} {
				for suffix := 0; suffix <= 3; suffix++ {
					t.Run(fmt.Sprintf("workers%d/suffix%d", workers, suffix), func(t *testing.T) {
						env, txs := setup()
						var splits []int
						for i := 1; i < 3-suffix; i++ {
							splits = append(splits, i)
						}
						got, _ := lifecycleStream(t, env, txs, splits, suffix, workers)
						requireSameLifecycleOutcome(t, whole, got)
					})
				}
			}
		})
	}
}

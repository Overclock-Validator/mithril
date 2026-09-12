package genesis

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/addresses"
	bin "github.com/gagliardetto/binary"
	"github.com/stretchr/testify/require"
)

func TestDeterministicGenesis(t *testing.T) {
	ctx := t.Context()
	c := multiValidatorConfig()
	g, resolved, err := Build(ctx, c)
	require.NoError(t, err)
	first, err := Encode(g)
	require.NoError(t, err)
	slices.Reverse(c.Accounts)
	slices.Reverse(c.Validators)
	c.CreationTime = "2027-04-05T01:07:08-05:00"
	again, resolvedAgain, err := Build(ctx, c)
	require.NoError(t, err)
	second, err := Encode(again)
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Equal(t, resolved, resolvedAgain)
	slices.Reverse(g.Accounts)
	third, err := Encode(g)
	require.NoError(t, err)
	require.Equal(t, first, third)
	var a, b bytes.Buffer
	require.NoError(t, WriteArchive(&a, first))
	require.NoError(t, WriteArchive(&b, second))
	require.Equal(t, a.Bytes(), b.Bytes())
	archive, _, err := ReadGenesisFromArchive(bytes.NewReader(a.Bytes()))
	require.NoError(t, err)
	roundtrip, err := Encode(archive)
	require.NoError(t, err)
	require.Equal(t, first, roundtrip)
	root := t.TempDir()
	for name, data := range map[string][]byte{"raw": first, "archive": a.Bytes()} {
		path := filepath.Join(root, name)
		require.NoError(t, os.WriteFile(path, data, 0644))
		read, _, err := ReadGenesisFromFile(path)
		require.NoError(t, err)
		result, err := Encode(read)
		require.NoError(t, err)
		require.Equal(t, first, result)
	}
}
func TestRejectInvalidConfig(t *testing.T) {
	tests := map[string]func(*Config){
		"profile": func(c *Config) { c.Profile = "untracked-latest" }, "missing time": func(c *Config) { c.CreationTime = "" }, "fractional time": func(c *Config) { c.CreationTime = "2026-01-01T00:00:00.5Z" }, "no validator": func(c *Config) { c.Validators = nil },
		"malformed key": func(c *Config) { c.Accounts[0].Address = "not a key" }, "zero key": func(c *Config) { c.Accounts[0].Address = addresses.SystemProgramAddrStr }, "duplicate": func(c *Config) { c.Accounts = append(c.Accounts, c.Accounts[0]) }, "validator conflict": func(c *Config) { c.Accounts[0].Address = c.Validators[0].Identity }, "builtin conflict": func(c *Config) { c.Accounts[0].Address = addresses.ZkTokenProofProgramAddrStr }, "feature conflict": func(c *Config) { c.Accounts[0].Address = ProfileFeatures()[0].Address },
		"bad bls": func(c *Config) { c.Validators[0].BLSPublicKey = "00" }, "infinity bls": func(c *Config) { c.Validators[0].BLSPublicKey = "c0" + strings.Repeat("00", 47) }, "not subgroup": func(c *Config) { c.Validators[0].BLSPublicKey = strings.Repeat("ab", 48) }, "commission": func(c *Config) { c.Validators[0].InflationCommissionBPS = 10001 }, "unfunded identity": func(c *Config) { c.Validators[0].IdentityLamports = 0 }, "insufficient vote": func(c *Config) { c.Validators[0].VoteLamports = 1 }, "insufficient stake": func(c *Config) { c.Validators[0].StakeLamports = 2282880 }, "overflow": func(c *Config) { c.Accounts[0].Lamports = math.MaxUint64 }, "zero allocation": func(c *Config) { c.Accounts[0].Lamports = 0 },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			c := testConfig()
			mutate(&c)
			_, _, err := Build(t.Context(), c)
			require.Error(t, err)
		})
	}
	c := multiValidatorConfig()
	c.Validators[1].BLSPublicKey = c.Validators[0].BLSPublicKey
	_, _, err := Build(t.Context(), c)
	require.ErrorContains(t, err, "duplicate")
	c = multiValidatorConfig()
	c.Validators[1].StakeLamports = c.Validators[0].StakeLamports
	_, _, err = Build(t.Context(), c)
	require.ErrorContains(t, err, "tie-break")
	_, err = ParseConfig(strings.NewReader("unknown_field = 1\n"))
	require.Error(t, err)
	_, err = ParseConfig(strings.NewReader("creation_time = '2026-01-01T00:00:00Z'\n[[accounts]]\nlamports = -1\n"))
	require.Error(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = Build(ctx, testConfig())
	require.ErrorIs(t, err, context.Canceled)
}
func TestRejectTamperedGenesis(t *testing.T) {
	for name, change := range map[string]func(*Genesis){
		"missing feature": func(g *Genesis) { g.Accounts = g.Accounts[1:] }, "extra feature": func(g *Genesis) { g.Accounts[1].Data = append(g.Accounts[1].Data, 0) }, "fee change": func(g *Genesis) { g.Fees.BurnPercent++ }, "epoch change": func(g *Genesis) { g.EpochSchedule.SlotPerEpoch++ }, "reward pool": func(g *Genesis) { g.RewardPools = append(g.RewardPools, g.Accounts[0]) }, "builtin": func(g *Genesis) { g.Builtins = append(g.Builtins, BuiltinProgram{Key: "override"}) }, "hash setting": func(g *Genesis) { g.PohParams.HasHashesPerTick = true; g.PohParams.HashesPerTick = 1 },
	} {
		t.Run(name, func(t *testing.T) {
			g, _, err := Build(t.Context(), testConfig())
			require.NoError(t, err)
			change(g)
			_, err = ConstructInitialBank(t.Context(), g)
			require.Error(t, err)
		})
	}
}

var errWriter = errors.New("injected write error")

type failWriter struct{ remaining int }

func (w *failWriter) Write(p []byte) (int, error) {
	if len(p) > w.remaining {
		return 0, errWriter
	}
	w.remaining -= len(p)
	return len(p), nil
}
func TestGenesisSerializationFailures(t *testing.T) {
	g, _, err := Build(t.Context(), testConfig())
	require.NoError(t, err)
	raw, err := Encode(g)
	require.NoError(t, err)
	for _, offset := range []int{0, 8, 16, 40, 48, len(raw) - 1} {
		err = g.MarshalWithEncoder(bin.NewBinEncoder(&failWriter{offset}))
		require.ErrorIs(t, err, errWriter)
	}
	// Option encodings, both legacy reserved words, and UTF-8 builtin names.
	g.PohParams.HasTickCount = true
	g.PohParams.TickCount = 123
	g.PohParams.HasHashesPerTick = true
	g.PohParams.HashesPerTick = 7
	g.Unused = 567
	g.Unused2 = 890
	g.Builtins = []BuiltinProgram{{Key: "builtin"}}
	raw, err = Encode(g)
	require.NoError(t, err)
	decoded, _, err := Decode(raw)
	require.NoError(t, err)
	again, err := Encode(decoded)
	require.NoError(t, err)
	require.Equal(t, raw, again)
	for _, length := range []int{0, 1, 8, 15, 100, len(raw) - 1} {
		_, _, err = Decode(raw[:length])
		require.Error(t, err)
	}
	_, _, err = Decode(append(bytes.Clone(raw), 0))
	require.ErrorContains(t, err, "trailing")
	hostile := bytes.Clone(raw)
	binary.LittleEndian.PutUint64(hostile[8:16], math.MaxUint64)
	_, _, err = Decode(hostile)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	g.CreationTime = g.CreationTime.Add(time.Nanosecond)
	_, err = Encode(g)
	require.Error(t, err)
}
func TestCreateRefusesExistingAndCancellation(t *testing.T) {
	dir := t.TempDir()
	_, err := CreateFiles(t.Context(), testConfig(), dir)
	require.ErrorContains(t, err, "already exist")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	path := filepath.Join(dir, "new")
	_, err = CreateFiles(ctx, testConfig(), path)
	require.ErrorIs(t, err, context.Canceled)
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err))
}

func TestRawTimestampCanResembleArchiveMagic(t *testing.T) {
	g, _, err := Build(t.Context(), testConfig())
	require.NoError(t, err)
	g.CreationTime = time.Unix(0x685a42, 0).UTC() // little-endian bytes spell BZh
	raw, err := Encode(g)
	require.NoError(t, err)
	require.True(t, bytes.HasPrefix(raw, []byte("BZh")))
	path := filepath.Join(t.TempDir(), "input")
	require.NoError(t, os.WriteFile(path, raw, 0644))
	read, _, err := ReadGenesisFromFile(path)
	require.NoError(t, err)
	require.Equal(t, g.CreationTime, read.CreationTime)
}

func TestArchiveIntegrity(t *testing.T) {
	g, _, err := Build(t.Context(), testConfig())
	require.NoError(t, err)
	raw, err := Encode(g)
	require.NoError(t, err)
	var compressed bytes.Buffer
	require.NoError(t, WriteArchive(&compressed, raw))
	for _, cut := range []int{1, 10, compressed.Len() / 2} {
		_, _, err := ReadGenesisFromArchive(bytes.NewReader(compressed.Bytes()[:compressed.Len()-cut]))
		require.Error(t, err, "truncated bzip2 stream")
	}
	makeTar := func(name string, typ byte, duplicate bool) []byte {
		var data bytes.Buffer
		tw := tar.NewWriter(&data)
		hdr := &tar.Header{Name: name, Typeflag: typ, Size: int64(len(raw)), Mode: 0644}
		if typ == tar.TypeSymlink {
			hdr.Size = 0
			hdr.Linkname = "outside"
		}
		require.NoError(t, tw.WriteHeader(hdr))
		if typ != tar.TypeSymlink {
			_, err := tw.Write(raw)
			require.NoError(t, err)
		}
		if duplicate {
			require.NoError(t, tw.WriteHeader(hdr))
			_, err := tw.Write(raw)
			require.NoError(t, err)
		}
		require.NoError(t, tw.Close())
		return data.Bytes()
	}
	for _, data := range [][]byte{makeTar("../genesis.bin", tar.TypeReg, false), makeTar("genesis.bin", tar.TypeSymlink, false), makeTar("genesis.bin", tar.TypeReg, true)} {
		_, _, err = ReadGenesisFromArchive(bytes.NewReader(data))
		require.Error(t, err)
	}
	valid := makeTar("genesis.bin", tar.TypeReg, false)
	_, _, err = ReadGenesisFromArchive(bytes.NewReader(valid))
	require.NoError(t, err)
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	_, err = zw.Write(valid)
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	_, _, err = ReadGenesisFromArchive(bytes.NewReader(gz.Bytes()))
	require.NoError(t, err)
	corrupt := bytes.Clone(gz.Bytes())
	corrupt[len(corrupt)-8] ^= 0xff // gzip CRC, after all tar data
	_, _, err = ReadGenesisFromArchive(bytes.NewReader(corrupt))
	require.Error(t, err)
	_, _, err = ReadGenesisFromArchive(bytes.NewReader(make([]byte, 10*1024*1024+1)))
	require.ErrorContains(t, err, "exceeds")
}

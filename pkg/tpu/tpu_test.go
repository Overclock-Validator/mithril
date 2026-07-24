package tpu

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/sigverifytelemetry"
)

const breakTx = `
01cd 381f 2c7a 02e7 8131 e2dd 7973 e950
0ee5 f641 e277 5214 2d40 a1d0 2680 7261
373a 360b 9bb0 c1b2 0527 d05f ded6 9854
15ce f10c 7e1e ecf7 9936 1f62 7391 a717
0c01 0001 030b e57d dad5 f720 6443 5af8
88f3 ea02 fb25 0872 708a ef6d 2af7 fb0d
b3a7 fd60 902d cb74 4542 57c0 11b2 f71d
b25c 7097 820a f6ba 23a8 581e f842 03c1
f458 161b 81a1 3135 a853 75d0 0a62 175a
a55b 90d5 b1b1 cd35 e740 0ff4 b648 ee90
d1cd 6134 3b79 7e54 aae3 3c97 7e48 8f09
2d48 a4b5 367e 88e1 6cc6 c3bb 8c66 22fa
4491 add8 6701 0201 0102 000d          
`

/*
(*solana.Transaction)(0xc0001efea0)({
 Signatures: ([]solana.Signature) (len=1 cap=1) {
  (solana.Signature) (len=64 cap=64) 64uYQJaaAbLinRXWH661uzDvdZaWFTrh8RiN7LKN18eGs2XcngX1NqRi5w8YcXzxPKodbfG1dD8XsyeNHHVhMb1g
 },
 Message: (solana.Message) {
  AccountKeys: ([]solana.PublicKey) (len=6 cap=8) {
   (solana.PublicKey) (len=32 cap=32) Gxwia5TTd63XbSMB9AhV5LtPQucGKnvjYzaUQ3iAH7GV,
   (solana.PublicKey) (len=32 cap=32) 2X122BRxKJGvjmcjQdJUouTFxKbtFLnfZWA3Uz6ST9sD,
   (solana.PublicKey) (len=32 cap=32) 4LUro5jaPaTurXK737QAxgJywdhABnFAMQkXX4ZyqqaZ,
   (solana.PublicKey) (len=32 cap=32) 9gpfTc4zsndJSdpnpXQbey16L5jW2GWcKeY3PLixqU4,
   (solana.PublicKey) (len=32 cap=32) 9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin,
   (solana.PublicKey) (len=32 cap=32) 11111111111111111111111111111111
  },
  Header: (solana.MessageHeader) {
   NumRequiredSignatures: (uint8) 1,
   NumReadonlySignedAccounts: (uint8) 0,
   NumReadonlyUnsignedAccounts: (uint8) 2
  },
  RecentBlockhash: (solana.Hash) (len=32 cap=32) EuMLgzXp8c467FynAUwSErE4EJrbRmds6SJA5vDH2s8b,
  Instructions: ([]solana.CompiledInstruction) (len=2 cap=2) {
   (solana.CompiledInstruction) {
    ProgramIDIndex: (uint16) 4,
    AccountCount: (bin.Varuint16) 5,
    DataLength: (bin.Varuint16) 7,
    Accounts: ([]uint16) (len=5 cap=8) {
     (uint16) 1,
     (uint16) 2,
     (uint16) 3,
     (uint16) 2,
     (uint16) 2
    },
    Data: (solana.Base58) (len=7 cap=7) 12VeXEVRR
   },
   (solana.CompiledInstruction) {
    ProgramIDIndex: (uint16) 5,
    AccountCount: (bin.Varuint16) 2,
    DataLength: (bin.Varuint16) 12,
    Accounts: ([]uint16) (len=2 cap=4) {
     (uint16) 0,
     (uint16) 0
    },
    Data: (solana.Base58) (len=12 cap=12) 3Bxs43NbWyF9ibYB
   }
  }
 }
})
*/

const tpuTx = `
01fd 740a 299c 8140 4158 2e9e d4cc 7c96
0c11 ead6 eefc 2681 ae9e df71 cb31 d2ca
608b 62a1 3555 f268 82fb 622c 9c60 132c
a362 d64d 6906 ed47 012d af73 f1b6 d805
0f01 0002 06ed 341d e66d 6788 6d10 97e4
ac2b cfd5 00c7 9411 872b 0f47 ed52 5786
ce23 f750 0a16 8b21 8f1e 3359 a2be 9952
02b4 d07f 1978 eecf e602 2fed dd83 3509
9f9e 25af 0031 9098 8c12 0f69 b745 18e1
5183 8682 b982 2fa5 7d3f 529d 87d2 0b05
0ceb c02b 1a02 39ac 5042 f7fd afc3 f269
19a7 9644 8de8 142c d2ee d20d c08e cf80
8b79 b09d 6985 0f2d 6e02 a47a f824 d09a
b69d c42d 70cb 28cb fa24 9fb7 ee57 b9d2
56c1 2762 ef00 0000 0000 0000 0000 0000
0000 0000 0000 0000 0000 0000 0000 0000
0000 0000 00ce 9121 49be dcfc c6d4 82a1
7a1a 6ae3 d823 35a8 57e4 d86e 2c3f b730
e519 cca5 a802 0405 0102 0302 0207 0003
0000 000b 0005 0200 000c 0200 0000 0f0b
0000 0000 0000
`

func parseHexdump(s string) []byte {
	s = strings.ReplaceAll(s, " ", "")
	s = strings.ReplaceAll(s, "\n", "")
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}

func TestParseTx(t *testing.T) {
	ts := []string{
		breakTx,
		tpuTx,
	}

	for _, s := range ts {
		b := parseHexdump(s)
		_, err := ParseTx(b)
		if err != nil {
			t.Error(err)
		}
	}
}

func BenchmarkParseTx(b *testing.B) {
	r := parseHexdump(tpuTx)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := ParseTx(r)
		if err != nil {
			b.Error(err)
		}
	}
}

func TestVerifyTxSig(t *testing.T) {
	tx, err := ParseTx(parseHexdump(tpuTx))
	if err != nil {
		panic(err)
	}

	if !VerifyTxSig(tx) {
		t.Error("tx verify failed")
	}
}

func TestVerifyTxSigTelemetryPreservesVerdict(t *testing.T) {
	tx, err := ParseTx(parseHexdump(tpuTx))
	if err != nil {
		t.Fatal(err)
	}
	sigverifytelemetry.Enable(8)
	t.Cleanup(sigverifytelemetry.Disable)

	if !VerifyTxSig(tx) || !VerifyTxSig(tx) {
		t.Fatal("telemetry changed a valid transaction verdict")
	}
	s := sigverifytelemetry.Current()
	wantSignatures := uint64(len(tx.Signatures) * 2)
	if s.Transactions != 2 || s.Signatures != wantSignatures || s.VerificationAttempts != wantSignatures || s.ExactDuplicateHits != uint64(len(tx.Signatures)) {
		t.Fatalf("telemetry snapshot = %+v", s)
	}
	for i, entry := range s.VerificationTrace {
		if entry.Outcome != sigverifytelemetry.VerificationOutcomeValid {
			t.Fatalf("trace[%d] outcome = %v", i, entry.Outcome)
		}
	}
}

func TestVerifyTxSigTelemetryRecordsInvalidOutcome(t *testing.T) {
	tx, err := ParseTx(parseHexdump(tpuTx))
	if err != nil {
		t.Fatal(err)
	}
	tx.Signatures[0][0] ^= 1
	sigverifytelemetry.Enable(8)
	t.Cleanup(sigverifytelemetry.Disable)

	if VerifyTxSig(tx) {
		t.Fatal("mutated signature verified")
	}
	trace := sigverifytelemetry.Current().VerificationTrace
	if len(trace) != 1 || trace[0].Outcome != sigverifytelemetry.VerificationOutcomeInvalid {
		t.Fatalf("invalid trace = %+v", trace)
	}
}

func BenchmarkVerifyTxSig(b *testing.B) {
	tx, err := ParseTx(parseHexdump(tpuTx))
	if err != nil {
		panic(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		VerifyTxSig(tx)
	}
}

func BenchmarkVerifyTxSigTelemetry(b *testing.B) {
	tx, err := ParseTx(parseHexdump(tpuTx))
	if err != nil {
		b.Fatal(err)
	}

	for _, tc := range []struct {
		name    string
		enabled bool
	}{
		{name: "disabled"},
		{name: "enabled", enabled: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			if tc.enabled {
				sigverifytelemetry.Enable(65536)
			} else {
				sigverifytelemetry.Disable()
			}
			defer sigverifytelemetry.Disable()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if !VerifyTxSig(tx) {
					b.Fatal("transaction failed verification")
				}
			}
		})
	}
}

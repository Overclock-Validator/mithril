package alpenglow

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/gagliardetto/solana-go"
)

func TestEncodeMessageVoteGolden(t *testing.T) {
	hash := testHashSeq(0x20)
	sig := testSignatureSeq(0x80)
	msg := NewVoteMessage(NewNotarizationVote(0x0102030405060708, hash), sig, 0xbeef)
	msg.ShredVersion = 0xcafe

	got, err := EncodeMessage(msg)
	if err != nil {
		t.Fatalf("EncodeMessage: %v", err)
	}

	want := readHexFixture(t, "testdata/agave_votor_vote_notarize.hex")

	if !bytes.Equal(got, want) {
		t.Fatalf("golden mismatch:\n got %x\nwant %x", got, want)
	}

	decoded, err := DecodeMessage(got)
	if err != nil {
		t.Fatalf("DecodeMessage: %v", err)
	}
	wantDecoded := msg
	wantVote := *msg.Vote
	wantVote.Rank = 0
	wantDecoded.Vote = &wantVote
	if !reflect.DeepEqual(decoded, wantDecoded) {
		t.Fatalf("roundtrip mismatch:\n got %#v\nwant %#v", decoded, wantDecoded)
	}
	if got[len(got)-2] != 0xfe || got[len(got)-1] != 0xca {
		t.Fatalf("shred version is not the final little-endian u16: %x", got[len(got)-2:])
	}
}

func TestEncodeMessageCertificateGolden(t *testing.T) {
	sig := testSignatureSeq(0x11)
	msg := NewCertificateMessage(Certificate{
		Type:      CertificateSkip,
		Slot:      0x1112131415161718,
		Signature: sig,
		Bitmap:    []byte{0xaa, 0xbb, 0xcc},
	})
	msg.ShredVersion = 0xcafe

	got, err := EncodeMessage(msg)
	if err != nil {
		t.Fatalf("EncodeMessage: %v", err)
	}

	want := readHexFixture(t, "testdata/agave_votor_certificate_skip.hex")

	if !bytes.Equal(got, want) {
		t.Fatalf("golden mismatch:\n got %x\nwant %x", got, want)
	}
	const bitmapLengthOffset = 1 + 1 + 8 + BLSSignatureSize
	if gotLen := binary.LittleEndian.Uint64(got[bitmapLengthOffset : bitmapLengthOffset+8]); gotLen != 3 {
		t.Fatalf("certificate bitmap wincode length = %d, want u64(3)", gotLen)
	}

	decoded, err := DecodeMessage(got)
	if err != nil {
		t.Fatalf("DecodeMessage: %v", err)
	}
	if !reflect.DeepEqual(decoded, msg) {
		t.Fatalf("roundtrip mismatch:\n got %#v\nwant %#v", decoded, msg)
	}
}

func TestVoteRoundTripsAllVariants(t *testing.T) {
	hash := testHashSeq(0x40)
	tests := []Vote{
		NewNotarizationVote(10, hash),
		NewFinalizationVote(11),
		NewSkipVote(12),
		NewNotarizationFallbackVote(13, hash),
		NewSkipFallbackVote(14),
		NewGenesisVote(15, hash),
	}

	for _, want := range tests {
		encoded, err := EncodeVote(want)
		if err != nil {
			t.Fatalf("%s encode: %v", want.Type, err)
		}
		got, err := DecodeVote(encoded)
		if err != nil {
			t.Fatalf("%s decode: %v", want.Type, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s roundtrip mismatch:\n got %#v\nwant %#v", want.Type, got, want)
		}
	}
}

func TestCertificateRoundTripsAllVariants(t *testing.T) {
	hash := testHashSeq(0x60)
	sig := testSignatureSeq(0x22)
	tests := []Certificate{
		{Type: CertificateFinalize, Slot: 20, Signature: sig, Bitmap: []byte{0x01}},
		{Type: CertificateFinalizeFast, Slot: 21, BlockHash: hash, Signature: sig, Bitmap: []byte{0x01, 0x02}},
		{Type: CertificateNotarize, Slot: 22, BlockHash: hash, Signature: sig, Bitmap: []byte{0x01, 0x02, 0x03}},
		{Type: CertificateNotarizeFallback, Slot: 23, BlockHash: hash, Signature: sig, Bitmap: []byte{0x04}},
		{Type: CertificateSkip, Slot: 24, Signature: sig, Bitmap: []byte{0x05}},
		{Type: CertificateGenesis, Slot: 25, BlockHash: hash, Signature: sig, Bitmap: []byte{0x06}},
	}

	for _, want := range tests {
		encoded, err := EncodeCertificate(want)
		if err != nil {
			t.Fatalf("%s encode: %v", want.Type, err)
		}
		got, err := DecodeCertificate(encoded)
		if err != nil {
			t.Fatalf("%s decode: %v", want.Type, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s roundtrip mismatch:\n got %#v\nwant %#v", want.Type, got, want)
		}
	}
}

func TestVersionedWireMessageRoundTripsAllVariants(t *testing.T) {
	hash := testHashSeq(0x30)
	sig := testSignatureSeq(0x70)
	messages := []Message{
		NewVoteMessage(NewNotarizationVote(10, hash), sig, 1),
		NewVoteMessage(NewFinalizationVote(11), sig, 2),
		NewVoteMessage(NewSkipVote(12), sig, 3),
		NewVoteMessage(NewNotarizationFallbackVote(13, hash), sig, 4),
		NewVoteMessage(NewSkipFallbackVote(14), sig, 5),
		NewVoteMessage(NewGenesisVote(15, hash), sig, 6),
		NewCertificateMessage(Certificate{Type: CertificateFinalize, Slot: 20, Signature: sig, Bitmap: []byte{1}}),
		NewCertificateMessage(Certificate{Type: CertificateFinalizeFast, Slot: 21, BlockHash: hash, Signature: sig, Bitmap: []byte{2}}),
		NewCertificateMessage(Certificate{Type: CertificateNotarize, Slot: 22, BlockHash: hash, Signature: sig, Bitmap: []byte{3}}),
		NewCertificateMessage(Certificate{Type: CertificateNotarizeFallback, Slot: 23, BlockHash: hash, Signature: sig, Bitmap: []byte{4}}),
		NewCertificateMessage(Certificate{Type: CertificateSkip, Slot: 24, Signature: sig, Bitmap: []byte{5}}),
		NewCertificateMessage(Certificate{Type: CertificateGenesis, Slot: 25, BlockHash: hash, Signature: sig, Bitmap: []byte{6}}),
	}
	for i := range messages {
		messages[i].ShredVersion = 10638
		encoded, err := EncodeMessage(messages[i])
		if err != nil {
			t.Fatalf("message %d encode: %v", i, err)
		}
		decoded, err := DecodeMessage(encoded)
		if err != nil {
			t.Fatalf("message %d decode: %v", i, err)
		}
		want := messages[i]
		if want.Vote != nil {
			wantVote := *want.Vote
			wantVote.Rank = 0
			want.Vote = &wantVote
		}
		if !reflect.DeepEqual(decoded, want) {
			t.Fatalf("message %d roundtrip mismatch:\n got %#v\nwant %#v", i, decoded, want)
		}
	}
}

func TestVoteMessageWireOmitsRank(t *testing.T) {
	sig := testSignatureSeq(0x55)
	withRank := NewVoteMessage(NewSkipVote(99), sig, 0xbeef)
	withoutRank := NewVoteMessage(NewSkipVote(99), sig, 0)
	withRank.ShredVersion = 0xcafe
	withoutRank.ShredVersion = 0xcafe

	gotWithRank, err := EncodeMessage(withRank)
	if err != nil {
		t.Fatalf("EncodeMessage with rank: %v", err)
	}
	gotWithoutRank, err := EncodeMessage(withoutRank)
	if err != nil {
		t.Fatalf("EncodeMessage without rank: %v", err)
	}
	if !bytes.Equal(gotWithRank, gotWithoutRank) {
		t.Fatalf("rank changed v4.3 wire bytes:\nwith    %x\nwithout %x", gotWithRank, gotWithoutRank)
	}

	decoded, err := DecodeMessage(gotWithRank)
	if err != nil {
		t.Fatalf("DecodeMessage: %v", err)
	}
	if decoded.Vote == nil || decoded.Vote.Rank != 0 {
		t.Fatalf("decoded wire rank = %v, want unset rank 0", decoded.Vote)
	}

	bareWithRank, err := EncodeVoteMessage(*withRank.Vote)
	if err != nil {
		t.Fatalf("EncodeVoteMessage with rank: %v", err)
	}
	bareWithoutRank, err := EncodeVoteMessage(*withoutRank.Vote)
	if err != nil {
		t.Fatalf("EncodeVoteMessage without rank: %v", err)
	}
	if !bytes.Equal(bareWithRank, bareWithoutRank) {
		t.Fatalf("rank changed bare vote-message bytes:\nwith    %x\nwithout %x", bareWithRank, bareWithoutRank)
	}
	bareDecoded, err := DecodeVoteMessage(bareWithRank)
	if err != nil {
		t.Fatalf("DecodeVoteMessage: %v", err)
	}
	if bareDecoded.Rank != 0 {
		t.Fatalf("decoded bare rank = %d, want unset rank 0", bareDecoded.Rank)
	}
}

func TestDecodeMessageRejectsLegacyRankBearingVote(t *testing.T) {
	v43 := readHexFixture(t, "testdata/agave_votor_vote_notarize.hex")
	legacy := make([]byte, 0, len(v43)+2)
	legacy = append(legacy, v43[:len(v43)-2]...)
	legacy = append(legacy, 0xef, 0xbe) // legacy advertised rank 0xbeef
	legacy = append(legacy, v43[len(v43)-2:]...)

	_, err := DecodeMessage(legacy)
	if err == nil {
		t.Fatal("expected legacy rank-bearing Votor vote to be rejected")
	}
	if !strings.Contains(err.Error(), "trailing bytes") {
		t.Fatalf("legacy vote error = %v, want trailing-byte rejection", err)
	}
}

func TestCodecRejectsInvalidInputs(t *testing.T) {
	if _, err := DecodeMessage([]byte{2}); err == nil {
		t.Fatalf("expected invalid wire version error")
	}
	if _, err := DecodeVote([]byte{
		0x99, 0x00, 0x00, 0x00,
		0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}); err == nil {
		t.Fatalf("expected invalid vote tag error")
	}

	if _, err := EncodeVoteMessage(VoteMessage{
		Vote:      NewFinalizationVote(1),
		Signature: []byte{0x01},
	}); err == nil {
		t.Fatalf("expected invalid signature length error")
	}

	encoded, err := EncodeMessage(NewVoteMessage(NewFinalizationVote(1), testSignatureSeq(0x33), 0))
	if err != nil {
		t.Fatalf("EncodeMessage: %v", err)
	}
	if _, err := DecodeMessage(append(encoded, 0xff)); err == nil {
		t.Fatalf("expected trailing byte error")
	}

	certBytes, err := EncodeCertificate(Certificate{
		Type:      CertificateSkip,
		Slot:      1,
		Signature: testSignatureSeq(0x44),
		Bitmap:    []byte{0x01, 0x02},
	})
	if err != nil {
		t.Fatalf("EncodeCertificate: %v", err)
	}
	if _, err := DecodeCertificateWithOptions(certBytes, DecodeOptions{MaxBitmapSize: 1}); err == nil {
		t.Fatalf("expected bitmap limit error")
	}
}

func testHashSeq(start byte) solana.Hash {
	var h solana.Hash
	for i := range h {
		h[i] = start + byte(i)
	}
	return h
}

func readHexFixture(t *testing.T, path string) []byte {
	t.Helper()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture %s: %v", path, err)
	}
	out, err := hex.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatalf("decode fixture %s: %v", path, err)
	}
	return out
}

func testSignatureSeq(start byte) []byte {
	sig := make([]byte, BLSSignatureSize)
	for i := range sig {
		sig[i] = start + byte(i)
	}
	return sig
}

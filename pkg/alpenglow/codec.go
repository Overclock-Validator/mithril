package alpenglow

import (
	"fmt"

	"github.com/gagliardetto/solana-go"

	"github.com/Overclock-Validator/mithril/pkg/wincode"
)

const (
	BLSSignatureSize     = 192
	DefaultMaxBitmapSize = 1 << 20

	hashSize = 32
)

const votorWireVersionV1 uint8 = 1

const (
	votorWireKindNotarVote         uint8 = 1
	votorWireKindFinalizeVote      uint8 = 2
	votorWireKindSkipVote          uint8 = 3
	votorWireKindNotarFallbackVote uint8 = 4
	votorWireKindSkipFallbackVote  uint8 = 5
	votorWireKindGenesisVote       uint8 = 6
	votorWireKindFinalizeCert      uint8 = 7
	votorWireKindFastFinalizeCert  uint8 = 8
	votorWireKindNotarCert         uint8 = 9
	votorWireKindNotarFallbackCert uint8 = 10
	votorWireKindSkipCert          uint8 = 11
	votorWireKindGenesisCert       uint8 = 12
)

const (
	votePayloadTagNotarize         uint8 = 1
	votePayloadTagFinalize         uint8 = 2
	votePayloadTagSkip             uint8 = 3
	votePayloadTagNotarizeFallback uint8 = 4
	votePayloadTagSkipFallback     uint8 = 5
	votePayloadTagGenesis          uint8 = 6

	votorVoteTagNotarize         uint32 = 0
	votorVoteTagFinalize         uint32 = 1
	votorVoteTagSkip             uint32 = 2
	votorVoteTagNotarizeFallback uint32 = 3
	votorVoteTagSkipFallback     uint32 = 4
	votorVoteTagGenesis          uint32 = 5
)

const (
	votorCertificateTagFinalize         uint32 = 0
	votorCertificateTagFinalizeFast     uint32 = 1
	votorCertificateTagNotarize         uint32 = 2
	votorCertificateTagNotarizeFallback uint32 = 3
	votorCertificateTagSkip             uint32 = 4
	votorCertificateTagGenesis          uint32 = 5
)

type DecodeOptions struct {
	MaxBitmapSize uint64
}

func DefaultDecodeOptions() DecodeOptions {
	return DecodeOptions{MaxBitmapSize: DefaultMaxBitmapSize}
}

func EncodeMessage(msg Message) ([]byte, error) {
	if err := msg.ValidateBasic(); err != nil {
		return nil, err
	}

	w := wincode.NewWriter(256)
	w.WriteU8(votorWireVersionV1)
	switch {
	case msg.Vote != nil:
		if err := encodeWireVoteMessageInto(w, *msg.Vote); err != nil {
			return nil, err
		}
	case msg.Certificate != nil:
		if err := encodeWireCertificateInto(w, *msg.Certificate); err != nil {
			return nil, err
		}
	}
	w.WriteU16(msg.ShredVersion)
	return w.Bytes(), nil
}

func DecodeMessage(data []byte) (Message, error) {
	return DecodeMessageWithOptions(data, DefaultDecodeOptions())
}

func DecodeMessageWithOptions(data []byte, opts DecodeOptions) (Message, error) {
	opts = normalizeDecodeOptions(opts)
	r := wincode.NewReader(data)
	version, err := r.ReadU8()
	if err != nil {
		return Message{}, err
	}
	if version != votorWireVersionV1 {
		return Message{}, fmt.Errorf("alpenglow: unknown Votor wire version %d", version)
	}

	msg, err := decodeWireMessageV1From(r, opts)
	if err != nil {
		return Message{}, err
	}
	msg.ShredVersion, err = r.ReadU16()
	if err != nil {
		return Message{}, err
	}

	if err := r.EnsureEOF(); err != nil {
		return Message{}, err
	}
	if err := msg.ValidateBasic(); err != nil {
		return Message{}, err
	}
	return msg, nil
}

func encodeWireVoteMessageInto(w *wincode.Writer, msg VoteMessage) error {
	var kind uint8
	switch msg.Vote.Type {
	case VoteTypeNotarize:
		kind = votorWireKindNotarVote
	case VoteTypeFinalize:
		kind = votorWireKindFinalizeVote
	case VoteTypeSkip:
		kind = votorWireKindSkipVote
	case VoteTypeNotarizeFallback:
		kind = votorWireKindNotarFallbackVote
	case VoteTypeSkipFallback:
		kind = votorWireKindSkipFallbackVote
	case VoteTypeGenesis:
		kind = votorWireKindGenesisVote
	default:
		return fmt.Errorf("alpenglow: invalid vote type %q", msg.Vote.Type)
	}
	w.WriteU8(kind)
	w.WriteU64(msg.Vote.Slot)
	if msg.Vote.Type == VoteTypeNotarize || msg.Vote.Type == VoteTypeNotarizeFallback || msg.Vote.Type == VoteTypeGenesis {
		writeHash(w, msg.Vote.BlockHash)
	}
	if err := w.WriteFixedBytes("vote signature", msg.Signature, BLSSignatureSize); err != nil {
		return err
	}
	return nil
}

func encodeWireCertificateInto(w *wincode.Writer, cert Certificate) error {
	var kind uint8
	switch cert.Type {
	case CertificateFinalize:
		kind = votorWireKindFinalizeCert
	case CertificateFinalizeFast:
		kind = votorWireKindFastFinalizeCert
	case CertificateNotarize:
		kind = votorWireKindNotarCert
	case CertificateNotarizeFallback:
		kind = votorWireKindNotarFallbackCert
	case CertificateSkip:
		kind = votorWireKindSkipCert
	case CertificateGenesis:
		kind = votorWireKindGenesisCert
	default:
		return fmt.Errorf("alpenglow: invalid certificate type %q", cert.Type)
	}
	w.WriteU8(kind)
	w.WriteU64(cert.Slot)
	if cert.Type == CertificateFinalizeFast || cert.Type == CertificateNotarize || cert.Type == CertificateNotarizeFallback || cert.Type == CertificateGenesis {
		writeHash(w, cert.BlockHash)
	}
	if err := w.WriteFixedBytes("certificate signature", cert.Signature, BLSSignatureSize); err != nil {
		return err
	}
	w.WriteByteVec(cert.Bitmap)
	return nil
}

func decodeWireMessageV1From(r *wincode.Reader, opts DecodeOptions) (Message, error) {
	kind, err := r.ReadU8()
	if err != nil {
		return Message{}, err
	}
	slot, err := r.ReadU64()
	if err != nil {
		return Message{}, err
	}

	var hash solana.Hash
	switch kind {
	case votorWireKindNotarVote, votorWireKindNotarFallbackVote, votorWireKindGenesisVote,
		votorWireKindFastFinalizeCert, votorWireKindNotarCert, votorWireKindNotarFallbackCert, votorWireKindGenesisCert:
		hash, err = readHash(r)
		if err != nil {
			return Message{}, err
		}
	}

	switch kind {
	case votorWireKindNotarVote, votorWireKindFinalizeVote, votorWireKindSkipVote,
		votorWireKindNotarFallbackVote, votorWireKindSkipFallbackVote, votorWireKindGenesisVote:
		signature, err := r.ReadBytes(BLSSignatureSize)
		if err != nil {
			return Message{}, err
		}
		var vote Vote
		switch kind {
		case votorWireKindNotarVote:
			vote = NewNotarizationVote(slot, hash)
		case votorWireKindFinalizeVote:
			vote = NewFinalizationVote(slot)
		case votorWireKindSkipVote:
			vote = NewSkipVote(slot)
		case votorWireKindNotarFallbackVote:
			vote = NewNotarizationFallbackVote(slot, hash)
		case votorWireKindSkipFallbackVote:
			vote = NewSkipFallbackVote(slot)
		case votorWireKindGenesisVote:
			vote = NewGenesisVote(slot, hash)
		}
		// Agave v4.3 authenticates the sender at the Votor transport layer and
		// derives rank from that identity. Rank is deliberately absent from the
		// wire message and is injected by the receiver after decoding.
		msg := VoteMessage{Vote: vote, Signature: signature}
		return Message{Vote: &msg}, nil

	case votorWireKindFinalizeCert, votorWireKindFastFinalizeCert, votorWireKindNotarCert,
		votorWireKindNotarFallbackCert, votorWireKindSkipCert, votorWireKindGenesisCert:
		signature, err := r.ReadBytes(BLSSignatureSize)
		if err != nil {
			return Message{}, err
		}
		bitmap, err := r.ReadByteVec(opts.MaxBitmapSize)
		if err != nil {
			return Message{}, err
		}
		cert := Certificate{Slot: slot, BlockHash: hash, Signature: signature, Bitmap: bitmap}
		switch kind {
		case votorWireKindFinalizeCert:
			cert.Type = CertificateFinalize
		case votorWireKindFastFinalizeCert:
			cert.Type = CertificateFinalizeFast
		case votorWireKindNotarCert:
			cert.Type = CertificateNotarize
		case votorWireKindNotarFallbackCert:
			cert.Type = CertificateNotarizeFallback
		case votorWireKindSkipCert:
			cert.Type = CertificateSkip
		case votorWireKindGenesisCert:
			cert.Type = CertificateGenesis
		}
		return Message{Certificate: &cert}, nil
	default:
		return Message{}, fmt.Errorf("alpenglow: unknown Votor wire message kind %d", kind)
	}
}

func EncodeVote(vote Vote) ([]byte, error) {
	if err := vote.ValidateBasic(); err != nil {
		return nil, err
	}
	w := wincode.NewWriter(48)
	if err := encodeVoteInto(w, vote); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

// EncodeVotePayloadToSign matches Agave's compact BLS vote-signing payload.
func EncodeVotePayloadToSign(vote Vote, shredVersion uint16) ([]byte, error) {
	if err := vote.ValidateBasic(); err != nil {
		return nil, err
	}
	w := wincode.NewWriter(48)
	if err := encodeVotePayloadToSignInto(w, vote, shredVersion); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

func DecodeVote(data []byte) (Vote, error) {
	r := wincode.NewReader(data)
	vote, err := decodeVoteFrom(r)
	if err != nil {
		return Vote{}, err
	}
	if err := r.EnsureEOF(); err != nil {
		return Vote{}, err
	}
	return vote, vote.ValidateBasic()
}

func EncodeVoteMessage(msg VoteMessage) ([]byte, error) {
	if err := msg.ValidateBasic(); err != nil {
		return nil, err
	}
	w := wincode.NewWriter(256)
	if err := encodeVoteMessageInto(w, msg); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

func DecodeVoteMessage(data []byte) (VoteMessage, error) {
	r := wincode.NewReader(data)
	msg, err := decodeVoteMessageFrom(r)
	if err != nil {
		return VoteMessage{}, err
	}
	if err := r.EnsureEOF(); err != nil {
		return VoteMessage{}, err
	}
	return msg, msg.ValidateBasic()
}

func EncodeCertificate(cert Certificate) ([]byte, error) {
	if err := cert.ValidateBasic(); err != nil {
		return nil, err
	}
	w := wincode.NewWriter(256 + len(cert.Bitmap))
	if err := encodeCertificateInto(w, cert); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

func DecodeCertificate(data []byte) (Certificate, error) {
	return DecodeCertificateWithOptions(data, DefaultDecodeOptions())
}

func DecodeCertificateWithOptions(data []byte, opts DecodeOptions) (Certificate, error) {
	opts = normalizeDecodeOptions(opts)
	r := wincode.NewReader(data)
	cert, err := decodeCertificateFrom(r, opts)
	if err != nil {
		return Certificate{}, err
	}
	if err := r.EnsureEOF(); err != nil {
		return Certificate{}, err
	}
	return cert, cert.ValidateBasic()
}

func encodeVoteMessageInto(w *wincode.Writer, msg VoteMessage) error {
	if err := encodeVoteInto(w, msg.Vote); err != nil {
		return err
	}
	if err := w.WriteFixedBytes("vote signature", msg.Signature, BLSSignatureSize); err != nil {
		return err
	}
	return nil
}

func decodeVoteMessageFrom(r *wincode.Reader) (VoteMessage, error) {
	vote, err := decodeVoteFrom(r)
	if err != nil {
		return VoteMessage{}, err
	}
	signature, err := r.ReadBytes(BLSSignatureSize)
	if err != nil {
		return VoteMessage{}, err
	}
	return VoteMessage{Vote: vote, Signature: signature}, nil
}

func encodeVotePayloadToSignInto(w *wincode.Writer, vote Vote, shredVersion uint16) error {
	switch vote.Type {
	case VoteTypeNotarize:
		w.WriteBytes([]byte{votePayloadTagNotarize})
		w.WriteU64(vote.Slot)
		writeHash(w, vote.BlockHash)
	case VoteTypeFinalize:
		w.WriteBytes([]byte{votePayloadTagFinalize})
		w.WriteU64(vote.Slot)
	case VoteTypeSkip:
		w.WriteBytes([]byte{votePayloadTagSkip})
		w.WriteU64(vote.Slot)
	case VoteTypeNotarizeFallback:
		w.WriteBytes([]byte{votePayloadTagNotarizeFallback})
		w.WriteU64(vote.Slot)
		writeHash(w, vote.BlockHash)
	case VoteTypeSkipFallback:
		w.WriteBytes([]byte{votePayloadTagSkipFallback})
		w.WriteU64(vote.Slot)
	case VoteTypeGenesis:
		w.WriteBytes([]byte{votePayloadTagGenesis})
		w.WriteU64(vote.Slot)
		writeHash(w, vote.BlockHash)
	default:
		return fmt.Errorf("alpenglow: invalid vote type %q", vote.Type)
	}
	w.WriteU16(shredVersion)
	return nil
}

func encodeVoteInto(w *wincode.Writer, vote Vote) error {
	switch vote.Type {
	case VoteTypeNotarize:
		w.WriteU32(votorVoteTagNotarize)
		w.WriteU64(vote.Slot)
		writeHash(w, vote.BlockHash)
	case VoteTypeFinalize:
		w.WriteU32(votorVoteTagFinalize)
		w.WriteU64(vote.Slot)
	case VoteTypeSkip:
		w.WriteU32(votorVoteTagSkip)
		w.WriteU64(vote.Slot)
	case VoteTypeNotarizeFallback:
		w.WriteU32(votorVoteTagNotarizeFallback)
		w.WriteU64(vote.Slot)
		writeHash(w, vote.BlockHash)
	case VoteTypeSkipFallback:
		w.WriteU32(votorVoteTagSkipFallback)
		w.WriteU64(vote.Slot)
	case VoteTypeGenesis:
		w.WriteU32(votorVoteTagGenesis)
		w.WriteU64(vote.Slot)
		writeHash(w, vote.BlockHash)
	default:
		return fmt.Errorf("alpenglow: invalid vote type %q", vote.Type)
	}
	return nil
}

func decodeVoteFrom(r *wincode.Reader) (Vote, error) {
	tag, err := r.ReadU32()
	if err != nil {
		return Vote{}, err
	}

	slot, err := r.ReadU64()
	if err != nil {
		return Vote{}, err
	}

	switch tag {
	case votorVoteTagNotarize:
		hash, err := readHash(r)
		if err != nil {
			return Vote{}, err
		}
		return NewNotarizationVote(slot, hash), nil
	case votorVoteTagFinalize:
		return NewFinalizationVote(slot), nil
	case votorVoteTagSkip:
		return NewSkipVote(slot), nil
	case votorVoteTagNotarizeFallback:
		hash, err := readHash(r)
		if err != nil {
			return Vote{}, err
		}
		return NewNotarizationFallbackVote(slot, hash), nil
	case votorVoteTagSkipFallback:
		return NewSkipFallbackVote(slot), nil
	case votorVoteTagGenesis:
		hash, err := readHash(r)
		if err != nil {
			return Vote{}, err
		}
		return NewGenesisVote(slot, hash), nil
	default:
		return Vote{}, fmt.Errorf("alpenglow: unknown Votor vote tag %d", tag)
	}
}

func encodeCertificateInto(w *wincode.Writer, cert Certificate) error {
	switch cert.Type {
	case CertificateFinalize:
		w.WriteU32(votorCertificateTagFinalize)
		w.WriteU64(cert.Slot)
	case CertificateFinalizeFast:
		w.WriteU32(votorCertificateTagFinalizeFast)
		w.WriteU64(cert.Slot)
		writeHash(w, cert.BlockHash)
	case CertificateNotarize:
		w.WriteU32(votorCertificateTagNotarize)
		w.WriteU64(cert.Slot)
		writeHash(w, cert.BlockHash)
	case CertificateNotarizeFallback:
		w.WriteU32(votorCertificateTagNotarizeFallback)
		w.WriteU64(cert.Slot)
		writeHash(w, cert.BlockHash)
	case CertificateSkip:
		w.WriteU32(votorCertificateTagSkip)
		w.WriteU64(cert.Slot)
	case CertificateGenesis:
		w.WriteU32(votorCertificateTagGenesis)
		w.WriteU64(cert.Slot)
		writeHash(w, cert.BlockHash)
	default:
		return fmt.Errorf("alpenglow: invalid certificate type %q", cert.Type)
	}

	if err := w.WriteFixedBytes("certificate signature", cert.Signature, BLSSignatureSize); err != nil {
		return err
	}
	w.WriteByteVec(cert.Bitmap)
	return nil
}

func decodeCertificateFrom(r *wincode.Reader, opts DecodeOptions) (Certificate, error) {
	tag, err := r.ReadU32()
	if err != nil {
		return Certificate{}, err
	}

	slot, err := r.ReadU64()
	if err != nil {
		return Certificate{}, err
	}

	cert := Certificate{Slot: slot}
	switch tag {
	case votorCertificateTagFinalize:
		cert.Type = CertificateFinalize
	case votorCertificateTagFinalizeFast:
		cert.Type = CertificateFinalizeFast
		hash, err := readHash(r)
		if err != nil {
			return Certificate{}, err
		}
		cert.BlockHash = hash
	case votorCertificateTagNotarize:
		cert.Type = CertificateNotarize
		hash, err := readHash(r)
		if err != nil {
			return Certificate{}, err
		}
		cert.BlockHash = hash
	case votorCertificateTagNotarizeFallback:
		cert.Type = CertificateNotarizeFallback
		hash, err := readHash(r)
		if err != nil {
			return Certificate{}, err
		}
		cert.BlockHash = hash
	case votorCertificateTagSkip:
		cert.Type = CertificateSkip
	case votorCertificateTagGenesis:
		cert.Type = CertificateGenesis
		hash, err := readHash(r)
		if err != nil {
			return Certificate{}, err
		}
		cert.BlockHash = hash
	default:
		return Certificate{}, fmt.Errorf("alpenglow: unknown Votor certificate tag %d", tag)
	}

	cert.Signature, err = r.ReadBytes(BLSSignatureSize)
	if err != nil {
		return Certificate{}, err
	}
	cert.Bitmap, err = r.ReadByteVec(opts.MaxBitmapSize)
	if err != nil {
		return Certificate{}, err
	}
	return cert, nil
}

func writeHash(w *wincode.Writer, hash solana.Hash) {
	w.WriteBytes(hash[:])
}

func readHash(r *wincode.Reader) (solana.Hash, error) {
	raw, err := r.ReadBytes(hashSize)
	if err != nil {
		return solana.Hash{}, err
	}
	var hash solana.Hash
	copy(hash[:], raw)
	return hash, nil
}

func normalizeDecodeOptions(opts DecodeOptions) DecodeOptions {
	if opts.MaxBitmapSize == 0 {
		opts.MaxBitmapSize = DefaultMaxBitmapSize
	}
	return opts
}

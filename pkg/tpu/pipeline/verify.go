package pipeline

import (
	"github.com/Overclock-Validator/mithril/pkg/sigverifytelemetry"
	"github.com/Overclock-Validator/mithril/pkg/tpu/packet"
	"github.com/Overclock-Validator/mithril/pkg/tpu/sigverify"
	"github.com/gagliardetto/solana-go"
)

func verifyPacket(data []byte) bool {
	return sigverify.VerifyPacket(data)
}

type telemetrySigverifyPacket struct {
	packet      packet.Packet
	transaction *solana.Transaction
	signatures  uint16
}

// prepareTelemetrySigverifyPacket parses a claimed profiling packet once and
// mirrors VerifyTransaction's structural gates for its signature-lane count.
// Keeping the parsed transaction avoids distorting the profiling path with a
// second parse; ordinary packet processing remains unchanged.
func prepareTelemetrySigverifyPacket(pkt packet.Packet) telemetrySigverifyPacket {
	prepared := telemetrySigverifyPacket{packet: pkt}
	tx, err := sigverify.ParseTx(pkt.Data())
	if err != nil {
		return prepared
	}
	prepared.transaction = tx
	required := int(tx.Message.Header.NumRequiredSignatures)
	if required == 0 || len(tx.Signatures) != required || required > len(tx.Message.AccountKeys) {
		return prepared
	}
	if required > int(^uint16(0)) {
		prepared.signatures = ^uint16(0)
		return prepared
	}
	prepared.signatures = uint16(required)
	return prepared
}

func verifyTelemetrySigverifyPacket(prepared telemetrySigverifyPacket, dispatch sigverifytelemetry.DispatchJob) bool {
	return prepared.transaction != nil && sigverify.VerifyTransactionInDispatch(prepared.transaction, dispatch)
}

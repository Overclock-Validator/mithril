package pipeline

import tpusigverify "github.com/Overclock-Validator/mithril/pkg/tpu/sigverify"

// batchVerifier keeps the pipeline's dependency on the TPU sigverify package
// confined to this file, so pipeline.go can use the unqualified name for
// Mithril's shared verification package.
type batchVerifier = tpusigverify.BatchVerifier

func verifyPacket(data []byte) bool {
	return tpusigverify.VerifyPacket(data)
}

func verifyPacketWithTxV1(data []byte, enabled func() bool) bool {
	return tpusigverify.VerifyPacketWithTxV1(data, enabled)
}

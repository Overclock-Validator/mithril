package alpenglow

import (
	"fmt"
	"testing"
)

func BenchmarkObserverVoteWithRetainedCertificates(b *testing.B) {
	o := NewObserver()
	for slot := uint64(1); slot <= DefaultMaxTrackedCertificates; slot++ {
		_, err := o.ObserveCertificate(Certificate{Type: CertificateNotarize, Slot: slot,
			BlockHash: testHash(1), IncludedStake: 80, TotalStake: 100})
		if err != nil {
			b.Fatal(err)
		}
	}
	msg := VoteMessage{Vote: NewSkipVote(5000), Rank: 1}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := o.ObserveVote(msg); err != nil {
			b.Fatal(err)
		}
	}
}

// Model a full diagnostic history, with a small unresolved frontier or an
// entirely unresolved history as a worst case. These are observer-only costs:
// no transactions, BLS verification, or network traffic are included.
func BenchmarkObserverEmptyReplay(b *testing.B) {
	for _, pending := range []int{0, 32, DefaultMaxTrackedCertificates} {
		for _, skips := range []int{0, 4} {
			b.Run(fmt.Sprintf("pending=%d/skips=%d", pending, skips), func(b *testing.B) {
				o := NewObserver()
				for slot := uint64(1); slot <= DefaultMaxTrackedCertificates; slot++ {
					if slot <= uint64(DefaultMaxTrackedCertificates-pending) {
						o.ObserveReplayBlock(ReplayBlockObservation{Block: BlockID{Slot: slot, Hash: testHash(1)}})
					}
					_, err := o.ObserveCertificate(Certificate{Type: CertificateNotarize, Slot: slot,
						BlockHash: testHash(1), IncludedStake: 80, TotalStake: 100})
					if err != nil {
						b.Fatal(err)
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					base := uint64(DefaultMaxTrackedCertificates + 1 + i*(skips+1))
					for n := 0; n < skips; n++ {
						o.ObserveReplayBlock(ReplayBlockObservation{Block: BlockID{Slot: base + uint64(n)}})
					}
					o.ObserveReplayBlock(ReplayBlockObservation{Block: BlockID{Slot: base + uint64(skips), Hash: testHash(1)}})
				}
			})
		}
	}
}

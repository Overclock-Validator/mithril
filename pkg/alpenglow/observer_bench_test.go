package alpenglow

import "testing"

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

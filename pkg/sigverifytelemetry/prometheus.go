package sigverifytelemetry

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	metricCapacity = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "mithril_sigverify_telemetry_capacity",
		Help: "Bounded exact-history capacity; zero means workload telemetry is disabled.",
	})
	metricTransactions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mithril_sigverify_workload_transactions_total",
		Help: "Transactions presented to signature verification while workload telemetry is enabled.",
	}, []string{"source"})
	metricSignatures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mithril_sigverify_workload_signatures_total",
		Help: "Signatures presented to verification while workload telemetry is enabled.",
	}, []string{"source"})
	metricVerificationAttempts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mithril_sigverify_workload_verification_attempts_total",
		Help: "Individual signature verification attempts actually reached while workload telemetry is enabled.",
	}, []string{"source"})
	metricSignaturesPerTransaction = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "mithril_sigverify_signatures_per_transaction",
		Help:    "Number of signatures in each transaction presented to verification.",
		Buckets: prometheus.LinearBuckets(1, 1, 17),
	}, []string{"source"})
	metricMessageBytes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "mithril_sigverify_message_bytes",
		Help:    "Signed message bytes per transaction.",
		Buckets: []float64{0, 64, 128, 200, 256, 512, 1024, 1232},
	}, []string{"source"})
	metricExactDuplicateHits = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mithril_sigverify_exact_duplicate_hits_total",
		Help: "Verification attempts exactly equal to a recent process-wide public-key/signature/message tuple; source is the current verifier.",
	}, []string{"source"})
	metricKeyReuseHits = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mithril_sigverify_public_key_reuse_hits_total",
		Help: "Verification attempts whose public key occurred in the bounded process-wide history; source is the current verifier.",
	}, []string{"source"})
	metricKeyReuseDistance = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "mithril_sigverify_public_key_reuse_distance",
		Help:    "Verification attempts between uses of the same public key.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 18),
	}, []string{"source"})
	metricQueueOccupancy = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "mithril_sigverify_queue_occupancy",
		Help:    "Sigverify queue depth at worker claims, separated by passive observation or scheduling simulation.",
		Buckets: append([]float64{0}, prometheus.ExponentialBuckets(1, 2, 14)...),
	}, []string{"source", "collection_mode"})
	metricNaturalBatchWidth = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "mithril_sigverify_natural_batch_width",
		Help: "Signature lanes in explicit nonblocking scheduling-simulation claims; passive dispatches are excluded.",
		Buckets: []float64{
			1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 32, 64,
		},
	}, []string{"source", "collection_mode"})
)

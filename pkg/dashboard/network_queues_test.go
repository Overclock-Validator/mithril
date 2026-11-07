package dashboard

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestNodeNetworkQueuesContract(t *testing.T) {
	now := time.Now()
	snapshot, capture := nodeNetworkSourceFixture(now)
	d := capture.Detail
	d.Receiver, d.Interfaces = nil, nil
	d.ReceiverReason, d.InterfacesReason = "waiting_source", "not_provided"
	d.TPUQueues = &TPUQueuesDetail{Scope: "tpu_pipeline_queue_samples", Generation: 7,
		StartedAt: now.Add(-time.Second).UTC(), SampledAt: now.UTC(),
		Ingress: TPUQueueGauge{Capacity: 4096}, DedupOutput: TPUQueueGauge{Length: 1<<53 + 1, Capacity: ^uint64(0)},
		Verified: TPUQueueGauge{Length: 9, Capacity: 9}}
	body, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeNodeNetworkDetail(body)
	if err != nil || !reflect.DeepEqual(decoded.TPUQueues, d.TPUQueues) {
		t.Fatal("queue-only document lost exact scalars")
	}
	for name, mutate := range map[string]func(*TPUQueuesDetail){
		"scope":                func(q *TPUQueuesDetail) { q.Scope = "occupancy_percent" },
		"generation":           func(q *TPUQueuesDetail) { q.Generation = 0 },
		"zero_capacity":        func(q *TPUQueuesDetail) { q.Ingress.Capacity = 0 },
		"length_over_capacity": func(q *TPUQueuesDetail) { q.Verified.Length++ },
		"before_node":          func(q *TPUQueuesDetail) { q.StartedAt = snapshot.StartedAt.Add(-time.Second) },
		"future":               func(q *TPUQueuesDetail) { q.SampledAt = d.CapturedAt.Add(2 * time.Second) },
		"stale": func(q *TPUQueuesDetail) {
			q.StartedAt, q.SampledAt = now.Add(-time.Minute).UTC(), now.Add(-30*time.Second).UTC()
		},
	} {
		t.Run(name, func(t *testing.T) {
			bad, q := d, *d.TPUQueues
			mutate(&q)
			bad.TPUQueues = &q
			if validateNodeNetworkDetail(bad) == nil {
				t.Fatal("invalid queue observation accepted")
			}
		})
	}
	for _, text := range []string{
		strings.Replace(string(body), `"generation":"7"`, `"generation":"7","generation":"7"`, 1),
		strings.Replace(string(body), `"length":"0"`, `"length":"00"`, 1),
		strings.Replace(string(body), `"length":"0"`, `"length":0`, 1),
		strings.Replace(string(body), `"scope":"tpu_pipeline_queue_samples"`, `"scope":"tpu_pipeline_queue_samples","extra":0`, 1),
	} {
		if _, err := decodeNodeNetworkDetail([]byte(text)); err == nil {
			t.Fatal("noncanonical queue body accepted")
		}
	}
	old := d
	old.TPUQueues = nil
	oldBody, _ := json.Marshal(old)
	if _, err := decodeNodeNetworkDetail(oldBody); err != nil {
		t.Fatal("legacy omission refused")
	}
	nullBody := strings.TrimSuffix(string(oldBody), "}") + `,"tpu_queues":null}`
	if _, err := decodeNodeNetworkDetail([]byte(nullBody)); err == nil {
		t.Fatal("explicit null optional queues accepted")
	}
	until := now.Add(time.Second)
	family, err := preparePublicNodeNetwork(d, now, until)
	if err != nil {
		t.Fatal(err)
	}
	var public PublicNodeNetworkDetail
	if json.Unmarshal([]byte(family.bodies[0]+family.tail), &public) != nil || !reflect.DeepEqual(public.TPUQueues, d.TPUQueues) {
		t.Fatal("public projection changed queues")
	}
	if _, err := preparePublicNodeNetwork(d, now, now.Add(NetworkDetailFreshFor+time.Second)); err == nil {
		t.Fatal("public wrapper renewed original sample")
	}
	capture.Detail, capture.FreshUntil = d, until
	source := &networkDetailSource{nodeSample: func(time.Time) (NodeNetworkCapture, error) { return capture, nil }}
	source.update(snapshot, now)
	if p := source.latest.Load(); p == nil || p.reason != "" || !strings.Contains(p.body, `"tpu_queues"`) {
		t.Fatal("private source refused independent queues")
	}
	capture.FreshUntil = now.Add(NetworkDetailFreshFor + time.Second)
	source.update(snapshot, now)
	if p := source.latest.Load(); p == nil || p.reason != "source_time_invalid" {
		t.Fatal("private source renewed queue sample")
	}
}

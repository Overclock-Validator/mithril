package dashboard

import (
	"encoding/json"
	"testing"
	"time"
)

func TestFastHintCurrentPublicationSelection(t *testing.T) {
	for _, scenario := range []string{"stable fresh", "fresh replacement", "nil selection replaced", "expired selection replaced", "stable nil", "stable expired", "stable changed binding", "replacement mismatches current binding"} {
		t.Run(scenario, func(t *testing.T) {
			c, old := fastV1Fixture(t)
			value := old.value
			value.Sequence++
			value.SampledAt = time.Now().UTC()
			value.Records = []ExecutedSlot{{At: value.SampledAt, Slot: 4, Bankhash: old.value.Records[0].Bankhash}}
			body, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			next := &fastExecutedPublication{value: value, body: body, stamp: old.stamp, freshUntil: old.freshUntil, invalidated: make(chan struct{})}
			selected, want := old, old
			switch scenario {
			case "fresh replacement":
				c.fast.replace(next)
				want = next
			case "nil selection replaced":
				selected = nil
				c.fast.replace(next)
				want = next
			case "expired selection replaced":
				old.freshUntil = time.Now().Add(-time.Millisecond)
				c.fast.replace(next)
				want = next
			case "stable nil":
				c.fast.replace(nil)
				selected = nil
				want = nil
			case "stable expired":
				old.freshUntil = time.Now().Add(-time.Millisecond)
				want = nil
			case "stable changed binding":
				gate := *c.detailGate.Load()
				gate.Rewinds++
				c.detailGate.Store(&gate)
				want = nil
			case "replacement mismatches current binding":
				next.value.Binding.Rewinds++
				c.fast.replace(next)
				want = nil
			}
			got, remaining, settled := c.currentFastHint(selected)
			if !settled || got != want {
				t.Fatalf("selected=%p current=%p: got %p settled=%v, want %p settled=true", selected, c.fast.latest.Load(), got, settled, want)
			}
			if got == nil && remaining != 0 || got != nil && remaining.Milliseconds() < 1 {
				t.Fatalf("invalid own admission budget: publication=%p remaining=%v", got, remaining)
			}
		})
	}
}

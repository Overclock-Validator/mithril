//go:build linux

package metrics

import (
	"testing"
	"time"
)

func TestHostSamplerLinuxCapture(t *testing.T) {
	before := time.Now()
	s := NewHostSampler(true)
	o, ok := s.Sample()
	after := time.Now()
	if !ok || !o.HasCPU || !o.HasLoad || !o.HasMemory {
		t.Fatalf("native Linux capture = %+v,%v", o, ok)
	}
	if o.StartedAt.Before(before) || o.SampledAt.After(after) || o.CPU.ObservedAt.Before(o.StartedAt) || o.CPU.ObservedAt.After(o.SampledAt) {
		t.Fatal("capture is outside actual call")
	}
	if o.CPU.HasRate {
		t.Fatal("first capture has a rate")
	}
}

func BenchmarkHostSamplerLinux(b *testing.B) {
	s := NewHostSampler(true)
	b.ReportAllocs()
	for b.Loop() {
		s.Sample()
	}
}

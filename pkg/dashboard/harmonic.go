package dashboard

import "time"

// Harmonic describes local configuration and the latest service observation.
// Strategy is requested locally, not confirmation of remote acceptance.
type Harmonic struct {
	Enabled     bool                 `json:"enabled"`
	Strategy    string               `json:"strategy"`
	Observation *HarmonicObservation `json:"observation"`
}

// HarmonicObservation is a detached sample from one service instance. Health,
// relayer usability and successful local advertisement are distinct facts;
// they do not establish peer receipt or successful future block production.
type HarmonicObservation struct {
	StartedAt                  time.Time `json:"started_at"`
	SampledAt                  time.Time `json:"sampled_at"`
	BlockEngineHealthy         bool      `json:"block_engine_healthy"`
	RelayerConfigured          bool      `json:"relayer_configured"`
	RelayerUsable              bool      `json:"relayer_usable"`
	RelayerTPUAdvertised       bool      `json:"relayer_tpu_advertised"`
	HarmonicClientIDAdvertised bool      `json:"harmonic_client_id_advertised"`
}

// PublicHarmonic omits service internals, endpoints, credentials and counters.
// Nullable gauges distinguish an absent observation from a measured false.
type PublicHarmonic struct {
	State                      string     `json:"state"`
	SampledAt                  *time.Time `json:"sampled_at"`
	Strategy                   *string    `json:"strategy"`
	BlockEngineHealthy         *bool      `json:"block_engine_healthy"`
	RelayerConfigured          *bool      `json:"relayer_configured"`
	RelayerUsable              *bool      `json:"relayer_usable"`
	RelayerTPUAdvertised       *bool      `json:"relayer_tpu_advertised"`
	HarmonicClientIDAdvertised *bool      `json:"harmonic_client_id_advertised"`
}

func publicHarmonic(s *Snapshot, state string, now time.Time) *PublicHarmonic {
	if s == nil || s.Harmonic == nil || s.Mode != "validator" || !s.Alpenglow ||
		state != "fresh" && state != "stale" {
		return nil
	}
	h := s.Harmonic
	if !h.Enabled {
		if h.Strategy != "" || h.Observation != nil {
			return nil
		}
		return &PublicHarmonic{State: "disabled"}
	}
	switch h.Strategy {
	case "fifo", "fba", "mrev":
	default:
		return nil
	}
	strategy := h.Strategy
	p := &PublicHarmonic{State: "waiting_source", Strategy: &strategy}
	if h.Observation == nil {
		return p
	}
	v := *h.Observation
	if s.StartedAt.IsZero() || v.StartedAt.IsZero() || v.StartedAt.Before(s.StartedAt) ||
		v.SampledAt.Before(v.StartedAt) || !validPublicTime(v.SampledAt, now) ||
		v.SampledAt.After(s.SampledAt.Add(time.Second)) {
		return nil
	}
	p.State = state
	if now.Sub(v.SampledAt) > StaleAfter {
		p.State = "stale"
	}
	p.SampledAt = &v.SampledAt
	p.BlockEngineHealthy, p.RelayerConfigured, p.RelayerUsable = &v.BlockEngineHealthy, &v.RelayerConfigured, &v.RelayerUsable
	p.RelayerTPUAdvertised, p.HarmonicClientIDAdvertised = &v.RelayerTPUAdvertised, &v.HarmonicClientIDAdvertised
	return p
}

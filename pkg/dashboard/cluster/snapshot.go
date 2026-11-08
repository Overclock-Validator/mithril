// Package cluster collects explicitly independent, public cluster observations.
// It does not read node state or perform network I/O on dashboard viewer requests.
package cluster

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/mr-tron/base58"
)

const (
	// MaxBytes bounds the complete persisted public snapshot.
	MaxBytes = 4 << 20
	// MaxRows bounds each provider and the joined directory.
	MaxRows = 2048
	// MaxPerformanceHistory bounds provider-reported historical samples.
	MaxPerformanceHistory = 120
	// GenesisHash identifies the only supported community-cluster profile.
	GenesisHash = "9mi4SxvrMwaErcAN9r8wAAsX8YTKedGNogzQCuhTZ7Nz"
	// MaxAge is the maximum source/capture age accepted as current by this package.
	MaxAge = 2 * time.Minute
	// RankPolicy names the collector's derived competition rank, not Titan's UI order.
	RankPolicy = "derived_inclusion_ratio_desc_opportunities_desc"
	// CountryAttributionURL is required when displaying DB-IP-derived countries.
	CountryAttributionURL = "https://db-ip.com/"
)

// Count is an exact unsigned decimal integer on the JSON wire.
type Count string

// Snapshot is an independent external-cluster observation, never linked-node state.
type Snapshot struct {
	SchemaVersion      int              `json:"schema_version"`
	Scope              string           `json:"scope"`
	Profile            string           `json:"profile"`
	Network            string           `json:"network"`
	GenesisHash        string           `json:"genesis_hash"`
	CapturedAt         time.Time        `json:"captured_at"`
	Epoch              *Count           `json:"epoch"`
	Reference          *RPCReference    `json:"rpc_reference,omitempty"`
	Performance        *RPCPerformance  `json:"rpc_performance,omitempty"`
	PerformanceHistory []RPCPerformance `json:"rpc_performance_history,omitempty"`
	SolstatSummary     *SolstatSummary  `json:"solstat_summary,omitempty"`
	FastState          string           `json:"fast_state"`
	Providers          []ProviderStatus `json:"providers"`
	Validators         []Validator      `json:"validators"`
}

// SolstatSummary contains dated provider estimates, not linked-node measurements.
type SolstatSummary struct {
	Epoch                  Count    `json:"epoch"`
	TotalValidators        Count    `json:"total_validators"`
	ActiveValidators       Count    `json:"active_validators"`
	DelinquentValidators   Count    `json:"delinquent_validators"`
	TotalStakeSOL          string   `json:"total_stake_sol"`
	AverageSkipRatePercent *float64 `json:"average_skip_rate_percent,omitempty"`
	MedianSkipRatePercent  *float64 `json:"median_skip_rate_percent,omitempty"`
}

// RPCReference is one dated getEpochInfo response, not a network tip.
type RPCReference struct {
	Slot         Count     `json:"slot"`
	EpochStart   Count     `json:"epoch_start"`
	SlotsInEpoch Count     `json:"slots_in_epoch"`
	ObservedAt   time.Time `json:"observed_at"`
}

// RPCPerformance is a single provider-reported sample. ObservedAt is response
// receipt time; the RPC does not supply the sample's own wall-clock timestamp.
type RPCPerformance struct {
	Slot                Count     `json:"slot"`
	Transactions        Count     `json:"transactions"`
	NonVoteTransactions *Count    `json:"non_vote_transactions,omitempty"`
	Slots               Count     `json:"slots"`
	PeriodSeconds       uint16    `json:"period_seconds"`
	ObservedAt          time.Time `json:"observed_at"`
}

// ProviderStatus keeps provider availability and binding separate from values.
type ProviderStatus struct {
	ID             string     `json:"id"`
	State          string     `json:"state"`
	Reason         string     `json:"reason"`
	SourceURL      string     `json:"source_url"`
	SourceAt       *time.Time `json:"source_at"`
	SourceTimeKind string     `json:"source_time_kind"`
	CapturedAt     time.Time  `json:"captured_at"`
	Epoch          *Count     `json:"epoch"`
	GenesisHash    string     `json:"genesis_hash"`
	Binding        string     `json:"binding"`
	RowCount       Count      `json:"row_count"`
}

// Validator contains only allowlisted public identifiers and provider measurements.
// StakeSOL is a provider decimal SOL estimate, not an exact lamport balance.
type Validator struct {
	Identity            string      `json:"identity"`
	VoteAccount         string      `json:"vote_account"`
	DirectorySource     string      `json:"directory_source"`
	Name                string      `json:"name,omitempty"`
	Version             string      `json:"version,omitempty"`
	StakeSOL            *string     `json:"stake_sol,omitempty"`
	CommissionPercent   *float64    `json:"commission_percent,omitempty"`
	Delinquent          *bool       `json:"delinquent,omitempty"`
	LastVote            *Count      `json:"last_vote,omitempty"`
	RootSlot            *Count      `json:"root_slot,omitempty"`
	CurrentEpochCredits *Count      `json:"current_epoch_credits,omitempty"`
	VoteLagSlots        *Count      `json:"vote_lag_slots,omitempty"`
	LeaderSlots         *Count      `json:"leader_slots,omitempty"`
	BlocksProduced      *Count      `json:"blocks_produced,omitempty"`
	SkipRatePercent     *float64    `json:"skip_rate_percent,omitempty"`
	EfficiencyPercent   *float64    `json:"efficiency_percent,omitempty"`
	EfficiencyRank      *Count      `json:"efficiency_rank,omitempty"`
	CountryCode         string      `json:"country_code,omitempty"`
	CountrySource       string      `json:"country_source,omitempty"`
	Inclusion           []Inclusion `json:"inclusion"`
	Fast                *FastScore  `json:"fast,omitempty"`
}

// Inclusion describes Titan's observed certificate cohort, not all chain slots.
type Inclusion struct {
	Window          string    `json:"window"`
	Epoch           Count     `json:"epoch"`
	Opportunities   Count     `json:"opportunities"`
	Included        Count     `json:"included"`
	Missed          Count     `json:"missed"`
	Percent         *float64  `json:"percent"`
	Rank            *Count    `json:"rank"`
	CohortSize      Count     `json:"cohort_size"`
	Observations    Count     `json:"observations"`
	RankPolicy      string    `json:"rank_policy"`
	FirstSlot       Count     `json:"first_slot"`
	LastSlot        Count     `json:"last_slot"`
	FirstObservedAt time.Time `json:"first_observed_at"`
	LastObservedAt  time.Time `json:"last_observed_at"`
}

// FastScore reserves an explicitly provider-specific score; no current adapter supplies it.
type FastScore struct {
	Provider string   `json:"provider"`
	Percent  *float64 `json:"percent"`
	Rank     *Count   `json:"rank"`
}

var sourceURLs = map[string]string{
	"rpc":     "https://rpc.ag.validblocks.com/",
	"solstat": "https://solstat.noders.services/api/validators?network=alpenglow",
	"titan":   "https://observatory-origin.titananalytics.io/analytics/api/stream",
	"geoip":   "https://db-ip.com/",
	"puffin":  "https://puffin-validator.xyz/alpenglow",
}

var decimalSOL = regexp.MustCompile(`^(0|[1-9][0-9]{0,19})(\.[0-9]{1,18})?$`)

func count(n uint64) Count { return Count(strconv.FormatUint(n, 10)) }

func number(c Count) (uint64, error) {
	if c == "" || len(c) > 20 || (len(c) > 1 && c[0] == '0') || strings.IndexFunc(string(c), func(r rune) bool { return r < '0' || r > '9' }) >= 0 {
		return 0, errors.New("invalid decimal count")
	}
	return strconv.ParseUint(string(c), 10, 64)
}

func keyOK(s string) bool {
	if len(s) < 32 || len(s) > 44 {
		return false
	}
	b, err := base58.Decode(s)
	return err == nil && len(b) == 32 && base58.Encode(b) == s
}

func textOK(s string, max int) bool {
	return len(s) <= max && utf8.ValidString(s) && strings.IndexFunc(s, unicode.IsControl) < 0
}

func percentOK(p *float64) bool {
	return p == nil || (!math.IsNaN(*p) && !math.IsInf(*p, 0) && *p >= 0 && *p <= 100)
}

func validateSolstatSummary(v *SolstatSummary, epoch *Count) error {
	if v == nil || epoch == nil || v.Epoch != *epoch || !decimalSOL.MatchString(v.TotalStakeSOL) || !percentOK(v.AverageSkipRatePercent) || !percentOK(v.MedianSkipRatePercent) {
		return errors.New("invalid Solstat summary")
	}
	total, e1 := number(v.TotalValidators)
	active, e2 := number(v.ActiveValidators)
	delinquent, e3 := number(v.DelinquentValidators)
	if e1 != nil || e2 != nil || e3 != nil || total > MaxRows || active > total || delinquent > total-active {
		return errors.New("invalid Solstat validator counts")
	}
	return nil
}

func countryOK(s string) bool {
	// ISO 3166-1 alpha-2 plus XK, which the source explicitly places in Europe.
	const codes = " AD AE AF AG AI AL AM AO AQ AR AS AT AU AW AX AZ BA BB BD BE BF BG BH BI BJ BL BM BN BO BQ BR BS BT BV BW BY BZ CA CC CD CF CG CH CI CK CL CM CN CO CR CU CV CW CX CY CZ DE DJ DK DM DO DZ EC EE EG EH ER ES ET FI FJ FK FM FO FR GA GB GD GE GF GG GH GI GL GM GN GP GQ GR GS GT GU GW GY HK HM HN HR HT HU ID IE IL IM IN IO IQ IR IS IT JE JM JO JP KE KG KH KI KM KN KP KR KW KY KZ LA LB LC LI LK LR LS LT LU LV LY MA MC MD ME MF MG MH MK ML MM MN MO MP MQ MR MS MT MU MV MW MX MY MZ NA NC NE NF NG NI NL NO NP NR NU NZ OM PA PE PF PG PH PK PL PM PN PR PS PT PW PY QA RE RO RS RU RW SA SB SC SD SE SG SH SI SJ SK SL SM SN SO SR SS ST SV SX SY SZ TC TD TF TG TH TJ TK TL TM TN TO TR TT TV TW TZ UA UG UM US UY UZ VA VC VE VG VI VN VU WF WS XK YE YT ZA ZM ZW "
	return len(s) == 2 && strings.Contains(codes, " "+s+" ")
}

func reasonOK(reason string) bool {
	switch reason {
	case "", "not_collected", "provider_retired", "request_failed", "invalid_payload", "source_stale", "rpc_gossip_unavailable", "rpc_unavailable", "rpc_genesis_mismatch", "rpc_epoch_invalid", "rpc_genesis_recheck_failed", "rpc_epoch_changed", "rpc_vote_accounts_invalid":
		return true
	default:
		return false
	}
}

// Validate rejects unsafe, inconsistent, or incorrectly bound persisted snapshots.
// Freshness relative to wall time remains a reader decision; historical files are valid data.
func Validate(s Snapshot) error {
	if s.SchemaVersion != 1 || s.Scope != "independent_cluster" || s.Profile != "alpenglow" || s.Network != "alpenglow-community" || s.GenesisHash != GenesisHash || s.CapturedAt.IsZero() || s.FastState != "unavailable" || len(s.Validators) > MaxRows || len(s.Providers) != len(sourceURLs) {
		return errors.New("invalid cluster snapshot envelope")
	}
	if s.Epoch != nil {
		if _, err := number(*s.Epoch); err != nil {
			return err
		}
	}
	providers := make(map[string]ProviderStatus, len(s.Providers))
	for _, p := range s.Providers {
		if sourceURLs[p.ID] == "" || sourceURLs[p.ID] != p.SourceURL || providers[p.ID].ID != "" || p.CapturedAt.IsZero() || p.CapturedAt.After(s.CapturedAt) || !reasonOK(p.Reason) {
			return errors.New("invalid provider identity or time")
		}
		if p.State != "fresh" && p.State != "unavailable" && p.State != "invalid" && p.State != "retired" && p.State != "stale" {
			return errors.New("invalid provider state")
		}
		if (p.State == "fresh") != (p.Reason == "") || (p.State == "retired" && p.ID != "puffin") {
			return errors.New("provider state and reason disagree")
		}
		timeKind := map[string]string{"rpc": "response", "solstat": "provider", "titan": "provider", "geoip": "response"}[p.ID]
		if (p.SourceAt == nil) != (p.SourceTimeKind == "") || (p.SourceTimeKind != "" && p.SourceTimeKind != timeKind) || (p.SourceAt != nil && p.SourceAt.IsZero()) {
			return errors.New("invalid provider time provenance")
		}
		if p.State != "fresh" && (p.Binding != "" || p.GenesisHash != "") {
			return errors.New("unavailable provider cannot claim verified binding")
		}
		n, err := number(p.RowCount)
		if err != nil || n > MaxRows {
			return errors.New("invalid provider row count")
		}
		if p.Epoch != nil {
			if _, err := number(*p.Epoch); err != nil {
				return err
			}
		}
		if p.State == "fresh" {
			binding := map[string]string{"rpc": "rpc_genesis_before_after", "solstat": "rpc_profile_epoch_checked", "titan": "payload_genesis_epoch_checked", "geoip": "rpc_gossip_local_geoip"}[p.ID]
			if binding == "" || p.Binding != binding || p.SourceTimeKind != timeKind || p.GenesisHash != GenesisHash || p.SourceAt == nil || p.SourceAt.IsZero() || p.SourceAt.After(p.CapturedAt.Add(5*time.Second)) || p.CapturedAt.Sub(*p.SourceAt) > MaxAge || p.Epoch == nil || s.Epoch == nil || *p.Epoch != *s.Epoch {
				return errors.New("invalid fresh provider binding")
			}
		}
		providers[p.ID] = p
	}
	puffin := providers["puffin"]
	if !(puffin.State == "unavailable" && puffin.Reason == "not_collected") && !(puffin.State == "retired" && puffin.Reason == "provider_retired") {
		return errors.New("invalid unavailable FAST capability")
	}
	if s.SolstatSummary != nil && (providers["solstat"].State != "fresh" || validateSolstatSummary(s.SolstatSummary, s.Epoch) != nil) {
		return errors.New("invalid Solstat summary binding")
	}
	if r := s.Reference; r != nil {
		p := providers["rpc"]
		slot, slotErr := number(r.Slot)
		start, startErr := number(r.EpochStart)
		length, lengthErr := number(r.SlotsInEpoch)
		if slotErr != nil || startErr != nil || lengthErr != nil || length == 0 ||
			start > math.MaxUint64-(length-1) || slot < start || slot-start >= length ||
			p.State != "fresh" || p.SourceAt == nil || !p.SourceAt.Equal(r.ObservedAt) {
			return errors.New("invalid RPC reference observation")
		}
	}
	validatePerformance := func(sample *RPCPerformance) error {
		p := providers["rpc"]
		slot, slotErr := number(sample.Slot)
		tx, txErr := number(sample.Transactions)
		_, slotsErr := number(sample.Slots)
		if s.Reference == nil || slotErr != nil || txErr != nil || slotsErr != nil || sample.PeriodSeconds == 0 ||
			p.State != "fresh" || p.SourceAt == nil || sample.ObservedAt.IsZero() || sample.ObservedAt.After(*p.SourceAt) ||
			p.SourceAt.Sub(sample.ObservedAt) > MaxAge {
			return errors.New("invalid RPC performance observation")
		}
		reference, _ := number(s.Reference.Slot)
		if slot > reference {
			return errors.New("RPC performance slot exceeds reference")
		}
		if sample.NonVoteTransactions != nil {
			nonVote, err := number(*sample.NonVoteTransactions)
			if err != nil || nonVote > tx {
				return errors.New("invalid RPC non-vote count")
			}
		}
		return nil
	}
	if s.Performance != nil {
		if err := validatePerformance(s.Performance); err != nil {
			return err
		}
	}
	if len(s.PerformanceHistory) > MaxPerformanceHistory {
		return errors.New("RPC performance history exceeds limit")
	}
	var previous uint64
	for i := range s.PerformanceHistory {
		sample := &s.PerformanceHistory[i]
		if err := validatePerformance(sample); err != nil {
			return err
		}
		slot, _ := number(sample.Slot)
		if i > 0 && (slot >= previous || !sample.ObservedAt.Equal(s.PerformanceHistory[0].ObservedAt)) {
			return errors.New("invalid RPC performance history order or receipt")
		}
		previous = slot
	}
	if len(s.Validators) > 0 && (providers["rpc"].State != "fresh" || s.Epoch == nil) {
		return errors.New("directory lacks verified RPC binding")
	}
	seen := make(map[string]bool, len(s.Validators))
	for _, v := range s.Validators {
		if !keyOK(v.Identity) || !keyOK(v.VoteAccount) || seen[v.VoteAccount] || !textOK(v.Name, 128) || !textOK(v.Version, 64) || (v.DirectorySource != "solstat" && v.DirectorySource != "titan") || providers[v.DirectorySource].State != "fresh" || v.Fast != nil {
			return errors.New("invalid validator row")
		}
		seen[v.VoteAccount] = true
		if v.StakeSOL != nil && !decimalSOL.MatchString(*v.StakeSOL) {
			return errors.New("invalid provider SOL estimate")
		}
		if !percentOK(v.CommissionPercent) || !percentOK(v.SkipRatePercent) || !percentOK(v.EfficiencyPercent) {
			return errors.New("invalid directory percentage")
		}
		if v.DirectorySource != "solstat" && (v.RootSlot != nil || v.CurrentEpochCredits != nil || v.VoteLagSlots != nil) {
			return errors.New("invalid vote observation source")
		}
		for _, c := range []*Count{v.LastVote, v.RootSlot, v.CurrentEpochCredits, v.VoteLagSlots, v.LeaderSlots, v.BlocksProduced, v.EfficiencyRank} {
			if c != nil {
				if _, err := number(*c); err != nil {
					return err
				}
			}
		}
		if v.EfficiencyRank != nil {
			rank, _ := number(*v.EfficiencyRank)
			if rank == 0 || rank > MaxRows {
				return errors.New("invalid efficiency rank")
			}
		}
		if v.BlocksProduced != nil && v.LeaderSlots != nil {
			produced, _ := number(*v.BlocksProduced)
			assigned, _ := number(*v.LeaderSlots)
			if produced > assigned {
				return errors.New("production exceeds assignments")
			}
		}
		if (v.CountryCode == "") != (v.CountrySource == "") || (v.CountryCode != "" && (!countryOK(v.CountryCode) || v.CountrySource != "dbip" || providers["geoip"].State != "fresh")) {
			return errors.New("invalid country estimate")
		}
		if len(v.Inclusion) > 3 || (len(v.Inclusion) > 0 && providers["titan"].State != "fresh") {
			return errors.New("invalid inclusion provider")
		}
		windows := make(map[string]bool, 3)
		for _, x := range v.Inclusion {
			if windows[x.Window] || (x.Window != "1000" && x.Window != "5000" && x.Window != "epoch") || x.RankPolicy != RankPolicy || s.Epoch == nil || x.Epoch != *s.Epoch {
				return errors.New("invalid inclusion window")
			}
			windows[x.Window] = true
			first, firstErr := number(x.FirstSlot)
			last, lastErr := number(x.LastSlot)
			if firstErr != nil || lastErr != nil || first > last || x.FirstObservedAt.IsZero() || x.LastObservedAt.IsZero() || x.FirstObservedAt.After(x.LastObservedAt) || x.LastObservedAt.After(s.CapturedAt.Add(5*time.Second)) {
				return errors.New("invalid inclusion coverage")
			}
			values := make([]uint64, 0, 5)
			for _, c := range []Count{x.Opportunities, x.Included, x.Missed, x.CohortSize, x.Observations} {
				n, err := number(c)
				if err != nil {
					return err
				}
				values = append(values, n)
			}
			op, included, missed, cohort, observed := values[0], values[1], values[2], values[3], values[4]
			if included > op || missed != op-included || op > observed || (observed > 0 && observed-1 > last-first) || cohort > MaxRows || !percentOK(x.Percent) || (x.Window == "1000" && observed > 1000) || (x.Window == "5000" && observed > 5000) {
				return errors.New("invalid inclusion arithmetic")
			}
			if op == 0 {
				if x.Percent != nil || x.Rank != nil {
					return errors.New("zero opportunities must have no score or rank")
				}
			} else {
				if x.Percent == nil || x.Rank == nil || math.Abs(*x.Percent-float64(included)/float64(op)*100) > 0.0001 {
					return errors.New("invalid inclusion percentage")
				}
				rank, err := number(*x.Rank)
				if err != nil || rank == 0 || rank > cohort {
					return errors.New("invalid inclusion rank")
				}
			}
		}
	}
	return nil
}

// ReadFile reads one bounded regular UTF-8 cache file without following a symlink.
func ReadFile(path string) (Snapshot, error) {
	var s Snapshot
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return s, errors.New("cache path must be clean and absolute")
	}
	before, err := os.Lstat(path)
	if err != nil {
		return s, err
	}
	if !before.Mode().IsRegular() || before.Size() > MaxBytes {
		return s, errors.New("cache is not a bounded regular file")
	}
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return s, err
	}
	defer f.Close()
	after, err := f.Stat()
	if err != nil || !after.Mode().IsRegular() || after.Size() > MaxBytes || !os.SameFile(before, after) {
		return s, errors.New("cache changed while opening")
	}
	b, err := io.ReadAll(io.LimitReader(f, MaxBytes+1))
	if err != nil {
		return s, err
	}
	if len(b) > MaxBytes || !utf8.Valid(b) {
		return s, errors.New("cache exceeds size or UTF-8 bounds")
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(&s); err != nil {
		return s, err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return s, errors.New("cache has trailing JSON")
	}
	return s, Validate(s)
}

// WriteFile atomically replaces a validated cache with owner-write/group-read mode.
// The caller provisions the directory/group; no directories or services are created.
func WriteFile(path string, s Snapshot) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("cache path must be clean and absolute")
	}
	if err := Validate(s); err != nil {
		return err
	}
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	if len(b)+1 > MaxBytes {
		return errors.New("cluster snapshot exceeds size bound")
	}
	if info, err := os.Lstat(path); err == nil && !info.Mode().IsRegular() {
		return errors.New("cache destination is not a regular file")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".cluster-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(append(b, '\n')); err == nil {
		err = f.Chmod(0640)
	}
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err := os.Rename(f.Name(), path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync cache directory: %w", err)
	}
	return nil
}

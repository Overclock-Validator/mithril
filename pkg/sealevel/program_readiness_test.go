package sealevel

import (
	"bufio"
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/fixtures"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/progtrace"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func readinessRecords(t *testing.T, buf *bytes.Buffer, kind string) []map[string]any {
	t.Helper()
	var out []map[string]any
	sc := bufio.NewScanner(buf)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var rec map[string]any
		require.NoError(t, json.Unmarshal(sc.Bytes(), &rec))
		if kind == "" || rec["kind"] == kind {
			out = append(out, rec)
		}
	}
	return out
}

func programCacheWeighing(weight uint32) *accountsdb.AccountsDb {
	cache := accountsdb.NewWeightedCache[solana.PublicKey, *accountsdb.ProgramCacheEntry](64, func(solana.PublicKey, *accountsdb.ProgramCacheEntry) uint32 { return weight })
	return &accountsdb.AccountsDb{ProgramCache: cache}
}

// A compile the cache refuses is recorded as compiled and refused, never as
// admitted, and the next preload compiles the program again.
func TestReadinessRecordsRefusedAdmission(t *testing.T) {
	elf := fixtures.Load(t, "sbpf", "noop_aligned.so")
	f := features.NewFeaturesDefault()
	key := benchPubkey(0xa1)
	db := programCacheWeighing(10) // heavier than a tenth of the 64-unit budget

	var buf bytes.Buffer
	stop := progtrace.Start(&buf, 64, 1000)
	for job := uint64(1); job <= 2; job++ {
		loaded, admitted, cached := PreloadProgramForJob(db, key, elf, f, 500, job)
		assert.True(t, loaded)
		assert.False(t, admitted)
		assert.False(t, cached)
	}
	assert.True(t, PreloadProgram(db, key, elf, f), "the old result still says only that it compiled")
	stop()
	_, held := db.MaybeGetProgramFromCache(key)
	assert.False(t, held)

	recs := readinessRecords(t, &buf, "compile")
	require.Len(t, recs, 3)
	for i, rec := range recs {
		assert.Equal(t, "prefetch", rec["exec"], i)
		assert.Equal(t, "absent", rec["cache"], i)
		assert.Equal(t, "ok", rec["result"], i)
		assert.Equal(t, "refused", rec["publish"], i)
		assert.Equal(t, key.String(), rec["key"], i)
		assert.NotContains(t, rec, "prior_admit_ns", i)
		assert.NotContains(t, rec, "t_published_ns", i)
		assert.Equal(t, float64(len(elf)), rec["bytes"], i)
	}
	assert.Equal(t, float64(500), recs[0]["slot"])
	assert.Equal(t, float64(2), recs[1]["job"])
	assert.Equal(t, recs[0]["source_digest"], recs[1]["source_digest"])
}

// An admitted entry is remembered, so a later compile of the same key says
// whether the entry was explicitly removed in between or went missing.
func TestReadinessRecordsAdmissionThenRemoval(t *testing.T) {
	elf := fixtures.Load(t, "sbpf", "noop_aligned.so")
	f := features.NewFeaturesDefault()
	key := benchPubkey(0xa2)
	db := programCacheWeighing(1)

	var buf bytes.Buffer
	stop := progtrace.Start(&buf, 64, 1000)
	loaded, admitted, cached := PreloadProgramForJob(db, key, elf, f, 600, 1)
	require.True(t, loaded && admitted && !cached)
	loaded, admitted, cached = PreloadProgramForJob(db, key, elf, f, 601, 2)
	require.True(t, !loaded && !admitted && cached, "an entry for the key is left alone")
	db.RemoveProgramFromCache(key)
	loaded, admitted, _ = PreloadProgramForJob(db, key, elf, f, 602, 3)
	require.True(t, loaded && admitted)
	assert.False(t, PreloadProgram(db, benchPubkey(0xa3), []byte("not an elf"), f))
	stop()

	all := readinessRecords(t, &buf, "")
	var compiles, removals []map[string]any
	for _, rec := range all {
		switch rec["kind"] {
		case "compile":
			compiles = append(compiles, rec)
		case "invalidate":
			removals = append(removals, rec)
		}
	}
	require.Len(t, compiles, 3)
	require.Len(t, removals, 1)
	assert.Equal(t, key.String(), removals[0]["key"])
	assert.True(t, strings.Contains(removals[0]["reason"].(string), "TestReadinessRecordsAdmissionThenRemoval"), removals[0]["reason"])

	first, again, bad := compiles[0], compiles[1], compiles[2]
	assert.Equal(t, "admitted", first["publish"])
	assert.NotContains(t, first, "prior_admit_ns")
	assert.Equal(t, "admitted", again["publish"])
	assert.Equal(t, first["t_published_ns"], again["prior_admit_ns"])
	assert.Equal(t, first["source_digest"], again["prior_source_digest"])
	assert.Equal(t, removals[0]["t_ns"], again["prior_invalidated_ns"])
	assert.GreaterOrEqual(t, again["t_start_ns"].(float64), removals[0]["t_ns"].(float64))
	assert.Equal(t, "load_error", bad["result"])
	assert.Equal(t, "not_reached", bad["publish"])
}

// Early executions never publish: each one that misses compiles the program
// again, and each compile is recorded as skipped, with the cache still empty.
func TestReadinessRecordsRepeatedSpeculativeMisses(t *testing.T) {
	elf := fixtures.Load(t, "sbpf", "noop_aligned.so")
	key := benchPubkey(0xa4)
	db := programCacheWeighing(1)
	slotCtx := &SlotCtx{Slot: 700, AccountsDb: db}
	entry := &accountsdb.ProgramCacheEntry{}
	entry.BindSource(elf, features.NewFeaturesDefault())

	var buf bytes.Buffer
	stop := progtrace.Start(&buf, 64, 1000)
	start := time.Now()
	for range 3 {
		early := &ExecutionCtx{SlotCtx: slotCtx, Speculative: true, PrivatePrograms: true, Features: *features.NewFeaturesDefault()}
		publish := publishProgram(early, key, entry)
		require.Equal(t, "skipped_speculative", publish)
		traceExecutionCompile(early, key, elf, "absent", start, time.Now(), "ok", publish)
		_, held := db.MaybeGetProgramFromCache(key)
		require.False(t, held, "an early execution must not publish")
	}
	canonical := &ExecutionCtx{SlotCtx: slotCtx, Features: *features.NewFeaturesDefault()}
	publish := publishProgram(canonical, key, entry)
	require.Equal(t, "admitted", publish)
	traceExecutionCompile(canonical, key, elf, "absent", start, time.Now(), "ok", publish)
	require.Equal(t, "skipped_no_db", publishProgram(&ExecutionCtx{}, key, entry))
	stop()

	recs := readinessRecords(t, &buf, "compile")
	require.Len(t, recs, 4)
	for i, rec := range recs[:3] {
		assert.Equal(t, "work_ahead", rec["exec"], i)
		assert.Equal(t, "skipped_speculative", rec["publish"], i)
		assert.Equal(t, true, rec["private_programs"], i)
		assert.NotContains(t, rec, "prior_admit_ns", i)
		assert.Equal(t, float64(700), rec["slot"], i)
	}
	assert.Equal(t, "canonical", recs[3]["exec"])
	assert.Equal(t, "admitted", recs[3]["publish"])
	assert.Equal(t, false, recs[3]["private_programs"])
}

func TestProgramCacheEntrySourceMismatch(t *testing.T) {
	elf := fixtures.Load(t, "sbpf", "noop_aligned.so")
	f := features.NewFeaturesDefault()
	entry := &accountsdb.ProgramCacheEntry{}
	assert.Equal(t, "source_mismatch", entry.SourceMismatch(elf, f), "an unbound entry matches nothing")
	entry.BindSource(elf, f)
	other := features.NewFeaturesDefault()
	other.EnableFeature(features.VirtualAddressSpaceAdjustments, 0)
	changed := bytes.Clone(elf)
	changed[len(changed)-1] ^= 0xff
	assert.Equal(t, "", entry.SourceMismatch(elf, f))
	assert.Equal(t, "feature_mismatch", entry.SourceMismatch(elf, other))
	assert.Equal(t, "source_mismatch", entry.SourceMismatch(changed, f))
	assert.Equal(t, "source_and_feature_mismatch", entry.SourceMismatch(changed, other))
}

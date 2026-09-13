package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestParseRewardName(t *testing.T) {
	ok := []struct {
		name   string
		kind   string
		slot   uint64
		format string
	}{
		{"epoch_boundary_calculated_rewards_slot_285000000.json", "calculated", 285000000, "json"},
		{"epoch_boundary_voting_rewards_slot_9.json", "voting", 9, "json"},
		{"epoch_boundary_calculated_rewards_slot_42.csv", "calculated", 42, "csv"},
	}
	for _, c := range ok {
		k, s, f, got := parseRewardName(c.name)
		if !got || k != c.kind || s != c.slot || f != c.format {
			t.Errorf("%s -> (%q,%d,%q,%v), want (%q,%d,%q,true)", c.name, k, s, f, got, c.kind, c.slot, c.format)
		}
	}
	for _, bad := range []string{
		"mithril.log", "epoch_boundary_rewards_slot_1.json",
		"epoch_boundary_other_rewards_slot_1.json",
		"epoch_boundary_voting_rewards_slot_1.csv",
		"epoch_boundary_calculated_rewards_slot_abc.json",
		"epoch_boundary_calculated_rewards_slot_1.txt",
		"", "random.json",
	} {
		if _, _, _, ok := parseRewardName(bad); ok {
			t.Errorf("parseRewardName(%q) should fail", bad)
		}
	}
}

func writeRewardFixture(t *testing.T, dir, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func newRewardDir(t *testing.T) (string, string) {
	t.Helper()
	logDir := t.TempDir()
	dir := filepath.Join(logDir, "rewards")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	return logDir, dir
}

func calculatedRewardFixture(slot, epoch uint64) string {
	return calculatedRewardFixtureWithVoting(slot, epoch, 2, 1, 10)
}

func calculatedRewardFixtureWithVoting(slot, epoch uint64, tracked int, rewardCount, votingLamports uint64) string {
	const stakingLamports = uint64(9_007_199_254_740_993)
	totalLamports := votingLamports + stakingLamports
	return `{
  "slot":` + jsonNumber(slot) + `,
  "epoch":` + jsonNumber(epoch) + `,
  "generated_at":"2026-07-13T00:00:00Z",
  "rewards_csv":"rewards/epoch_boundary_calculated_rewards_slot_` + jsonNumber(slot) + `.csv",
  "num_partitions":2,
	"voting":{"tracked_accounts":` + strconv.Itoa(tracked) + `,"reward_count":` + jsonNumber(rewardCount) + `,"total_lamports":` + jsonNumber(votingLamports) + `},
	"staking":{"record_count":2,"reward_count":1,"credits_only_count":1,"total_lamports":` + jsonNumber(stakingLamports) + `,"expected_record_count":2},
	"totals":{"record_count":` + jsonNumber(rewardCount+2) + `,"reward_count":` + jsonNumber(rewardCount+1) + `,"credits_only_count":1,"total_lamports":` + jsonNumber(totalLamports) + `}
}`
}

func inspectRewardSlotForTest(t *testing.T, ctx context.Context, logDir, dir, layout string, slot uint64) rewardsSlotOutput {
	t.Helper()
	out, err := inspectRewardSlot(ctx, logDir, dir, layout, slot)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func votingRewardFixture(slot, epoch uint64) string {
	return `{
  "slot":` + jsonNumber(slot) + `,
  "epoch":` + jsonNumber(epoch) + `,
  "reward_type":"Voting",
  "generated_at":"2026-07-13T00:00:01Z",
  "rpc_endpoint":"https://rpc.example/SUPER_PATH_SECRET?api-key=SUPER_QUERY_SECRET",
  "local":{"tracked_vote_accounts":2,"reward_count":1,"total_lamports":10,"rewards":[{"pubkey":"vote-1","lamports":10}]},
  "source_block":{"reward_count":1,"total_lamports":10,"rewards":[{"pubkey":"vote-1","lamports":10}]},
  "local_vs_source":{"left_total_lamports":10,"right_total_lamports":10,"total_lamports_delta":0,"mismatched_count":0,"missing_in_left":0,"missing_in_right":0},
  "rpc_confirmed_error":"Bearer SUPER_ERROR_SECRET",
  "rpc_finalized":{"reward_count":1,"total_lamports":10,"rewards":[{"pubkey":"vote-1","lamports":10}]},
  "local_vs_rpc_finalized":{"left_total_lamports":10,"right_total_lamports":10,"total_lamports_delta":0,"mismatched_count":0,"missing_in_left":0,"missing_in_right":0},
  "source_vs_rpc_finalized":{"left_total_lamports":10,"right_total_lamports":10,"total_lamports_delta":0,"mismatched_count":0,"missing_in_left":0,"missing_in_right":0},
  "mismatches":[]
}`
}

func largeVotingRewardFixture(slot, epoch uint64, count int) string {
	var rewards strings.Builder
	rewards.Grow(count * 80)
	rewards.WriteByte('[')
	for i := 0; i < count; i++ {
		if i > 0 {
			rewards.WriteByte(',')
		}
		fmt.Fprintf(&rewards, `{"pubkey":"%s%012d","lamports":1}`, strings.Repeat("1", 32), i+1)
	}
	rewards.WriteByte(']')
	list := rewards.String()
	countText := strconv.Itoa(count)
	return `{"slot":` + jsonNumber(slot) + `,"epoch":` + jsonNumber(epoch) + `,"reward_type":"Voting","generated_at":"2026-07-13T00:00:01Z",` +
		`"local":{"tracked_vote_accounts":` + countText + `,"reward_count":` + countText + `,"total_lamports":` + countText + `,"rewards":` + list + `},` +
		`"source_block":{"reward_count":` + countText + `,"total_lamports":` + countText + `,"rewards":` + list + `},` +
		`"local_vs_source":{"left_total_lamports":` + countText + `,"right_total_lamports":` + countText + `,"total_lamports_delta":0,"mismatched_count":0,"missing_in_left":0,"missing_in_right":0},"mismatches":[]}`
}

func jsonNumber(v uint64) string { return strconv.FormatUint(v, 10) }

func removeRewardJSONPath(t *testing.T, body, path string) []byte {
	t.Helper()
	var root map[string]json.RawMessage
	if err := json.Unmarshal([]byte(body), &root); err != nil {
		t.Fatal(err)
	}
	var remove func(map[string]json.RawMessage, []string)
	remove = func(object map[string]json.RawMessage, parts []string) {
		if len(parts) == 1 {
			delete(object, parts[0])
			return
		}
		var nested map[string]json.RawMessage
		if err := json.Unmarshal(object[parts[0]], &nested); err != nil {
			t.Fatal(err)
		}
		remove(nested, parts[1:])
		encoded, err := json.Marshal(nested)
		if err != nil {
			t.Fatal(err)
		}
		object[parts[0]] = encoded
	}
	remove(root, strings.Split(path, "."))
	data, err := json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestReadRewardsListAndLayoutPrecedence(t *testing.T) {
	logDir := t.TempDir()
	flat := filepath.Join(logDir, "rewards")
	latestRun := filepath.Join(logDir, "run-2", "rewards")
	if err := os.MkdirAll(flat, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(latestRun, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("run-2", filepath.Join(logDir, "latest")); err != nil {
		t.Fatal(err)
	}
	writeRewardFixture(t, flat, "epoch_boundary_calculated_rewards_slot_1.json", `{}`)
	writeRewardFixture(t, latestRun, "epoch_boundary_calculated_rewards_slot_9.json", `{}`)
	writeRewardFixture(t, latestRun, "epoch_boundary_voting_rewards_slot_100.json", `{}`)

	dir, layout, err := resolveRewardsDir(logDir)
	if err != nil {
		t.Fatal(err)
	}
	wantLatest, err := filepath.EvalSymlinks(latestRun)
	if err != nil {
		t.Fatal(err)
	}
	if layout != "latest" || dir != wantLatest {
		t.Fatalf("resolveRewardsDir = %q, %q; want latest %q", dir, layout, wantLatest)
	}
	arts, total, truncated, err := listRewardArtifacts(context.Background(), logDir, dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 || !truncated || len(arts) != 1 || arts[0].Slot != 100 {
		t.Fatalf("list = %+v total=%d truncated=%v", arts, total, truncated)
	}
}

func TestResolveRewardsDoesNotFallBackWhenActiveRunHasNoRewardsDir(t *testing.T) {
	logDir := t.TempDir()
	flat := filepath.Join(logDir, "rewards")
	if err := os.Mkdir(flat, 0o755); err != nil {
		t.Fatal(err)
	}
	writeRewardFixture(t, flat, "epoch_boundary_calculated_rewards_slot_1.json", `{}`)
	run := filepath.Join(logDir, "run-current")
	if err := os.Mkdir(run, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("run-current", filepath.Join(logDir, "latest")); err != nil {
		t.Fatal(err)
	}
	dir, layout, err := resolveRewardsDir(logDir)
	if err != nil {
		t.Fatal(err)
	}
	if dir != "" || layout != "latest" {
		t.Fatalf("stale flat rewards were selected: dir=%q layout=%q", dir, layout)
	}
}

func TestResolveRewardsFlatDirectorySymlinkEscapeRejected(t *testing.T) {
	outside := t.TempDir()
	logDir := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(logDir, "rewards")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := resolveRewardsDir(logDir); err == nil {
		t.Fatal("escaping flat rewards symlink was accepted")
	}
}

func TestReadRewardsPreservesLatestLayoutWhenUnavailable(t *testing.T) {
	logDir := t.TempDir()
	if err := os.Mkdir(filepath.Join(logDir, "run-current"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("run-current", filepath.Join(logDir, "latest")); err != nil {
		t.Fatal(err)
	}
	session := startInMemorySession(t, Config{
		LogDir: logDir, MetricsURL: "http://127.0.0.1:1/", RPCURL: "http://127.0.0.1:1/",
	})
	text, isErr := callToolText(t, session, "mithril_read_rewards", nil)
	if isErr || !strings.Contains(text, `"layout":"latest"`) || !strings.Contains(text, `"state":"unavailable"`) {
		t.Fatalf("latest no-data state lost its layout: isError=%v text=%q", isErr, text)
	}
}

func TestInspectRewardSlotCompleteAndSecretSafe(t *testing.T) {
	logDir, dir := newRewardDir(t)
	writeRewardFixture(t, dir, "epoch_boundary_calculated_rewards_slot_100.json", calculatedRewardFixture(100, 7))
	writeRewardFixture(t, dir, "epoch_boundary_calculated_rewards_slot_100.csv", "record_type,lamports\n")
	writeRewardFixture(t, dir, "epoch_boundary_voting_rewards_slot_100.json", votingRewardFixture(100, 7))

	out := inspectRewardSlotForTest(t, context.Background(), logDir, dir, "flat", 100)
	if out.ArtifactState != "complete" || !out.Found || out.Verification != "reference_matched" {
		t.Fatalf("unexpected output: %+v", out)
	}
	if out.Calculated == nil || out.Calculated.TotalLamports != "9007199254741003" {
		t.Fatalf("large total was not preserved: %+v", out.Calculated)
	}
	wire, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"SUPER_PATH_SECRET", "SUPER_QUERY_SECRET", "SUPER_ERROR_SECRET", "rpc_endpoint", "rpc_confirmed_error"} {
		if strings.Contains(string(wire), secret) {
			t.Errorf("secret-bearing field leaked into output: %q", secret)
		}
	}
}

func TestInspectRewardSlotRejectsCrossArtifactMismatch(t *testing.T) {
	logDir, dir := newRewardDir(t)
	writeRewardFixture(t, dir, "epoch_boundary_calculated_rewards_slot_100.json", calculatedRewardFixtureWithVoting(100, 7, 3, 1, 10))
	writeRewardFixture(t, dir, "epoch_boundary_calculated_rewards_slot_100.csv", "record_type,lamports\n")
	writeRewardFixture(t, dir, "epoch_boundary_voting_rewards_slot_100.json", votingRewardFixture(100, 7))

	out := inspectRewardSlotForTest(t, context.Background(), logDir, dir, "flat", 100)
	if out.ArtifactState != "invalid" || out.Verification != "unavailable" || !strings.Contains(strings.Join(out.Issues, " "), "summaries do not match") {
		t.Fatalf("cross-artifact mismatch was accepted: %+v", out)
	}
}

func TestInspectRewardSlotPropagatesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := inspectRewardSlot(ctx, t.TempDir(), t.TempDir(), "flat", 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled slot inspection error = %v", err)
	}
}

func TestInspectRewardSlotAcceptsLargeCSVMetadataOnly(t *testing.T) {
	logDir, dir := newRewardDir(t)
	writeRewardFixture(t, dir, "epoch_boundary_calculated_rewards_slot_100.json", calculatedRewardFixture(100, 7))
	writeRewardFixture(t, dir, "epoch_boundary_voting_rewards_slot_100.json", votingRewardFixture(100, 7))
	csv := filepath.Join(dir, "epoch_boundary_calculated_rewards_slot_100.csv")
	writeRewardFixture(t, dir, filepath.Base(csv), "record_type,lamports\n")
	if err := os.Truncate(csv, maxRewardArtifactBytes+1); err != nil {
		t.Fatal(err)
	}
	out := inspectRewardSlotForTest(t, context.Background(), logDir, dir, "flat", 100)
	if out.ArtifactState != "complete" || !out.Found {
		t.Fatalf("large valid CSV should be accepted without reading rows: %+v", out)
	}
}

func TestInspectRewardSlotStreamsVotingArtifactLargerThanLegacyCap(t *testing.T) {
	logDir, dir := newRewardDir(t)
	body := largeVotingRewardFixture(100, 7, 20_000)
	if len(body) <= maxRewardArtifactBytes {
		t.Fatalf("fixture is %d bytes, want more than legacy %d-byte cap", len(body), maxRewardArtifactBytes)
	}
	writeRewardFixture(t, dir, "epoch_boundary_calculated_rewards_slot_100.json", calculatedRewardFixtureWithVoting(100, 7, 20_000, 20_000, 20_000))
	writeRewardFixture(t, dir, "epoch_boundary_calculated_rewards_slot_100.csv", "record_type,lamports\n")
	writeRewardFixture(t, dir, "epoch_boundary_voting_rewards_slot_100.json", body)

	out := inspectRewardSlotForTest(t, context.Background(), logDir, dir, "flat", 100)
	if out.ArtifactState != "complete" || out.Verification != "unavailable" || out.Voting == nil || out.Voting.LocalSourceStatus != "matched" {
		t.Fatalf("streamed production-shaped artifact was not accepted: %+v", out)
	}
}

func TestInspectRewardSlotRejectsVotingArtifactBeyondStreamingCap(t *testing.T) {
	logDir, dir := newRewardDir(t)
	path := filepath.Join(dir, "epoch_boundary_voting_rewards_slot_5.json")
	if err := os.WriteFile(path, []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, maxVotingRewardArtifactBytes+1); err != nil {
		t.Fatal(err)
	}

	out := inspectRewardSlotForTest(t, context.Background(), logDir, dir, "flat", 5)
	if out.ArtifactState != "invalid" || !strings.Contains(strings.Join(out.Issues, " "), "byte limit") {
		t.Fatalf("oversized voting artifact was not rejected by the streaming cap: %+v", out)
	}
}

func TestInspectRewardSlotPartialAndInvalid(t *testing.T) {
	t.Run("voting only is partial", func(t *testing.T) {
		logDir, dir := newRewardDir(t)
		writeRewardFixture(t, dir, "epoch_boundary_voting_rewards_slot_5.json", votingRewardFixture(5, 1))
		out := inspectRewardSlotForTest(t, context.Background(), logDir, dir, "flat", 5)
		if out.ArtifactState != "partial" || !out.Found || len(out.MissingParts) != 2 {
			t.Fatalf("unexpected partial result: %+v", out)
		}
	})

	t.Run("symlink is invalid", func(t *testing.T) {
		logDir, dir := newRewardDir(t)
		outside := filepath.Join(t.TempDir(), "outside.json")
		if err := os.WriteFile(outside, []byte(votingRewardFixture(5, 1)), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, filepath.Join(dir, "epoch_boundary_voting_rewards_slot_5.json")); err != nil {
			t.Fatal(err)
		}
		out := inspectRewardSlotForTest(t, context.Background(), logDir, dir, "flat", 5)
		if out.ArtifactState != "invalid" || len(out.Issues) == 0 {
			t.Fatalf("symlink must be invalid: %+v", out)
		}
	})

	t.Run("malformed calculated JSON is invalid", func(t *testing.T) {
		logDir, dir := newRewardDir(t)
		writeRewardFixture(t, dir, "epoch_boundary_calculated_rewards_slot_5.json", "{")

		out := inspectRewardSlotForTest(t, context.Background(), logDir, dir, "flat", 5)
		if out.ArtifactState != "invalid" || len(out.Issues) == 0 || len(out.AvailableParts) != 0 {
			t.Fatalf("malformed calculated JSON was accepted: %+v", out)
		}
	})
}

func TestParseCalculatedRewardRejectsOverflowedTotals(t *testing.T) {
	data := []byte(`{
		"slot":1,"epoch":1,"generated_at":"2026-01-01T00:00:00Z","rewards_csv":"rewards.csv","num_partitions":1,
		"voting":{"tracked_accounts":1,"reward_count":18446744073709551615,"total_lamports":18446744073709551615},
		"staking":{"record_count":1,"reward_count":1,"credits_only_count":0,"total_lamports":1,"expected_record_count":1},
		"totals":{"record_count":1,"reward_count":0,"credits_only_count":0,"total_lamports":0}
	}`)
	if _, err := parseCalculatedReward(data, 1); err == nil {
		t.Fatal("overflowed reward totals must be rejected")
	}
}

func TestParseCalculatedRewardRequiresEverySummaryField(t *testing.T) {
	for _, path := range []string{
		"slot", "epoch", "generated_at", "rewards_csv", "num_partitions",
		"voting.tracked_accounts", "voting.reward_count", "voting.total_lamports",
		"staking.record_count", "staking.reward_count", "staking.credits_only_count", "staking.total_lamports", "staking.expected_record_count",
		"totals.record_count", "totals.reward_count", "totals.credits_only_count", "totals.total_lamports",
	} {
		t.Run(path, func(t *testing.T) {
			data := removeRewardJSONPath(t, calculatedRewardFixture(1, 1), path)
			if _, err := parseCalculatedReward(data, 1); err == nil {
				t.Fatalf("calculated artifact missing %s was accepted", path)
			}
		})
	}
}

func TestParseCalculatedRewardRejectsInconsistentRecordCounts(t *testing.T) {
	for _, replacement := range []struct {
		old string
		new string
	}{
		{`"expected_record_count":2`, `"expected_record_count":3`},
		{`"credits_only_count":1,"total_lamports":9007199254740993`, `"credits_only_count":0,"total_lamports":9007199254740993`},
		{`"record_count":3,"reward_count":2`, `"record_count":4,"reward_count":2`},
	} {
		body := strings.Replace(calculatedRewardFixture(1, 1), replacement.old, replacement.new, 1)
		if _, err := parseCalculatedReward([]byte(body), 1); err == nil {
			t.Fatalf("inconsistent record counts were accepted after %q -> %q", replacement.old, replacement.new)
		}
	}
}

func TestParseVotingRewardRequiresSnapshotsAndComparisonFields(t *testing.T) {
	for _, path := range []string{
		"epoch",
		"local", "local.tracked_vote_accounts", "local.reward_count", "local.total_lamports", "local.rewards",
		"source_block", "source_block.reward_count", "source_block.total_lamports", "source_block.rewards",
		"local_vs_source",
		"local_vs_source.left_total_lamports", "local_vs_source.right_total_lamports", "local_vs_source.total_lamports_delta",
		"local_vs_source.mismatched_count", "local_vs_source.missing_in_left", "local_vs_source.missing_in_right",
	} {
		t.Run(path, func(t *testing.T) {
			data := removeRewardJSONPath(t, votingRewardFixture(1, 1), path)
			if _, err := parseVotingRewardReader(context.Background(), bytes.NewReader(data), 1); err == nil {
				t.Fatalf("voting artifact missing %s was accepted", path)
			}
		})
	}
}

func TestParseVotingRewardValidatesSnapshotAndComparisonCoherence(t *testing.T) {
	valid := votingRewardFixture(1, 1)
	for name, body := range map[string]string{
		"snapshot count": strings.Replace(valid,
			`"local":{"tracked_vote_accounts":2,"reward_count":1`,
			`"local":{"tracked_vote_accounts":2,"reward_count":2`, 1),
		"snapshot total": strings.Replace(valid,
			`"local":{"tracked_vote_accounts":2,"reward_count":1,"total_lamports":10`,
			`"local":{"tracked_vote_accounts":2,"reward_count":1,"total_lamports":11`, 1),
		"missing reward lamports": strings.Replace(valid,
			`"rewards":[{"pubkey":"vote-1","lamports":10}]`,
			`"rewards":[{"pubkey":"vote-1"}]`, 1),
		"bad delta": strings.Replace(valid,
			`"local_vs_source":{"left_total_lamports":10,"right_total_lamports":10,"total_lamports_delta":0`,
			`"local_vs_source":{"left_total_lamports":10,"right_total_lamports":10,"total_lamports_delta":1`, 1),
		"comparison snapshot total": strings.Replace(valid,
			`"local_vs_rpc_finalized":{"left_total_lamports":10,"right_total_lamports":10,"total_lamports_delta":0`,
			`"local_vs_rpc_finalized":{"left_total_lamports":11,"right_total_lamports":10,"total_lamports_delta":1`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseVotingRewardReader(context.Background(), strings.NewReader(body), 1); err == nil {
				t.Fatal("incoherent voting artifact was accepted")
			}
		})
	}
}

func TestParseVotingRewardRequiresConsistentRPCOutcome(t *testing.T) {
	valid := votingRewardFixture(1, 1)
	withSnapshotAndError := strings.Replace(valid,
		`"rpc_confirmed_error":"Bearer SUPER_ERROR_SECRET",`,
		`"rpc_confirmed":{"reward_count":1,"total_lamports":10,"rewards":[{"pubkey":"vote-1","lamports":10}]},"rpc_confirmed_error":"Bearer SUPER_ERROR_SECRET",`, 1)
	missingOutcome := string(removeRewardJSONPath(t, valid, "rpc_confirmed_error"))
	missingComparison := string(removeRewardJSONPath(t, valid, "source_vs_rpc_finalized"))
	for name, body := range map[string]string{
		"snapshot and error": withSnapshotAndError,
		"missing outcome":    missingOutcome,
		"missing comparison": missingComparison,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseVotingRewardReader(context.Background(), strings.NewReader(body), 1); err == nil {
				t.Fatal("inconsistent RPC outcome was accepted")
			}
		})
	}
}

func TestParseVotingRewardAcceptsCoherentMismatchWithoutRPC(t *testing.T) {
	body := votingRewardFixture(1, 1)
	body = strings.Replace(body,
		`"source_block":{"reward_count":1,"total_lamports":10,"rewards":[{"pubkey":"vote-1","lamports":10}]}`,
		`"source_block":{"reward_count":1,"total_lamports":12,"rewards":[{"pubkey":"vote-1","lamports":12}]}`, 1)
	body = strings.Replace(body,
		`"local_vs_source":{"left_total_lamports":10,"right_total_lamports":10,"total_lamports_delta":0,"mismatched_count":0`,
		`"local_vs_source":{"left_total_lamports":10,"right_total_lamports":12,"total_lamports_delta":-2,"mismatched_count":1`, 1)
	for _, path := range []string{
		"rpc_endpoint", "rpc_confirmed_error", "rpc_finalized", "local_vs_rpc_finalized", "source_vs_rpc_finalized",
	} {
		body = string(removeRewardJSONPath(t, body, path))
	}
	artifact, err := parseVotingRewardReader(context.Background(), strings.NewReader(body), 1)
	if err != nil {
		t.Fatalf("coherent no-RPC mismatch was rejected: %v", err)
	}
	if artifact.LocalVsSource.matched() {
		t.Fatal("coherent mismatch was classified as matched")
	}
}

func TestParseVotingRewardDoesNotEchoUnexpectedType(t *testing.T) {
	const secret = "ATTACKER_CONTROLLED_SECRET"
	body := `{"slot":1,"reward_type":"` + strings.Repeat(secret, 1000) + `","generated_at":"2026-01-01T00:00:00Z"}`
	_, err := parseVotingRewardReader(context.Background(), strings.NewReader(body), 1)
	if err == nil {
		t.Fatal("unexpected reward type must be rejected")
	}
	if strings.Contains(err.Error(), secret) || len(err.Error()) > 128 {
		t.Fatalf("error echoed untrusted reward_type: length=%d", len(err.Error()))
	}
}

func TestParseVotingRewardBoundsKnownComparisonNesting(t *testing.T) {
	nested := strings.Repeat(`{"x":`, maxRewardJSONDepth) + `0` + strings.Repeat(`}`, maxRewardJSONDepth)
	body := strings.Replace(votingRewardFixture(1, 1), `"local_vs_source":{`, `"local_vs_source":{"nested":`+nested+`,`, 1)
	_, err := parseVotingRewardReader(context.Background(), strings.NewReader(body), 1)
	if err == nil || !strings.Contains(err.Error(), "nesting limit") {
		t.Fatalf("deep comparison error = %v", err)
	}
}

func TestParseVotingRewardBoundsNumericErrors(t *testing.T) {
	secretNumber := strings.Repeat("9", 100_000)
	body := strings.Replace(votingRewardFixture(1, 1), `"slot":1`, `"slot":`+secretNumber, 1)
	_, err := parseVotingRewardReader(context.Background(), strings.NewReader(body), 1)
	if err == nil {
		t.Fatal("oversized numeric value was accepted")
	}
	if len(err.Error()) > maxRewardIssueBytes || strings.Contains(err.Error(), secretNumber[:64]) {
		t.Fatalf("numeric parse error echoed untrusted input: length=%d", len(err.Error()))
	}
}

func TestFileMetadataChangedDetectsSameSizeRewrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "artifact.json")
	if err := os.WriteFile(path, []byte("before"), 0o600); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("after!"), 0o600); err != nil {
		t.Fatal(err)
	}
	changedAt := before.ModTime().Add(time.Second)
	if err := os.Chtimes(path, changedAt, changedAt); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !fileMetadataChanged(before, after) {
		t.Fatal("same-size rewrite was not detected")
	}
}

func TestReadRootFileCapsAndCancellation(t *testing.T) {
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := os.WriteFile(filepath.Join(dir, "exact"), make([]byte, 16), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readRootFile(context.Background(), root, "exact", 16); err != nil {
		t.Fatalf("exact cap should pass: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "over"), make([]byte, 17), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readRootFile(context.Background(), root, "over", 16); err == nil {
		t.Fatal("cap+1 must fail")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := readRootFile(ctx, root, "exact", 16); err == nil {
		t.Fatal("canceled read must fail")
	}
}

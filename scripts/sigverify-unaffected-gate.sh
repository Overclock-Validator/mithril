#!/bin/sh
set -eu

export LC_ALL=C
umask 077

fail() {
	echo "sigverify-unaffected-gate: $*" >&2
	exit 1
}

[ "$#" -le 2 ] || fail "usage: $0 [pinned-core] [fresh-outside-worktree-result-dir]"

for tool in git go python3 taskset sha256sum find sort xargs cmp tar awk sed grep date mktemp cp mv rm env uname mkdir; do
	command -v "$tool" >/dev/null 2>&1 || fail "$tool is required"
done

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
repo_root=$(git -C "$script_dir/.." rev-parse --show-toplevel) || \
	fail "script is not inside a Git worktree"
repo_root=$(CDPATH= cd -- "$repo_root" && pwd -P)

core=${1:-2}
case "$core" in
	''|*[!0-9]*) fail "pinned core must be one non-negative CPU number" ;;
esac
default_result_dir=$(python3 -c 'import os, sys; print(os.path.realpath(sys.argv[1]))' \
	"$repo_root/../mithril-sigverify-unaffected-results")
result_arg=${2:-$default_result_dir}
result_dir=$(python3 -c 'import os, sys; print(os.path.realpath(os.path.abspath(sys.argv[1])))' \
	"$result_arg")

[ "$result_dir" != "/" ] || fail "refusing to use / as the result directory"
[ "$result_dir" != "$repo_root" ] || fail "refusing to use the worktree root as the result directory"
case "$result_dir/" in
	"$repo_root"/*)
		fail "result directory must be outside the Mithril worktree so artifacts cannot contaminate source provenance"
		;;
esac
if [ -e "$result_dir" ] || [ -L "$result_dir" ]; then
	fail "result directory already exists; choose a fresh path: $result_dir"
fi

if [ "$(uname -s)" != "Linux" ] || [ "$(uname -m)" != "x86_64" ]; then
	fail "release measurement requires Linux x86_64"
fi
taskset -c "$core" true >/dev/null 2>&1 || fail "pinned core '$core' is unavailable"
cpu_model=$(awk -F: '/^model name[[:space:]]*:/{sub(/^[[:space:]]+/, "", $2); print $2; exit}' /proc/cpuinfo)
[ -n "$cpu_model" ] || fail "could not determine CPU model"
normalized_cpu_model=$(printf '%s\n' "$cpu_model" | awk '{$1=$1; print}')
case "$normalized_cpu_model" in
	*"AMD Ryzen 7 PRO 8700GE"*) release_cpu_match=true ;;
	*) fail "release gate requires an AMD Ryzen 7 PRO 8700GE; found '$normalized_cpu_model'" ;;
esac

cd "$repo_root"
baseline_revision=9f43c94203a5f705c8bff09033fa5995448132cc
harness=scripts/sigverifyunaffectedbench/unaffected_tpu_test.go
evaluator=scripts/sigverify-unaffected-evaluate.py
manifest_script=scripts/sigverify-unaffected-source-manifest.py
if ! grep -q "const unaffectedBaselineRevision = \"$baseline_revision\"" "$harness"; then
	fail "benchmark harness baseline constant does not match $baseline_revision"
fi
git cat-file -e "$baseline_revision^{commit}" || fail "missing baseline commit $baseline_revision"

mkdir -p "$result_dir"
started_utc=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
printf 'state=incomplete\nstarted_utc=%s\n' "$started_utc" >"$result_dir/run-status.txt"
python3 "$manifest_script" --repo "$repo_root" --output "$result_dir/source-manifest-start.tsv"

working_dir=$(mktemp -d "${TMPDIR:-/tmp}/mithril-sigverify-unaffected.XXXXXX")
cleanup() {
	rm -rf -- "$working_dir"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM

baseline_archive="$working_dir/baseline.tar"
git archive --format=tar --output="$baseline_archive" "$baseline_revision"
baseline_dir="$working_dir/baseline"
mkdir -p "$baseline_dir"
tar -xf "$baseline_archive" -C "$baseline_dir"
mkdir -p "$baseline_dir/scripts/sigverifyunaffectedbench"
cp "$harness" "$baseline_dir/scripts/sigverifyunaffectedbench/unaffected_tpu_test.go"
cmp "$harness" "$baseline_dir/scripts/sigverifyunaffectedbench/unaffected_tpu_test.go" || \
	fail "baseline harness copy differs from current harness"

current_binary="$working_dir/current.test"
baseline_binary="$working_dir/baseline.test"
go test -c -o "$current_binary" ./scripts/sigverifyunaffectedbench
(
	cd "$baseline_dir"
	go test -c -o "$baseline_binary" ./scripts/sigverifyunaffectedbench
)

run_clean() {
	env \
		-u MITHRIL_SIGVERIFY_TELEMETRY_CAPACITY \
		-u MITHRIL_SIGVERIFY_TELEMETRY_SCHEDULING_SIMULATION \
		-u MITHRIL_SIGVERIFY_TELEMETRY_OUTPUT \
		GOMAXPROCS=1 "$@"
}

run_clean taskset -c "$core" "$current_binary" \
	-test.run '^TestUnaffectedFixturePassesEveryPublicPath$' -test.count=1 \
	>"$result_dir/current-correctness.txt"
run_clean taskset -c "$core" "$baseline_binary" \
	-test.run '^TestUnaffectedFixturePassesEveryPublicPath$' -test.count=1 \
	>"$result_dir/baseline-correctness.txt"

current_binary_sha256=$(sha256sum "$current_binary" | awk '{print $1}')
baseline_binary_sha256=$(sha256sum "$baseline_binary" | awk '{print $1}')
source_tree_sha256=$(sed -n 's/^source_tree_sha256=//p' "$result_dir/source-manifest-start.tsv")
case "$source_tree_sha256" in
	????????????????????????????????????????????????????????????????) ;;
	*) fail "source manifest did not produce one SHA-256 digest" ;;
esac

git status --porcelain=v1 --untracked-files=all >"$result_dir/git-status.txt"
git diff --binary --no-ext-diff --no-color HEAD >"$result_dir/git-diff.binary.patch"
{
	printf 'baseline_revision=%s\n' "$baseline_revision"
	printf 'current_head=%s\n' "$(git rev-parse HEAD)"
	printf 'source_tree_sha256=%s\n' "$source_tree_sha256"
	printf 'harness_git_blob=%s\n' "$(git hash-object "$harness")"
	printf 'harness_sha256=%s\n' "$(sha256sum "$harness" | awk '{print $1}')"
	printf 'gate_script_sha256=%s\n' "$(sha256sum "$script_dir/sigverify-unaffected-gate.sh" | awk '{print $1}')"
	printf 'evaluator_sha256=%s\n' "$(sha256sum "$evaluator" | awk '{print $1}')"
	printf 'manifest_script_sha256=%s\n' "$(sha256sum "$manifest_script" | awk '{print $1}')"
	printf 'baseline_archive_sha256=%s\n' "$(sha256sum "$baseline_archive" | awk '{print $1}')"
	printf 'current_binary_sha256=%s\n' "$current_binary_sha256"
	printf 'baseline_binary_sha256=%s\n' "$baseline_binary_sha256"
	printf 'git_status_sha256=%s\n' "$(sha256sum "$result_dir/git-status.txt" | awk '{print $1}')"
	printf 'git_diff_sha256=%s\n' "$(sha256sum "$result_dir/git-diff.binary.patch" | awk '{print $1}')"
	printf 'cpu_model=%s\n' "$cpu_model"
	printf 'release_cpu_match=%s\n' "$release_cpu_match"
	printf 'pinned_core=%s\n' "$core"
	printf 'telemetry_environment=forced-unset\n'
} >"$result_dir/provenance.txt"
go version >"$result_dir/go-version.txt"
{
	go version -m "$current_binary"
	go version -m "$baseline_binary"
} >"$result_dir/binary-build-info.txt"

current_output="$result_dir/current.txt"
baseline_output="$result_dir/baseline.txt"
: >"$current_output"
: >"$baseline_output"
: >"$result_dir/run-order.txt"

run_benchmark() {
	binary=$1
	output=$2
	run_clean taskset -c "$core" "$binary" \
		-test.run '^$' \
		-test.bench '^BenchmarkDisabledTelemetryPublicVerification$' \
		-test.benchmem -test.benchtime=3s -test.count=1 \
		>>"$output"
}

iteration=1
while [ "$iteration" -le 10 ]; do
	if [ $((iteration % 2)) -eq 1 ]; then
		printf 'round=%d order=current,baseline\n' "$iteration" >>"$result_dir/run-order.txt"
		run_benchmark "$current_binary" "$current_output"
		run_benchmark "$baseline_binary" "$baseline_output"
	else
		printf 'round=%d order=baseline,current\n' "$iteration" >>"$result_dir/run-order.txt"
		run_benchmark "$baseline_binary" "$baseline_output"
		run_benchmark "$current_binary" "$current_output"
	fi
	iteration=$((iteration + 1))
done

if ! python3 "$evaluator" "$result_dir" >"$result_dir/gate-summary.txt"; then
	cat "$result_dir/gate-summary.txt" >&2
	fail "disabled-telemetry A/B failed"
fi
cat "$result_dir/gate-summary.txt"

python3 "$manifest_script" --repo "$repo_root" --output "$result_dir/source-manifest-end.tsv"
if ! cmp -s "$result_dir/source-manifest-start.tsv" "$result_dir/source-manifest-end.tsv"; then
	fail "source tree changed during the A/B run"
fi

completed_utc=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
printf 'state=complete\nstarted_utc=%s\ncompleted_utc=%s\n' \
	"$started_utc" "$completed_utc" >"$result_dir/run-status.txt"
(
	cd "$result_dir"
	find . -type f ! -name SHA256SUMS ! -name SHA256SUMS.partial -print0 |
		LC_ALL=C sort -z |
		xargs -0 sha256sum >SHA256SUMS.partial
	mv SHA256SUMS.partial SHA256SUMS
	sha256sum -c SHA256SUMS >/dev/null
)

echo "sigverify-unaffected-gate: complete, checksummed results written to $result_dir"

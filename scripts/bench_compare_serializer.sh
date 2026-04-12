#!/usr/bin/env bash

set -euo pipefail

usage() {
	cat <<'EOF'
Usage:
  scripts/bench_compare_serializer.sh --mode old [BENCH_REGEX]
  scripts/bench_compare_serializer.sh --mode commits <REF_OR_TAG> [BENCH_REGEX]

Modes:
  old      Compare local cjson/serializer.go (new) vs cjson/serializer.go.old (old)
  commits  Compare current commit (new) vs provided ref/tag (old)

Defaults:
  BENCH_REGEX BenchmarkSerializer

Outputs:
  scripts/result_serializer_new.md
  scripts/result_serializer_old.md
  scripts/result_summary_test.md
EOF
}

MODE=""
if [[ "${1:-}" == "--mode" || "${1:-}" == "-m" ]]; then
	MODE="${2:-}"
	shift 2
fi

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
	usage
	exit 0
fi

if [[ "${MODE}" != "old" && "${MODE}" != "commits" ]]; then
	echo "Invalid mode: '${MODE}'. Use --mode old or --mode commits" >&2
	exit 1
fi

ROOT_DIR="$(git rev-parse --show-toplevel)"
RESULT_DIR="${ROOT_DIR}/scripts"
NEW_MD="${RESULT_DIR}/result_serializer_new.md"
OLD_MD="${RESULT_DIR}/result_serializer_old.md"
SUMMARY_MD="${RESULT_DIR}/result_summary_test.md"

TMP_DIR="$(mktemp -d)"
NEW_TXT="${TMP_DIR}/new.txt"
OLD_TXT="${TMP_DIR}/old.txt"
NEW_TSV="${TMP_DIR}/new.tsv"
OLD_TSV="${TMP_DIR}/old.tsv"

cleanup() {
	rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

BENCH_REGEX="BenchmarkSerializer"
OLD_REF=""
if [[ "${MODE}" == "old" ]]; then
	BENCH_REGEX="${1:-BenchmarkSerializer}"
else
	OLD_REF="${1:-}"
	if [[ -z "${OLD_REF}" ]]; then
		echo "Mode commits requires <REF_OR_TAG>" >&2
		exit 1
	fi
	BENCH_REGEX="${2:-BenchmarkSerializer}"
fi

BENCH_CMD=(go test ./cjson -bench "${BENCH_REGEX}" -benchmem -run '^$')

run_bench() {
	local run_dir="$1"
	local out_txt="$2"
	(
		cd "${run_dir}"
		"${BENCH_CMD[@]}"
	) | tee "${out_txt}"
}

parse_ns_tsv() {
	local in_txt="$1"
	local out_tsv="$2"
	awk '
		/^Benchmark/ {
			name=$1
			ns=$3
			gsub("ns/op","",ns)
			printf "%s\t%s\n", name, ns
		}
	' "${in_txt}" | sort > "${out_tsv}"
}

write_result_md() {
	local label="$1"
	local source_txt="$2"
	local out_md="$3"
	cat > "${out_md}" <<EOF
# Serializer benchmark (${label})

Command:
\`\`\`bash
${BENCH_CMD[*]}
\`\`\`

Raw output:
\`\`\`
$(cat "${source_txt}")
\`\`\`
EOF
}

write_summary_md() {
	local out_md="$1"
	local new_tsv="$2"
	local old_tsv="$3"
	cat > "${out_md}" <<EOF
# Serializer benchmark summary

## Compared
- New: current serializer implementation
- Old: baseline serializer implementation

## Table (lower ns/op is better)

| Benchmark | New ns/op | Old ns/op | Old/New speedup |
|---|---:|---:|---:|
EOF

	join -t $'\t' "${new_tsv}" "${old_tsv}" \
		| awk -F'\t' '
		{
			newv=$2+0
			oldv=$3+0
			speed=(newv>0)?oldv/newv:0
			printf "| %s | %.1f | %.1f | %.2fx |\n", $1, newv, oldv, speed
		}
	' >> "${out_md}"
}

ensure_bench_data() {
	local tsv="$1"
	local label="$2"
	if [[ ! -s "${tsv}" ]]; then
		echo "No benchmark rows matched '${BENCH_REGEX}' for ${label}" >&2
		exit 2
	fi
}

if [[ "${MODE}" == "old" ]]; then
	SERIALIZER="${ROOT_DIR}/cjson/serializer.go"
	SERIALIZER_OLD="${ROOT_DIR}/cjson/serializer.go.old"
	SERIALIZER_NEW="${ROOT_DIR}/cjson/serializer.go.new"

	if [[ ! -f "${SERIALIZER}" || ! -f "${SERIALIZER_OLD}" ]]; then
		echo "Required files are missing. Need both:" >&2
		echo "  ${SERIALIZER}" >&2
		echo "  ${SERIALIZER_OLD}" >&2
		exit 1
	fi

	echo "Run benchmark for NEW (current serializer.go)..."
	run_bench "${ROOT_DIR}" "${NEW_TXT}"

	restore_files() {
		if [[ -f "${SERIALIZER_NEW}" ]]; then
			mv -f "${SERIALIZER}" "${SERIALIZER_OLD}" 2>/dev/null || true
			mv -f "${SERIALIZER_NEW}" "${SERIALIZER}"
		fi
	}
	trap 'restore_files; cleanup' EXIT

	mv "${SERIALIZER}" "${SERIALIZER_NEW}"
	mv "${SERIALIZER_OLD}" "${SERIALIZER}"

	echo "Run benchmark for OLD (serializer.go.old)..."
	run_bench "${ROOT_DIR}" "${OLD_TXT}"

	restore_files
	trap cleanup EXIT
else
	ORIG_REF="$(git -C "${ROOT_DIR}" rev-parse --abbrev-ref HEAD)"
	if [[ "${ORIG_REF}" == "HEAD" ]]; then
		ORIG_REF="$(git -C "${ROOT_DIR}" rev-parse HEAD)"
	fi

	if [[ -n "$(git -C "${ROOT_DIR}" status --porcelain)" ]]; then
		echo "Working tree is not clean. Commit/stash changes before --mode commits." >&2
		exit 1
	fi

	echo "Run benchmark for NEW (current commit)..."
	run_bench "${ROOT_DIR}" "${NEW_TXT}"

	restore_git_ref() {
		git -C "${ROOT_DIR}" checkout "${ORIG_REF}" >/dev/null 2>&1 || true
	}
	trap 'restore_git_ref; cleanup' EXIT

	echo "Checkout OLD ref: ${OLD_REF}"
	git -C "${ROOT_DIR}" checkout --detach "${OLD_REF}" >/dev/null
	echo "Run benchmark for OLD (${OLD_REF})..."
	run_bench "${ROOT_DIR}" "${OLD_TXT}"

	restore_git_ref
	trap cleanup EXIT
fi

parse_ns_tsv "${NEW_TXT}" "${NEW_TSV}"
parse_ns_tsv "${OLD_TXT}" "${OLD_TSV}"
ensure_bench_data "${NEW_TSV}" "new"
ensure_bench_data "${OLD_TSV}" "old"

write_result_md "new" "${NEW_TXT}" "${NEW_MD}"
write_result_md "old" "${OLD_TXT}" "${OLD_MD}"
write_summary_md "${SUMMARY_MD}" "${NEW_TSV}" "${OLD_TSV}"

echo
echo "Saved:"
echo "  ${NEW_MD}"
echo "  ${OLD_MD}"
echo "  ${SUMMARY_MD}"

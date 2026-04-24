#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/compare_rust_to_python.sh --forward <path> --reverse <path> [options]

Compare Python CLI output against Rust pipelines for one or more quality thresholds.

Required arguments:
  --forward <path>      Forward input BAM/SAM path.
  --reverse <path>      Reverse input BAM/SAM path.

Optional arguments:
  --work-dir <path>     Working directory for outputs/logs (default: correctness_work)
  --threads <n>         Thread count for both Python and Rust runs (default: 1)
  --qualities <list>    Comma-separated qualities (default: 0,20)
  --rust-bin <path>     Rust binary path (default: rust/bellerophon-rs/target/release/bellerophon-rs)
  --pipelines <list>    Comma-separated Rust pipelines (default: legacy-temp,pair-temp,direct)
  -h, --help            Show this help text.
USAGE
}

FORWARD=""
REVERSE=""
WORK_DIR="correctness_work"
THREADS="1"
QUALITIES="0,20"
RUST_BIN="rust/bellerophon-rs/target/release/bellerophon-rs"
PIPELINES="legacy-temp,pair-temp,direct"

while [ "$#" -gt 0 ]; do
  case "$1" in
    --forward)
      FORWARD="$2"
      shift 2
      ;;
    --reverse)
      REVERSE="$2"
      shift 2
      ;;
    --work-dir)
      WORK_DIR="$2"
      shift 2
      ;;
    --threads)
      THREADS="$2"
      shift 2
      ;;
    --qualities)
      QUALITIES="$2"
      shift 2
      ;;
    --rust-bin)
      RUST_BIN="$2"
      shift 2
      ;;
    --pipelines)
      PIPELINES="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [ -z "$FORWARD" ] || [ -z "$REVERSE" ]; then
  echo "Error: --forward and --reverse are required." >&2
  usage >&2
  exit 2
fi

mkdir -p "$WORK_DIR"

SUMMARY_CSV="$WORK_DIR/correctness_summary.csv"
printf 'quality,pipeline,records_equal,flagstat_equal,python_bam,rust_bam\n' > "$SUMMARY_CSV"

IFS=',' read -r -a QUALITY_ARRAY <<< "$QUALITIES"
IFS=',' read -r -a PIPELINE_ARRAY <<< "$PIPELINES"

any_diff=0

for quality in "${QUALITY_ARRAY[@]}"; do
  quality_trimmed="${quality//[[:space:]]/}"
  python_bam="$WORK_DIR/python_q${quality_trimmed}.bam"
  python_log="$WORK_DIR/python_q${quality_trimmed}.log"

  python -m bellerophon.cli \
    --forward "$FORWARD" \
    --reverse "$REVERSE" \
    --output "$python_bam" \
    --quality "$quality_trimmed" \
    --threads "$THREADS" \
    --log-level INFO >"$python_log" 2>&1

  for pipeline in "${PIPELINE_ARRAY[@]}"; do
    pipeline_trimmed="${pipeline//[[:space:]]/}"
    rust_bam="$WORK_DIR/rust_${pipeline_trimmed}_q${quality_trimmed}.bam"
    rust_log="$WORK_DIR/rust_${pipeline_trimmed}_q${quality_trimmed}.log"
    tmp_dir="$WORK_DIR/tmp_${pipeline_trimmed}_q${quality_trimmed}"

    "$RUST_BIN" \
      --forward "$FORWARD" \
      --reverse "$REVERSE" \
      --output "$rust_bam" \
      --quality "$quality_trimmed" \
      --threads "$THREADS" \
      --log-level info \
      --pipeline "$pipeline_trimmed" \
      --tmp-dir "$tmp_dir" >"$rust_log" 2>&1

    python_records="$WORK_DIR/python_q${quality_trimmed}.records.sam"
    rust_records="$WORK_DIR/rust_${pipeline_trimmed}_q${quality_trimmed}.records.sam"
    records_diff="$WORK_DIR/diff_records_${pipeline_trimmed}_q${quality_trimmed}.txt"

    samtools view "$python_bam" > "$python_records"
    samtools view "$rust_bam" > "$rust_records"

    records_equal="yes"
    if ! diff -u "$python_records" "$rust_records" > "$records_diff"; then
      records_equal="no"
      any_diff=1
      echo "Record diff failed: $records_diff" >&2
    fi

    python_flagstat="$WORK_DIR/python_q${quality_trimmed}.flagstat.txt"
    rust_flagstat="$WORK_DIR/rust_${pipeline_trimmed}_q${quality_trimmed}.flagstat.txt"
    flagstat_diff="$WORK_DIR/diff_flagstat_${pipeline_trimmed}_q${quality_trimmed}.txt"

    samtools flagstat "$python_bam" > "$python_flagstat"
    samtools flagstat "$rust_bam" > "$rust_flagstat"

    flagstat_equal="yes"
    if ! diff -u "$python_flagstat" "$rust_flagstat" > "$flagstat_diff"; then
      flagstat_equal="no"
      any_diff=1
      echo "Flagstat diff failed: $flagstat_diff" >&2
    fi

    printf '%s,%s,%s,%s,%s,%s\n' \
      "$quality_trimmed" \
      "$pipeline_trimmed" \
      "$records_equal" \
      "$flagstat_equal" \
      "$python_bam" \
      "$rust_bam" >> "$SUMMARY_CSV"
  done
done

if [ "$any_diff" -ne 0 ]; then
  echo "Correctness mismatches detected. See diff files under: $WORK_DIR" >&2
  exit 1
fi

echo "All comparisons passed. Summary: $SUMMARY_CSV"

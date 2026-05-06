#!/usr/bin/env bash
#
# run_all_indexes.sh — Run the n_triple_10M benchmark for every index type
#                      and save all output to a specified directory.
#
# Usage:
#   ./run_all_indexes.sh [output_dir]
#
# If output_dir is not specified, defaults to ./benchmark_results
#

set -euo pipefail

OUTPUT_DIR="${1:-./benchmark_results}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULT_DIR="${OUTPUT_DIR}/${TIMESTAMP}"

INDEX_TYPES=(
    "buckets"
    "buckets"
    "pso"
    "partial_hexastore"
    "hexastore"
    "dynamic_hexastore"
    "ops"
    "osp"
    "pos"
    "sop"
    "spo"
    "table"
)

echo "=============================================="
echo "  Kolibrie Index Benchmark Runner"
echo "=============================================="
echo "Output directory: ${RESULT_DIR}"
echo "Index types:      ${INDEX_TYPES[*]}"
echo "=============================================="

mkdir -p "${RESULT_DIR}"

echo ""
echo "[BUILD] Compiling in release mode..."
cargo build --release --example n_triple_10M 2>&1 | tee "${RESULT_DIR}/build.log"
echo "[BUILD] Done."
echo ""

for idx_type in "${INDEX_TYPES[@]}"; do
    OUTPUT_FILE="${RESULT_DIR}/${idx_type}.txt"

    echo "=============================================="
    echo "[RUN] INDEX_TYPE=${idx_type}"
    echo "      Output: ${OUTPUT_FILE}"
    echo "=============================================="

    INDEX_TYPE="${idx_type}" \
        cargo run --release --example n_triple_10M \
        2>&1 | tee "${OUTPUT_FILE}"

    echo ""
    echo "[DONE] ${idx_type} -> ${OUTPUT_FILE}"
    echo ""
done

echo "=============================================="
echo "  All benchmarks complete!"
echo "  Results in: ${RESULT_DIR}"
echo "=============================================="
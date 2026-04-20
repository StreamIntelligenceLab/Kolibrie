#!/usr/bin/env bash

set -e

# Configuration
TRIPLES=1000000
WINDOW_SIZE=100000
SLIDE_SIZE=50000
SUBJECTS=300000
PREDICATES=100
OBJECTS=100000

echo "Building Kolibrie benchmark..."
cargo build --release --example synthetic_stream_benchmark

# Setup Data Directory
mkdir -p benchmark_dataset
echo "Generating native Kolibrie synthetic dataset and rules..."
python generate_synthetic_stream.py \
    --triples $TRIPLES \
    --subjects $SUBJECTS \
    --predicates $PREDICATES \
    --objects $OBJECTS \
    --window_size $WINDOW_SIZE \
    --slide_size $SLIDE_SIZE \
    --output_nt benchmark_dataset/synthetic_1M.nt \
    --output_queries benchmark_dataset/synthetic_queries.json

INDEXES=("hexastore" "partial_hexastore" "buckets" "pso" "spo" "pos" "table")

for IDX in "${INDEXES[@]}"; do
    echo "=========================================================="
    echo "Running Stream Benchmark for Index Type: $IDX"
    echo "Window Size: $WINDOW_SIZE | Slide Size: $SLIDE_SIZE"
    echo "=========================================================="
    
    export INDEX_TYPE=$IDX
    export SLIDE_SIZE=$SLIDE_SIZE
    export WINDOW_SIZE=$WINDOW_SIZE
    
    "../../../.././target/release/examples/synthetic_stream_benchmark"
    
    echo "Finished $IDX"
    echo ""
done
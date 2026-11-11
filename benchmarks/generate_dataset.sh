#!/bin/bash

echo "Starting automated ML dataset generation..."

# Loop through different value sizes (1KB, 4KB, 16KB)
for valsize in 1024 4096 16384; do
    # Loop through different concurrency levels
    for concurrency in 10 20 50; do
        # Loop through different read ratios (90% read, 50% read, 10% read)
        for readratio in 0.9 0.5 0.1; do
            echo "Running: valsize=$valsize, concurrency=$concurrency, readratio=$readratio"
            go run ./benchmarks -type mixed -ops 2000 -concurrency $concurrency -valsize $valsize -readratio $readratio -output dataset_train.csv
            sleep 2 # Brief pause to let the cluster breathe
        done
    done
done

echo "Dataset generation complete! Check results/dataset_train.csv"

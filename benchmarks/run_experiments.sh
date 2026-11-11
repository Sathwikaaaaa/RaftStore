#!/bin/bash

echo "Starting full ML grid search dataset generation..."

# Clear out the old dummy dataset
rm -f results/dataset_train.csv

# Test 4 different batch sizes to give the model variance
for batch in 10 50 100 250; do
    echo "================================================="
    echo "Starting cluster with MaxBatchSize=$batch"
    echo "================================================="
    
    # Start nodes in the background
    go run ./cmd/server/main.go -id node1 -batchsize $batch > /dev/null 2>&1 &
    PID1=$!
    go run ./cmd/server/main.go -id node2 -batchsize $batch > /dev/null 2>&1 &
    PID2=$!
    go run ./cmd/server/main.go -id node3 -batchsize $batch > /dev/null 2>&1 &
    PID3=$!
    
    # Give the cluster 5 seconds to elect a leader
    sleep 5
    
    # Run various workloads (different sizes, concurrency, and types)
    for valsize in 1024 4096; do
        for concurrency in 10 30; do
            for type in "put" "mixed" "get"; do
                echo "Benchmarking: type=$type, size=$valsize, conc=$concurrency"
                go run ./benchmarks -type $type -ops 1500 -concurrency $concurrency -valsize $valsize -output dataset_train.csv
                sleep 1
            done
        done
    done
    
    # Kill the cluster processes before the next loop
    echo "Shutting down cluster..."
    kill -9 $PID1 $PID2 $PID3
    sleep 2
done

echo "Grid search complete! Rich dataset generated."

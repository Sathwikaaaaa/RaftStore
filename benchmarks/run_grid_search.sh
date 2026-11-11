#!/bin/bash

echo "Starting massive YCSB A-F dataset generation (Targeting 10,800 samples)..."
echo "Note: This will take several hours. Best run overnight."

# Do not delete the old dataset if you want to append, otherwise uncomment below
# rm -f results/raftstore_workload_dataset.csv

BATCH_SIZES=(16 32 64 128 256)
PIPELINE_DEPTHS=(1 2 4 8 16)
VALUE_SIZES=(512 1024 4096)
CONCURRENCY_LEVELS=(5 15 30 50)
WORKLOADS=("A" "B" "C" "D" "E" "F")

# 5 x 5 x 3 x 4 x 6 x 3 runs = 10,800 samples
for batch in "${BATCH_SIZES[@]}"; do
    for pipeline in "${PIPELINE_DEPTHS[@]}"; do
        echo "================================================="
        echo "Config: MaxBatchSize=$batch, MaxPipelineDepth=$pipeline"
        echo "================================================="
        
        # Start cluster
        go run ./cmd/server/main.go -id node1 -batchsize $batch -pipelinedepth $pipeline > /dev/null 2>&1 &
        PID1=$!
        go run ./cmd/server/main.go -id node2 -batchsize $batch -pipelinedepth $pipeline > /dev/null 2>&1 &
        PID2=$!
        go run ./cmd/server/main.go -id node3 -batchsize $batch -pipelinedepth $pipeline > /dev/null 2>&1 &
        PID3=$!
        
        sleep 4 # Wait for leader election
        
        for wtype in "${WORKLOADS[@]}"; do
            for valsize in "${VALUE_SIZES[@]}"; do
                for conc in "${CONCURRENCY_LEVELS[@]}"; do
                    # Run 3 times per configuration to capture variance
                    for run_id in {1..3}; do
                        go run ./benchmarks -type $wtype -ops 400 -concurrency $conc -valsize $valsize -batchsize $batch -pipelinedepth $pipeline -output raftstore_workload_dataset.csv
                    done
                done
            done
        done
        
        kill -9 $PID1 $PID2 $PID3
        sleep 2
    done
done

echo "Massive dataset collection complete!"

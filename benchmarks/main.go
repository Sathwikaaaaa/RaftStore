package main

import (
	"flag"
	"fmt"
	"log"
)

func main() {
	address := flag.String("address", "localhost:5001", "Raft server gRPC address")
	metricsURL := flag.String("metrics", "http://localhost:6001/metrics", "Leader HTTP metrics endpoint")
	totalOps := flag.Int("ops", 1000, "Total number of operations")
	concurrency := flag.Int("concurrency", 10, "Concurrent worker threads")
	keyCount := flag.Int("keys", 100, "Key space cardinality")
	valueSize := flag.Int("valsize", 1024, "Value size in bytes (e.g., 1024 for 1KB)")
	workloadType := flag.String("type", "mixed", "Workload type: put, get, delete, mixed")
	readRatio := flag.Float64("readratio", 0.8, "Read ratio for mixed workloads (0.0 to 1.0)")
	outputFile := flag.String("output", "dataset_train.csv", "Output CSV filename")

	flag.Parse()

	config := WorkloadConfig{
		TotalOps:     *totalOps,
		KeyCount:     *keyCount,
		ValueSize:    *valueSize,
		ReadRatio:    *readRatio,
		WorkloadType: *workloadType,
	}

	fmt.Printf("=== RaftStore Benchmark & Data Collector ===\n")
	fmt.Printf("Target Address : %s\n", *address)
	fmt.Printf("Metrics URL    : %s\n", *metricsURL)
	fmt.Printf("Workload Type  : %s\n", config.WorkloadType)
	fmt.Printf("Total Ops      : %d\n", config.TotalOps)
	fmt.Printf("Value Size     : %d bytes\n", config.ValueSize)
	fmt.Printf("--------------------------------------------\n")

	requests := GenerateWorkload(config)
	runner := NewBenchmarkRunner(*address, *concurrency, requests)

	fmt.Printf("Running benchmark load...\n")
	stats, err := runner.Run()
	if err != nil {
		log.Fatalf("Benchmark failed: %v", err)
	}

	fmt.Printf("Fetching server telemetry for dataset...\n")
	serverMetrics, err := FetchServerMetrics(*metricsURL)
	if err != nil {
		log.Printf("Warning: Could not fetch server metrics: %v", err)
	}

	fmt.Printf("\n=== Execution Results ===\n")
	fmt.Printf("Throughput       : %.2f ops/sec\n", stats.ThroughputOpsSec)
	fmt.Printf("Avg Latency      : %.2f ms\n", stats.AvgLatencyMs)
	fmt.Printf("P99 Latency      : %.2f ms\n", stats.P99LatencyMs)
	fmt.Printf("Batch Size (Srv) : %d\n", serverMetrics.Raft.MaxBatchSize)
	fmt.Printf("Write Amp (Srv)  : %.2f\n", serverMetrics.Storage.WriteAmplification)

	if err := SaveResultsToCSV(*outputFile, stats, config, serverMetrics); err != nil {
		log.Printf("Warning: Failed to save results to CSV: %v", err)
	}
}
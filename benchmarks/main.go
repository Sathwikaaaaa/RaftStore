package main

import (
	"flag"
	"fmt"
	"log"
)

func main() {
	address := flag.String("address", "localhost:5001", "Raft server gRPC address")
	totalOps := flag.Int("ops", 1000, "Total number of operations")
	concurrency := flag.Int("concurrency", 10, "Concurrent worker threads")
	keyCount := flag.Int("keys", 100, "Key space cardinality")
	valueSize := flag.Int("valsize", 1024, "Value size in bytes (e.g., 1024 for 1KB)")
	workloadType := flag.String("type", "mixed", "Workload type: put, get, delete, mixed")
	readRatio := flag.Float64("readratio", 0.8, "Read ratio for mixed workloads (0.0 to 1.0)")
	outputFile := flag.String("output", "benchmark_run.csv", "Output CSV filename")

	flag.Parse()

	config := WorkloadConfig{
		TotalOps:     *totalOps,
		KeyCount:     *keyCount,
		ValueSize:    *valueSize,
		ReadRatio:    *readRatio,
		WorkloadType: *workloadType,
	}

	fmt.Printf("=== RaftStore Benchmark Generator ===\n")
	fmt.Printf("Target Address : %s\n", *address)
	fmt.Printf("Workload Type  : %s\n", config.WorkloadType)
	fmt.Printf("Total Ops      : %d\n", config.TotalOps)
	fmt.Printf("Concurrency    : %d\n", *concurrency)
	fmt.Printf("Key Count      : %d\n", config.KeyCount)
	fmt.Printf("Value Size     : %d bytes\n", config.ValueSize)
	fmt.Printf("-------------------------------------\n")

	requests := GenerateWorkload(config)
	runner := NewBenchmarkRunner(*address, *concurrency, requests)

	fmt.Printf("Running benchmark...\n")
	stats, err := runner.Run()
	if err != nil {
		log.Fatalf("Benchmark failed: %v", err)
	}

	fmt.Printf("\n=== Benchmark Results ===\n")
	fmt.Printf("Total Operations : %d\n", stats.TotalOperations)
	fmt.Printf("Successful Ops   : %d\n", stats.SuccessfulOps)
	fmt.Printf("Failed Ops       : %d\n", stats.FailedOps)
	fmt.Printf("Total Duration   : %.2f sec\n", stats.TotalDuration.Seconds())
	fmt.Printf("Throughput       : %.2f ops/sec\n", stats.ThroughputOpsSec)
	fmt.Printf("Avg Latency      : %.2f ms\n", stats.AvgLatencyMs)
	fmt.Printf("P50 Latency      : %.2f ms\n", stats.P50LatencyMs)
	fmt.Printf("P95 Latency      : %.2f ms\n", stats.P95LatencyMs)
	fmt.Printf("P99 Latency      : %.2f ms\n", stats.P99LatencyMs)
	fmt.Printf("Min / Max Latency: %.2f ms / %.2f ms\n", stats.MinLatencyMs, stats.MaxLatencyMs)

	if err := SaveResultsToCSV(*outputFile, stats, config); err != nil {
		log.Printf("Warning: Failed to save results to CSV: %v", err)
	}
}
